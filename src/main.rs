use anyhow::Result;
use axum::response::{IntoResponse, Response};
use redis::AsyncCommands;
use serde::Serialize;
use std::env;
use std::net::SocketAddr;
use tokio::signal;
use tracing::{debug, error, info, warn};
use std::time::Duration; 
use crate::caching::redis_client::RedisClient;
use crate::routes::create_router;
use crate::setup::{initialize_executor, start_workers};

mod languages;
mod caching;
mod controllers;
mod executor;
mod models;
mod monitoring;
mod queue_management;
mod routes;
mod sandbox;
mod setup;
mod types;


async fn initialize_sandbox_pool(redis_client: &RedisClient) -> Result<()> {
    const READY_POOL_KEY: &str = "evalx:sandboxes:ready";
    const CLEANUP_POOL_KEY: &str = "evalx:sandboxes:cleanup";
    const POOL_SIZE: u16 = 30;

    info!("Initializing sandbox ID pool with {} boxes", POOL_SIZE);
    let mut conn = redis_client.get_multiplexed_async_connection().await?;

    // Clear any existing state
    let _: () = conn.del(READY_POOL_KEY).await?;
    let _: () = conn.del(CLEANUP_POOL_KEY).await?;

    // Initialize all boxes
    let mut pipe = redis::pipe();
    for i in 0..POOL_SIZE {
        // Clean up any existing box first
        // Clean up any existing box first
        let _ = tokio::process::Command::new("isolate")
        .arg("--cg")
        .arg(format!("--box-id={}", i))
        .arg("--cleanup")
        .status().await;
        // Initialize the box
        let init_status = tokio::process::Command::new("isolate")
        .arg("--cg")
        .arg(format!("--box-id={}", i))
        .arg("--init")
        .status().await;
        
        if init_status.is_ok() && init_status.unwrap().success() {
            pipe.rpush(READY_POOL_KEY, i);
        }
    }
    let _: () = pipe.query_async(&mut conn).await?;
    
    let final_count: u64 = conn.llen(READY_POOL_KEY).await?;
    info!("Sandbox pool initialized with {} ready boxes", final_count);
    Ok(())
}

async fn emergency_pool_recovery(redis_client: &RedisClient) -> Result<()> {
    const READY_POOL_KEY: &str = "evalx:sandboxes:ready";
    const CLEANUP_POOL_KEY: &str = "evalx:sandboxes:cleanup";
    
    info!("Performing emergency pool recovery...");
    
    let mut conn = redis_client.get_multiplexed_async_connection().await?;
    
    // Clear both pools
    let _: () = conn.del(READY_POOL_KEY).await?;
    let _: () = conn.del(CLEANUP_POOL_KEY).await?;
    
    // Reinitialize with proper permissions
    for i in 0..30 {
        // Clean up any existing box first
        let _ = tokio::process::Command::new("isolate")
            .arg("--cg")
            .arg(format!("--box-id={}", i))
            .arg("--cleanup")
            .status().await;
        
        // Initialize the box
        let init_status = tokio::process::Command::new("isolate")
            .arg("--cg")
            .arg(format!("--box-id={}", i))
            .arg("--init")
            .status().await;
        
        if init_status.is_ok() && init_status.unwrap().success() {
            let _: () = conn.rpush(READY_POOL_KEY, i).await?;
        }
    }
    
    let final_count: u64 = conn.llen(READY_POOL_KEY).await?;
    info!("Emergency recovery completed. {} boxes ready.", final_count);
    Ok(())
}


#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("server");

    let redis_client = RedisClient::new()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize Redis pool: {}", e))?;

    // Pass the single client instance to be used everywhere.
    let (executor, tx, queue_manager) = initialize_executor(redis_client.clone()).await?;

    // ADDED: Initialize the sandbox ID pool in Redis before starting services.
    // MODIFIED: Only initialize sandbox pool in worker mode
    if mode == "worker" {
        if let Err(e) = initialize_sandbox_pool(&redis_client).await {
            error!(
                "FATAL: Could not initialize the sandbox pool in Redis: {}. Shutting down.",
                e
            );
            panic!("Failed to initialize sandbox pool: {}", e);
        }
    } else {
        info!("API Server mode: Sandbox pool initialization skipped (handled by worker)");
    }

    

    let health_client = redis_client.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            interval.tick().await;
            if let Err(e) = health_client.diagnose_pool_health().await {
                error!("Pool health check failed: {}", e);
            }
        }
    });


    if mode == "worker" {
        info!("Starting in WORKER mode");
        let manager_client = redis_client.clone();
        tokio::spawn(async move {
            info!("Starting Sandbox Manager (recycling) task...");
            const READY_POOL_KEY: &str = "evalx:sandboxes:ready";
            const CLEANUP_POOL_KEY: &str = "evalx:sandboxes:cleanup";

            loop {
                let mut conn = match manager_client.get_multiplexed_async_connection().await {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Sandbox Manager failed to get Redis connection: {}", e);
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        continue;
                    }
                };

                // Use LPOP instead of BRPOP for cleanup queue
                let box_id: Option<u16> = conn.lpop(CLEANUP_POOL_KEY, None).await.unwrap_or(None);
                
                if let Some(box_id) = box_id {
                    debug!("Sandbox Manager: Recycling box #{}", box_id);
                    
                    // Clean up any existing box first
// Cleanup the used box
                    let _ = tokio::process::Command::new("isolate")
                        .arg("--cg")
                        .arg(format!("--box-id={}", box_id))
                        .arg("--cleanup")
                        .status().await;
                    // Re-initialize it to make it pristine
                    let init_status = tokio::process::Command::new("isolate")
                        .arg("--cg")
                        .arg(format!("--box-id={}", box_id))
                        .arg("--init")
                        .status().await;

                    // If successful, return it to the ready pool
                    if init_status.is_ok() && init_status.unwrap().success() {
                        let _: Result<(),_> = conn.rpush(READY_POOL_KEY, box_id).await;
                        debug!("Sandbox Manager: Box #{} is now ready.", box_id);
                    } else {
                        error!("Sandbox Manager: Failed to re-initialize box #{}. It will not be returned to the pool.", box_id);
                    }
                } else {
                    // No boxes to clean, sleep briefly
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        });
        // The worker type is no longer needed. All workers are now identical.
        start_workers(queue_manager, redis_client).await;
        signal::ctrl_c().await?;
        info!("Worker shutting down gracefully");
    } else {
        info!("Starting in SERVER mode");
        let app = create_router(executor.clone(), tx, redis_client, queue_manager);

        let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
        info!("Server running on http://{}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await?;
        let server = axum::serve(listener, app);

        tokio::select! {
            result = server => {
                if let Err(e) = result {
                    error!("Server error: {}", e);
                }
            }
            _ = signal::ctrl_c() => {
                info!("Server shutting down gracefully");
            }
        }
    }

    Ok(())
}

pub struct AppResponse<T>(pub Result<T, anyhow::Error>);

impl<T: Serialize> IntoResponse for AppResponse<T> {
    fn into_response(self) -> Response {
        match self.0 {
            Ok(data) => {
                let json = serde_json::to_string(&data).unwrap_or_default();
                (
                    axum::http::StatusCode::OK,
                    [(axum::http::header::CONTENT_TYPE, "application/json")],
                    json,
                )
                    .into_response()
            }
            Err(err) => {
                let error_msg = format!("{{\"error\":\"{}\"}}", err);
                (
                    axum::http::StatusCode::BAD_REQUEST,
                    [(axum::http::header::CONTENT_TYPE, "application/json")],
                    error_msg,
                )
                    .into_response()
            }
        }
    }
}
