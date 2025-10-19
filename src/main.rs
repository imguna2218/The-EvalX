use anyhow::Result;
use axum::response::{IntoResponse, Response};
use redis::AsyncCommands;
use serde::Serialize;
use std::env;
use std::net::SocketAddr;
use tokio::signal;
use tracing::{error, info, warn};
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
    const POOL_KEY: &str = "evalx:sandbox_ids:available";
    const POOL_SIZE: u16 = 1000;
    info!("Verifying and initializing sandbox pool in Redis...");

    let mut conn = redis_client.get_multiplexed_async_connection().await?;
    let current_size: usize = conn.scard(POOL_KEY).await.unwrap_or(0);

    // If the pool is empty or significantly depleted, wipe it and recreate it.
    // This makes startup robust against stale data from previous crashes.
    if current_size < POOL_SIZE as usize {
        if current_size > 0 {
            warn!(
                "Sandbox pool '{}' is incomplete (size: {}/{}) or stale. Recreating...",
                POOL_KEY, current_size, POOL_SIZE
            );
            conn.del(POOL_KEY).await?;
        } else {
            info!("Sandbox pool '{}' does not exist. Creating now...", POOL_KEY);
        }

        let mut pipe = redis::pipe();
        for i in 0..POOL_SIZE {
            pipe.sadd(POOL_KEY, i);
        }
        pipe.query_async(&mut conn).await?;
        info!(
            "Successfully populated sandbox pool with {} IDs.",
            POOL_SIZE
        );
    } else {
        info!("Sandbox pool is healthy and full ({} IDs).", current_size);
    }

    if let Err(e) = redis_client.check_pool_health().await {
        error!("SANDBOX POOL HEALTH CHECK FAILED: {}", e);
        return Err(e);
    }

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
    if let Err(e) = initialize_sandbox_pool(&redis_client).await {
        error!(
            "FATAL: Could not initialize the sandbox pool in Redis: {}. Shutting down.",
            e
        );
        // This is a critical failure. The system cannot function without the sandbox pool.
        panic!("Failed to initialize sandbox pool: {}", e);
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
        // CHANGED: Workers now require a specific type ('fast' or 'jvm').
        let worker_type = args.get(2).map(String::as_str).unwrap_or("");
        if worker_type!= "fast" && worker_type!= "jvm" {
            error!("FATAL: Invalid or missing worker type. Must be 'fast' or 'jvm'.");
            panic!("Usage:./target/release/evalx worker <fast|jvm>");
        }

        info!("Starting in WORKER mode, type: {}", worker_type);
        start_workers(queue_manager, redis_client, worker_type).await;
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
