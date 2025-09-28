use anyhow::Result;
use std::env;
use std::net::SocketAddr;
use tokio::signal;
use tracing::{info, error};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use crate::caching::redis_client::RedisClient;
use crate::queue_management::QueueManager;
use crate::setup::{initialize_executor, start_workers};
use crate::routes::create_router;

mod queue_management;
mod executor;
mod models;
mod types;
mod controllers;
mod setup;
mod routes;
mod container_management;
mod caching;
mod compilers;
mod monitoring; 

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    
    let args: Vec<String> = env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("server");

    let (executor, tx, queue_manager) = initialize_executor().await?;
    let redis_client = RedisClient::new().map_err(|e| anyhow::anyhow!("Failed to initialize Redis: {}", e))?;

    if mode == "worker" {
        info!("Starting in WORKER mode");
        start_workers(queue_manager, redis_client).await;
        // Keep the worker running until a shutdown signal is received
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
                executor.cleanup().await?;
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
                ).into_response()
            }
            Err(err) => {
                let error_msg = format!("{{\"error\":\"{}\"}}", err);
                (
                    axum::http::StatusCode::BAD_REQUEST,
                    [(axum::http::header::CONTENT_TYPE, "application/json")],
                    error_msg,
                ).into_response()
            }
        }
    }
}