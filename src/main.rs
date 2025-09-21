use anyhow::Result;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use std::env;
use std::net::SocketAddr;
use tokio::signal;
use tracing::info;

mod caching;
mod compilers;
mod container_management;
mod controllers;
mod executor;
mod models;
mod monitoring;
mod queue_management;
mod routes;
mod setup;
mod types;

// MODIFIED: Functions are now imported from the refactored setup module
use crate::setup::{initialize_shared_state, run_workers};
use crate::routes::create_router;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // Get command-line arguments
    let args: Vec<String> = env::args().collect();

    // Initialize the shared state that both app and workers will need
    let (executor, tx, queue_manager, redis_client) = initialize_shared_state().await?;

    // Check if the "worker" argument was passed
    if args.get(1).map_or(false, |arg| arg == "worker") {
        // --- WORKER MODE ---
        info!("Starting in WORKER mode...");
        // Run the worker logic and wait for CTRL+C
        tokio::select! {
            _ = run_workers(queue_manager, redis_client) => {
                info!("Worker process finished.");
            }
            _ = signal::ctrl_c() => {
                info!("Shutting down workers gracefully");
                executor.cleanup().await?;
            }
        }
    } else {
        // --- SERVER MODE (Default) ---
        info!("Starting in SERVER mode...");
        let app = create_router(executor.clone(), tx, redis_client, queue_manager);
        let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
        info!("Server running on http://{}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await?;
        let server = axum::serve(listener, app);

        // Wait for the server to run or for a CTRL+C signal
        tokio::select! {
            result = server => {
                if let Err(e) = result {
                    eprintln!("Server error: {}", e);
                }
            }
            _ = signal::ctrl_c() => {
                info!("Shutting down server gracefully");
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