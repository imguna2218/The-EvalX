use axum::{routing::post, Router, routing::get};
use std::sync::Arc;
use tower_http::cors::{CorsLayer, Any};
use tower_http::trace::TraceLayer;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower::ServiceBuilder;
use tower_http::timeout::TimeoutLayer;
use std::time::Duration;
use tokio::sync::broadcast;
use crate::types::index::{CodeExecutor, ExecutionNotification};
use crate::controllers::executionControllers::{handle_execute, handle_execute_parallel, handle_execute_batch};
use crate::controllers::notificationControllers::handle_ws_upgrade;
use crate::caching::redis_client::RedisClient;
use crate::queue_management::QueueManager; 

pub fn create_router(
    executor: Arc<CodeExecutor>,
    tx: Arc<broadcast::Sender<ExecutionNotification>>,
    redis_client: RedisClient,
    queue_manager: Arc<QueueManager>,
) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let state = (executor, tx, redis_client, queue_manager);

    Router::new()
        .route("/execute", post(handle_execute))
        .route("/execute-parallel", post(handle_execute_parallel))
        .nest(
            "/execute/batch",
            Router::new()
                .route("/", post(handle_execute_batch))
                .layer(
                    ServiceBuilder::new()
                        .layer(RequestBodyLimitLayer::new(10 * 1024 * 1024))  // 10MB limit for large batches
                        .layer(TimeoutLayer::new(Duration::from_secs(60)))
                        .layer(ConcurrencyLimitLayer::new(4)),
                ),
        )
        .route("/ws", get(handle_ws_upgrade))
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
}