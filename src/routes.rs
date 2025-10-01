use axum::{
    body::Body as AxumBody,
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
    Router,
};
use prometheus::{Encoder, TextEncoder};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::broadcast;
use tower::ServiceBuilder;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::caching::redis_client::RedisClient;
// MODIFIED: Removed the import for the deleted `handle_execute_parallel` function.
use crate::controllers::executionControllers::{
    handle_execute, handle_execute_batch, handle_submission_status,
};
use crate::controllers::notificationControllers::handle_ws_upgrade;
use crate::monitoring::metrics::{HTTP_REQUESTS_TOTAL, HTTP_REQUEST_DURATION_SECONDS};
use crate::queue_management::QueueManager;
use crate::types::index::{CodeExecutor, ExecutionNotification};

// Handler function to gather and serve Prometheus metrics.
async fn metrics_handler() -> (StatusCode, String) {
    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Could not encode metrics: {}", e),
        );
    }
    (
        StatusCode::OK,
        String::from_utf8(buffer).unwrap_or_default(),
    )
}

// Middleware to track HTTP request metrics.
async fn track_metrics(req: Request<AxumBody>, next: Next) -> Response {
    let start = Instant::now();
    let path = req.uri().path().to_string();
    let method = req.method().clone();

    let response = next.run(req).await;

    let latency = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();

    // Increment request counter and observe latency.
    HTTP_REQUESTS_TOTAL
        .with_label_values(&[method.as_str(), &path, &status])
        .inc();
    HTTP_REQUEST_DURATION_SECONDS
        .with_label_values(&[method.as_str(), &path])
        .observe(latency);

    response
}

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
        // MODIFIED: Removed the route for the deleted `handle_execute_parallel` function.
        .route("/execute/batch", post(handle_execute_batch))
        .route("/submissions/:token", get(handle_submission_status))
        .route("/ws", get(handle_ws_upgrade))
        .route("/metrics", get(metrics_handler))
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(middleware::from_fn(track_metrics))
                .layer(cors),
        )
        .with_state(state)
}