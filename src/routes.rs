use axum::{
    extract::Request,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use prometheus::{Encoder, TextEncoder};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tower::limit::ConcurrencyLimitLayer;
use tower::ServiceBuilder;
use tower_http::cors::{Any, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::caching::redis_client::RedisClient;
use crate::controllers::executionControllers::{
    handle_execute, handle_execute_batch, handle_execute_parallel, handle_submission_status,
};
use crate::controllers::notificationControllers::handle_ws_upgrade;
// NEW: Import the metrics you defined
use crate::monitoring::metrics::{HTTP_REQUESTS_TOTAL, HTTP_REQUEST_DURATION_SECONDS};
use crate::queue_management::QueueManager;
use crate::types::index::{CodeExecutor, ExecutionNotification};

// NEW: This is the handler for the /metrics endpoint.
// It gathers all registered metrics and returns them in the text format Prometheus expects.
async fn metrics_handler() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    let metric_families = prometheus::gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    (
        axum::http::StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        buffer,
    )
}

// NEW: This is the middleware function that will run for every incoming request.
// It records the total number of requests and how long each one takes to process.
pub async fn track_metrics(req: Request, next: Next) -> Response {
    let start = Instant::now();
    let path = req.uri().path().to_owned();
    let method = req.method().clone();

    // Run the actual route handler
    let response = next.run(req).await;

    let latency = start.elapsed().as_secs_f64();

    // Record the metrics
    HTTP_REQUEST_DURATION_SECONDS
        .with_label_values(&[method.as_str(), &path])
        .observe(latency);

    HTTP_REQUESTS_TOTAL
        .with_label_values(&[method.as_str(), &path])
        .inc();

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

    // MODIFIED: The router is now split into two parts:
    // 1. The main application routes that we want to track.
    // 2. The final router that includes the /metrics endpoint and layers.
    let app_routes = Router::new()
        .route("/execute", post(handle_execute))
        .route("/execute-parallel", post(handle_execute_parallel))
        .nest(
            "/execute/batch",
            Router::new().route("/", post(handle_execute_batch)).layer(
                ServiceBuilder::new()
                    .layer(RequestBodyLimitLayer::new(10 * 1024 * 1024)) // 10MB limit
                    .layer(TimeoutLayer::new(Duration::from_secs(60)))
                    .layer(ConcurrencyLimitLayer::new(4)),
            ),
        )
        .route("/submissions/:token", get(handle_submission_status))
        .route("/ws", get(handle_ws_upgrade))
        // This layer applies the tracking middleware to all the routes defined above
        .route_layer(middleware::from_fn(track_metrics));

    Router::new()
        .merge(app_routes) // Combine the app routes
        .route("/metrics", get(metrics_handler)) // Add the metrics endpoint
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
}