use lazy_static::lazy_static;
use prometheus::{
    register_int_gauge_vec, register_int_counter_vec, register_histogram_vec,
    IntGaugeVec, IntCounterVec, HistogramVec,
};

lazy_static! {
    // --- Request Metrics ---
    pub static ref HTTP_REQUESTS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "http_requests_total",
        "Total number of HTTP requests made.",
        &["method", "path"]
    ).unwrap();

    // FIXED: The fourth argument is now the vector of buckets directly.
    pub static ref HTTP_REQUEST_DURATION_SECONDS: HistogramVec = register_histogram_vec!(
        "http_request_duration_seconds",
        "The HTTP request latencies in seconds.",
        &["method", "path"],
        vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    ).unwrap();

    // --- Queue Metrics ---
    pub static ref QUEUE_DEPTH: IntGaugeVec = register_int_gauge_vec!(
        "evalx_queue_depth_total",
        "The number of tasks currently in a specific language queue.",
        &["language_version"]
    ).unwrap();

    // --- Container Metrics ---
    pub static ref CONTAINER_USAGE: IntGaugeVec = register_int_gauge_vec!(
        "evalx_containers_in_use_total",
        "The number of containers currently in use for a specific language.",
        &["language_version"]
    ).unwrap();

    // --- Cache Metrics ---
    pub static ref CACHE_EVENTS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "evalx_cache_events_total",
        "The total number of cache hits and misses.",
        &["event_type"] // "hit" or "miss"
    ).unwrap();
}