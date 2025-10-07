use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{Semaphore, Mutex, RwLock}; // MODIFIED: Imported tokio's RwLock
use std::time::Instant;
use sysinfo::System;
use crate::caching::redis_client::RedisClient;
use crate::models::response::EvaluationResult;

// NOTE: This struct is part of the old queuing model and will be replaced by Broccoli.
// It is kept for now to ensure compatibility with the current queue manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionTask {
    pub id: String,
    pub requests: Vec<crate::models::request::ExecutionRequest>,
    pub execution_type: ExecutionType,
    pub user_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExecutionType {
    Single,
    Parallel,
    Batch,
}

/// ADDED: Represents the two operational states for concurrency control.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConcurrencyState {
    /// The system is healthy and can run at full parallelism.
    Nominal,
    /// The system is under load and parallelism should be reduced.
    Strained,
}


/// MODIFIED: The CodeExecutor now holds the Redis client and state for adaptive concurrency.
/// Its responsibilities are to limit concurrency and provide the necessary context
/// for intelligent, real-time batch parallelization adjustments.
pub struct CodeExecutor {
    pub semaphore: Arc<Semaphore>,
    // MODIFIED: Switched to tokio's async RwLock
    pub last_java_warmup: Arc<RwLock<Instant>>,
    /// MODIFIED: System handle for monitoring memory usage for progressive backoff.
    pub system: Arc<Mutex<System>>,
    /// ADDED: A Redis client for checking queue depth.
    pub redis_client: RedisClient,
    // MODIFIED: Switched to tokio's async RwLock
    /// This is used to implement hysteresis (a cooldown period).
    pub concurrency_state: Arc<RwLock<(ConcurrencyState, Instant)>>,
}

// ADDED: Manual Debug implementation since some fields (like RedisClient) don't derive it.
// This prevents compilation errors while keeping debugging output clean.
impl std::fmt::Debug for CodeExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodeExecutor")
            .field("semaphore", &self.semaphore)
            .field("last_java_warmup", &self.last_java_warmup)
            // Other fields are omitted for brevity in debug output.
            .finish_non_exhaustive()
    }
}


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionNotification {
    pub id: String,
    pub status: String,
    pub results: Option<Vec<EvaluationResult>>,
}