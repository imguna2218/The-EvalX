use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{broadcast, Semaphore};
// MODIFIED: All obsolete imports related to Docker and the old executor have been removed.
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

/// MODIFIED: The CodeExecutor is now a very simple struct.
/// Its only responsibility is to limit concurrency using a semaphore.
/// All Docker-related fields and configurations have been removed.
#[derive(Debug, Clone)]
pub struct CodeExecutor {
    pub semaphore: Arc<Semaphore>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionNotification {
    pub id: String,
    pub status: String,
    pub results: Option<Vec<EvaluationResult>>,
}