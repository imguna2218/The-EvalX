use crate::models::request::ExecutionRequest;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionTask {
    pub id: String,
    pub requests: Vec<ExecutionRequest>,
    pub execution_type: ExecutionType,
    pub user_id: Option<String>,
    /// ADDED: Unix timestamp (seconds) of when the task was created.
    pub created_at: u64,
    /// ADDED: Tracks the number of times this task has been attempted.
    pub retry_count: u8,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExecutionType {
    Single,
    Parallel,
    Batch,
}