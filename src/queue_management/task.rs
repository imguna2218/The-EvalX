use crate::models::request::ExecutionRequest;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionTask {
    pub id: String,
    pub requests: Vec<ExecutionRequest>,
    pub execution_type: ExecutionType,
    pub user_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExecutionType {
    Single,
    Parallel,
    Batch,
}