use crate::models::request::ExecutionRequest;

#[derive(Debug, Clone)]
pub struct ExecutionTask {
    pub id: String,
    pub requests: Vec<ExecutionRequest>,
    pub execution_type: ExecutionType,
    pub user_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionType {
    Single,
    Parallel,
    Batch,
}