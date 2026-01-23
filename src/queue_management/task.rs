use crate::models::request::ExecutionRequest;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompileTask {
    pub parent_token: String,
    pub requests: Vec<ExecutionRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunTask {
    pub parent_token: String,
    pub index: usize,
    pub artifact_key: String,
    pub stdin: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutionTask {
    CompileBatch(CompileTask),
    RunTestcase(RunTask),
}