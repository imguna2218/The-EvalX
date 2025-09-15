use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore, broadcast};
use std::collections::HashMap;
use bollard::Docker;
use crate::models::request::ExecutionRequest;
use crate::models::response::EvaluationResult;

#[derive(Debug, Clone)]
pub struct LanguageConfig {
    pub image: String,
    pub command_format: Vec<String>,
    pub resource_limits: ContainerResourceLimits,
    pub env: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ContainerResourceLimits {
    pub memory: String,
    pub cpu_shares: i64,
}

#[derive(Debug, Clone)]
pub struct ExecutionTask {
    pub id: String,
    pub requests: Vec<ExecutionRequest>,
    pub task_type: ExecutionTaskType,
    pub notification_tx: Arc<broadcast::Sender<ExecutionNotification>>,
}

#[derive(Debug, Clone)]
pub enum ExecutionTaskType {
    Single,
    Parallel,
    Batch,
}

#[derive(Debug, Clone)]
pub struct CodeExecutor {
    pub docker: Docker,
    pub semaphore: Arc<Semaphore>,
    pub container_pool: Arc<Mutex<HashMap<String, Vec<String>>>>,
    pub language_configs: HashMap<String, LanguageConfig>,
    pub task_queue: Arc<Mutex<Vec<ExecutionTask>>>,
    pub task_notify: Arc<tokio::sync::Notify>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionNotification {
    pub id: String,
    pub status: String,
    // CHANGED: This now holds a vector of results for notifications.
    pub results: Option<Vec<EvaluationResult>>,
}