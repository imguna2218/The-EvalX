use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};
use crate::queue_management::{ExecutionQueue, ExecutionTask, ExecutionType};
use crate::types::index::CodeExecutor;
use crate::caching::redis_client::RedisClient;
use crate::models::request::ExecutionRequest;
use crate::models::response::EvaluationResult;
use anyhow::Result;
use uuid::Uuid;
use crate::container_management::container_pool::available_containers;
use tracing::debug;

#[derive(Clone)]
pub struct QueueManager {
    queue: ExecutionQueue,
    tasks: Arc<RwLock<std::collections::HashMap<String, ExecutionTask>>>,
    executor: Arc<CodeExecutor>,
}

impl QueueManager {
    pub fn new(executor: Arc<CodeExecutor>) -> Self {
        QueueManager {
            queue: ExecutionQueue::new(),
            tasks: Arc::new(RwLock::new(std::collections::HashMap::new())),
            executor,
        }
    }

    pub async fn add_task(
        &self,
        requests: Vec<ExecutionRequest>,
        execution_type: ExecutionType,
        priority: u8,
        redis_client: RedisClient,
    ) -> Result<Vec<EvaluationResult>> {
        let task_id = Uuid::new_v4().to_string();
        let first_request = requests.first().cloned().ok_or_else(|| anyhow::anyhow!("Empty request list"))?;
        let language = first_request.language.trim();
        let version = first_request.version.trim();
        let required_containers = match execution_type {
            ExecutionType::Single => 1,
            ExecutionType::Parallel => requests.len(),
            ExecutionType::Batch => requests.len(),
        };

        let available = available_containers(&self.executor, language, version).await?;
        debug!(
            "Task {}: Adding {} requests for {}:{} ({} of {} containers available)",
            task_id, requests.len(), language, version, available, required_containers
        );
        if available >= required_containers {
            debug!("Task {}: Executing immediately", task_id);
            match execution_type {
                ExecutionType::Single => {
                    let result = self.executor.clone().execute(first_request, redis_client).await?;
                    debug!("Task {}: Single execution completed", task_id);
                    Ok(vec![result])
                }
                ExecutionType::Parallel => {
                    let results = self.executor.clone().execute_parallel(requests, redis_client).await;
                    debug!("Task {}: Parallel execution completed with {} results", task_id, results.len());
                    Ok(results)
                }
                ExecutionType::Batch => {
                    let results = self.executor.clone().execute_batch(requests, redis_client).await?;
                    debug!("Task {}: Batch execution completed with {} results", task_id, results.len());
                    Ok(results)
                }
            }
        } else {
            debug!("Task {}: Enqueuing due to insufficient containers", task_id);
            let task = ExecutionTask {
                id: task_id.clone(),
                requests,
                execution_type,
                user_id: None,
            };

            {
                let mut tasks = self.tasks.write().await;
                tasks.insert(task_id.clone(), task);
            }

            self.queue.enqueue(task_id.clone(), language, version, priority).await;
            debug!("Task {}: Enqueued successfully", task_id);
            Ok(vec![])
        }
    }

    pub async fn process_next(&self, language: &str, version: &str, redis_client: &mut RedisClient) -> Result<Vec<EvaluationResult>> {
        if let Some(task_id) = self.queue.dequeue(language, version).await {
            debug!("Dequeued task {} for {}:{}", task_id, language, version);
            let task = {
                let tasks = self.tasks.read().await;
                tasks.get(&task_id).cloned()
            };

            if let Some(task) = task {
                let results = match task.execution_type {
                    ExecutionType::Single => {
                        if let Some(request) = task.requests.first() {
                            let result = self.executor.clone().execute(request.clone(), redis_client.clone()).await?;
                            debug!("Task {}: Single execution completed", task_id);
                            vec![result]
                        } else {
                            debug!("Task {}: No requests found", task_id);
                            vec![]
                        }
                    }
                    ExecutionType::Parallel => {
                        let results = self.executor.clone().execute_parallel(task.requests, redis_client.clone()).await;
                        debug!("Task {}: Parallel execution completed with {} results", task_id, results.len());
                        results
                    }
                    ExecutionType::Batch => {
                        let results = self.executor.clone().execute_batch(task.requests, redis_client.clone()).await?;
                        debug!("Task {}: Batch execution completed with {} results", task_id, results.len());
                        results
                    }
                };

                {
                    let mut tasks = self.tasks.write().await;
                    tasks.remove(&task_id);
                    debug!("Task {}: Removed from tasks map", task_id);
                }

                Ok(results)
            } else {
                debug!("Task {}: Not found in tasks map", task_id);
                Err(anyhow::anyhow!("Task not found"))
            }
        } else {
            debug!("No tasks in queue for {}:{}", language, version);
            Err(anyhow::anyhow!("No tasks in queue for {}:{}", language, version))
        }
    }

    pub async fn start_worker(&self, language: String, version: String, redis_client: RedisClient) {
        let mut redis_client = redis_client;
        for _ in 0..32 { // Reduced to 32 workers to minimize contention
            let queue_manager = self.clone();
            let language = language.clone();
            let version = version.clone();
            let mut redis_client = redis_client.clone();
            tokio::spawn(async move {
                loop {
                    if let Err(e) = queue_manager.process_next(&language, &version, &mut redis_client).await {
                        debug!("Worker for {}:{}: No tasks or error: {}", language, version, e);
                        // Increased backoff to 150ms to balance speed and stability
                        sleep(Duration::from_millis(150)).await;
                    }
                    queue_manager.executor.task_notify.notified().await;
                }
            });
        }
        debug!("Started 32 workers for {}:{}", language, version); // Updated log
    }
}
