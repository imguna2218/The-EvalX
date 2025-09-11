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
use redis::AsyncCommands;

#[derive(Clone)]
pub struct QueueManager {
    queue: ExecutionQueue,
    tasks: Arc<RwLock<std::collections::HashMap<String, ExecutionTask>>>,
    executor: Arc<CodeExecutor>,
    redis_client: RedisClient,
}

impl QueueManager {
    pub fn new(executor: Arc<CodeExecutor>, redis_client: RedisClient) -> Self {
        QueueManager {
            queue: ExecutionQueue::new(redis_client.clone()),
            tasks: Arc::new(RwLock::new(std::collections::HashMap::new())),
            executor,
            redis_client,
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

        // Check if queue is empty and containers are available for immediate execution
        let queue_key = format!("queue:{}:{}", language, version);
        let mut conn = self.redis_client.get_async_connection().await?;
        let queue_length: i64 = conn.llen(&queue_key).await?;
        if available >= required_containers && queue_length == 0 {
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
            debug!("Task {}: Enqueuing due to insufficient containers or non-empty queue", task_id);
            let task = ExecutionTask {
                id: task_id.clone(),
                requests,
                execution_type,
                user_id: None,
            };

            // Store task in Redis and local map
            let task_json = serde_json::to_string(&task)?;
            let task_key = format!("task:{}", task_id);
            conn.set_ex::<_, _, ()>(&task_key, task_json, 3600).await?; // Expire after 1 hour
            {
                let mut tasks = self.tasks.write().await;
                tasks.insert(task_id.clone(), task);
            }

            // Enqueue task with priority
            self.queue.enqueue(task_id.clone(), language, version, priority).await;
            debug!("Task {}: Enqueued successfully", task_id);
            Ok(vec![])
        }
    }

    pub async fn process_next(&self, language: &str, version: &str, redis_client: &mut RedisClient) -> Result<Vec<EvaluationResult>> {
        if let Some(task_id) = self.queue.dequeue(language, version).await {
            debug!("Dequeued task {} for {}:{}", task_id, language, version);
            let task_key = format!("task:{}", task_id);
            let mut conn = self.redis_client.get_async_connection().await?;
            let task_json: Option<String> = conn.get(&task_key).await?;
            let task: ExecutionTask = if let Some(json) = task_json {
                serde_json::from_str(&json)?
            } else {
                debug!("Task {}: Not found in Redis", task_id);
                return Err(anyhow::anyhow!("Task not found in Redis"));
            };

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

            // Clean up task from Redis and local map
            conn.del::<_, ()>(&task_key).await?;
            {
                let mut tasks = self.tasks.write().await;
                tasks.remove(&task_id);
                debug!("Task {}: Removed from tasks map and Redis", task_id);
            }

            Ok(results)
        } else {
            debug!("No tasks in queue for {}:{}", language, version);
            Err(anyhow::anyhow!("No tasks in queue for {}:{}", language, version))
        }
    }

    pub async fn start_worker(&self, language: String, version: String, redis_client: RedisClient) {
        let redis_client = redis_client;
        for _ in 0..32 { // Keep 32 workers as in original
            let queue_manager = self.clone();
            let language = language.clone();
            let version = version.clone();
            let mut redis_client = redis_client.clone();
            tokio::spawn(async move {
                loop {
                    if let Err(e) = queue_manager.process_next(&language, &version, &mut redis_client).await {
                        debug!("Worker for {}:{}: No tasks or error: {}", language, version, e);
                        // Use 1s polling with BRPOP
                        sleep(Duration::from_secs(1)).await;
                    }
                }
            });
        }
        debug!("Started 32 workers for {}:{}", language, version);
    }
}