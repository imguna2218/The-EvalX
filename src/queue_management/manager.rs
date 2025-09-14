use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};
use crate::queue_management::{ExecutionQueue, ExecutionTask, ExecutionType};
use crate::types::index::{CodeExecutor, ExecutionNotification};
use crate::caching::redis_client::RedisClient;
use crate::models::request::ExecutionRequest;
use crate::models::response::EvaluationResult;
use anyhow::Result;
use uuid::Uuid;
use crate::container_management::container_pool::available_containers;
use tracing::debug;
use redis::AsyncCommands;
use tracing::{info, error};
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct QueueManager {
    queue: ExecutionQueue,
    tasks: Arc<RwLock<std::collections::HashMap<String, ExecutionTask>>>,
    executor: Arc<CodeExecutor>,
    redis_client: RedisClient,
    notification_tx: Arc<broadcast::Sender<ExecutionNotification>>,
}

impl QueueManager {
    pub fn new(executor: Arc<CodeExecutor>, redis_client: RedisClient, notification_tx: Arc<broadcast::Sender<ExecutionNotification>>) -> Self {
        QueueManager {
            queue: ExecutionQueue::new(redis_client.clone()),
            tasks: Arc::new(RwLock::new(std::collections::HashMap::new())),
            executor,
            redis_client,
            notification_tx,
        }
    }

    pub async fn add_task(
        &self,
        requests: Vec<ExecutionRequest>,
        execution_type: ExecutionType,
        priority: i64,
        redis_client: RedisClient,
    ) -> Result<Vec<EvaluationResult>, anyhow::Error> {
        if requests.is_empty() {
            return Ok(vec![]);
        }

        let first_request = &requests[0];
        let language = first_request.language.as_ref().unwrap_or(&"".to_string()).trim().to_string();
        let version = first_request.version.as_ref().unwrap_or(&"".to_string()).trim().to_string();

        let task_id = Uuid::new_v4().to_string();
        let task = ExecutionTask {
            id: task_id.clone(),
            requests,
            execution_type,
            user_id: None,
        };

        let serialized_task = serde_json::to_string(&task)?;
        let queue_key = format!("queue:{}:{}", language, version);
        let priority_key = format!("priority:{}:{}", language, version);
        let task_key = format!("task:{}", &task_id);

        let mut conn = redis_client.get_async_connection().await?;

        // Store the serialized task
        conn.set::<_, _, ()>(&task_key, &serialized_task).await?;

        // Add to priority set (score first, then member)
        conn.zadd::<_, _, _, ()>(&priority_key, &task_id, priority).await?;

        // Add to queue
        conn.lpush::<_, _, ()>(&queue_key, &task_id).await?;

        // Check if immediate execution is possible
        let queue_length: i64 = conn.llen::<_, i64>(&queue_key).await?;
        if queue_length == 1 {
            // Queue was empty, execute immediately
            let executor_clone = self.executor.clone();
            let results = executor_clone.execute_batch(task.requests, redis_client).await?;
            let _ = self.notify_completion(&task_id, &results);
            // Clean up task
            let _: () = conn.del::<_, ()>(&task_key).await?;
            Ok(results)
        } else {
            // Enqueued, notify
            let _ = self.notify_completion(&task_id, &vec![]);
            Ok(vec![])
        }
    }

    pub async fn start_worker(&self, language: String, version: String, redis_client: RedisClient) {
        info!("Starting worker for language: {}, version: {}", language, version);
        let queue_key = format!("queue:{}:{}", language, version);
        let priority_key = format!("priority:{}:{}", language, version);

        loop {
            let mut conn = match redis_client.get_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to get Redis connection: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            // Use BRPOP for blocking pop with 1-second timeout
            let result: Option<(String, String)> = conn.brpop(&queue_key, 1).await.expect("Failed to dequeue task");
            if let Some((_, task_id)) = result {
                // Remove from priority set
                conn.zrem::<_, _, ()>(&priority_key, &task_id).await.expect("Failed to remove task from priority set");
                
                // Get the task
                let task_key = format!("task:{}", &task_id);
                let serialized_task: Option<String> = conn.get(&task_key).await.expect("Failed to get task");
                if let Some(serialized_task) = serialized_task {
                    if let Ok(task) = serde_json::from_str::<ExecutionTask>(&serialized_task) {
                        let executor_clone = self.executor.clone();
                        match executor_clone.execute_batch(task.requests, redis_client.clone()).await {
                            Ok(results) => {
                                let _ = self.notify_completion(&task_id, &results);
                            }
                            Err(e) => {
                                error!("Execution failed for task {}: {}", task_id, e);
                                let _ = self.notify_completion(&task_id, &vec![]);
                            }
                        }
                        // Clean up task
                        let _: () = conn.del::<_, ()>(&task_key).await.expect("Failed to delete task");
                    }
                }
            }
        }
    }

    pub async fn remove(&self, task_id: &str, language: &str, version: &str) {
        let queue_key = format!("queue:{}:{}", language, version);
        let priority_key = format!("priority:{}:{}", language, version);
        let task_key = format!("task:{}", task_id);
        let mut conn = self.redis_client.get_async_connection().await.expect("Failed to get Redis connection");
        
        // Remove from queue, priority set, and task storage
        conn.lrem::<_, _, ()>(&queue_key, 0, task_id).await.expect("Failed to remove task from queue");
        conn.zrem::<_, _, ()>(&priority_key, task_id).await.expect("Failed to remove task from priority set");
        conn.del::<_, ()>(&task_key).await.expect("Failed to delete task");
    }

    // New method: Notify completion via broadcast (for Phase 1 notifications)
    pub fn notify_completion(&self, task_id: &str, results: &Vec<EvaluationResult>) {
        let notification = ExecutionNotification {
            id: task_id.to_string(),
            status: if results.is_empty() { "queued".to_string() } else { "completed".to_string() },
            result: if results.is_empty() { None } else { Some(results[0].clone()) },  // Or aggregate
        };
        let _ = self.notification_tx.send(notification);
    }
}