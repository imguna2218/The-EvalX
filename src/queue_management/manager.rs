use std::sync::Arc;
use tokio::sync::RwLock;
use crate::queue_management::{ExecutionQueue, ExecutionTask, ExecutionType};
use crate::types::index::{CodeExecutor, ExecutionNotification};
use crate::caching::redis_client::RedisClient;
use crate::models::request::ExecutionRequest;
use crate::models::response::{EvaluationResult, SubmissionStatus};
use anyhow::Result;
use uuid::Uuid;
use tracing::{info, error};
use redis::AsyncCommands;
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
        let status_key = format!("status:{}", &task_id);

        let mut conn = redis_client.get_async_connection().await?;

        // Store the serialized task
        conn.set::<_, _, ()>(&format!("task:{}", &task_id), &serialized_task).await?;

        // Set initial status to Queued
        conn.set::<_, _, ()>(&status_key, "queued").await?;

        // Add to priority set (score first, then member)
        conn.zadd::<_, _, _, ()>(&priority_key, &task_id, priority).await?;

        // Add to queue
        conn.lpush::<_, _, ()>(&queue_key, &task_id).await?;

        // Notify queued status
        let _ = self.notify_completion(&task_id, &vec![], SubmissionStatus::Queued);

        Ok(vec![])
    }

    pub async fn start_worker(&self, language: String, version: String, redis_client: RedisClient) -> Result<(), anyhow::Error> {
        info!("Starting worker for language: {}, version: {}", language, version);
        let queue_key = format!("queue:{}:{}", language, version);
        let priority_key = format!("priority:{}:{}", language, version);

        loop {
            let mut conn = match redis_client.get_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to get Redis connection: {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
            };

            // Use BRPOP for blocking pop with 1-second timeout
            let result: Option<(String, String)> = match conn.brpop(&queue_key, 1).await {
                Ok(res) => res,
                Err(e) => {
                    error!("Failed to dequeue task: {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
            };

            if let Some((_, task_id)) = result {
                let status_key = format!("status:{}", &task_id);
                // Remove from priority set
                if let Err(e) = conn.zrem::<_, _, ()>(&priority_key, &task_id).await {
                    error!("Failed to remove task from priority set: {}", e);
                    conn.set::<_, _, ()>(&status_key, "failed").await.unwrap_or_else(|e| error!("Failed to set status to failed: {}", e));
                    continue;
                }
                
                // Get the task
                let task_key = format!("task:{}", &task_id);
                let serialized_task: Option<String> = match conn.get(&task_key).await {
                    Ok(res) => res,
                    Err(e) => {
                        error!("Failed to get task: {}", e);
                        conn.set::<_, _, ()>(&status_key, "failed").await.unwrap_or_else(|e| error!("Failed to set status to failed: {}", e));
                        continue;
                    }
                };

                if let Some(serialized_task) = serialized_task {
                    let task: ExecutionTask = match serde_json::from_str(&serialized_task) {
                        Ok(t) => t,
                        Err(e) => {
                            error!("Failed to deserialize task {}: {}", task_id, e);
                            conn.set::<_, _, ()>(&status_key, "failed").await.unwrap_or_else(|e| error!("Failed to set status to failed: {}", e));
                            let _ = self.notify_completion(&task_id, &vec![], SubmissionStatus::Failed);
                            continue;
                        }
                    };

                    // Set status to Processing
                    if let Err(e) = conn.set::<_, _, ()>(&status_key, "processing").await {
                        error!("Failed to set status to processing: {}", e);
                        continue;
                    }

                    // Notify processing status
                    let _ = self.notify_completion(&task_id, &vec![], SubmissionStatus::Processing);

                    let executor_clone = self.executor.clone();
                    let result_key = format!("result:{}", &task_id);
                    match executor_clone.execute_batch(task.requests, redis_client.clone(), Some(task_id.clone())).await {
                        Ok(results) => {
                            // Store results in Redis
                            let serialized_results = serde_json::to_string(&results)?;
                            conn.set_ex::<_, _, ()>(&result_key, serialized_results, 3600).await?;
                            // Set status to Completed
                            conn.set::<_, _, ()>(&status_key, "completed").await.unwrap_or_else(|e| error!("Failed to set status to completed: {}", e));
                            let _ = self.notify_completion(&task_id, &results, SubmissionStatus::Completed);
                        }
                        Err(e) => {
                            error!("Execution failed for task {}: {}", task_id, e);
                            let error_result = vec![EvaluationResult::error_result(format!("Execution failed: {}", e))];
                            let serialized_results = serde_json::to_string(&error_result)?;
                            conn.set_ex::<_, _, ()>(&result_key, serialized_results, 3600).await?;
                            // Set status to Failed
                            conn.set::<_, _, ()>(&status_key, "failed").await.unwrap_or_else(|e| error!("Failed to set status to failed: {}", e));
                            let _ = self.notify_completion(&task_id, &error_result, SubmissionStatus::Failed);
                        }
                    }

                    // Clean up task
                    if let Err(e) = conn.del::<_, ()>(&task_key).await {
                        error!("Failed to delete task {}: {}", task_id, e);
                    }
                } else {
                    conn.set::<_, _, ()>(&status_key, "failed").await.unwrap_or_else(|e| error!("Failed to set status to failed: {}", e));
                }
            }
        }
    }

    pub async fn remove(&self, task_id: &str, language: &str, version: &str) {
        let queue_key = format!("queue:{}:{}", language, version);
        let priority_key = format!("priority:{}:{}", language, version);
        let task_key = format!("task:{}", task_id);
        let result_key = format!("result:{}", task_id);
        let status_key = format!("status:{}", task_id);
        let mut conn = self.redis_client.get_async_connection().await.expect("Failed to get Redis connection");
        
        // Remove from queue, priority set, task storage, result, and status
        conn.lrem::<_, _, ()>(&queue_key, 0, task_id).await.expect("Failed to remove task from queue");
        conn.zrem::<_, _, ()>(&priority_key, task_id).await.expect("Failed to remove task from priority set");
        conn.del::<_, ()>(&task_key).await.expect("Failed to delete task");
        conn.del::<_, ()>(&result_key).await.expect("Failed to delete result");
        conn.del::<_, ()>(&status_key).await.expect("Failed to delete status");
    }

    pub fn notify_completion(&self, task_id: &str, results: &Vec<EvaluationResult>, status: SubmissionStatus) {
        let notification = ExecutionNotification {
            id: task_id.to_string(),
            status: match status {
                SubmissionStatus::Queued => "queued".to_string(),
                SubmissionStatus::Processing => "processing".to_string(),
                SubmissionStatus::Completed => "completed".to_string(),
                SubmissionStatus::Failed => "failed".to_string(),
            },
            result: if results.is_empty() { None } else { Some(results[0].clone()) },
        };
        let _ = self.notification_tx.send(notification);
    }
}