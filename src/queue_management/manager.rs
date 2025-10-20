use std::sync::Arc;
use tokio::sync::RwLock;
use crate::queue_management::{ExecutionTask, ExecutionType};
use crate::types::index::{CodeExecutor, ExecutionNotification};
use crate::caching::redis_client::RedisClient;
use crate::models::request::ExecutionRequest;
use crate::models::response::{EvaluationResult, SubmissionStatus};
use anyhow::Result;
use uuid::Uuid;
use tracing::{info, error, warn};
use redis::AsyncCommands;
use std::env;
use tokio::sync::broadcast;
// ADDED: Import the prometheus metric for queue depth.
use crate::monitoring::metrics::QUEUE_DEPTH;

#[derive(Clone)]
pub struct QueueManager {
    queue: crate::queue_management::ExecutionQueue,
    tasks: Arc<RwLock<std::collections::HashMap<String, ExecutionTask>>>,
    executor: Arc<CodeExecutor>,
    redis_client: RedisClient,
    notification_tx: Arc<broadcast::Sender<ExecutionNotification>>,
}

impl QueueManager {
    // Load result TTL from environment
    
        
    pub fn new(executor: Arc<CodeExecutor>, redis_client: RedisClient, notification_tx: Arc<broadcast::Sender<ExecutionNotification>>) -> Self {
        QueueManager {
            queue: crate::queue_management::ExecutionQueue::new(redis_client.clone()),
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
    ) -> Result<String, anyhow::Error> {
        if requests.is_empty() {
            return Err(anyhow::anyhow!("Request list cannot be empty"));
        }

        let first_request = &requests[0];
        let language = first_request.language.as_ref().unwrap_or(&"".to_string()).trim().to_string();
        let version = first_request.version.as_ref().unwrap_or(&"".to_string()).trim().to_string();
        let task_id = Uuid::new_v4().to_string();
        
        // MODIFIED: Initialize the `created_at` and `retry_count` fields.
        let task = ExecutionTask {
            id: task_id.clone(),
            requests,
            execution_type,
            user_id: None,
            created_at: std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH)?.as_secs(),
            retry_count: 0, // Initialize retry count for a new task.
        };

        let serialized_task = serde_json::to_string(&task)?;
        // CHANGED: Route tasks to one of two queues based on language.
        // All tasks now go to a single, unified queue.
        let queue_name = "evalx_jobs";

        let queue_key = format!("queue:{}", queue_name);
        let priority_key = format!("priority:{}", queue_name);

        let status_key = format!("status:{}", &task_id);
        // Increment the queue depth gauge for the unified queue.
        QUEUE_DEPTH.with_label_values(&[queue_name]).inc();

        let mut conn = redis_client.get_multiplexed_async_connection().await?;

        conn.set::<_, _, ()>(&format!("task:{}", &task_id), &serialized_task).await?;
        conn.set::<_, _, ()>(&status_key, "queued").await?;
        conn.zadd::<_, _, _, ()>(&priority_key, &task_id, priority).await?;
        conn.lpush::<_, _, ()>(&queue_key, &task_id).await?;
        
        self.notify_completion(&task_id, &vec![], SubmissionStatus::Queued);
        Ok(task_id)
    }

    pub async fn start_worker(&self, queue_name: String, redis_client: RedisClient) -> Result<(), anyhow::Error> {
        let result_ttl: usize = env::var("RESULT_TTL_SECONDS")
        .unwrap_or_else(|_| "200".to_string())
        .parse()
        .unwrap_or(200);
        info!("Starting worker for queue: {}", queue_name);
        let queue_key = format!("queue:{}", queue_name);
        let priority_key = format!("priority:{}", queue_name);
        
        // MODIFIED: Load configuration from environment variables once.
        let max_retries: u8 = env::var("MAX_RETRIES").unwrap_or_else(|_| "3".to_string()).parse()?;
        let dlq_name = env::var("DEAD_LETTER_QUEUE_NAME").unwrap_or_else(|_| "evalx:dead_letter_queue".to_string());

        loop {
            let mut conn = match redis_client.get_multiplexed_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to get multiplexed Redis connection for worker: {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    continue;
                }
            };
            
            let result: Option<(String, String)> = match conn.brpop(&queue_key, 1).await {
                Ok(res) => res,
                Err(e) => {
                    error!("Failed to dequeue task: {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    continue;
                }
            };
            
            if let Some((_, task_id)) = result {
                QUEUE_DEPTH.with_label_values(&[&queue_name]).dec();
                info!("Dequeued task: {} from queue: {}", task_id, queue_name);
                let status_key = format!("status:{}", &task_id);
                
                if let Err(e) = conn.zrem::<_, _, ()>(&priority_key, &task_id).await {
                    error!("Failed to remove task {} from priority set: {}", task_id, e);
                    conn.set::<_, _, ()>(&status_key, "failed").await.unwrap_or_else(|e| error!("Failed to set status to failed for task {}: {}", task_id, e));
                    continue;
                }
                
                let task_key = format!("task:{}", &task_id);
                let serialized_task: Option<String> = match conn.get(&task_key).await {
                    Ok(res) => res,
                    Err(e) => {
                        error!("Failed to get task {}: {}", task_id, e);
                        conn.set::<_, _, ()>(&status_key, "failed").await.unwrap_or_else(|e| error!("Failed to set status to failed for task {}: {}", task_id, e));
                        continue;
                    }
                };
                
                if let Some(serialized_task) = serialized_task {
                    let mut task: ExecutionTask = match serde_json::from_str::<ExecutionTask>(&serialized_task) {
                        Ok(t) => t,
                        Err(e) => {
                            error!("Failed to deserialize task {}: {}", task_id, e);
                            conn.set::<_, _, ()>(&status_key, "failed").await.unwrap_or_else(|e| error!("Failed to set status to failed for task {}: {}", task_id, e));
                            self.notify_completion(&task_id, &vec![], SubmissionStatus::Failed);
                            continue;
                        }
                    };
                    
                    // CORRECTED: Calculate and record queue wait time using the task's timestamp.
                    let now_unix = std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH)?.as_secs();
                    let wait_time = now_unix.saturating_sub(task.created_at);
                    crate::monitoring::metrics::QUEUE_WAIT_TIME_SECONDS.with_label_values(&[&queue_name]).observe(wait_time as f64);
                    
                    if let Err(e) = conn.set::<_, _, ()>(&status_key, "processing").await {
                        error!("Failed to set status to processing for task {}: {}", task_id, e);
                        continue;
                    }

                    self.notify_completion(&task_id, &vec![], SubmissionStatus::Processing);
                    let executor_clone = self.executor.clone();
                    let result_key = format!("result:{}", &task_id);
                    
                    match executor_clone.execute_batch(task.requests.clone(), redis_client.clone(), Some(task_id.clone())).await {
                        Ok(results) => {
                            let serialized_results = serde_json::to_string(&results)?;
                            conn.set_ex::<_, _, ()>(&result_key, serialized_results, result_ttl).await?;
                            if let Err(e) = conn.set::<_, _, ()>(&status_key, "completed").await {
                                error!("Failed to set status to completed for task {}: {}", task_id, e);
                            }
                            self.notify_completion(&task_id, &results, SubmissionStatus::Completed);
                        }
                        Err(e) => {
                            // MODIFIED: Retry and Dead Letter Queue Logic
                            error!("Execution failed for task {}: {}. Retry {}/{}", task_id, e, task.retry_count + 1, max_retries);
                            task.retry_count += 1;

                            if task.retry_count >= max_retries {
                                warn!("Task {} failed after {} retries. Moving to Dead Letter Queue.", task_id, max_retries);
                                let error_result = vec![EvaluationResult::error_result(format!("Execution failed after {} retries: {}", max_retries, e))];
                                let serialized_results = serde_json::to_string(&error_result)?;
                                conn.set_ex::<_, _, ()>(&result_key, serialized_results, result_ttl).await?;
                                if let Err(e) = conn.set::<_, _, ()>(&status_key, "failed").await {
                                    error!("Failed to set status to failed for task {}: {}", task_id, e);
                                }
                                
                                // Move to DLQ
                                let failed_task_payload = serde_json::to_string(&task).unwrap_or_default();
                                conn.lpush(&dlq_name, failed_task_payload).await?;

                                self.notify_completion(&task_id, &error_result, SubmissionStatus::Failed);
                            } else {
                                // Re-queue the task for another attempt.
                                warn!("Re-queuing task {} for retry.", task_id);
                                let updated_task_payload = serde_json::to_string(&task)?;
                                conn.set(&task_key, updated_task_payload).await?;
                                // Re-add to the original queue
                                conn.lpush(&queue_key, &task_id).await?;
                                conn.zadd(&priority_key, &task_id, 1).await?; // Re-queue with normal priority
                                // No notification, it's still "processing" from the user's perspective.
                            }
                        }
                    }

                    // MODIFIED: Only delete the task key if the task was successful or moved to DLQ.
                    // If it's being retried, the task data must remain.
                    if let Ok(status) = conn.get::<_, String>(&status_key).await {
                        if status == "completed" || status == "failed" {
                             if let Err(e) = conn.del::<_, ()>(&task_key).await {
                                error!("Failed to delete task {}: {}", task_id, e);
                            }
                        }
                    }
                } else {
                    error!("Task {} not found in Redis", task_id);
                    conn.set::<_, _, ()>(&status_key, "failed").await.unwrap_or_else(|e| error!("Failed to set status to failed for task {}: {}", task_id, e));
                }
            }
        }
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
            results: if results.is_empty() { None } else { Some(results.clone()) },
        };
        let _ = self.notification_tx.send(notification);
    }
}