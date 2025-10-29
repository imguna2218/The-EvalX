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