use std::sync::Arc;
use tokio::sync::RwLock;
use crate::queue_management::task::ExecutionTask;
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
        task: ExecutionTask,
        priority: i64,
        redis_client: 
        RedisClient,
    ) -> Result<String, anyhow::Error> {
        
        let task_id = Uuid::new_v4().to_string();
        let (parent_token, status_key) = match &task {
            ExecutionTask::CompileBatch(compile_task) => {
                (
                    compile_task.parent_token.clone(),
                    format!("status:{}", compile_task.parent_token)
                )
            },
            ExecutionTask::RunTestcase(run_task) => {
                (
                    run_task.parent_token.clone(),
                    format!("status:{}", run_task.parent_token)
                )
            }
        };

        let serialized_task = serde_json::to_string(&task)?;
        let queue_name = "evalx_jobs";

        let queue_key = format!("queue:{}", queue_name);
        let priority_key = format!("priority:{}", queue_name);

        QUEUE_DEPTH.with_label_values(&[queue_name]).inc();

        let mut conn = redis_client.get_multiplexed_async_connection().await?;

        conn.set::<_, _, ()>(&format!("task:{}", &task_id), &serialized_task).await?;
        conn.set_nx::<_, _, ()>(&status_key, "queued").await?;
        conn.zadd::<_, _, _, ()>(&priority_key, &task_id, priority).await?;
        conn.lpush::<_, _, ()>(&queue_key, &task_id).await?;
        
        self.notify_completion(&parent_token, &vec![], SubmissionStatus::Queued);
        Ok(parent_token)
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