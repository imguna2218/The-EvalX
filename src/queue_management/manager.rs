use std::sync::Arc;
use tokio::sync::RwLock;
use crate::queue_management::task::ExecutionTask;
use crate::types::index::{CodeExecutor, ExecutionNotification};
use crate::caching::redis_client::RedisClient;
use crate::models::response::{EvaluationResult, SubmissionStatus};
use anyhow::Result;
use uuid::Uuid;
use tracing::{info, error, warn};
use redis::AsyncCommands;
use std::env;
use tokio::sync::broadcast;
use crate::monitoring::metrics::QUEUE_DEPTH;
use aws_sdk_sqs::Client as SqsClient;

#[derive(Clone)]
pub struct QueueManager {
    tasks: Arc<RwLock<std::collections::HashMap<String, ExecutionTask>>>,
    executor: Arc<CodeExecutor>,
    redis_client: RedisClient,
    notification_tx: Arc<broadcast::Sender<ExecutionNotification>>,
    sqs_client: SqsClient,
    sqs_url: String,
}

impl QueueManager {
    pub fn new(
        executor: Arc<CodeExecutor>, 
        redis_client: RedisClient, 
        notification_tx: Arc<broadcast::Sender<ExecutionNotification>>,
        sqs_client: SqsClient
    ) -> Self {
        let sqs_url = env::var("SQS_QUEUE_URL").expect("SQS_QUEUE_URL must be set");
        
        QueueManager {
            tasks: Arc::new(RwLock::new(std::collections::HashMap::new())),
            executor,
            redis_client,
            notification_tx,
            sqs_client,
            sqs_url,
        }
    }

    pub async fn add_task(
        &self,
        task: ExecutionTask,
        _priority: i64,
        redis_client: RedisClient,
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
        
        let mut conn = redis_client.get_multiplexed_async_connection().await?;
        conn.set_nx::<_, _, ()>(&status_key, "queued").await?;
        
        self.sqs_client
            .send_message()
            .queue_url(&self.sqs_url)
            .message_body(serialized_task)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send to SQS: {}", e))?;

        QUEUE_DEPTH.with_label_values(&["evalx_jobs"]).inc();

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