use std::sync::Arc;
use tokio::sync::RwLock;
use crate::queue_management::task::ExecutionTask;
use crate::types::index::{CodeExecutor, ExecutionNotification};
use crate::caching::redis_client::RedisClient;
use crate::models::response::{EvaluationResult, SubmissionStatus};
use anyhow::Result;
use uuid::Uuid;
use tracing::{info, error, warn, debug};
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
    billing_queue_url: String,
}

impl QueueManager {
    pub fn new(
        executor: Arc<CodeExecutor>, 
        redis_client: RedisClient, 
        notification_tx: Arc<broadcast::Sender<ExecutionNotification>>,
        sqs_client: SqsClient,
        billing_queue_url: String,
    ) -> Self {
        let sqs_url = env::var("SQS_QUEUE_URL").expect("SQS_QUEUE_URL must be set"); // [cite: 330, 331]
        
        QueueManager {
            tasks: Arc::new(RwLock::new(std::collections::HashMap::new())), // [cite: 331]
            executor, // [cite: 331]
            redis_client, // [cite: 331]
            notification_tx, // [cite: 331]
            sqs_client, // [cite: 331]
            sqs_url, // [cite: 331]
            billing_queue_url,
        }
    }

    pub async fn add_task(
    &self,
    task: ExecutionTask,
    _priority: i64,
    redis_client: RedisClient,
    billing_token: String,
    ) -> Result<String, anyhow::Error> {
        // Internal Job Token (UUID)
        let parent_token = match &task {
            ExecutionTask::CompileBatch(t) => t.parent_token.clone(),
            ExecutionTask::RunTestcase(t) => t.parent_token.clone(),
        };

        // Fallback: Use dummy token if Authorization header was missing
        let final_billing_token = if billing_token.trim().is_empty() {
            "dummy-token-123".to_string()
        } else {
            billing_token
        };

        let status_key = format!("status:{}", parent_token);
        let mut conn = redis_client.get_multiplexed_async_connection().await?;
        conn.set_nx::<_, _, ()>(&status_key, "queued").await?;
        
        // 1. Send FULL PAYLOAD (Code + Testcases) to Job Queue
        let serialized_task = serde_json::to_string(&task)?;
        self.sqs_client
            .send_message()
            .queue_url(&self.sqs_url)
            .message_body(serialized_task)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send to Job SQS: {}", e))?;

        // 2. Send AUTH TOKEN ONLY to Billing Queue
        let billing_payload = serde_json::json!({
            "billing_token": final_billing_token,
            "job_id": parent_token,
            "status": "initiated",
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        }).to_string();

        self.sqs_client
            .send_message()
            .queue_url(&self.billing_queue_url)
            .message_body(billing_payload)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send to Billing SQS: {}", e))?;

        QUEUE_DEPTH.with_label_values(&["evalx_jobs"]).inc();
        self.notify_completion(&parent_token, &vec![], SubmissionStatus::Queued);
        
        Ok(parent_token)
    }

    pub fn notify_completion(&self, task_id: &str, results: &Vec<EvaluationResult>, status: SubmissionStatus) {
        let notification = ExecutionNotification {
            id: task_id.to_string(), // [cite: 337]
            status: match status {
                SubmissionStatus::Queued => "queued".to_string(), // [cite: 337]
                SubmissionStatus::Processing => "processing".to_string(), 
                SubmissionStatus::Completed => "completed".to_string(), 
                SubmissionStatus::Failed => "failed".to_string(), 
            },
            results: if results.is_empty() { None } else { Some(results.clone()) }, 
        };
        let _ = self.notification_tx.send(notification); 
    }
}