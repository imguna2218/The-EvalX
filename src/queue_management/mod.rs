use crate::caching::redis_client::RedisClient;
use redis::AsyncCommands;
use tracing::info;

pub mod task;
pub mod manager;

pub use task::{ExecutionTask, ExecutionType};
pub use manager::QueueManager;

#[derive(Debug, Clone)]
pub struct ExecutionQueue {
    redis_client: RedisClient,
}

impl ExecutionQueue {
    pub fn new(redis_client: RedisClient) -> Self {
        ExecutionQueue { redis_client }
    }

    pub async fn enqueue(&self, task_id: String, language: &str, version: &str, priority: u8, task: &ExecutionTask) {
        let queue_key = format!("queue:{}:{}", language, version);
        let priority_key = format!("priority:{}:{}", language, version);
        let task_key = format!("task:{}", &task_id);
        let status_key = format!("status:{}", &task_id);
        let mut conn = self.redis_client.get_multiplexed_async_connection().await.expect("Failed to get Redis connection");
        
        // Serialize task and log
        let serialized_task = serde_json::to_string(task).expect("Failed to serialize task");
        info!("Enqueuing task {} for language: {}, version: {}, type: {:?}", task_id, language, version, task.execution_type);
        info!("Serialized task content: {}", serialized_task);
        
        // Store serialized task
        conn.set::<_, _, ()>(&task_key, &serialized_task).await.expect("Failed to store task");
        
        // Add task to queue
        conn.lpush::<_, _, ()>(&queue_key, &task_id).await.expect("Failed to enqueue task");
        
        // Add to priority set with proper score and member
        conn.zadd::<_, _, _, ()>(&priority_key, &task_id, priority as i64).await.expect("Failed to set priority");
        
        // Set initial status to Queued
        conn.set::<_, _, ()>(&status_key, "queued").await.expect("Failed to set initial status");
    }

    pub async fn dequeue(&self, language: &str, version: &str) -> Option<String> {
        let queue_key = format!("queue:{}:{}", language, version);
        let priority_key = format!("priority:{}:{}", language, version);
        let mut conn = self.redis_client.get_multiplexed_async_connection().await.expect("Failed to get Redis connection");
        
        // Use BRPOP for blocking pop with 1-second timeout
        let result: Option<(String, String)> = conn.brpop(&queue_key, 1).await.expect("Failed to dequeue task");
        if let Some((_, task_id)) = result {
            // Remove from priority set
            conn.zrem::<_, _, ()>(&priority_key, &task_id).await.expect("Failed to remove task from priority set");
            Some(task_id)
        } else {
            None
        }
    }

    pub async fn remove(&self, task_id: &str, language: &str, version: &str) {
        let queue_key = format!("queue:{}:{}", language, version);
        let priority_key = format!("priority:{}:{}", language, version);
        let task_key = format!("task:{}", task_id);
        let status_key = format!("status:{}", task_id);
        let mut conn = self.redis_client.get_multiplexed_async_connection().await.expect("Failed to get Redis connection");
        
        // Remove from queue, priority set, task storage, and status
        conn.lrem::<_, _, ()>(&queue_key, 0, task_id).await.expect("Failed to remove task from queue");
        conn.zrem::<_, _, ()>(&priority_key, task_id).await.expect("Failed to remove task from priority set");
        conn.del::<_, ()>(&task_key).await.expect("Failed to delete task");
        conn.del::<_, ()>(&status_key).await.expect("Failed to delete status");
    }
}