use crate::caching::redis_client::RedisClient;
use redis::AsyncCommands;
use tracing::info;

pub mod task;
pub mod manager;

pub use task::ExecutionTask;
pub use manager::QueueManager;

#[derive(Debug, Clone)]
pub struct ExecutionQueue {
    redis_client: RedisClient,
}

impl ExecutionQueue {
    pub fn new(redis_client: RedisClient) -> Self {
        ExecutionQueue { redis_client }
    }
}