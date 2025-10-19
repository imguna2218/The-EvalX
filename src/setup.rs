// src/setup.rs

use anyhow::Result;
use std::env;
use std::sync::Arc;
use tokio::sync::{broadcast, Semaphore, Mutex, RwLock};
use tracing::{error, info};
use std::time::Instant;
use crate::caching::redis_client::RedisClient;
use crate::queue_management::QueueManager;
use crate::types::index::{CodeExecutor, ExecutionNotification, ConcurrencyState};
use sysinfo::{System, SystemExt};
use crate::languages::manager::LanguageRegistry;

pub async fn initialize_executor(
    redis_client: RedisClient, // ACCEPT the one true client.
) -> Result<(
    Arc<CodeExecutor>,
    Arc<broadcast::Sender<ExecutionNotification>>,
    Arc<QueueManager>,
)> {
    dotenv::dotenv().ok();
    
    let language_registry = Arc::new(LanguageRegistry::new()?);

    let max_sandboxes = env::var("MAX_CONCURRENT_SANDBOXES")
        .unwrap_or_else(|_| "500".to_string())
        .parse::<usize>()?;
    info!("Max concurrent Isolate sandboxes: {}", max_sandboxes);

    let executor = Arc::new(CodeExecutor {
        semaphore: Arc::new(Semaphore::new(max_sandboxes)),
        last_java_warmup: Arc::new(RwLock::new(Instant::now())),
        system: Arc::new(Mutex::new(System::new_all())),
        redis_client: redis_client.clone(), // This now correctly uses the client passed into the function.
        concurrency_state: Arc::new(RwLock::new((
            ConcurrencyState::Nominal,
            Instant::now(),
        ))),
        language_registry,
    });
    
    let (tx, _) = broadcast::channel::<ExecutionNotification>(1024);
    let tx = Arc::new(tx);
    let queue_manager = Arc::new(QueueManager::new(
        executor.clone(),
        redis_client.clone(),
        tx.clone(),
    ));

    let monitor_client = redis_client.clone();
    tokio::spawn(async move {
        monitor_client.monitor_redis_memory().await;
    });
    
    Ok((executor, tx, queue_manager))
}

// ENTIRE FUNCTION REPLACED
pub async fn start_workers(
    queue_manager: Arc<QueueManager>,
    redis_client: RedisClient,
    worker_type: &str,
) {
    let queue_name = format!("{}-lane", worker_type);
    info!("Spawning a dedicated worker for the '{}' queue.", queue_name);

    tokio::spawn(async move {
        if let Err(e) = queue_manager
           .start_worker(queue_name.clone(), redis_client)
           .await
        {
            error!("Worker for queue '{}' failed: {}", queue_name, e);
        }
    });
}