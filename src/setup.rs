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
        system: Arc::new(Mutex::new(System::new_all())),
        redis_client: redis_client.clone(), 
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


pub async fn start_workers(
    queue_manager: Arc<QueueManager>,
    redis_client: RedisClient,
) {
    // Automatically detect the number of CPU cores and spawn one worker for each.
    let num_workers = num_cpus::get().max(1);
    let queue_name = "evalx_jobs".to_string();
    info!(
        "Spawning {} identical workers for the unified '{}' queue.",
        num_workers, queue_name
    );

    let mut handles = Vec::new();

    for i in 0..num_workers {
        let manager_clone = queue_manager.clone();
        let client_clone = redis_client.clone();
        let q_name = queue_name.clone();
        let handle = tokio::spawn(async move {
            info!("Worker #{} starting...", i + 1);
            if let Err(e) = manager_clone.start_worker(q_name, client_clone).await {
                error!("Worker #{} failed: {}", i + 1, e);
            }
        });
        handles.push(handle);
    }

    // Keep the main worker process alive by waiting on all spawned tasks.
    futures_util::future::join_all(handles).await;
}