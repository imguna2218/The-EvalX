use anyhow::Result;
use bollard::Docker;
use std::env;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{error, info};

use crate::caching::redis_client::RedisClient;
use crate::container_management::container_pool::init_container_pool;
use crate::container_management::init::new_executor;
use crate::queue_management::QueueManager;
use crate::types::index::{CodeExecutor, ExecutionNotification};

// MODIFIED: This function now only initializes the shared state (executor, channels, etc.)
// It NO LONGER starts the workers.
pub async fn initialize_shared_state() -> Result<(
    Arc<CodeExecutor>,
    Arc<broadcast::Sender<ExecutionNotification>>,
    Arc<QueueManager>,
    RedisClient,
)> {
    dotenv::dotenv().ok();
    let max_containers = env::var("MAX_CONCURRENT_CONTAINERS")
        .unwrap_or_else(|_| "500".to_string())
        .parse::<usize>()?;
    info!("MAX_CONCURRENT_CONTAINERS: {}", max_containers);

    let pool_size = env::var("CONTAINER_POOL_SIZE")
        .unwrap_or_else(|_| "50".to_string())
        .parse::<usize>()?;
    info!("CONTAINER_POOL_SIZE: {}", pool_size);

    let redis_client = RedisClient::new()?;
    let docker = Docker::connect_with_local_defaults()?;
    let executor = Arc::new(new_executor(docker, max_containers));

    let (tx, _) = broadcast::channel::<ExecutionNotification>(1024);
    let tx = Arc::new(tx);

    let queue_manager = Arc::new(QueueManager::new(
        executor.clone(),
        redis_client.clone(),
        tx.clone(),
    ));

    // This part still runs to pre-warm the container pool on startup for both app and workers.
    let languages_for_pool = vec![
        ("python".to_string(), "3.9".to_string()),
        // MODIFIED: Corrected the version for the Java 21 image
        ("java".to_string(), "21".to_string()),
        ("java11".to_string(), "11".to_string()),
        ("c".to_string(), "11".to_string()),
        ("cpp".to_string(), "11".to_string()),
        ("javascript".to_string(), "18".to_string()),
    ];
    info!("Pre-warming container pool...");
    init_container_pool(&executor, languages_for_pool, pool_size).await?;

    Ok((executor, tx, queue_manager, redis_client))
}

// NEW: This new function contains the logic to start the worker processes.
// It will be called only when the program is started with the "worker" argument.
pub async fn run_workers(
    queue_manager: Arc<QueueManager>,
    redis_client: RedisClient,
) -> Result<()> {
    let languages = vec![
        ("python".to_string(), "3.9".to_string()),
        // MODIFIED: Corrected the version to ensure the worker listens on the correct queue
        ("java".to_string(), "21".to_string()),
        ("java11".to_string(), "11".to_string()),
        ("c".to_string(), "11".to_string()),
        ("cpp".to_string(), "11".to_string()),
        ("javascript".to_string(), "18".to_string()),
    ];

    let mut worker_handles = vec![];

    for (language, version) in languages {
        let queue_manager_clone = queue_manager.clone();
        let redis_client_clone = redis_client.clone();
        let handle = tokio::spawn(async move {
            info!("Worker spawned for language: {}, version: {}", language, version);
            if let Err(e) = queue_manager_clone
                .start_worker(language.clone(), version.clone(), redis_client_clone)
                .await
            {
                error!(
                    "Worker for language: {}, version: {} failed: {}",
                    language, version, e
                );
            }
        });
        worker_handles.push(handle);
    }

    // Keep the worker process alive by waiting on all spawned tasks
    futures_util::future::join_all(worker_handles).await;

    Ok(())
}