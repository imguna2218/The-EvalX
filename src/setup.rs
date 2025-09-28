use anyhow::Result;
use bollard::Docker;
use std::env;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, error};

use crate::types::index::{CodeExecutor, ExecutionNotification};
// use crate::container_management::container_pool::init_container_pool; // No longer needed here
use crate::container_management::init::new_executor;
use crate::caching::redis_client::RedisClient;
use crate::queue_management::QueueManager;

// This function now ONLY sets up the shared state. It no longer starts workers.
pub async fn initialize_executor() -> Result<(Arc<CodeExecutor>, Arc<broadcast::Sender<ExecutionNotification>>, Arc<QueueManager>)> {
    dotenv::dotenv().ok();
    
    let max_containers = env::var("MAX_CONCURRENT_CONTAINERS")
        .unwrap_or_else(|_| "500".to_string())
        .parse::<usize>()?;
    info!("MAX_CONCURRENT_CONTAINERS: {}", max_containers);
        
    let _pool_size = env::var("CONTAINER_POOL_SIZE") // a worker does not need to know this
        .unwrap_or_else(|_| "50".to_string())
        .parse::<usize>()?;

    let redis_client = RedisClient::new()?;

    let docker = Docker::connect_with_local_defaults()?;
    let executor = Arc::new(new_executor(docker, max_containers));
    let (tx, _) = broadcast::channel::<ExecutionNotification>(1024);
    let tx = Arc::new(tx);
    let queue_manager = Arc::new(QueueManager::new(executor.clone(), redis_client.clone(), tx.clone()));

    // DELETED: The call to init_container_pool and the worker spawning loop have been removed.
    // info!("Pre-warming container pool...");
    // init_container_pool(&executor, languages, pool_size).await?;
    // info!("Container pool initialization skipped for workers.");

    Ok((executor, tx, queue_manager))
}

// This new function contains the logic to start workers, to be called by worker containers.
pub async fn start_workers(queue_manager: Arc<QueueManager>, redis_client: RedisClient) {
    let languages = vec![
        ("python".to_string(), "3.9".to_string()),
        ("pypy".to_string(), "3.9".to_string()),
        ("java".to_string(), "java-slim-executor".to_string()),
        ("java11".to_string(), "11".to_string()),
        ("c".to_string(), "11".to_string()),
        ("cpp".to_string(), "11".to_string()),
        ("javascript".to_string(), "18".to_string()),
    ];

    for (language, version) in languages {
        let queue_manager_clone = queue_manager.clone();
        let language_clone = language.clone();
        let version_clone = version.clone();
        let redis_client_clone = redis_client.clone();

        tokio::spawn(async move {
            info!("Worker spawned for language: {}, version: {}", language_clone, version_clone);
            if let Err(e) = queue_manager_clone.start_worker(language_clone.clone(), version_clone.clone(), redis_client_clone).await {
                error!("Worker failed for language: {}, version: {}: {}", language_clone, version_clone, e);
            }
        });
    }
}