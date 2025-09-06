use anyhow::Result;
use bollard::Docker;
use std::env;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::info;

use crate::types::index::{CodeExecutor, ExecutionNotification};
use crate::container_management::container_pool::init_container_pool;
use crate::container_management::init::new_executor;
use crate::caching::redis_client::RedisClient;
use crate::queue_management::QueueManager;

pub async fn initialize_executor() -> Result<(Arc<CodeExecutor>, Arc<broadcast::Sender<ExecutionNotification>>, Arc<QueueManager>)> {
    dotenv::dotenv().ok();
    
    let max_containers = env::var("MAX_CONCURRENT_CONTAINERS")
        .unwrap_or_else(|_| "10".to_string())
        .parse::<usize>()?;
    info!("MAX_CONCURRENT_CONTAINERS: {}", max_containers);
        
    let pool_size = env::var("CONTAINER_POOL_SIZE")
        .unwrap_or_else(|_| "5".to_string())
        .parse::<usize>()?;
    info!("CONTAINER_POOL_SIZE: {}", pool_size);

    let docker = Docker::connect_with_local_defaults()?;
    let executor = Arc::new(new_executor(docker, max_containers));
    let (tx, _) = broadcast::channel::<ExecutionNotification>(1024);
    let tx = Arc::new(tx);
    let queue_manager = Arc::new(QueueManager::new(executor.clone()));

    start_task_processor(executor.clone(), tx.clone(), queue_manager.clone());
    
    let languages = vec![
        ("python".to_string(), "3.9".to_string()),
        ("pypy".to_string(), "3.9".to_string()),
        ("java".to_string(), "java-slim-executor".to_string()),
        ("java11".to_string(), "11".to_string()),
        ("c".to_string(), "11".to_string()),
        ("cpp".to_string(), "11".to_string()),
        ("javascript".to_string(), "18".to_string()),
    ];
    
    info!("Pre-warming container pool...");
    init_container_pool(&executor, languages, pool_size).await?;
    info!("Container pool initialized");

    Ok((executor, tx, queue_manager))
}

fn start_task_processor(executor: Arc<CodeExecutor>, tx: Arc<broadcast::Sender<ExecutionNotification>>, queue_manager: Arc<QueueManager>) {
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
        let _executor_clone = executor.clone();
        let _tx_clone = tx.clone();
        let queue_manager_clone = queue_manager.clone();
        let language_clone = language.clone();
        let version_clone = version.clone();

        tokio::spawn(async move {
            let redis_client = RedisClient::new().unwrap();
            queue_manager_clone.start_worker(language_clone, version_clone, redis_client).await;
        });
    }
}
