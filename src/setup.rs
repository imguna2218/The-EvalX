use anyhow::Result;
use std::env;
use std::sync::Arc;
use tokio::sync::{broadcast, Semaphore};
use tracing::{error, info};

use crate::caching::redis_client::RedisClient;
use crate::queue_management::QueueManager;
use crate::types::index::{CodeExecutor, ExecutionNotification};

/// MODIFIED: This function is now much simpler.
/// It no longer needs to connect to Docker or read complex language configs.
/// It only initializes the shared state, including the new lean CodeExecutor.
pub async fn initialize_executor() -> Result<(
    Arc<CodeExecutor>,
    Arc<broadcast::Sender<ExecutionNotification>>,
    Arc<QueueManager>,
)> {
    dotenv::dotenv().ok();

    // This variable now controls the number of concurrent Isolate sandboxes, not Docker containers.
    let max_sandboxes = env::var("MAX_CONCURRENT_SANDBOXES")
        .unwrap_or_else(|_| "500".to_string())
        .parse::<usize>()?;
    info!("Max concurrent Isolate sandboxes: {}", max_sandboxes);

    let redis_client = RedisClient::new()?;

    // The new executor only needs a semaphore to control concurrency.
    let executor = Arc::new(CodeExecutor {
        semaphore: Arc::new(Semaphore::new(max_sandboxes)),
    });

    let (tx, _) = broadcast::channel::<ExecutionNotification>(1024);
    let tx = Arc::new(tx);
    let queue_manager = Arc::new(QueueManager::new(
        executor.clone(),
        redis_client.clone(),
        tx.clone(),
    ));

    Ok((executor, tx, queue_manager))
}

/// Starts the background workers that process tasks from the queue.
pub async fn start_workers(queue_manager: Arc<QueueManager>, redis_client: RedisClient) {
    // MODIFIED: The list of languages is now specific, matching the language/version
    // strings that the API will receive from clients. This ensures workers listen
    // on the correct queues (e.g., "queue:java11:11").
    let languages = vec![
        ("python".to_string(), "3.9".to_string()),
        ("java".to_string(), "21".to_string()),
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
            info!(
                "Worker spawned for language: {}, version: {}",
                language_clone, version_clone
            );
            if let Err(e) = queue_manager_clone
                .start_worker(language_clone.clone(), version_clone.clone(), redis_client_clone)
                .await
            {
                error!(
                    "Worker failed for language: {}, version: {}: {}",
                    language_clone, version_clone, e
                );
            }
        });
    }
}