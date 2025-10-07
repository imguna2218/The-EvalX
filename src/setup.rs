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
// ADDED: Import the new LanguageRegistry
use crate::languages::manager::LanguageRegistry;

pub async fn initialize_executor() -> Result<(
    Arc<CodeExecutor>,
    Arc<broadcast::Sender<ExecutionNotification>>,
    Arc<QueueManager>,
)> {
    dotenv::dotenv().ok();
    
    // ADDED: Initialize the language registry from config files at startup.
    let language_registry = Arc::new(LanguageRegistry::new()?);

    let max_sandboxes = env::var("MAX_CONCURRENT_SANDBOXES")
        .unwrap_or_else(|_| "500".to_string())
        .parse::<usize>()?;
    info!("Max concurrent Isolate sandboxes: {}", max_sandboxes);

    let redis_client = RedisClient::new().await?;
    
    let executor = Arc::new(CodeExecutor {
        semaphore: Arc::new(Semaphore::new(max_sandboxes)),
        last_java_warmup: Arc::new(RwLock::new(Instant::now())),
        system: Arc::new(Mutex::new(System::new_all())),
        redis_client: redis_client.clone(),
        concurrency_state: Arc::new(RwLock::new((
            ConcurrencyState::Nominal,
            Instant::now(),
        ))),
        // ADDED: Pass the initialized registry to the executor.
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

/// MODIFIED: This function now dynamically starts workers based on loaded configurations.
pub async fn start_workers(
    queue_manager: Arc<QueueManager>,
    redis_client: RedisClient,
    language_registry: Arc<LanguageRegistry> // ADDED: Pass the registry to the workers setup.
) {
    // MODIFIED: The list of languages is now retrieved dynamically from the registry.
    let languages = language_registry.list_all();
    
    if languages.is_empty() {
        error!("No language configurations were loaded. No workers will be started.");
        return;
    }

    for lang_config in languages {
        let queue_manager_clone = queue_manager.clone();
        // MODIFIED: Get language and version from the config struct.
        let language_clone = lang_config.name.clone();
        let version_clone = lang_config.version.clone();
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