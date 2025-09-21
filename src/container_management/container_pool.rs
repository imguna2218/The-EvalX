use anyhow::Result;
use bollard::container::{Config, CreateContainerOptions, RemoveContainerOptions, StartContainerOptions};
use std::collections::HashMap;
use tokio::task;
use tracing::{info, warn};

// NEW: Import the container usage metric
use crate::monitoring::metrics::CONTAINER_USAGE;
use crate::types::index::CodeExecutor;

// NOTE: Dynamic scaling constants are kept, but the primary fix is the waiting logic in get_container.
const DEFAULT_POOL_SCALE_UP_THRESHOLD: u8 = 90;
const DEFAULT_POOL_SCALE_FACTOR: usize = 5;

pub async fn init_container_pool(executor: &CodeExecutor, languages: Vec<(String, String)>, count_per_language: usize) -> Result<()> {
    for (language, version) in languages {
        let key = format!("{}:{}", language, version);
        let mut containers = Vec::with_capacity(count_per_language);
        
        for _ in 0..count_per_language {
            if let Ok(container_id) = create_idle_container(executor, &language, &version).await {
                containers.push(container_id);
            }
        }
        
        let mut pool = executor.container_pool.lock().await;
        pool.insert(key.clone(), containers);
        info!("Initialized pool for {} with {} containers", key, count_per_language);
        // Initialize the metric gauge to 0 for this language pool
        CONTAINER_USAGE.with_label_values(&[&key]).set(0);
    }
    
    Ok(())
}

pub async fn create_idle_container(executor: &CodeExecutor, language: &str, version: &str) -> Result<String> {
    let language_config = executor.language_configs.get(language).ok_or_else(|| {
        anyhow::anyhow!("Unsupported language: {}", language)
    })?;
    
    let config = Config {
        image: Some(language_config.image.clone()),
        tty: Some(true),
        attach_stdin: Some(true),
        attach_stdout: Some(true),
        attach_stderr: Some(true),
        host_config: Some(bollard::service::HostConfig {
            memory: Some(64 * 1024 * 1024),
            cpu_shares: Some(1024),
            ..Default::default()
        }),
        env: Some(language_config.env.clone()),
        entrypoint: Some(vec!["sleep".to_string(), "infinity".to_string()]),
        ..Default::default()
    };
    
    let container = executor.docker
        .create_container(
            None::<CreateContainerOptions<String>>,
            config,
        )
        .await?;
        
    executor.docker.start_container(
        &container.id,
        None::<StartContainerOptions<String>>,
    ).await?;
    
    Ok(container.id)
}

// MODIFIED: This function is now completely rewritten to wait for a container.
pub async fn get_container(executor: &CodeExecutor, language: &str, version: &str) -> Result<String> {
    let key = format!("{}:{}", language, version);

    loop {
        // 1. Acquire a lock on the container pool
        let mut pool = executor.container_pool.lock().await;

        // 2. Check if a container is available in the pool for the specified language
        if let Some(container_id) = pool.get_mut(&key).and_then(|containers| containers.pop()) {
            info!("Reusing container {} for {}", container_id, key);
            
            // Increment the in-use container metric
            CONTAINER_USAGE.with_label_values(&[&key]).inc();

            // Optional: You can add the dynamic scaling logic here if needed
            // For now, the waiting logic is the primary fix.
            
            // 3. If a container is found, return it and exit the loop
            return Ok(container_id);
        }

        // 4. If no container is available, prepare to wait.
        // We get a future that will complete when notify() is called.
        let notified = executor.task_notify.notified();
        
        // 5. IMPORTANT: We release the lock *before* waiting.
        // If we don't, no other task can ever return a container to the pool.
        drop(pool);
        
        // 6. Wait for a notification that a container has been returned to the pool.
        notified.await;

        // The loop will now repeat, and this task will try to acquire a container again.
    }
}

pub async fn return_container(executor: &CodeExecutor, language: &str, version: &str, container_id: &str) {
    let pool_key = format!("{}:{}", language, version);
    let mut pool = executor.container_pool.lock().await;
    
    if let Some(containers) = pool.get_mut(&pool_key) {
        containers.push(container_id.to_string());
    } else {
        // This case can happen if the pool was empty and is being refilled.
        pool.insert(pool_key.clone(), vec![container_id.to_string()]);
    }

    info!("Returned container {} to pool for {}", container_id, pool_key);
    
    // Decrement the in-use container metric
    CONTAINER_USAGE.with_label_values(&[&pool_key]).dec();

    // Notify ONE waiting task that a container is now available.
    executor.task_notify.notify_one();
}

pub async fn remove_container(executor: &CodeExecutor, container_id: &str) -> Result<()> {
    executor.docker
        .remove_container(
            container_id,
            Some(RemoveContainerOptions {
                force: true,
                ..Default::default()
            }),
        )
        .await?;
    info!("Removed failed container {}", container_id);
    Ok(())
}

// The rest of the functions remain the same as they are not part of the core issue.
pub async fn available_containers(executor: &CodeExecutor, language: &str, version: &str) -> Result<usize> {
    let key = format!("{}:{}", language, version);
    let pool = executor.container_pool.lock().await;
    Ok(pool.get(&key).map_or(0, |containers| containers.len()))
}

fn calculate_pool_utilization(current_size: usize) -> u8 {
    if current_size > 0 {
        return 50;
    }
    0
}

async fn scale_up_pool(executor: &CodeExecutor, language: &str, version: &str, count: usize) -> Result<()> {
    info!("Scaling up pool for {}:{} by adding {} containers", language, version, count);
    
    for _ in 0..count {
        if let Ok(container_id) = create_idle_container(executor, language, version).await {
            let key = format!("{}:{}", language, version);
            let mut pool = executor.container_pool.lock().await;
            
            if let Some(containers) = pool.get_mut(&key) {
                containers.push(container_id);
            } else {
                pool.insert(key, vec![container_id]);
            }
        }
    }
    
    info!("Successfully scaled up pool for {}:{}", language, version);
    Ok(())
}

pub async fn get_pool_stats(executor: &CodeExecutor) -> HashMap<String, usize> {
    let pool = executor.container_pool.lock().await;
    let mut stats = HashMap::new();
    
    for (key, containers) in pool.iter() {
        stats.insert(key.clone(), containers.len());
    }
    
    stats
}