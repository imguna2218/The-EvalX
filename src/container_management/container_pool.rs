use anyhow::Result;
use bollard::container::{Config, CreateContainerOptions, RemoveContainerOptions, StartContainerOptions};
use std::collections::HashMap;
use tokio::sync::Mutex;
use tracing::{info, debug, warn};
use crate::types::index::CodeExecutor;
use std::sync::Arc;
use tokio::task;

// Add these constants for dynamic scaling
const DEFAULT_POOL_SCALE_UP_THRESHOLD: u8 = 90;
const DEFAULT_POOL_SCALE_DOWN_THRESHOLD: u8 = 10;
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
        pool.insert(key, containers);
        info!("Initialized pool for {}:{} with {} containers", language, version, count_per_language);
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
            Some(CreateContainerOptions {
                name: "",
                platform: None,
            }),
            config,
        )
        .await?;
        
    executor.docker.start_container(
        &container.id,
        None::<StartContainerOptions<String>>,
    ).await?;
    
    Ok(container.id)
}

pub async fn get_container(executor: &CodeExecutor, language: &str, version: &str) -> Result<Option<String>> {
    let key = format!("{}:{}", language, version);
    let mut pool = executor.container_pool.lock().await;
    
    if let Some(containers) = pool.get_mut(&key) {
        if let Some(container_id) = containers.pop() {
            info!("Reusing container {} for {}:{}", container_id, language, version);
            
            // Check if we need to scale up the pool
            let current_pool_size = containers.len();
            let utilization = calculate_pool_utilization(current_pool_size);
            if utilization > DEFAULT_POOL_SCALE_UP_THRESHOLD {
                let executor_clone = executor.clone();
                let language_clone = language.to_string();
                let version_clone = version.to_string();
                
                // Spawn background task to scale up pool
                task::spawn(async move {
                    if let Err(e) = scale_up_pool(&executor_clone, &language_clone, &version_clone, DEFAULT_POOL_SCALE_FACTOR).await {
                        warn!("Failed to scale up pool for {}:{}: {}", language_clone, version_clone, e);
                    }
                });
            }
            
            return Ok(Some(container_id));
        }
    }
    
    Ok(None)
}

pub async fn return_container(executor: &CodeExecutor, language: &str, version: &str, container_id: &str) {
    let pool_key = format!("{}:{}", language, version);
    let mut pool = executor.container_pool.lock().await;
    if let Some(containers) = pool.get_mut(&pool_key) {
        containers.push(container_id.to_string()); // Clone here for HashMap
        info!("Returned container {} to pool for {}:{}", container_id, language, version);
    } else {
        let mut containers = Vec::new();
        containers.push(container_id.to_string()); // Clone here for HashMap
        pool.insert(pool_key, containers);
        info!("Created new pool for {}:{} with container {}", language, version, container_id);
    }
    executor.task_notify.notify_one(); // Notify queue when container is returned
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

pub async fn available_containers(executor: &CodeExecutor, language: &str, version: &str) -> Result<usize> {
    let key = format!("{}:{}", language, version);
    let pool = executor.container_pool.lock().await;
    Ok(pool.get(&key).map_or(0, |containers| containers.len()))
}

// Helper function to calculate pool utilization percentage based on current size
fn calculate_pool_utilization(current_size: usize) -> u8 {
    if current_size > 0 {
        // For simplicity, we assume 50% utilization when we can't track exact usage
        // In a real implementation, you'd track actual usage statistics
        // This is a placeholder that will trigger scaling logic
        return 50;
    }
    0
}

// Scale up the pool by adding more containers
async fn scale_up_pool(executor: &CodeExecutor, language: &str, version: &str, count: usize) -> Result<()> {
    info!("Scaling up pool for {}:{} by adding {} containers", language, version, count);
    
    for _ in 0..count {
        if let Ok(container_id) = create_idle_container(executor, language, version).await {
            let key = format!("{}:{}", language, version);
            let mut pool = executor.container_pool.lock().await;
            
            if let Some(containers) = pool.get_mut(&key) {
                containers.push(container_id);
            } else {
                let mut containers = Vec::new();
                containers.push(container_id);
                pool.insert(key, containers);
            }
        }
    }
    
    info!("Successfully scaled up pool for {}:{}", language, version);
    Ok(())
}

// Scale down the pool by removing excess containers
async fn scale_down_pool(executor: &CodeExecutor, language: &str, version: &str, count: usize) -> Result<()> {
    info!("Scaling down pool for {}:{} by removing {} containers", language, version, count);
    
    let key = format!("{}:{}", language, version);
    let mut pool = executor.container_pool.lock().await;
    
    if let Some(containers) = pool.get_mut(&key) {
        for _ in 0..count.min(containers.len()) {
            if let Some(container_id) = containers.pop() {
                let executor_clone = executor.clone();
                let container_id_clone = container_id.clone();
                
                // Spawn background task to remove container
                task::spawn(async move {
                    if let Err(e) = remove_container(&executor_clone, &container_id_clone).await {
                        warn!("Failed to remove container {} during scale down: {}", container_id_clone, e);
                    }
                });
            }
        }
    }
    
    info!("Successfully scaled down pool for {}:{}", language, version);
    Ok(())
}

// Get pool statistics for monitoring
pub async fn get_pool_stats(executor: &CodeExecutor) -> HashMap<String, usize> {
    let pool = executor.container_pool.lock().await;
    let mut stats = HashMap::new();
    
    for (key, containers) in pool.iter() {
        stats.insert(key.clone(), containers.len());
    }
    
    stats
}