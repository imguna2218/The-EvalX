use anyhow::Result;
use bollard::container::{Config, CreateContainerOptions, RemoveContainerOptions, StartContainerOptions};
use std::collections::HashMap;
use tokio::sync::Mutex;
use tracing::{info, debug};
use crate::types::index::CodeExecutor;

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
            return Ok(Some(container_id));
        }
    }
    
    Ok(None)
}

pub async fn return_container(executor: &CodeExecutor, language: &str, version: &str, container_id: String) {
    let key = format!("{}:{}", language, version);
    let mut pool = executor.container_pool.lock().await;
    
    if let Some(containers) = pool.get_mut(&key) {
        containers.push(container_id.clone());
        info!("Returned container {} to pool for {}:{}", container_id, language, version);
    } else {
        let mut containers = Vec::new();
        containers.push(container_id.clone());
        pool.insert(key, containers);
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
