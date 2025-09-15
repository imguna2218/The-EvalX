use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Semaphore, Mutex, Notify};
use bollard::Docker;
use crate::types::index::{CodeExecutor, LanguageConfig, ContainerResourceLimits};

pub fn new_executor(docker: Docker, max_containers: usize) -> CodeExecutor {
    let mut language_configs = HashMap::new();
    
    // FIXED: The command now executes the script file instead of a string.
    language_configs.insert("python".to_string(), LanguageConfig {
        image: "python:3.9".to_string(),
        command_format: vec!["python".to_string(), "/app/main.py".to_string()],
        resource_limits: ContainerResourceLimits {
            memory: "192m".to_string(),
            cpu_shares: 256,
        },
        env: vec!["PYTHONUNBUFFERED=1".to_string()],
    });
    
    // FIXED: The command now executes the script file instead of a string.
    language_configs.insert("pypy".to_string(), LanguageConfig {
        image: "pypy:3.9".to_string(),
        command_format: vec!["pypy".to_string(), "/app/main.py".to_string()],
        resource_limits: ContainerResourceLimits {
            memory: "192m".to_string(),
            cpu_shares: 256,
        },
        env: vec!["PYTHONUNBUFFERED=1".to_string()],
    });

    language_configs.insert("java".to_string(), LanguageConfig {
        image: "java-slim-executor".to_string(),
        command_format: vec![
            "java".to_string(),
            "-Xmx1024m".to_string(),
            "-Xms512m".to_string(),
            "Main".to_string(),
        ],
        resource_limits: ContainerResourceLimits {
            memory: "1024m".to_string(),
            cpu_shares: 256,
        },
        env: vec![],
    });

    language_configs.insert("java11".to_string(), LanguageConfig {
        image: "java11-slim-executor".to_string(),
        command_format: vec![
            "java".to_string(),
            "-Xmx1024m".to_string(),
            "-Xms512m".to_string(),
            "Main".to_string(),
        ],
        resource_limits: ContainerResourceLimits {
            memory: "1024m".to_string(),
            cpu_shares: 256,
        },
        env: vec![],
    });

    language_configs.insert("c".to_string(), LanguageConfig {
        image: "gcc-slim-executor".to_string(),
        command_format: vec!["./main".to_string()],
        resource_limits: ContainerResourceLimits {
            memory: "384m".to_string(),
            cpu_shares: 256,
        },
        env: vec![],
    });

    language_configs.insert("cpp".to_string(), LanguageConfig {
        image: "gcc:11".to_string(),
        command_format: vec!["./main".to_string()],
        resource_limits: ContainerResourceLimits {
            memory: "384m".to_string(),
            cpu_shares: 256,
        },
        env: vec![],
    });

    language_configs.insert("javascript".to_string(), LanguageConfig {
        image: "node:18".to_string(),
        command_format: vec!["node".to_string(), "main.js".to_string()],
        resource_limits: ContainerResourceLimits { 
            memory: "192m".to_string(),
            cpu_shares: 256
        },
        env: vec![],
    });

    CodeExecutor { 
        docker, 
        semaphore: Arc::new(Semaphore::new(max_containers.min(500))),
        container_pool: Arc::new(Mutex::new(HashMap::new())),
        language_configs,
        task_queue: Arc::new(Mutex::new(Vec::new())),
        task_notify: Arc::new(Notify::new()),
    }
}
