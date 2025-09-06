use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Semaphore, Mutex, Notify};
use bollard::Docker;
use crate::types::index::{CodeExecutor, LanguageConfig, ContainerResourceLimits};

pub fn new_executor(docker: Docker, max_containers: usize) -> CodeExecutor {
    let mut language_configs = HashMap::new();
    
    language_configs.insert("python".to_string(), LanguageConfig {
        image: "python:3.9".to_string(),
        command_format: vec!["python".to_string(), "-c".to_string(), "{}".to_string()],
        resource_limits: ContainerResourceLimits {
            memory: "192m".to_string(), // Reduced for better resource usage
            cpu_shares: 256, // Adjusted for balanced allocation
        },
        env: vec!["PYTHONUNBUFFERED=1".to_string()],
    });
    
    language_configs.insert("pypy".to_string(), LanguageConfig {
        image: "pypy:3.9".to_string(),
        command_format: vec!["pypy".to_string(), "-c".to_string(), "{}".to_string()],
        resource_limits: ContainerResourceLimits {
            memory: "192m".to_string(), // Reduced for better resource usage
            cpu_shares: 256, // Adjusted for balanced allocation
        },
        env: vec!["PYTHONUNBUFFERED=1".to_string()],
    });

    language_configs.insert("java".to_string(), LanguageConfig {
        image: "java-slim-executor".to_string(),
        command_format: vec![
            "sh".to_string(),
            "-c".to_string(),
            "javac Main.java && java -Xmx512m Main".to_string(),
        ],
        resource_limits: ContainerResourceLimits {
            memory: "512m".to_string(), // Reduced for better resource usage
            cpu_shares: 256, // Adjusted for balanced allocation
        },
        env: vec![],
    });

    language_configs.insert("java11".to_string(), LanguageConfig {
        image: "java11-slim-executor".to_string(),
        command_format: vec![
            "sh".to_string(),
            "-c".to_string(),
            "javac Main.java && java -Xmx512m Main".to_string(),
        ],
        resource_limits: ContainerResourceLimits {
            memory: "512m".to_string(), // Reduced for better resource usage
            cpu_shares: 256, // Adjusted for balanced allocation
        },
        env: vec![],
    });

    language_configs.insert("c".to_string(), LanguageConfig {
        image: "gcc-slim-executor".to_string(),
        command_format: vec![
            "sh".to_string(),
            "-c".to_string(),
            "gcc -o main main.c && ./main".to_string(),
        ],
        resource_limits: ContainerResourceLimits {
            memory: "384m".to_string(), // Reduced for better resource usage
            cpu_shares: 256, // Adjusted for balanced allocation
        },
        env: vec![],
    });

    language_configs.insert("cpp".to_string(), LanguageConfig {
        image: "gcc:11".to_string(),
        command_format: vec![
            "sh".to_string(),
            "-c".to_string(),
            "g++ -o main main.cpp && ./main".to_string(),
        ],
        resource_limits: ContainerResourceLimits {
            memory: "384m".to_string(), // Reduced for better resource usage
            cpu_shares: 256, // Adjusted for balanced allocation
        },
        env: vec![],
    });

    language_configs.insert("javascript".to_string(), LanguageConfig {
        image: "node:18".to_string(), // Must match the image you built
        command_format: vec!["node".to_string(), "main.js".to_string()],
        resource_limits: ContainerResourceLimits { 
            memory: "192m".to_string(), // Reduced for better resource usage
            cpu_shares: 256 // Adjusted for balanced allocation
        },
        env: vec![],
    });

    CodeExecutor { 
        docker, 
        semaphore: Arc::new(Semaphore::new(max_containers.min(5))),
        container_pool: Arc::new(Mutex::new(HashMap::new())),
        language_configs,
        task_queue: Arc::new(Mutex::new(Vec::new())),
        task_notify: Arc::new(Notify::new()),
    }
}