use axum::{
    extract::{State, Json},
};
use std::sync::Arc;
use uuid::Uuid;
use std::env;
use tracing::debug;
use tokio::sync::broadcast;
use crate::types::index::{CodeExecutor, ExecutionNotification, ExecutionTask, ExecutionTaskType};
use crate::models::request::ExecutionRequest;
use crate::models::response::EvaluationResult;
use crate::AppResponse;
use crate::caching::redis_client::RedisClient;
use crate::queue_management::{QueueManager, ExecutionType};
use sha2::{Sha256, Digest};
use hex::ToHex;

pub async fn handle_execute(
    State((_executor, tx, mut redis_client, queue_manager)): State<(Arc<CodeExecutor>, Arc<broadcast::Sender<ExecutionNotification>>, RedisClient, Arc<QueueManager>)>,
    Json(request): Json<ExecutionRequest>,
) -> AppResponse<EvaluationResult> {
    if let Some(timeout) = request.timeout {
        if timeout < 0.0 {
            return AppResponse(Err(anyhow::anyhow!(
                "Invalid timeout value: timeout cannot be negative"
            )));
        }
    }

    let execution_id = Uuid::new_v4().to_string();
    
    let _ = tx.send(ExecutionNotification {
        id: execution_id.clone(),
        status: "queued".to_string(),
        result: None,
    });
    
    let cache_key = format!(
        "evalx:exec:{}:{}:{}:{}:{}",
        request.language,
        request.version,
        request.timeout.unwrap_or(1.0),
        Sha256::digest(&request.code).encode_hex::<String>(),
        request.stdin.as_ref().map_or("".to_string(), |s| Sha256::digest(s.as_bytes()).encode_hex::<String>())
    );
  
    if let Ok(Some(cached_result)) = redis_client.get_from_cache(&cache_key) {
        debug!("Cache hit for key: {}", cache_key);
        return AppResponse(Ok(cached_result));
    }

    let results = match queue_manager.add_task(vec![request], ExecutionType::Single, 1, redis_client).await {
        Ok(results) => results,
        Err(e) => return AppResponse(Err(anyhow::anyhow!("Failed to add task: {}", e))),
    };
    
    if !results.is_empty() {
        // Immediate execution occurred
        return AppResponse(Ok(results[0].clone()));
    }

    let mut rx = tx.subscribe();
    loop {
        if let Ok(notification) = rx.recv().await {
            if notification.id == execution_id {
                match notification.status.as_str() {
                    "completed" => {
                        if let Some(result) = notification.result {
                            return AppResponse(Ok(result));
                        }
                    }
                    "failed" => {
                        if let Some(result) = notification.result {
                            return AppResponse(Ok(result));
                        }
                        return AppResponse(Err(anyhow::anyhow!("Execution failed without result")));
                    }
                    _ => continue,
                }
            }
        }
    }
}

pub async fn handle_execute_parallel(
    State((_executor, tx, redis_client, _queue_manager)): State<(Arc<CodeExecutor>, Arc<broadcast::Sender<ExecutionNotification>>, RedisClient, Arc<QueueManager>)>,
    Json(requests): Json<Vec<ExecutionRequest>>,
) -> AppResponse<Vec<EvaluationResult>> {
    // Validate all requests
    for request in &requests {
        if let Some(timeout) = request.timeout {
            if timeout < 0.0 {
                return AppResponse(Err(anyhow::anyhow!(
                    "Invalid timeout value: timeout cannot be negative"
                )));
            }
        }
    }

    let execution_id = Uuid::new_v4().to_string();
    
    let max_requests = env::var("MAX_REQUESTS").unwrap_or_else(|_| "100".to_string()).parse::<usize>().unwrap();
    if requests.len() > max_requests {
        return AppResponse(Err(anyhow::anyhow!("Max requests exceeded. Max allowed requests: {}", max_requests)));
    }
    
    debug!("Queuing parallel task with ID: {}", execution_id);
    let _ = tx.send(ExecutionNotification {
        id: execution_id.clone(),
        status: "queued".to_string(),
        result: None,
    });

    let task = ExecutionTask {
        id: execution_id.clone(),
        requests: requests.clone(),
        task_type: ExecutionTaskType::Parallel,
        notification_tx: tx.clone(),
    };

    {
        let mut queue = _executor.task_queue.lock().await;
        queue.push(task);
        _executor.task_notify.notify_one();
    }

    let mut rx = tx.subscribe();
    let mut results = Vec::new();
    loop {
        if let Ok(notification) = rx.recv().await {
            debug!("Received notification for ID {}: {:?}", execution_id, notification);
            if notification.id == execution_id {
                match notification.status.as_str() {
                    "completed" => {
                        if let Some(result) = notification.result {
                            results.push(result);
                            if results.len() == requests.len() {
                                debug!("All results collected for ID {}: {:?}", execution_id, results);
                                return AppResponse(Ok(results));
                            }
                        }
                    }
                    "failed" => {
                        debug!("Execution failed for ID {}", execution_id);
                        return AppResponse(Err(anyhow::anyhow!("Execution failed")));
                    }
                    _ => continue,
                }
            }
        }
    }
}

pub async fn handle_execute_batch(
    State((_executor, tx, redis_client, queue_manager)): State<(Arc<CodeExecutor>, Arc<broadcast::Sender<ExecutionNotification>>, RedisClient, Arc<QueueManager>)>,
    Json(requests): Json<Vec<ExecutionRequest>>,
) -> AppResponse<Vec<EvaluationResult>> {
    // Validate all requests
    for request in &requests {
        if let Some(timeout) = request.timeout {
            if timeout < 0.0 {
                return AppResponse(Err(anyhow::anyhow!(
                    "Invalid timeout value: timeout cannot be negative"
                )));
            }
        }
    }

    // Check if the batch is empty
    if requests.is_empty() {
        return AppResponse(Err(anyhow::anyhow!("Batch request list cannot be empty")));
    }

    // Validate the first request
    let first_request = &requests[0];
    if first_request.language.trim().is_empty() {
        return AppResponse(Err(anyhow::anyhow!(
            "The first request in a batch must contain a non-empty language"
        )));
    }
    if first_request.version.trim().is_empty() {
        return AppResponse(Err(anyhow::anyhow!(
            "The first request in a batch must contain a non-empty version"
        )));
    }

    // For all supported languages, ensure the first request has non-empty code
    let is_supported_language = matches!(
        first_request.language.as_str(),
        "java" | "java11" | "c" | "cpp" | "python" | "javascript"
    );
    if is_supported_language && first_request.code.trim().is_empty() {
        return AppResponse(Err(anyhow::anyhow!(
            "The first request in a batch for language '{}' must contain non-empty code",
            first_request.language
        )));
    }

    let execution_id = Uuid::new_v4().to_string();
    
    let max_requests = env::var("MAX_REQUESTS")
        .unwrap_or_else(|_| "100".to_string())
        .parse::<usize>()
        .unwrap();
    if requests.len() > max_requests {
        return AppResponse(Err(anyhow::anyhow!(
            "Max requests exceeded. Max allowed requests: {}",
            max_requests
        )));
    }
    
    debug!("Queuing batch task with ID: {}", execution_id);
    let _ = tx.send(ExecutionNotification {
        id: execution_id.clone(),
        status: "queued".to_string(),
        result: None,
    });

    let results = match queue_manager.add_task(requests.clone(), ExecutionType::Batch, 1, redis_client).await {
        Ok(results) => results,
        Err(e) => return AppResponse(Err(anyhow::anyhow!("Failed to add task: {}", e))),
    };
    
    if !results.is_empty() {
        // Immediate execution occurred
        return AppResponse(Ok(results));
    }

    let mut rx = tx.subscribe();
    let mut results = Vec::new();
    loop {
        if let Ok(notification) = rx.recv().await {
            debug!("Received notification for ID {}: {:?}", execution_id, notification);
            if notification.id == execution_id {
                match notification.status.as_str() {
                    "completed" => {
                        if let Some(result) = notification.result {
                            results.push(result);
                            if results.len() == requests.len() {
                                debug!("All results collected for ID {}: {:?}", execution_id, results);
                                return AppResponse(Ok(results));
                            }
                        }
                    }
                    "failed" => {
                        debug!("Execution failed for ID {}", execution_id);
                        return AppResponse(Err(anyhow::anyhow!("Execution failed")));
                    }
                    _ => continue,
                }
            }
        }
    }
}
