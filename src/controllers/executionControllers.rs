use axum::{
    extract::{State, Json},
};
use crate::types::index::{CodeExecutor, ExecutionNotification};
use std::sync::Arc;
use uuid::Uuid;
use std::env;
use tracing::{debug, info, warn};
use tokio::sync::broadcast;
use crate::queue_management::{QueueManager, ExecutionType, ExecutionTask};
use crate::models::request::ExecutionRequest;
use crate::models::response::EvaluationResult;
use crate::AppResponse;
use crate::caching::redis_client::RedisClient;
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

    let results = match queue_manager.add_task(requests.clone(), ExecutionType::Parallel, 1, redis_client).await {
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


pub async fn handle_execute_batch(
    State((_executor, tx, redis_client, queue_manager)): State<(Arc<CodeExecutor>, Arc<broadcast::Sender<ExecutionNotification>>, RedisClient, Arc<QueueManager>)>,
    Json(mut requests): Json<Vec<ExecutionRequest>>,
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

    // Use the first request as template
    let first_request = requests[0].clone();
    let base_language = first_request.language.trim().to_string();
    let base_version = first_request.version.trim().to_string();
    let base_code = first_request.code.trim().to_string();

    // Validate the first request
    if base_language.is_empty() {
        return AppResponse(Err(anyhow::anyhow!(
            "The first request in a batch must contain a non-empty language"
        )));
    }
    if base_version.is_empty() {
        return AppResponse(Err(anyhow::anyhow!(
            "The first request in a batch must contain a non-empty version"
        )));
    }
    if base_code.is_empty() {
        return AppResponse(Err(anyhow::anyhow!(
            "The first request in a batch must contain non-empty code"
        )));
    }

    // Enforce that all subsequent requests use the same language, version, code, timeout (stdin can differ)
    for (i, req) in requests.iter_mut().enumerate().skip(1) {
        if !req.language.trim().is_empty() && req.language.trim() != base_language {
            warn!("Overriding language in request {} to match first request: '{}'", i + 1, base_language);
            req.language = base_language.clone();
        }
        if !req.version.trim().is_empty() && req.version.trim() != base_version {
            warn!("Overriding version in request {} to match first request: '{}'", i + 1, base_version);
            req.version = base_version.clone();
        }
        if !req.code.trim().is_empty() && req.code.trim() != base_code {
            warn!("Overriding code in request {} to match first request", i + 1);
            req.code = base_code.clone();
        }
        if req.timeout.is_some() && req.timeout != first_request.timeout {
            warn!("Overriding timeout in request {} to match first request", i + 1);
            req.timeout = first_request.timeout;
        }
        if req.memory_limit.is_some() && req.memory_limit != first_request.memory_limit {
            warn!("Overriding memory_limit in request {} to match first request", i + 1);
            req.memory_limit = first_request.memory_limit.clone();
        }
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