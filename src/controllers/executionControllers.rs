use axum::{
    extract::{State, Json, Path},
};
use crate::types::index::{CodeExecutor, ExecutionNotification};
use std::sync::Arc;
use std::env;
use tracing::{debug, info, warn};
use tokio::sync::broadcast;
use crate::queue_management::{QueueManager, ExecutionType};
use crate::models::request::ExecutionRequest;
use crate::models::response::{EvaluationResult, SubmissionResponse, SubmissionStatus};
use crate::AppResponse;
use crate::caching::redis_client::RedisClient;
use sha2::{Sha256, Digest};
use hex::ToHex;
use anyhow::anyhow;

pub async fn handle_execute(
    State((_executor, tx, redis_client, queue_manager)): State<(Arc<CodeExecutor>, Arc<broadcast::Sender<ExecutionNotification>>, RedisClient, Arc<QueueManager>)>,
    Json(request): Json<ExecutionRequest>,
) -> AppResponse<SubmissionResponse> {
    if let Some(timeout) = request.timeout {
        if timeout < 10 {
            return AppResponse(Err(anyhow!(
                "Invalid timeout value: timeout cannot be less than 10"
            )));
        }
    }
    
    // Check cache first before queueing
    let language = request.language.as_ref().unwrap_or(&"".to_string()).to_string();
    let version = request.version.as_ref().unwrap_or(&"".to_string()).to_string();
    let code = request.code.as_ref().unwrap_or(&"".to_string()).to_string();
    let stdin = request.stdin.clone();
    let timeout = request.timeout.unwrap_or(10);

    let cache_key = format!(
        "evalx:exec:{}:{}:{}:{}:{}",
        language, version, timeout,
        Sha256::digest(&code).encode_hex::<String>(),
        Sha256::digest(&stdin).encode_hex::<String>()
    );
  
    if let Ok(Some(cached_result)) = redis_client.get_from_cache_async(&cache_key).await {
        debug!("Cache hit for key: {}", cache_key);
        return AppResponse(Ok(SubmissionResponse {
            token: "cached".to_string(), // No real token for a cached, synchronous-like response
            status: SubmissionStatus::Completed,
            result: Some(cached_result),
        }));
    }

    // If not cached, add to queue
    match queue_manager.add_task(vec![request.clone()], ExecutionType::Single, 1, redis_client).await {
        Ok(task_id) => { // CHANGED: Capture the returned task_id
            let _ = tx.send(ExecutionNotification {
                id: task_id.clone(),
                status: "queued".to_string(),
                result: None,
            });
            AppResponse(Ok(SubmissionResponse {
                token: task_id, // CHANGED: Use the correct task_id as the token
                status: SubmissionStatus::Queued,
                result: None,
            }))
        }
        Err(e) => AppResponse(Err(anyhow!("Failed to add task: {}", e))),
    }
}

pub async fn handle_execute_parallel(
    State((_executor, tx, redis_client, queue_manager)): State<(Arc<CodeExecutor>, Arc<broadcast::Sender<ExecutionNotification>>, RedisClient, Arc<QueueManager>)>,
    Json(requests): Json<Vec<ExecutionRequest>>,
) -> AppResponse<SubmissionResponse> {
    // This function can be deprecated in favor of execute_batch, but we'll fix it too.
    if requests.is_empty() {
        return AppResponse(Err(anyhow!("Request list cannot be empty")));
    }
    
    let max_requests = env::var("MAX_REQUESTS").unwrap_or_else(|_| "100".to_string()).parse::<usize>().unwrap();
    if requests.len() > max_requests {
        return AppResponse(Err(anyhow!("Max requests exceeded. Max allowed requests: {}", max_requests)));
    }
    
    match queue_manager.add_task(requests.clone(), ExecutionType::Parallel, 1, redis_client).await {
        Ok(task_id) => { // CHANGED: Capture the returned task_id
            debug!("Queuing parallel task with ID: {}", task_id);
            let _ = tx.send(ExecutionNotification {
                id: task_id.clone(),
                status: "queued".to_string(),
                result: None,
            });
            AppResponse(Ok(SubmissionResponse {
                token: task_id, // CHANGED: Use the correct task_id as the token
                status: SubmissionStatus::Queued,
                result: None,
            }))
        }
        Err(e) => AppResponse(Err(anyhow!("Failed to add task: {}", e))),
    }
}

pub async fn handle_execute_batch(
    State((_executor, tx, redis_client, queue_manager)): State<(Arc<CodeExecutor>, Arc<broadcast::Sender<ExecutionNotification>>, RedisClient, Arc<QueueManager>)>,
    Json(requests): Json<Vec<ExecutionRequest>>,
) -> AppResponse<SubmissionResponse> {
    if requests.is_empty() {
        return AppResponse(Err(anyhow!("Batch request list cannot be empty")));
    }

    // --- Validation logic remains the same ---
    let first_request = requests[0].clone();
    let base_language = first_request.language.as_ref().unwrap_or(&String::new()).trim().to_string();
    let base_version = first_request.version.as_ref().unwrap_or(&String::new()).trim().to_string();
    let base_code = first_request.code.clone().unwrap_or_default();
    if base_language.is_empty() || base_version.is_empty() || base_code.is_empty() {
        return AppResponse(Err(anyhow!("The first request in a batch must contain language, version, and code")));
    }
    let mut normalized_requests = Vec::new();
    for (index, mut req) in requests.into_iter().enumerate() {
        req.language = Some(req.language.filter(|s| !s.trim().is_empty()).unwrap_or_else(|| base_language.clone()));
        req.version = Some(req.version.filter(|s| !s.trim().is_empty()).unwrap_or_else(|| base_version.clone()));
        if req.code.as_ref().map_or(true, |s| s.trim().is_empty()) {
            req.code = Some(base_code.clone());
            warn!("Propagated code to incomplete request at index {}", index);
        }
        req.timeout = req.timeout.or(first_request.timeout);
        normalized_requests.push(req);
    }
    // --- End Validation Logic ---
    
    let max_requests = env::var("MAX_REQUESTS").unwrap_or_else(|_| "100".to_string()).parse::<usize>().unwrap();
    if normalized_requests.len() > max_requests {
        return AppResponse(Err(anyhow!("Max requests exceeded. Max allowed requests: {}", max_requests)));
    }
    
    match queue_manager.add_task(normalized_requests.clone(), ExecutionType::Batch, 1, redis_client).await {
        Ok(task_id) => { // CHANGED: Capture the returned task_id
            info!("Validated and queued batch task with ID: {}", task_id);
            let _ = tx.send(ExecutionNotification {
                id: task_id.clone(),
                status: "queued".to_string(),
                result: None,
            });
            AppResponse(Ok(SubmissionResponse {
                token: task_id, // CHANGED: Use the correct task_id as the token
                status: SubmissionStatus::Queued,
                result: None,
            }))
        }
        Err(e) => AppResponse(Err(anyhow!("Failed to add task: {}", e))),
    }
}

pub async fn handle_submission_status(
    State((_executor, _tx, redis_client, _queue_manager)): State<(Arc<CodeExecutor>, Arc<broadcast::Sender<ExecutionNotification>>, RedisClient, Arc<QueueManager>)>,
    Path(token): Path<String>,
) -> AppResponse<SubmissionResponse> {
    let result_key = format!("result:{}", token);
    let status_key = format!("status:{}", token);
    
    // CHANGED: Use async redis calls
    match redis_client.get_from_cache_async::<Vec<EvaluationResult>>(&result_key).await {
        Ok(Some(results)) => {
            debug!("Found completed results for token: {}", token);
            AppResponse(Ok(SubmissionResponse {
                token,
                status: SubmissionStatus::Completed,
                result: results.into_iter().next(), // Return the first result for single-result view
            }))
        }
        Ok(None) => {
            // CHANGED: Use async redis calls
            match redis_client.get_from_cache_async::<String>(&status_key).await {
                Ok(Some(status)) if status == "processing" => {
                    debug!("Task is processing for token: {}", token);
                    AppResponse(Ok(SubmissionResponse {
                        token,
                        status: SubmissionStatus::Processing,
                        result: None,
                    }))
                }
                _ => {
                    debug!("No results yet for token: {}", token);
                    AppResponse(Ok(SubmissionResponse {
                        token,
                        status: SubmissionStatus::Queued,
                        result: None,
                    }))
                }
            }
        }
        Err(e) => AppResponse(Err(anyhow!("Failed to check submission status: {}", e))),
    }
}