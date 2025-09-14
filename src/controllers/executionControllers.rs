use axum::{
    extract::{State, Json, Path},
};
use crate::types::index::{CodeExecutor, ExecutionNotification};
use std::sync::Arc;
use uuid::Uuid;
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
    State((_executor, tx, mut redis_client, queue_manager)): State<(Arc<CodeExecutor>, Arc<broadcast::Sender<ExecutionNotification>>, RedisClient, Arc<QueueManager>)>,
    Json(request): Json<ExecutionRequest>,
) -> AppResponse<SubmissionResponse> {
    if let Some(timeout) = request.timeout {
        if timeout < 10 {
            return AppResponse(Err(anyhow!(
                "Invalid timeout value: timeout cannot be less than 10"
            )));
        }
    }

    let execution_id = Uuid::new_v4().to_string();
    
    let language = request.language.as_ref().unwrap_or(&"".to_string()).to_string();
    let version = request.version.as_ref().unwrap_or(&"".to_string()).to_string();
    let code = request.code.as_ref().unwrap_or(&"".to_string()).to_string();
    let stdin = request.stdin.clone();
    let timeout = request.timeout.unwrap_or(10);

    let cache_key = format!(
        "evalx:exec:{}:{}:{}:{}:{}",
        language,
        version,
        timeout,
        Sha256::digest(&code).encode_hex::<String>(),
        Sha256::digest(&stdin).encode_hex::<String>()
    );
  
    if let Ok(Some(cached_result)) = redis_client.get_from_cache(&cache_key) {
        debug!("Cache hit for key: {}", cache_key);
        return AppResponse(Ok(SubmissionResponse {
            token: execution_id,
            status: SubmissionStatus::Completed,
            result: Some(cached_result),
        }));
    }

    match queue_manager.add_task(vec![request.clone()], ExecutionType::Single, 1, redis_client).await {
        Ok(_) => {
            let _ = tx.send(ExecutionNotification {
                id: execution_id.clone(),
                status: "queued".to_string(),
                result: None,
            });
            AppResponse(Ok(SubmissionResponse {
                token: execution_id,
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
    for request in &requests {
        if let Some(timeout) = request.timeout {
            if timeout < 10 {
                return AppResponse(Err(anyhow!(
                    "Invalid timeout value: timeout cannot be less than 10"
                )));
            }
        }
    }

    let execution_id = Uuid::new_v4().to_string();
    
    let max_requests = env::var("MAX_REQUESTS").unwrap_or_else(|_| "100".to_string()).parse::<usize>().unwrap();
    if requests.len() > max_requests {
        return AppResponse(Err(anyhow!("Max requests exceeded. Max allowed requests: {}", max_requests)));
    }
    
    debug!("Queuing parallel task with ID: {}", execution_id);
    
    match queue_manager.add_task(requests.clone(), ExecutionType::Parallel, 1, redis_client).await {
        Ok(_) => {
            let _ = tx.send(ExecutionNotification {
                id: execution_id.clone(),
                status: "queued".to_string(),
                result: None,
            });
            AppResponse(Ok(SubmissionResponse {
                token: execution_id,
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

    let first_request = requests[0].clone();
    let base_language = first_request.language.as_ref().unwrap_or(&String::new()).trim().to_string();
    let base_version = first_request.version.as_ref().unwrap_or(&String::new()).trim().to_string();
    let base_code = first_request.code.clone().unwrap_or_default();

    if base_language.is_empty() {
        return AppResponse(Err(anyhow!(
            "The first request in a batch must contain a non-empty language"
        )));
    }
    if base_version.is_empty() {
        return AppResponse(Err(anyhow!(
            "The first request in a batch must contain a non-empty version"
        )));
    }
    if base_code.is_empty() {
        return AppResponse(Err(anyhow!(
            "The first request in a batch must contain non-empty code"
        )));
    }

    let mut normalized_requests = Vec::new();
    
    for (index, mut req) in requests.into_iter().enumerate() {
        if req.language.as_ref().map_or(true, |s| s.trim().is_empty()) {
            req.language = Some(base_language.clone());
        }
        
        if req.version.as_ref().map_or(true, |s| s.trim().is_empty()) {
            req.version = Some(base_version.clone());
        }
        
        if req.code.as_ref().map_or(true, |s| s.trim().is_empty()) {
            req.code = Some(base_code.clone());
            warn!("Propagated code to incomplete request at index {}", index);
        }
        
        if req.timeout.is_none() {
            req.timeout = first_request.timeout;
        }

        if req.stdin.is_empty() {
            req.stdin = "".to_string();
        }

        if let Some(timeout) = req.timeout {
            if timeout < 10 {
                return AppResponse(Err(anyhow!(
                    "Invalid timeout value in request {}: timeout cannot be less than 10", index + 1
                )));
            }
        }

        normalized_requests.push(req);
    }

    for (i, req) in normalized_requests.iter().enumerate().skip(1) {
        if req.language.as_ref().unwrap_or(&String::new()) != &base_language ||
           req.version.as_ref().unwrap_or(&String::new()) != &base_version ||
           req.code.as_ref().unwrap_or(&String::new()) != &base_code {
            return AppResponse(Err(anyhow!("Mismatched language/version/code in batch at index {}", i)));
        }
    }

    let execution_id = Uuid::new_v4().to_string();
    
    let max_requests = env::var("MAX_REQUESTS")
        .unwrap_or_else(|_| "100".to_string())
        .parse::<usize>()
        .unwrap();
    if normalized_requests.len() > max_requests {
        return AppResponse(Err(anyhow!(
            "Max requests exceeded. Max allowed requests: {}",
            max_requests
        )));
    }
    
    debug!("Queuing batch task with ID: {}", execution_id);
    
    match queue_manager.add_task(normalized_requests.clone(), ExecutionType::Batch, 1, redis_client).await {
        Ok(_) => {
            let _ = tx.send(ExecutionNotification {
                id: execution_id.clone(),
                status: "queued".to_string(),
                result: None,
            });
            AppResponse(Ok(SubmissionResponse {
                token: execution_id,
                status: SubmissionStatus::Queued,
                result: None,
            }))
        }
        Err(e) => AppResponse(Err(anyhow!("Failed to add task: {}", e))),
    }
}

pub async fn handle_submission_status(
    State((_executor, _tx, mut redis_client, _queue_manager)): State<(Arc<CodeExecutor>, Arc<broadcast::Sender<ExecutionNotification>>, RedisClient, Arc<QueueManager>)>,
    Path(token): Path<String>,
) -> AppResponse<SubmissionResponse> {
    let result_key = format!("result:{}", token);
    let status_key = format!("status:{}", token);
    
    match redis_client.get_from_cache::<Vec<EvaluationResult>>(&result_key) {
        Ok(Some(results)) => {
            debug!("Found completed results for token: {}", token);
            AppResponse(Ok(SubmissionResponse {
                token,
                status: SubmissionStatus::Completed,
                result: Some(results[0].clone()),
            }))
        }
        Ok(None) => {
            match redis_client.get_from_cache::<String>(&status_key) {
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