use axum::{
    extract::{Json, Path, State},
};
use anyhow::anyhow;
use hex::ToHex;
use sha2::{Digest, Sha256};
use std::env;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::caching::redis_client::RedisClient;
use crate::models::request::ExecutionRequest;
use crate::models::response::{EvaluationResult, SubmissionResponse, SubmissionStatus};
use crate::queue_management::{ExecutionType, QueueManager};
use crate::types::index::{CodeExecutor, ExecutionNotification};
use crate::AppResponse;

/// Handles a single code execution request.
/// This is now a wrapper that treats the single request as a "batch of one".
pub async fn handle_execute(
    State((_executor, tx, redis_client, queue_manager)): State<(
        Arc<CodeExecutor>,
        Arc<broadcast::Sender<ExecutionNotification>>,
        RedisClient,
        Arc<QueueManager>,
    )>,
    Json(request): Json<ExecutionRequest>,
) -> AppResponse<SubmissionResponse> {
    // --- 1. Quick Cache Check for Single, Simple Executions ---
    // This provides an immediate response for frequently run, identical code snippets.
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

    // Using a block to scope the mutable redis_client
    if let Ok(Some(cached_result)) = redis_client.get_from_cache_async(&cache_key, Some(&language)).await {
        debug!("Single execution cache hit for key: {}", cache_key);
        return AppResponse(Ok(SubmissionResponse {
            token: "cached".to_string(),
            status: SubmissionStatus::Completed,
            results: Some(vec![cached_result]),
        }));
    }

    // --- 2. Unified Batch Queuing ---
    // The single request is wrapped in a vector and queued as a batch.
    // This unifies the execution pipeline for maximum code reuse and simplicity.
    info!("Queuing single request as a batch of one.");
    
    match queue_manager
        .add_task(vec![request.clone()], ExecutionType::Batch, 1, redis_client)
        .await
    {
        Ok(task_id) => {
            let _ = tx.send(ExecutionNotification {
                id: task_id.clone(),
                status: "queued".to_string(),
                results: None,
            });
            
            AppResponse(Ok(SubmissionResponse {
                token: task_id,
                status: SubmissionStatus::Queued,
                results: None,
            }))
        }
        Err(e) => AppResponse(Err(anyhow!("Failed to add task: {}", e))),
    }
}

// NOTE: The `handle_execute_parallel` function has been removed.
// The new Isolate-based `execute_batch` is the single, optimized path for all
// concurrent executions. This simplifies the API and codebase. For executing
// different code snippets, clients should make multiple requests to the batch endpoint.

/// Handles a batch execution request for the SAME code against multiple test cases.
pub async fn handle_execute_batch(
    State((_executor, tx, redis_client, queue_manager)): State<(
        Arc<CodeExecutor>,
        Arc<broadcast::Sender<ExecutionNotification>>,
        RedisClient,
        Arc<QueueManager>,
    )>,
    Json(requests): Json<Vec<ExecutionRequest>>,
) -> AppResponse<SubmissionResponse> {
    if requests.is_empty() {
        return AppResponse(Err(anyhow!("Batch request list cannot be empty")));
    }

    // --- 1. Validate and Normalize the Batch ---
    // Ensures all requests in the batch use the same code, language, etc.
    // This is crucial for the "compile once, run many" optimization.
    let first_request = requests[0].clone();
    let base_language = first_request.language.as_ref().unwrap_or(&String::new()).trim().to_string();
    let base_version = first_request.version.as_ref().unwrap_or(&String::new()).trim().to_string();
    let base_code = first_request.code.clone().unwrap_or_default();
    
    if base_language.is_empty() || base_code.is_empty() {
        return AppResponse(Err(anyhow!("The first request in a batch must contain language and code")));
    }

    let mut normalized_requests = Vec::new();
    for (index, mut req) in requests.into_iter().enumerate() {
        req.language = Some(req.language.filter(|s| !s.trim().is_empty()).unwrap_or_else(|| base_language.clone()));
        // Note: version is less critical for Isolate but good practice to keep consistent.
        req.version = Some(req.version.filter(|s| !s.trim().is_empty()).unwrap_or_else(|| base_version.clone()));
        
        if req.code.as_ref().map_or(true, |s| s.trim().is_empty()) {
            req.code = Some(base_code.clone());
            warn!("Propagated code to incomplete request at index {}", index);
        }
        req.timeout = req.timeout.or(first_request.timeout);
        normalized_requests.push(req);
    }

    let max_requests = env::var("MAX_REQUESTS").unwrap_or_else(|_| "100".to_string()).parse::<usize>().unwrap();
    if normalized_requests.len() > max_requests {
        return AppResponse(Err(anyhow!("Max requests exceeded. Max allowed requests: {}", max_requests)));
    }
    
    // --- 2. Queue the Validated Batch Task ---
    match queue_manager
        .add_task(normalized_requests, ExecutionType::Batch, 1, redis_client)
        .await
    {
        Ok(task_id) => {
            info!("Validated and queued batch task with ID: {}", task_id);
            let _ = tx.send(ExecutionNotification {
                id: task_id.clone(),
                status: "queued".to_string(),
                results: None,
            });
            
            AppResponse(Ok(SubmissionResponse {
                token: task_id,
                status: SubmissionStatus::Queued,
                results: None,
            }))
        }
        Err(e) => AppResponse(Err(anyhow!("Failed to add task: {}", e))),
    }
}

/// Handles polling for the status and result of a submission token.
pub async fn handle_submission_status(
    State((_executor, _tx, redis_client, _queue_manager)): State<(
        Arc<CodeExecutor>,
        Arc<broadcast::Sender<ExecutionNotification>>,
        RedisClient,
        Arc<QueueManager>,
    )>,
    Path(token): Path<String>,
) -> AppResponse<SubmissionResponse> {
    let result_key = format!("result:{}", token);
    let status_key = format!("status:{}", token);

    // FIXED: Added the missing `None` argument.
    match redis_client.get_from_cache_async::<Vec<EvaluationResult>>(&result_key, None).await {
        Ok(Some(results)) => {
            debug!("Found completed results for token: {}", token);
            AppResponse(Ok(SubmissionResponse {
                token,
                status: SubmissionStatus::Completed,
                results: Some(results),
            }))
        }
        Ok(None) => {
            // FIXED: Added the missing `None` argument.
            match redis_client.get_from_cache_async::<String>(&status_key, None).await {
                Ok(Some(status)) if status == "processing" => {
                    debug!("Task is processing for token: {}", token);
                    AppResponse(Ok(SubmissionResponse {
                        token,
                        status: SubmissionStatus::Processing,
                        results: None,
                    }))
                }
                _ => {
                    // This can mean it's queued or the token is invalid/expired.
                    debug!("No results yet for token: {}", token);
                    AppResponse(Ok(SubmissionResponse {
                        token,
                        status: SubmissionStatus::Queued,
                        results: None,
                    }))
                }
            }
        }
        Err(e) => AppResponse(Err(anyhow!("Failed to check submission status: {}", e))),
    }
}