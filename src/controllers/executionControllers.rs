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
use uuid::Uuid;

use crate::caching::redis_client::RedisClient;
use crate::models::request::ExecutionRequest;
use crate::models::response::{EvaluationResult, SubmissionResponse, SubmissionStatus};
use crate::queue_management::QueueManager;
use crate::queue_management::task::CompileTask;
use crate::queue_management::ExecutionTask;
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
    // FIX: Read the default timeout from the environment instead of hardcoding it.
    let default_timeout: u64 = env::var("DEFAULT_TIMEOUT")
        .unwrap_or_else(|_| "10".to_string())
        .parse()
        .unwrap_or(10);
    let timeout = request.timeout.unwrap_or(default_timeout);
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
    let parent_token = Uuid::new_v4().to_string();
    let compile_task = CompileTask {
        parent_token: parent_token.clone(),
        requests: vec![request],
    };
    let task = ExecutionTask::CompileBatch(compile_task);

    info!("Queuing single request as CompileBatch task: {}", parent_token);
    match queue_manager
        .add_task(task, 1, redis_client)
        .await
    {
        Ok(_) => {
            let _ = tx.send(ExecutionNotification {
                id: parent_token.clone(),
                status: "queued".to_string(),
                
                results: None,
            });
            AppResponse(Ok(SubmissionResponse {
                token: parent_token,
                status: SubmissionStatus::Queued,
                results: None,
            }))
        }
        Err(e) => AppResponse(Err(anyhow!("Failed to add task: {}", e))),
    }
}


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

    let parent_token = Uuid::new_v4().to_string();
    let compile_task = CompileTask {
        parent_token: parent_token.clone(),
        requests,
    };
    let task = ExecutionTask::CompileBatch(compile_task);

    info!("Queuing batch request as CompileBatch task: {}", parent_token);
    match queue_manager
        .add_task(task, 1, redis_client)
        .await
    {
        Ok(_) => {
            info!("Validated and queued batch task with ID: {}", parent_token);
            let _ = tx.send(ExecutionNotification {
                id: parent_token.clone(),
                status: "queued".to_string(),
                results: None,
            });
            AppResponse(Ok(SubmissionResponse {
                token: parent_token,
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