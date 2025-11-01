use anyhow::{anyhow, Result};
use std::env;
use std::sync::Arc;
use tokio::sync::{broadcast, Semaphore, Mutex, RwLock};
use tracing::{debug, error, info, warn}; // Added debug, warn back
use std::time::Instant;

use crate::caching::redis_client::RedisClient;
use crate::queue_management::manager::QueueManager; // Corrected import path
use crate::queue_management::task::ExecutionTask; // Add import for ExecutionTask
use crate::types::index::{CodeExecutor, ExecutionNotification, ConcurrencyState};
use sysinfo::{System, SystemExt};
use crate::languages::manager::LanguageRegistry;
use crate::sandbox::local_pool::LocalSandboxPool; // Import the new local pool
use crate::models::response::{EvaluationResult, SubmissionStatus}; // Import response types
use crate::monitoring::metrics::QUEUE_DEPTH; // Import QUEUE_DEPTH metric
use redis::AsyncCommands; // Needed for worker loop logic


pub async fn initialize_executor(
    redis_client: RedisClient,
) -> Result<(
    Arc<CodeExecutor>,
    Arc<broadcast::Sender<ExecutionNotification>>,
    Arc<QueueManager>,
)> {
    dotenv::dotenv().ok();

    let language_registry = Arc::new(LanguageRegistry::new()?);

    // Read MAX_CONCURRENT_SANDBOXES which now acts more like a task limiter
    // than a direct sandbox limiter, since sandbox contention is handled by the local pool.
    let max_concurrent_tasks = env::var("MAX_CONCURRENT_SANDBOXES")
        .unwrap_or_else(|_| "50".to_string()) // Default to 50 tasks if not set
        .parse::<usize>()?;
    info!("Max concurrent execution tasks (Semaphore limit): {}", max_concurrent_tasks);

    let executor = Arc::new(CodeExecutor {
        semaphore: Arc::new(Semaphore::new(max_concurrent_tasks)),
        system: Arc::new(Mutex::new(System::new_all())),
        redis_client: redis_client.clone(),
        concurrency_state: Arc::new(RwLock::new((
            ConcurrencyState::Nominal,
            Instant::now(),
        ))),
        language_registry,
    });

    let (tx, _) = broadcast::channel::<ExecutionNotification>(1024);
    let tx = Arc::new(tx);
    let queue_manager = Arc::new(QueueManager::new(
        executor.clone(),
        redis_client.clone(), // QueueManager still needs Redis for queue ops
        tx.clone(),
    ));

    // Removed the Redis memory monitor task spawn, as custom eviction is removed

    Ok((executor, tx, queue_manager))
}


pub async fn start_workers(
    queue_manager: Arc<QueueManager>,
    redis_client: RedisClient, // Still needed for queue operations
    executor: Arc<CodeExecutor>, // Needed for execute_batch call
) {
    // Initialize the local sandbox pool for this worker machine
    let local_pool = match LocalSandboxPool::new().await {
        Ok(pool) => Arc::new(pool),
        Err(e) => {
            error!("FATAL: Failed to initialize local sandbox pool: {}. Shutting down worker process.", e);
            // In a real scenario, might want more robust error handling or reporting
            panic!("Failed to initialize local sandbox pool: {}", e);
        }
    };

    let num_software_workers = (num_cpus::get() * 3).max(1);
    let queue_name = "evalx_jobs".to_string(); // Unified queue name
    info!(
        "Spawning {} software worker tasks for the unified '{}' queue, using the local sandbox pool.",
        num_software_workers, queue_name
    );

    let mut handles = Vec::new();

    for i in 0..num_software_workers {
        let manager_clone = queue_manager.clone(); // Clone Arc for notifications
        let client_clone = redis_client.clone(); // Clone Redis client for queue interaction
        let q_name = queue_name.clone();
        let pool_clone = local_pool.clone(); // Clone Arc of the local pool
        let executor_clone = executor.clone(); // Clone Arc of the executor

        let handle = tokio::spawn(async move {
            info!("Software Worker Task #{} starting...", i + 1);

            // --- Worker Loop Logic (Moved from manager.rs) ---
            let result_ttl: usize = env::var("RESULT_TTL_SECONDS")
                .unwrap_or_else(|_| "200".to_string())
                .parse()
                .unwrap_or(200);
            let max_retries: u8 = env::var("MAX_RETRIES").unwrap_or_else(|_| "3".to_string()).parse().unwrap_or(3);
            let dlq_name = env::var("DEAD_LETTER_QUEUE_NAME").unwrap_or_else(|_| "evalx:dead_letter_queue".to_string());
            let queue_key = format!("queue:{}", q_name);
            let priority_key = format!("priority:{}", q_name);

            loop {
                // Moved Redis connection acquisition inside the loop for resilience
                let mut conn = match client_clone.get_multiplexed_async_connection().await {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Worker #{}: Failed to get Redis connection: {}. Retrying in 3s.", i + 1, e);
                        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                        continue; // Try getting connection again
                    }
                };

                let result: Option<(String, String)> = match conn.brpop(&queue_key, 5).await { // Increased timeout slightly
                    Ok(res) => res,
                    Err(e) => {
                        // Avoid logging timeout errors frequently, log other errors
                         if !format!("{:?}", e).contains("response was nil") { // Simple check for timeout
                           warn!("Worker #{}: Dequeue error ({}): {}. Retrying.", i + 1, q_name, e);
                        }
                        // Sleep briefly even on timeout to prevent cpu spinning if queue is truly empty
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue; // Continue to next loop iteration
                    }
                };


                if let Some((_queue_actual, task_id)) = result { // Destructure tuple from brpop
                    QUEUE_DEPTH.with_label_values(&[&q_name]).dec();
                    info!("Worker #{}: Dequeued task: {} from queue: {}", i + 1, task_id, q_name);
                    let status_key = format!("status:{}", &task_id);

                    // Attempt to remove from priority set, log error but continue if it fails
                    if let Err(e) = conn.zrem::<_, _, ()>(&priority_key, &task_id).await {
                        warn!("Worker #{}: Failed to remove task {} from priority set (might be a retry or already processed?): {}", i + 1, task_id, e);
                        // Don't necessarily fail the task here, maybe it was already removed
                    }

                    let task_key = format!("task:{}", &task_id);
                    let serialized_task: Option<String> = match conn.get(&task_key).await {
                         Ok(res) => res,
                         Err(e) => {
                            error!("Worker #{}: Failed to get task data {}: {}. Marking as failed.", i + 1, task_id, e);
                            // Best effort to mark as failed, ignore error if this also fails
                            let _ = conn.set::<_, _, ()>(&status_key, "failed").await;
                            manager_clone.notify_completion(&task_id, &vec![EvaluationResult::error_result("Internal Error: Failed to retrieve task data")], SubmissionStatus::Failed);
                            continue; // Move to next task
                        }
                    };

                    if let Some(serialized_task) = serialized_task {
                        let mut task: ExecutionTask = match serde_json::from_str(&serialized_task) {
                            Ok(t) => t,
                            Err(e) => {
                                error!("Worker #{}: Failed to deserialize task {}: {}. Marking as failed.", i + 1, task_id, e);
                                let _ = conn.set::<_, _, ()>(&status_key, "failed").await;
                                manager_clone.notify_completion(&task_id, &vec![EvaluationResult::error_result("Internal Error: Corrupted task data")], SubmissionStatus::Failed);
                                // Try to remove corrupted task data
                                let _ = conn.del::<_, ()>(&task_key).await;
                                continue;
                            }
                        };

                        // Calculate queue wait time
                        match std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH) {
                            Ok(now_unix) => {
                                 let wait_time = now_unix.as_secs().saturating_sub(task.created_at);
                                 crate::monitoring::metrics::QUEUE_WAIT_TIME_SECONDS.with_label_values(&[&q_name]).observe(wait_time as f64);
                            },
                            Err(e) => warn!("Worker #{}: System time error, cannot record queue wait time for task {}: {}", i+1, task_id, e),
                        }


                        // Set status to processing
                        if let Err(e) = conn.set::<_, _, ()>(&status_key, "processing").await {
                            error!("Worker #{}: Failed to set status to processing for task {}: {}. Skipping.", i + 1, task_id, e);
                            // If we can't even set processing, skip the task for now, it might get picked up again
                            // Re-add to queue? Could cause loops. Let's just skip.
                            continue;
                        }

                        // Send processing notification
                        manager_clone.notify_completion(&task_id, &vec![], SubmissionStatus::Processing);
                        let result_key = format!("result:{}", &task_id);

                        match executor_clone.clone().execute_batch(task.requests.clone(), pool_clone.clone(), Some(task_id.clone())).await {
                            Ok(results) => {
                                debug!("Worker #{}: Task {} execution successful.", i + 1, task_id);
                                let serialized_results = match serde_json::to_string(&results) {
                                    Ok(s) => s,
                                    Err(e) => {
                                        error!("Worker #{}: Failed to serialize results for task {}: {}. Storing error result.", i + 1, task_id, e);
                                        // Store an error result instead if serialization fails
                                        serde_json::to_string(&vec![EvaluationResult::error_result("Internal Error: Failed to serialize results")]).unwrap_or_default()
                                    }
                                };

                                // Store results with TTL
                                if let Err(e) = conn.set_ex::<_, _, ()>(&result_key, serialized_results, result_ttl).await {
                                     error!("Worker #{}: Failed to store results for task {}: {}. Results may be lost.", i + 1, task_id, e);
                                }

                                // Set status to completed
                                if let Err(e) = conn.set::<_, _, ()>(&status_key, "completed").await {
                                    error!("Worker #{}: Failed to set status to completed for task {}: {}", i + 1, task_id, e);
                                }

                                // Send completion notification
                                manager_clone.notify_completion(&task_id, &results, SubmissionStatus::Completed);
                            }
                            Err(e) => {
                                // Retry / DLQ Logic
                                error!("Worker #{}: Execution failed for task {}: {}. Retry {}/{}", i+1, task_id, e, task.retry_count + 1, max_retries);
                                task.retry_count += 1;

                                if task.retry_count >= max_retries {
                                    warn!("Worker #{}: Task {} failed after {} retries. Moving to DLQ.", i+1, task_id, max_retries);
                                    let error_result = vec![EvaluationResult::error_result(format!("Execution failed after {} retries: {}", max_retries, e))];

                                    // Store final error result
                                     match serde_json::to_string(&error_result) {
                                        Ok(serialized_error) => {
                                            if let Err(e_set) = conn.set_ex::<_, _, ()>(&result_key, serialized_error, result_ttl).await {
                                                 error!("Worker #{}: Failed to store final error result for DLQ task {}: {}", i + 1, task_id, e_set);
                                            }
                                        },
                                        Err(e_ser) => { // Should ideally not happen for error result
                                            error!("Worker #{}: Failed to serialize final error result for DLQ task {}: {}", i + 1, task_id, e_ser);
                                        }
                                     };

                                    // Set status to failed
                                    if let Err(e_stat) = conn.set::<_, _, ()>(&status_key, "failed").await {
                                        error!("Worker #{}: Failed to set status to failed for DLQ task {}: {}", i + 1, task_id, e_stat);
                                    }

                                    // Push to DLQ
                                    let failed_task_payload = serde_json::to_string(&task).unwrap_or_else(|err| {
                                        error!("Worker #{}: Failed to serialize task {} for DLQ push: {}", i+1, task_id, err);
                                        format!("{{\"error\":\"Serialization failed for DLQ\", \"task_id\":\"{}\"}}", task_id) // Fallback payload
                                    });
                                    if let Err(e_dlq) = conn.lpush::<_,_,()>(dlq_name.clone(), failed_task_payload).await {
                                        error!("Worker #{}: Failed to push task {} to DLQ '{}': {}", i + 1, task_id, dlq_name, e_dlq);
                                    }

                                    // Notify failure
                                    manager_clone.notify_completion(&task_id, &error_result, SubmissionStatus::Failed);
                                } else {
                                    // Re-queue for another attempt.
                                    warn!("Worker #{}: Re-queuing task {} for retry.", i + 1, task_id);

                                    // Update task data in Redis with incremented retry_count
                                    match serde_json::to_string(&task) {
                                         Ok(updated_task_payload) => {
                                            if let Err(e_set) = conn.set::<_,_,()>(&task_key, updated_task_payload).await {
                                                 error!("Worker #{}: Failed to update task data for retry {}: {}. Re-queueing with potentially old retry count.", i + 1, task_id, e_set);
                                            }
                                         },
                                         Err(e_ser) => { // Should ideally not happen
                                             error!("Worker #{}: Failed to serialize updated task data for retry {}: {}", i + 1, task_id, e_ser);
                                         }
                                     };

                                    // Re-add task ID to the list (queue) and priority set
                                    let mut pipe = redis::pipe();
                                    pipe.atomic() // Make re-queue atomic if possible
                                        .lpush(&queue_key, &task_id)
                                        .zadd(&priority_key, &task_id, 1); 

                                    if let Err(e_pipe) = pipe.query_async::<_, ()>(&mut conn).await {
                                         error!("Worker #{}: Failed to atomically re-queue task {} for retry: {}", i + 1, task_id, e_pipe);
                                    }

                                
                                }
                            }
                        }

                        // Task data cleanup (only if completed or failed, not on retry)
                        if let Ok(current_status) = conn.get::<_, String>(&status_key).await {
                             if current_status == "completed" || current_status == "failed" {
                                if let Err(e) = conn.del::<_, ()>(&task_key).await {
                                    warn!("Worker #{}: Failed to delete task data {} after completion/failure: {}", i + 1, task_id, e);
                                }
                            }
                        } else {
                             warn!("Worker #{}: Could not read status for task {} after processing, cannot clean up task data.", i + 1, task_id);
                        }

                    } else {
                        // This case should ideally not happen if brpop succeeded
                        error!("Worker #{}: Task ID {} dequeued but data not found in Redis. Task lost.", i + 1, task_id);
                        // Best effort to mark as failed
                        let _ = conn.set::<_, _, ()>(&format!("status:{}", &task_id), "failed").await;
                         manager_clone.notify_completion(&task_id, &vec![EvaluationResult::error_result("Internal Error: Task data lost after dequeue")], SubmissionStatus::Failed);
                    }
                }
                // If brpop timed out (result is None), the loop continues naturally
            }
            // If the loop exits, it indicates a severe issue (e.g., channel closed)
            error!("Software Worker Task #{} is terminating unexpectedly.", i + 1);
        });
        handles.push(handle);
    }

    // Keep the main worker process alive by waiting on all spawned worker tasks.
    futures_util::future::join_all(handles).await;
    info!("All software worker tasks have terminated."); // This might indicate an issue if unexpected
}