use anyhow::{anyhow, Result};
use std::env;
use std::sync::Arc;
use tokio::sync::{broadcast, Semaphore, Mutex, RwLock};
use tracing::{debug, error, info, warn}; // Added debug, warn back
use std::time::Instant;
use std::collections::HashMap;
use uuid::Uuid;

use crate::caching::redis_client::RedisClient;
use crate::queue_management::manager::QueueManager;
// Corrected import path
use crate::queue_management::task::{ExecutionTask, RunTask}; // Add import for ExecutionTask
use crate::types::index::{CodeExecutor, ExecutionNotification, ConcurrencyState};
use sysinfo::{System, SystemExt};
use crate::languages::manager::LanguageRegistry;
use crate::sandbox::local_pool::LocalSandboxPool;
// Import the new local pool
use crate::models::response::{EvaluationResult, SubmissionStatus}; // Import response types
use crate::monitoring::metrics::QUEUE_DEPTH; // Import QUEUE_DEPTH metric
use redis::AsyncCommands;
// Needed for worker loop logic

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
    let queue_name = "evalx_jobs".to_string();
    // Unified queue name
    info!(
        "Spawning {} software worker tasks for the unified '{}' queue, using the local sandbox pool.",
        num_software_workers, queue_name
    );
    let mut handles = Vec::new();

    for i in 0..num_software_workers {
        let manager_clone = queue_manager.clone();
        // Clone Arc for notifications
        let client_clone = redis_client.clone();
        // Clone Redis client for queue interaction
        let q_name = queue_name.clone();
        let pool_clone = local_pool.clone(); // Clone Arc of the local pool
        let executor_clone = executor.clone();
        // Clone Arc of the executor

        let handle = tokio::spawn(async move {
            info!("Software Worker Task #{} starting...", i + 1);

            let result_ttl: usize = env::var("RESULT_TTL_SECONDS")
                .unwrap_or_else(|_| "200".to_string())
                .parse()
                .unwrap_or(200);
            
            let queue_key = format!("queue:{}", q_name);
            let priority_key = format!("priority:{}", q_name);

            loop 
            {
                let mut conn = match client_clone.get_multiplexed_async_connection().await {
                    Ok(c) => c,
                    Err(e) => {
                       error!("Worker #{}: Failed to get Redis connection: {}. Retrying in 3s.", i + 1, e);
                        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                        continue;
                    }
                };

     
                let result: Option<(String, String)> = match conn.brpop(&queue_key, 5).await {
                    Ok(res) => res,
                    Err(e) => {
                        if !format!("{:?}", e).contains("response was nil") {
                           warn!("Worker #{}: Dequeue error ({}): {}. Retrying.", i + 1, q_name, e);
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                }
                };
                if let Some((_queue_actual, task_id)) = result {
                    QUEUE_DEPTH.with_label_values(&[&q_name]).dec();
                    info!("Worker #{}: Dequeued task: {} from queue: {}", i + 1, task_id, q_name);
                    
                    if let Err(e) = conn.zrem::<_, _, ()>(&priority_key, &task_id).await {
                        warn!("Worker #{}: Failed to remove task {} from priority set: {}", i + 1, task_id, e);
                    }

                    let task_key = format!("task:{}", &task_id);
                    let serialized_task: Option<String> = match conn.get(&task_key).await {
                         Ok(res) => res,
                         Err(e) => {
                            error!("Worker #{}: Failed to get task data {}: {}.", i + 1, task_id, e);
                            continue;
                        }
                    };
                    
                    if let Some(serialized_task) = serialized_task {
                        let task: ExecutionTask = match serde_json::from_str(&serialized_task) {
                            Ok(t) => t,
                            Err(e) => {
                                error!("Worker #{}: Failed to deserialize task {}: {}. Discarding.", i + 1, task_id, e);
                                let _ = conn.del::<_, ()>(&task_key).await;
                                continue;
                            }
                        };

                        let task_result: Result<(), anyhow::Error> = match task {
                            ExecutionTask::CompileBatch(compile_task) => {
                                let parent_token = compile_task.parent_token.clone();
                                let task_result: Result<(), anyhow::Error> = async {
                                    let status_key = format!("status:{}", &parent_token);
                                    info!("Worker #{}: Processing CompileBatch task {}", i + 1, parent_token);

                                    let _ = conn.set::<_, _, ()>(&status_key, "processing").await;
                                    manager_clone.notify_completion(&parent_token, &vec![], SubmissionStatus::Processing);
                                    
                                    let first_req = match compile_task.requests.first() {
                                        Some(req) => req,
                                        None => {
                                            error!("Worker #{}: CompileBatch task {} has no requests. Discarding.", i + 1, parent_token);
                                            return Ok(());
                                        }
                                    };

                                    let lang = first_req.language.as_ref().unwrap().trim();
                                    let version = first_req.version.as_ref().unwrap().trim();
                                    let code = first_req.code.as_ref().unwrap().trim();
                                    let lang_key = format!("{}:{}", lang, version);
                                    let default_timeout = first_req.timeout.unwrap_or(10);
                                    
                                    let config = match executor_clone.language_registry.get(&lang_key) {
                                        Some(cfg) => cfg,
                                        None => {
                                            error!("Worker #{}: No language config for {}. Failing task {}.", i + 1, lang_key, parent_token);
                                            let err_res = vec![EvaluationResult::error_result(format!("Unsupported language: {}", lang_key))];
                                            let _ = conn.set_ex::<_, _, ()>(format!("result:{}", parent_token), serde_json::to_string(&err_res).unwrap_or_default(), result_ttl).await;
                                            let _ = conn.set::<_, _, ()>(&status_key, "failed").await;
                                            manager_clone.notify_completion(&parent_token, &err_res, SubmissionStatus::Failed);
                                            return Ok(());
                                        }
                                    };

                                    match executor_clone.clone().compile_and_get_artifact(config, code, pool_clone.clone()).await {
                                        Err(compile_error_result) => {
                                            info!("Worker #{}: Compilation failed for task {}.", i + 1, 
 parent_token);
                                            let results = vec![compile_error_result];
                                            let result_key = format!("result:{}", parent_token);
                                            let _ = conn.set_ex::<_, _, ()>(&result_key, serde_json::to_string(&results).unwrap_or_default(), result_ttl).await;
                                            let _ = conn.set::<_, _, ()>(&status_key, "completed").await;
                                            manager_clone.notify_completion(&parent_token, &results, SubmissionStatus::Completed);
                                        },
                                        Ok((artifact_binary, compile_time)) => {
                                            info!("Worker #{}: Compilation success for {}. Fanning out {} test cases.", i + 1, 
 parent_token, compile_task.requests.len());
                                            let artifact_key = format!("artifact:{}", parent_token);
                                            let num_test_cases = compile_task.requests.len();
                                            let counter_key = format!("counter:{}", parent_token);
                                            let lang_key_redis = format!("lang:{}", parent_token);
                                            let timeout_key_redis = format!("timeout:{}", parent_token);
                                            let compile_time_key = format!("compile_time:{}", parent_token);
                                            
                                            let mut pipe = redis::pipe();
                                            pipe.set_ex(&artifact_key, artifact_binary, 3600);
                                            pipe.set(&counter_key, num_test_cases);
                                            pipe.set(&lang_key_redis, lang_key);
                                            pipe.set(&timeout_key_redis, default_timeout);
                                            pipe.set(&compile_time_key, compile_time);

                                            for (index, request) in compile_task.requests.into_iter().enumerate() {
                                                let run_task = RunTask {
                                       
                                                     parent_token: parent_token.clone(),
                                                    index,
                                  
                                                     artifact_key: artifact_key.clone(),
                                                    stdin: request.stdin.clone(),
                            
                                                 };
                                                let task = ExecutionTask::RunTestcase(run_task);
                                                let task_id = Uuid::new_v4().to_string();
                                                let serialized_run_task = serde_json::to_string(&task).unwrap_or_default();
                                                
                                                pipe.set(format!("task:{}", task_id), serialized_run_task);
                                                pipe.zadd(&priority_key, &task_id, 1);
                                                pipe.lpush(&queue_key, &task_id);
                                            }
                                            
                                            if let Err(e) = pipe.query_async::<_, ()>(&mut conn).await {
     
                                                         error!("Worker #{}: Failed to enqueue RunTestcase jobs for {}: {}", i + 1, parent_token, e);
                                            } else {
                                                QUEUE_DEPTH.with_label_values(&[&q_name]).add(num_test_cases as i64);
                                            }
                                        }
                                    }
                                    Ok(())
                                }.await;

                                if let Err(e) = task_result {
                                    error!("Worker #{}: CompileBatch task {} failed: {}", i + 1, parent_token, e);
                                }
                                Ok(())
                            },

                            ExecutionTask::RunTestcase(run_task) => {
                                let parent_token = run_task.parent_token.clone();
                                let index = run_task.index;
                                
                                let task_result: Result<(), anyhow::Error> = async {
                                    debug!("Worker #{}: Processing RunTestcase task {}:{}", i + 1, parent_token, index);
                                    let counter_key = format!("counter:{}", parent_token);
                                    let results_hash_key = format!("results:{}", parent_token);
                                    let kill_key = format!("kill:{}", parent_token);
                                    let compile_time_key = format!("compile_time:{}", parent_token);

                                    let kill_signal: Option<String> = conn.get(&kill_key).await?;
                                    let compile_time: f64 = conn.get::<_, Option<f64>>(&compile_time_key).await?.unwrap_or(0.0);

                                    let mut result = if kill_signal.is_some() {
                                        EvaluationResult::error_result("Skipped (previous test case failed)")
                                    } else {
                                         let artifact_key = run_task.artifact_key.clone();
                                        let lang_key: String = conn.get::<_, Option<String>>(format!("lang:{}", parent_token)).await?.ok_or_else(|| anyhow::anyhow!("Missing lang key"))?;
                                        let time_limit: u64 = conn.get::<_, Option<u64>>(format!("timeout:{}", parent_token)).await?.ok_or_else(|| anyhow::anyhow!("Missing timeout key"))?;
                                        let config = executor_clone.language_registry.get(&lang_key).ok_or_else(|| anyhow::anyhow!("Missing lang config"))?;
                                        let artifact_binary: Vec<u8> = conn.get::<_, Option<Vec<u8>>>(&artifact_key).await?.ok_or_else(|| anyhow::anyhow!("Missing artifact"))?;
                                        let run_result = executor_clone.clone().run_single_test_case(
                                            config,
                                            &artifact_binary,
                                             &run_task.stdin,
                                            time_limit,
                                         pool_clone.clone()
                                        ).await;
                                        if run_result.exit_code != 0 {
                                            info!("Worker #{}: Test case {}:{} failed. Setting kill switch.", i + 1, parent_token, index);
                                            let _ = conn.set_ex::<_, _, ()>(&kill_key, "true", 3600).await;
                                        }
                                        run_result
                                    };
                                    
                                    result.compile_time = compile_time;
                                    
                                    let serialized_result = serde_json::to_string(&result)?;
                                    conn.hset::<_, _, _, ()>(&results_hash_key, index, serialized_result).await?;

                                    let count: i64 = conn.decr(&counter_key, 1).await?;
                                    
                                    if count == 0 {
                                        info!("Worker #{}: Fan-in: Last worker for task {}. Assembling results.", i + 1, parent_token);
                                        
                                        let results_map: HashMap<String, String> = conn.hgetall(&results_hash_key).await?;
                                        let mut sorted_results_tuples: Vec<(usize, EvaluationResult)> = Vec::new();

                                        for (key_str, val_str) in results_map {
                                            let index_key = match key_str.parse::<usize>() {
                                                Ok(k) => k,
                                                Err(e) => {
                                                    error!("Worker #{}: Failed to parse result index key '{}' for {}: {}", i + 1, key_str, parent_token, e);
                                                    continue;
                                                }
                                            };
                                            match serde_json::from_str::<EvaluationResult>(&val_str) {
                                                Ok(res) => sorted_results_tuples.push((index_key, res)),
                                                Err(e) => error!("Worker #{}: Failed to parse result for task {} index {}: {}", i + 1, parent_token, key_str, e)
                                            }
                                        }
                                        
                                        sorted_results_tuples.sort_by_key(|(i, _)| *i);
                                        let final_results: Vec<EvaluationResult> = sorted_results_tuples.into_iter().map(|(_, r)| r).collect();
                                        let final_json = serde_json::to_string(&final_results)?;
                                        
                                        let result_key = format!("result:{}", parent_token);
                                        let status_key = format!("status:{}", parent_token);
                                        let artifact_key = run_task.artifact_key.clone();
                                        let lang_key_redis = format!("lang:{}", parent_token);
                                        let timeout_key_redis = format!("timeout:{}", parent_token);
                                        let compile_time_key = format!("compile_time:{}", parent_token);

                                        let _ = conn.set_ex::<_, _, ()>(&result_key, final_json, result_ttl).await;
                                        let _ = conn.set::<_, _, ()>(&status_key, "completed").await;
                                        manager_clone.notify_completion(&parent_token, &final_results, SubmissionStatus::Completed);
                                        
                                        let _ = conn.del::<_, ()>(&[artifact_key, results_hash_key, counter_key, kill_key, lang_key_redis, timeout_key_redis, compile_time_key]).await;
                                        info!("Worker #{}: Task {} complete. All keys cleaned up.", i + 1, parent_token);
                                    }
                                    Ok(())
                                }.await;

                                if let Err(e) = task_result {
                                    error!("Worker #{}: RunTestcase task {}:{} failed: {}", i + 1, parent_token, index, e);
                                }
                                Ok(())
                            }
                        };
                        
                        if let Err(e) = task_result {
                            error!("Worker #{}: A task failed processing: {}", i + 1, e);
                        }

                        if let Err(e) = conn.del::<_, ()>(&task_key).await {
                            warn!("Worker #{}: Failed to delete task data {} after processing: {}", i + 1, task_id, e);
                        }
                    } else {
                        error!("Worker #{}: Task ID {} dequeued but data not found in Redis. Task lost.", i + 1, task_id);
                    }
                }
            }
            
            error!("Software Worker Task #{} is terminating unexpectedly.", i + 1);
        });
        handles.push(handle);
    }

    // Keep the main worker process alive by waiting on all spawned worker tasks.
    futures_util::future::join_all(handles).await;
    info!("All software worker tasks have terminated."); // This might indicate an issue if unexpected
}