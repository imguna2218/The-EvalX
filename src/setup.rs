use anyhow::{anyhow, Result};
use std::env;
use std::sync::Arc;
use tokio::sync::{broadcast, Semaphore, Mutex, RwLock};
use tracing::{debug, error, info, warn};
use std::time::Instant;
use std::collections::HashMap;
use uuid::Uuid;

use crate::caching::redis_client::RedisClient;
use crate::queue_management::manager::QueueManager;
use crate::queue_management::task::{ExecutionTask, RunTask};
use crate::types::index::{CodeExecutor, ExecutionNotification, ConcurrencyState};
use sysinfo::{System, SystemExt};
use crate::languages::manager::LanguageRegistry;
use crate::sandbox::local_pool::LocalSandboxPool;
use crate::models::response::{EvaluationResult, SubmissionStatus};
use crate::monitoring::metrics::QUEUE_DEPTH;
use redis::AsyncCommands;
use core_affinity;

// ADDED: AWS Imports
use aws_config::BehaviorVersion;
use aws_sdk_sqs::Client as SqsClient;

pub async fn initialize_executor(
    redis_client: RedisClient,
) -> Result<(
    Arc<CodeExecutor>,
    Arc<broadcast::Sender<ExecutionNotification>>,
    Arc<QueueManager>,
)> {
    dotenv::dotenv().ok();

    let language_registry = Arc::new(LanguageRegistry::new()?);

    // ADDED: Initialize SQS Client
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let sqs_client = SqsClient::new(&config);
    // Check if URL is present at startup to fail early if missing
    let _ = env::var("SQS_QUEUE_URL").expect("SQS_QUEUE_URL must be set in .env");

    let max_concurrent_tasks = env::var("MAX_CONCURRENT_SANDBOXES")
        .unwrap_or_else(|_| "50".to_string()) 
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
    
    // Pass SQS Client to QueueManager
    let queue_manager = Arc::new(QueueManager::new(
        executor.clone(),
        redis_client.clone(),
        tx.clone(),
        sqs_client, 
    ));

    Ok((executor, tx, queue_manager))
}

pub async fn start_workers(
    queue_manager: Arc<QueueManager>,
    redis_client: RedisClient,
    executor: Arc<CodeExecutor>,
) {
    // 1. Initialize the local sandbox pool
    let local_pool = match LocalSandboxPool::new().await {
        Ok(pool) => Arc::new(pool),
        Err(e) => {
            error!("FATAL: Failed to initialize local sandbox pool: {}. Shutting down worker process.", e);
            panic!("Failed to initialize local sandbox pool: {}", e);
        }
    };

    // 2. Initialize SQS Client for Worker Thread
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let sqs_client = SqsClient::new(&config);
    let sqs_url = env::var("SQS_QUEUE_URL").expect("SQS_QUEUE_URL not set");

    // 3. Determine Core for Workers (The Last Core)
    let core_ids = core_affinity::get_core_ids().unwrap_or_else(|| {
        warn!("Failed to retrieve core IDs. Pinning will be disabled.");
        vec![]
    });
    
    // --- FIX: Calculate worker count BEFORE pinning ---
    // We use the total system cores (e.g., 12) * 3 = 36 workers
    let total_cores = num_cpus::get();
    let num_software_workers = (total_cores * 3).max(1);
    // ------------------------------------------------

    let worker_core = core_ids.last().cloned();

    if let Some(core) = &worker_core {
        info!("🚀 PINNING WORKER MANAGER TO CORE ID: {:?} (Total Cores Available: {})", core.id, core_ids.len());
    } else {
        warn!("⚠️ Could not determine a core to pin. Workers will float.");
    }

    // 4. Spawn a Dedicated OS Thread
    std::thread::spawn(move || {
        // A. Apply Pinning inside the thread
        if let Some(core) = worker_core {
            if core_affinity::set_for_current(core) {
                info!("✅ Worker thread successfully pinned to Core.");
            } else {
                error!("❌ Failed to pin worker thread!");
            }
        }

        // B. Build a Single-Threaded Tokio Runtime
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to build worker runtime");

        // C. Run the Workers
        runtime.block_on(async move {
            info!("Spawning {} software worker tasks (Calculated from {} System Cores)...", num_software_workers, total_cores);

            let mut handles = Vec::new();

            for i in 0..num_software_workers {
                let manager_clone = queue_manager.clone();
                let client_clone = redis_client.clone();
                let pool_clone = local_pool.clone();
                let executor_clone = executor.clone();
                let sqs_client_clone = sqs_client.clone();
                let sqs_url_clone = sqs_url.clone();

                let handle = tokio::spawn(async move {
                    info!("Worker Task #{} initialized.", i + 1);
                    run_worker_logic(
                        i, 
                        manager_clone, 
                        client_clone, 
                        pool_clone, 
                        executor_clone,
                        sqs_client_clone,
                        sqs_url_clone
                    ).await;
                });
                handles.push(handle);
            }

            futures_util::future::join_all(handles).await;
            info!("All software worker tasks have terminated.");
        });
    });

    info!("Worker Management Thread deployed with target: {} workers.", num_software_workers);
}

async fn run_worker_logic(
    i: usize,
    manager_clone: Arc<QueueManager>,
    client_clone: RedisClient,
    pool_clone: Arc<LocalSandboxPool>,
    executor_clone: Arc<CodeExecutor>,
    sqs_client: SqsClient,
    sqs_url: String,
) {
    let result_ttl: usize = env::var("RESULT_TTL_SECONDS")
        .unwrap_or_else(|_| "200".to_string())
        .parse()
        .unwrap_or(200);

    loop {
        // CHANGED: Receive from SQS instead of Redis BRPOP
        let receive_res = sqs_client
            .receive_message()
            .queue_url(&sqs_url)
            .max_number_of_messages(1)
            .wait_time_seconds(20) // Long polling
            .send()
            .await;

        let response = match receive_res {
            Ok(res) => res,
            Err(e) => {
                error!("Worker #{}: SQS Receive Error: {}", i + 1, e);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        let messages = response.messages.unwrap_or_default();
        if messages.is_empty() {
            continue;
        }

        let message = &messages[0];
        let receipt_handle = message.receipt_handle.as_ref().unwrap();
        let body = message.body.as_ref().unwrap();

        // CHANGED: Deserialize directly from SQS body (No secondary Redis GET needed)
        let task: ExecutionTask = match serde_json::from_str(body) {
            Ok(t) => t,
            Err(e) => {
                error!("Worker #{}: Failed to deserialize task: {}", i + 1, e);
                // Delete poison message so it doesn't loop forever
                let _ = sqs_client.delete_message().queue_url(&sqs_url).receipt_handle(receipt_handle).send().await;
                continue;
            }
        };

        let mut conn = match client_clone.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(e) => {
                error!("Worker #{}: Redis connection failed: {}", i + 1, e);
                continue;
            }
        };

        QUEUE_DEPTH.with_label_values(&["evalx_jobs"]).dec();

        let task_result: Result<(), anyhow::Error> = match task {
            ExecutionTask::CompileBatch(compile_task) => {
                let parent_token = compile_task.parent_token.clone();
                
                async {
                    let status_key = format!("status:{}", &parent_token);
                    info!("Worker #{}: Processing CompileBatch task {}", i + 1, parent_token);

                    let _ = conn.set::<_, _, ()>(&status_key, "processing").await;
                    manager_clone.notify_completion(&parent_token, &vec![], SubmissionStatus::Processing);

                    let first_req = match compile_task.requests.first() {
                        Some(req) => req,
                        None => { return Ok(()); }
                    };

                    let lang = first_req.language.as_ref().unwrap().trim();
                    let version = first_req.version.as_ref().unwrap().trim();
                    let code = first_req.code.as_ref().unwrap().trim();
                    let lang_key = format!("{}:{}", lang, version);
                    let default_timeout = first_req.timeout.unwrap_or(10);

                    let config = match executor_clone.language_registry.get(&lang_key) {
                        Some(cfg) => cfg,
                        None => {
                             let err_res = vec![EvaluationResult::error_result(format!("Unsupported language: {}", lang_key))];
                             let _ = conn.set_ex::<_, _, ()>(format!("result:{}", parent_token), serde_json::to_string(&err_res).unwrap_or_default(), result_ttl).await;
                             let _ = conn.set::<_, _, ()>(&status_key, "failed").await;
                             manager_clone.notify_completion(&parent_token, &err_res, SubmissionStatus::Failed);
                             return Ok(());
                        }
                    };

                    match executor_clone.clone().compile_and_get_artifact(config, code, pool_clone.clone()).await {
                        Err(compile_error) => {
                            let results = vec![compile_error];
                            let _ = conn.set_ex::<_, _, ()>(format!("result:{}", parent_token), serde_json::to_string(&results).unwrap_or_default(), result_ttl).await;
                            let _ = conn.set::<_, _, ()>(&status_key, "completed").await;
                            manager_clone.notify_completion(&parent_token, &results, SubmissionStatus::Completed);
                        },
                        Ok((artifact_binary, compile_time)) => {
                            info!("Worker #{}: Compilation success. Fan-out {} test cases to SQS.", i + 1, compile_task.requests.len());
                            
                            let artifact_key = format!("artifact:{}", parent_token);
                            let counter_key = format!("counter:{}", parent_token);
                            let lang_key_redis = format!("lang:{}", parent_token);
                            let timeout_key_redis = format!("timeout:{}", parent_token);
                            let compile_time_key = format!("compile_time:{}", parent_token);

                            let mut pipe = redis::pipe();
                            pipe.set_ex(&artifact_key, artifact_binary, 3600);
                            pipe.set(&counter_key, compile_task.requests.len());
                            pipe.set(&lang_key_redis, lang_key);
                            pipe.set(&timeout_key_redis, default_timeout);
                            pipe.set(&compile_time_key, compile_time);
                            pipe.query_async::<_, ()>(&mut conn).await?;

                            // CHANGED: Fan-out to SQS loop instead of Redis Pipeline
                            for (index, request) in compile_task.requests.into_iter().enumerate() {
                                let run_task = RunTask {
                                    parent_token: parent_token.clone(),
                                    index,
                                    artifact_key: artifact_key.clone(),
                                    stdin: request.stdin.clone(),
                                };
                                let sub_task = ExecutionTask::RunTestcase(run_task);
                                let body = serde_json::to_string(&sub_task)?;

                                if let Err(e) = sqs_client
                                    .send_message()
                                    .queue_url(&sqs_url)
                                    .message_body(body)
                                    .send()
                                    .await 
                                {
                                    error!("Worker #{}: Failed to send testcase {} to SQS: {}", i+1, index, e);
                                }
                            }
                        }
                    }
                    Ok(())
                }.await
            },

            ExecutionTask::RunTestcase(run_task) => {
                let parent_token = run_task.parent_token.clone();
                let index = run_task.index;

                async {
                    debug!("Worker #{}: Processing RunTestcase task {}:{}", i + 1, parent_token, index);
                    
                    let kill_key = format!("kill:{}", parent_token);
                    let kill_signal: Option<String> = conn.get(&kill_key).await?;
                    let compile_time_key = format!("compile_time:{}", parent_token);
                    let compile_time: f64 = conn.get::<_, Option<f64>>(&compile_time_key).await?.unwrap_or(0.0);

                    let mut result = if kill_signal.is_some() {
                        EvaluationResult::error_result("Skipped (previous test case failed)")
                    } else {
                        // Redis for State is PRESERVED
                        let artifact_key = run_task.artifact_key.clone();
                        let lang_key: String = conn.get::<_, Option<String>>(format!("lang:{}", parent_token)).await?.ok_or(anyhow!("Missing lang"))?;
                        let time_limit: u64 = conn.get::<_, Option<u64>>(format!("timeout:{}", parent_token)).await?.ok_or(anyhow!("Missing timeout"))?;
                        let config = executor_clone.language_registry.get(&lang_key).ok_or(anyhow!("Missing config"))?;
                        let artifact_binary: Vec<u8> = conn.get::<_, Option<Vec<u8>>>(&artifact_key).await?.ok_or(anyhow!("Missing artifact"))?;

                        let run_res = executor_clone.clone().run_single_test_case(config, &artifact_binary, &run_task.stdin, time_limit, pool_clone.clone()).await;
                        
                        if run_res.exit_code != 0 {
                            let _ = conn.set_ex::<_, _, ()>(&kill_key, "true", 3600).await;
                        }
                        run_res
                    };
                    result.compile_time = compile_time;

                    let results_hash_key = format!("results:{}", parent_token);
                    let serialized_result = serde_json::to_string(&result)?;
                    conn.hset::<_, _, _, ()>(&results_hash_key, index, serialized_result).await?;

                    let counter_key = format!("counter:{}", parent_token);
                    let count: i64 = conn.decr(&counter_key, 1).await?;

                    if count == 0 {
                        info!("Worker #{}: Fan-in: Last worker for task {}. Assembling results.", i+1, parent_token);
                        
                        let results_map: HashMap<String, String> = conn.hgetall(&results_hash_key).await?;
                        let mut sorted: Vec<(usize, EvaluationResult)> = Vec::new();
                        for (k, v) in results_map {
                            if let Ok(idx) = k.parse::<usize>() {
                                if let Ok(r) = serde_json::from_str(&v) { sorted.push((idx, r)); }
                            }
                        }
                        sorted.sort_by_key(|(i, _)| *i);
                        let final_res: Vec<EvaluationResult> = sorted.into_iter().map(|(_, r)| r).collect();
                        
                        let result_key = format!("result:{}", parent_token);
                        let status_key = format!("status:{}", parent_token);
                        let _ = conn.set_ex::<_, _, ()>(&result_key, serde_json::to_string(&final_res)?, result_ttl).await;
                        let _ = conn.set::<_, _, ()>(&status_key, "completed").await;
                        manager_clone.notify_completion(&parent_token, &final_res, SubmissionStatus::Completed);
                        
                        let _ = conn.del::<_, ()>(&[run_task.artifact_key, results_hash_key, counter_key, kill_key, format!("lang:{}", parent_token), format!("timeout:{}", parent_token), compile_time_key]).await;
                    }
                    Ok(())
                }.await
            }
        };

        if let Err(e) = task_result {
            error!("Worker #{}: Task processing failed: {}", i + 1, e);
            // DO NOT delete message if failed (SQS will retry)
        } else {
            // CHANGED: Delete from SQS on success
            if let Err(e) = sqs_client
                .delete_message()
                .queue_url(&sqs_url)
                .receipt_handle(receipt_handle)
                .send()
                .await 
            {
                error!("Worker #{}: Failed to delete message from SQS: {}", i + 1, e);
            }
        }
    }
}