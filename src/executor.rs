use tokio::process::Command;
use std::process::Stdio;
use tokio::io::AsyncWriteExt;
use tokio::fs;
use anyhow::{anyhow, Result};
use futures_util::future;
use hex::ToHex;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::SystemExt;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::caching::redis_client::RedisClient;
use crate::models::request::ExecutionRequest;
use crate::models::response::EvaluationResult;
use crate::sandbox::isolate::IsolateSandbox;
use crate::types::index::{CodeExecutor, ConcurrencyState};
use std::env;
use crate::sandbox::local_pool::LocalSandboxPool;

impl CodeExecutor {
    async fn calculate_optimal_concurrency(
        &self,
        language: &str,
        version: &str,
        batch_size: usize,
    ) -> usize {
        let QUEUE_DEPTH_THRESHOLD: i64 = env::var("QUEUE_DEPTH_THRESHOLD")
            .expect("QUEUE_DEPTH_THRESHOLD must be set in environment")
            .parse()
            .expect("QUEUE_DEPTH_THRESHOLD must be a valid i64");
        let HYSTERESIS_SECONDS: u64 = env::var("HYSTERESIS_SECONDS")
            .expect("HYSTERESIS_SECONDS must be set in environment")
            .parse()
            .expect("HYSTERESIS_SECONDS must be a valid u64");
        let language_version = format!("{}:{}", language, version);
        let queue_key = format!("queue:{}", language_version);
        let determined_state;

        let queue_depth: i64 = {
            let conn_result = self.redis_client.get_multiplexed_async_connection().await;
            if let Ok(mut conn) = conn_result {
                redis::cmd("LLEN")
                    .arg(&queue_key)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or(0)
            } else {
                warn!("Could not get Redis connection to check queue depth. Defaulting to 0.");
                0
            }
        };
        let mut state_lock = self.concurrency_state.write().await;
        let (last_state, last_change_time) = *state_lock;

        if queue_depth >= QUEUE_DEPTH_THRESHOLD {
            determined_state = ConcurrencyState::Strained;
            if last_state != ConcurrencyState::Strained {
                info!(
                    queue_key,
                    "Queue depth ({}) exceeded threshold. Switching to Strained mode.", queue_depth
                );
                *state_lock = (ConcurrencyState::Strained, Instant::now());
            }
        } else if last_state == ConcurrencyState::Strained
            && last_change_time.elapsed().as_secs() < HYSTERESIS_SECONDS
        {
            determined_state = ConcurrencyState::Strained;
            debug!(
                queue_key,
                "In hysteresis cooldown. Remaining in Strained mode."
            );
        } else {
            determined_state = ConcurrencyState::Nominal;
            if last_state != ConcurrencyState::Nominal {
                info!(
                    queue_key,
                    "Queue depth ({}) is below threshold. Switching back to Nominal mode.",
                    queue_depth
                );
                *state_lock = (ConcurrencyState::Nominal, Instant::now());
            }
        }

        drop(state_lock);
        let base_limit = match determined_state {
            ConcurrencyState::Nominal => 20,
            ConcurrencyState::Strained => match language {
                "java" | "java11" | "java21" => 8,
                "c" | "cpp" => 12,
                "python" | "javascript" => 10,
                _ => 8,
            },
        };
        let mut limit = base_limit.min(batch_size);

        let mut sys = self.system.lock().await;
        sys.refresh_memory();
        let total_mem = sys.total_memory();
        let used_mem = sys.used_memory();

        if total_mem > 0 {
            let mem_usage_percent = (used_mem as f64 / total_mem as f64) * 100.0;
            if mem_usage_percent > 80.0 {
                warn!(
                    "High memory usage ({:.2}%) detected. Applying emergency brake, halving concurrency.",
                    mem_usage_percent
                );
                limit = (limit / 2).max(1);
            }
        }

        limit
    }

    pub async fn execute_batch(
        self: Arc<Self>,
        requests: Vec<ExecutionRequest>,
        pool: Arc<LocalSandboxPool>,
        token: Option<String>,
    ) -> Result<Vec<EvaluationResult>> {
        if requests.is_empty() {
            return Ok(vec![]);
        }
        
        let start_time = Instant::now();
        let first_request = &requests[0];
        let base_language = first_request.language.as_ref().unwrap().trim().to_string();
        let base_version = first_request.version.as_ref().unwrap().trim().to_string();
        let base_code = first_request.code.as_ref().unwrap().trim().to_string();

        let lang_key = format!("{}:{}", base_language, base_version);
        let lang_config = self
            .language_registry
            .get(&lang_key)
            .ok_or_else(|| anyhow!("Unsupported language or version: {}", lang_key))?;
    


        info!(
            "Executing Isolate batch for language: {}, with {} test cases",
            base_language,
            requests.len()
        );

        let sandbox = IsolateSandbox;
        let mut compile_time = 0.0;
        let artifact: Vec<u8>;

        if lang_config.is_compiled {
            let code_hash = Sha256::digest(base_code.as_bytes()).encode_hex::<String>();
            let artifact_key = format!("evalx:artifact:isolate:{}:{}", &base_language, &code_hash);

            if let Some(cached_artifact) = self.redis_client
                .get_artifact_async(&artifact_key, &base_language)
                .await?
            {
                debug!("Using cached compilation artifact for {}", code_hash);
                artifact = cached_artifact.binary;
            } else {
                debug!("Compiling code with Isolate for language: {}", base_language);
                let compile_result = sandbox
                    .compile(&lang_config, &base_code, pool.clone())
                    .await?;
                compile_time = compile_result.compile_time;

                if !compile_result.success {
                    let error_result = EvaluationResult {
                        compile_time,
                        stdout: String::new(),
                        stderr: Some(compile_result.stderr),
                        exit_code: 1,
                        run_time: 0.0,
                        space_consumed: "0 KB".to_string(),
                    };
                    return Ok(vec![error_result; requests.len()]);
                }
                artifact = compile_result.binary.unwrap();
                self.redis_client
                    .set_artifact_async(&artifact_key, &base_language, &base_code, &artifact)
                    .await?;
            }
        } else {
            artifact = base_code.as_bytes().to_vec();
        }

        let shared_artifact = Arc::new(artifact);
        
        let default_timeout: u64 = env::var("DEFAULT_TIMEOUT")
            .unwrap_or_else(|_| "10".to_string())
            .parse()
            .unwrap_or(10);

        // --- REFACTORED: Thundering Herd Fix ---
        let mut tasks: Vec<JoinHandle<Result<EvaluationResult>>> = Vec::new();

        for request in requests {
            // Acquire a permit from the main semaphore BEFORE spawning the task.
            // This ensures we don't start more concurrent tasks than the system can handle.
            let permit = self.semaphore.clone().acquire_owned().await?;
            
            let config_clone = Arc::clone(&lang_config);
            let artifact_clone = Arc::clone(&shared_artifact);
            let pool_clone = pool.clone();

            let task = tokio::spawn(async move {
                let _permit = permit;

                let sandbox = IsolateSandbox;
                let time_limit = request.timeout.unwrap_or(default_timeout);

                let run_result = sandbox
                    .run(
                        &config_clone,
                        &artifact_clone,
                        &request.stdin,
                        time_limit,
                        pool_clone,
                    )
                    .await?;
                
                Ok(EvaluationResult {
                    compile_time: 0.0, // Compile time is added later
                    stdout: run_result.stdout,
                    stderr: if run_result.stderr.is_empty() {
                        None
                    } else {
                        Some(run_result.stderr)
                    },
                    exit_code: run_result.exit_code,
                    run_time: run_result.run_time,
                    space_consumed: format!("{} KB", run_result.memory_used_kb),
                })
            });
            tasks.push(task);
        }

        let task_outputs = future::join_all(tasks).await;

        let results: Vec<EvaluationResult> = task_outputs.into_iter().map(|res| {
            match res {
                Ok(Ok(mut result)) => { // Successfully executed
                    result.compile_time = compile_time;
                    result
                },
                Ok(Err(e)) => { // Execution failed with an application error
                    error!("Isolate task execution failed: {}", e);
                     let error_msg = e.to_string();
                    if error_msg.contains("currently in use by another process") || error_msg.contains("Failed to acquire a sandbox box ID") {
                        EvaluationResult::error_result(
                            "System busy: Execution resource temporarily unavailable".to_string(),
                        )
                    } else if error_msg.contains("No such file or directory") {
                        EvaluationResult::error_result(
                            "System error: Execution environment not available".to_string(),
                        )
                    } else {
                        EvaluationResult::error_result(format!("Execution failed: {}", error_msg))
                    }
                },
                Err(e) => { // Task panicked or was cancelled
                    error!("Tokio task failed to execute: {}", e);
                    EvaluationResult::error_result(format!("System error: Task failed to run: {}", e))
                }
            }
        }).collect();
        // --- END REFACTOR ---


        if let Some(token) = token {
            let result_key = format!("result:{}", token);
            if let Err(e) = self.redis_client
                .set_in_cache_async(&result_key, &results, 3600)
                .await
            {
                warn!(
                    "Failed to store final batch results for token {}: {}",
                    token, e
                );
            }
        }

        info!(
            "Isolate batch of {} requests completed in {:.4}s",
            results.len(),
            start_time.elapsed().as_secs_f64()
        );
        let total_execution_time = start_time.elapsed().as_secs_f64();
        crate::monitoring::metrics::EXECUTION_TIME_SECONDS
            .with_label_values(&[&base_language])
            .observe(total_execution_time);

        Ok(results)
    }
}

