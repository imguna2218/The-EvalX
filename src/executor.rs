use anyhow::{anyhow, Result};
use futures_util::{stream, StreamExt};
use hex::ToHex;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{Instant, Duration};
use sysinfo::{SystemExt};
use tracing::{debug, error, info, warn};
use crate::caching::redis_client::RedisClient;
use crate::models::request::ExecutionRequest;
use crate::models::response::EvaluationResult;
use crate::sandbox::isolate::IsolateSandbox;
use crate::types::index::{CodeExecutor, ConcurrencyState};
use std::env;

impl CodeExecutor {
    /// MODIFIED: warm-up now fetches the Java config from the registry.
    async fn warmup_java_environment(&self) -> Result<()> {
        info!("Attempting to warm up Java environment...");
        // Fetch the config for the primary Java version to warm up.
        let lang_key = "java:21";
        let java_config = match self.language_registry.get(lang_key) {
            Some(config) => config,
            None => {
                warn!("Could not find configuration for '{}'. Skipping Java warm-up.", lang_key);
                return Ok(());
            }
        };

        let warmup_code = r#"
        public class Warmup {
            public static void main(String[] args) {
                System.out.println("Warmup completed");
            }
        }
        "#.to_string();
        
        let sandbox = IsolateSandbox;
        // MODIFIED: Pass the full config object to the compile function.
        match sandbox.compile(&java_config, &warmup_code).await {
            Ok(_) => info!("Java environment warm-up completed successfully."),
            Err(e) => warn!("Java warm-up failed: {}, but continuing...", e),
        }
        
        Ok(())
    }

    /// This function remains unchanged for now, as its logic is about orchestration,
    /// not language-specifics. It could be refactored later to pull limits
    /// from the TOML files as well.
    async fn calculate_optimal_concurrency(&self, language: &str, version: &str, batch_size: usize) -> usize {
        // ... (existing implementation remains the same)
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
                redis::cmd("LLEN").arg(&queue_key).query_async(&mut conn).await.unwrap_or(0)
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
                info!(queue_key, "Queue depth ({}) exceeded threshold. Switching to Strained mode.", queue_depth);
                *state_lock = (ConcurrencyState::Strained, Instant::now());
            }
        } else if last_state == ConcurrencyState::Strained && last_change_time.elapsed().as_secs() < HYSTERESIS_SECONDS {
            determined_state = ConcurrencyState::Strained;
            debug!(queue_key, "In hysteresis cooldown. Remaining in Strained mode.");
        } else {
            determined_state = ConcurrencyState::Nominal;
             if last_state != ConcurrencyState::Nominal {
                info!(queue_key, "Queue depth ({}) is below threshold. Switching back to Nominal mode.", queue_depth);
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
        redis_client: RedisClient,
        token: Option<String>,
    ) -> Result<Vec<EvaluationResult>> {
        if requests.is_empty() {
            return Ok(vec![]);
        }
        let _permit = self.semaphore.acquire().await?;
        let start_time = Instant::now();
        let first_request = &requests[0];
        let base_language = first_request.language.as_ref().unwrap().trim().to_string();
        let base_version = first_request.version.as_ref().unwrap().trim().to_string();
        let base_code = first_request.code.as_ref().unwrap().trim().to_string();

        // --- MODIFIED: Fetch LanguageConfig from the registry ---
        let lang_key = format!("{}:{}", base_language, base_version);
        let lang_config = self.language_registry.get(&lang_key)
            .ok_or_else(|| anyhow!("Unsupported language or version: {}", lang_key))?;
        // ---

        // Java warm-up system
        if base_language.starts_with("java") {
            let now = Instant::now();
            let last_warmup = *self.last_java_warmup.read().await;
            if now.duration_since(last_warmup) > Duration::from_secs(120) {
                info!("Triggering Java environment warm-up");
                let executor_clone = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = executor_clone.warmup_java_environment().await {
                        warn!("Background Java warm-up failed: {}", e);
                    } else {
                        *executor_clone.last_java_warmup.write().await = Instant::now();
                    }
                });
            }
        }
        
        info!(
            "Executing Isolate batch for language: {}, with {} test cases",
            base_language,
            requests.len()
        );
        
        let sandbox = IsolateSandbox;
        let mut compile_time = 0.0;
        let artifact: Vec<u8>;

        // MODIFIED: Use `lang_config.is_compiled` instead of hardcoded match
        if lang_config.is_compiled {
            let code_hash = Sha256::digest(base_code.as_bytes()).encode_hex::<String>();
            let artifact_key = format!("evalx:artifact:isolate:{}:{}", &base_language, &code_hash);
            
            if let Some(cached_artifact) = redis_client.get_artifact_async(&artifact_key, &base_language).await? {
                debug!("Using cached compilation artifact for {}", code_hash);
                artifact = cached_artifact.binary;
            } else {
                debug!("Compiling code with Isolate for language: {}", base_language);
                // MODIFIED: Pass the full config object to compile
                let compile_result = sandbox.compile(&lang_config, &base_code).await?;
                compile_time = compile_result.compile_time;

                if !compile_result.success {
                    let error_result = EvaluationResult {
                        compile_time,
                        stdout: String::new(),
                        stderr: Some(compile_result.stderr),
                        exit_code: 1,
                        run_time: 0.0,
                        space_consumed: "0 MB".to_string(),
                    };
                    return Ok(vec![error_result; requests.len()]);
                }
                artifact = compile_result.binary.unwrap();
                redis_client.set_artifact_async(&artifact_key, &base_language, &base_code, &artifact).await?;
            }
        } else {
            artifact = base_code.as_bytes().to_vec();
        }

        let shared_artifact = Arc::new(artifact);
        let concurrency_limit = self.calculate_optimal_concurrency(&base_language, &base_version, requests.len()).await;
        info!("Running batch with adaptive concurrency limit of {}", concurrency_limit);

        // FIX: Read the default timeout from the environment once per batch.
        let default_timeout: u64 = env::var("DEFAULT_TIMEOUT")
            .unwrap_or_else(|_| "10".to_string())
            .parse()
            .unwrap_or(10);

        let results_stream = stream::iter(requests).map(|request| {
            let config_clone = Arc::clone(&lang_config);
            let artifact_clone = Arc::clone(&shared_artifact);
            
            async move {
                let sandbox = IsolateSandbox;
            
                // FIX: Determine the final time limit. Priority is: user request > .env default.
                let time_limit = request.timeout.unwrap_or(default_timeout);

                // FIX: Pass the final time limit to the run function.
                match sandbox.run(&config_clone, &artifact_clone, &request.stdin, time_limit).await {
                    Ok(run_result) => Ok(EvaluationResult {
                        compile_time: 0.0,
                        stdout: run_result.stdout,
                        stderr: if run_result.stderr.is_empty() { None } else { Some(run_result.stderr) },
                        exit_code: run_result.exit_code,
                        run_time: run_result.run_time,
                        space_consumed: format!("{} KB", run_result.memory_used_kb),
                    }),
                    Err(e) => {
                        error!("Isolate task execution failed: {}", e);
                        Err(e)
                    }
                }
            }
        }).buffer_unordered(concurrency_limit);
        
        let results: Vec<EvaluationResult> = results_stream.map(|res| {
            match res {
                Ok(mut result) => {
                    result.compile_time = compile_time;
                    result
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    if error_msg.contains("currently in use by another process") {
                        EvaluationResult::error_result("System busy: Execution resource temporarily unavailable".to_string())
                    } else if error_msg.contains("No such file or directory") {
                        EvaluationResult::error_result("System error: Execution environment not available".to_string())
                    } else {
                        EvaluationResult::error_result(format!("Execution failed: {}", error_msg))
                    }
                }
            }
        }).collect().await;

        if let Some(token) = token {
            let result_key = format!("result:{}", token);
            if let Err(e) = redis_client.set_in_cache_async(&result_key, &results, 3600).await {
                warn!("Failed to store final batch results for token {}: {}", token, e);
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