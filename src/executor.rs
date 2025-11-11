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
use crate::sandbox::isolate::{IsolateSandbox, PrewarmedSandbox};
use crate::types::index::{CodeExecutor, ConcurrencyState};
use std::env;
use crate::sandbox::local_pool::LocalSandboxPool;
use crate::languages::config::LanguageConfig;


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
                "java" |
                "java11" | "java21" => 8,
                "c" |
                "cpp" => 12,
                "python" |
                "javascript" => 10,
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

    pub async fn compile_and_get_artifact(
        self: Arc<Self>,
        config: Arc<LanguageConfig>,
        code: &str,
        pool: Arc<LocalSandboxPool>,
    ) -> Result<(Vec<u8>, f64), EvaluationResult> {
        let sandbox = IsolateSandbox;
        let language = &config.name;
    
        if config.is_compiled {
            let code_hash = Sha256::digest(code.as_bytes()).encode_hex::<String>();
            let artifact_key = format!("evalx:artifact:isolate:{}:{}", language, &code_hash);
    
            if let Ok(Some(cached_artifact)) = self.redis_client
                .get_artifact_async(&artifact_key, language)
                .await
            {
                debug!("Using cached compilation artifact for {}", code_hash);
                return Ok((cached_artifact.binary, 0.0));
            }
    
            debug!("Compiling code with Isolate for language: {}", language);
            let compile_result = match sandbox
                .compile(&config, code, pool.clone())
                .await
            {
                Ok(res) => res,
                Err(e) => {
              
                     error!("Compilation sandbox failed catastrophically: {}", e);
                    return Err(EvaluationResult::error_result(format!("Compiler failed: {}", e)));
                }
            };
            if !compile_result.success {
                let error_result = EvaluationResult {
                    compile_time: compile_result.compile_time,
                    stdout: String::new(),
                    stderr: Some(compile_result.stderr),
               
                     exit_code: 1,
                    run_time: 0.0,
                    space_consumed: "0 KB".to_string(),
                };
                return Err(error_result);
            }
    
            let artifact = compile_result.binary.unwrap_or_default();
            if let Err(e) = self.redis_client
                .set_artifact_async(&artifact_key, language, code, &artifact)
                .await
            {
                warn!("Failed to cache compilation artifact: {}", e);
            }
            Ok((artifact, compile_result.compile_time))
        } else {
            Ok((code.as_bytes().to_vec(), 0.0))
        }
    }
    
    pub async fn run_single_test_case(
        self: Arc<Self>,
        config: Arc<LanguageConfig>,
        artifact_binary: &[u8],
        stdin: &str,
        time_limit_s: u64,
        pool: Arc<LocalSandboxPool>,
    ) -> EvaluationResult {
        let sandbox_run = IsolateSandbox;
    
        match sandbox_run.run(
            &config,
            artifact_binary,
            stdin,
            time_limit_s,
            pool,
        ).await {
            Ok(run_result) => {
                let total_execution_time = run_result.run_time;
                crate::monitoring::metrics::EXECUTION_TIME_SECONDS
                    .with_label_values(&[&config.name])
                    .observe(total_execution_time);
    
                EvaluationResult {
                    compile_time: 0.0,
                    stdout: run_result.stdout,
                    stderr: if run_result.stderr.is_empty() { None } else { Some(run_result.stderr) },
                    exit_code: run_result.exit_code,
                    run_time: run_result.run_time,
                    space_consumed: format!("{} KB", run_result.memory_used_kb),
                }
            }
            Err(e) => {
                error!("Test case failed with sandbox error: {}", e);
                EvaluationResult::error_result(format!("Execution failed: {}", e))
            }
        }
    }
}
