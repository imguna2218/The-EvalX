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

// ADDED: AWS SQS Imports for Autoscaling Checks
use aws_config::BehaviorVersion;
use aws_sdk_sqs::Client as SqsClient;
use aws_sdk_sqs::types::QueueAttributeName;

impl CodeExecutor {
    // Calculates concurrency limit based on GLOBAL SQS Queue Depth
    async fn calculate_optimal_concurrency(
        &self,
        language: &str,
        _version: &str, 
        batch_size: usize,
    ) -> usize {
        let queue_depth_threshold: i64 = env::var("QUEUE_DEPTH_THRESHOLD")
            .unwrap_or_else(|_| "100".to_string())
            .parse()
            .expect("QUEUE_DEPTH_THRESHOLD must be a valid i64");

        let hysteresis_seconds: u64 = env::var("HYSTERESIS_SECONDS")
            .unwrap_or_else(|_| "30".to_string())
            .parse()
            .expect("HYSTERESIS_SECONDS must be a valid u64");
            
        let sqs_url = env::var("SQS_QUEUE_URL").expect("SQS_QUEUE_URL must be set");

        // Initialize SQS Client specifically for this check.
        // (Since we cannot modify the CodeExecutor struct definition to hold the client without breaking other files)
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let sqs_client = SqsClient::new(&config);

        let queue_depth: i64 = match sqs_client
            .get_queue_attributes()
            .queue_url(&sqs_url)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .send()
            .await 
        {
            Ok(res) => {
                res.attributes
                    .and_then(|attrs| attrs.get(&QueueAttributeName::ApproximateNumberOfMessages).cloned())
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(0)
            },
            Err(e) => {
                warn!("Failed to fetch SQS queue depth: {}. Defaulting to 0 for autoscaling.", e);
                0
            }
        };

        let determined_state;
        let mut state_lock = self.concurrency_state.write().await;
        let (last_state, last_change_time) = *state_lock;

        if queue_depth >= queue_depth_threshold {
            determined_state = ConcurrencyState::Strained;
            if last_state != ConcurrencyState::Strained {
                info!(
                    "SQS Global Queue Depth ({}) exceeded threshold. Switching to Strained mode.", 
                    queue_depth
                );
                *state_lock = (ConcurrencyState::Strained, Instant::now());
            }
        } else if last_state == ConcurrencyState::Strained
            && last_change_time.elapsed().as_secs() < hysteresis_seconds
        {
            determined_state = ConcurrencyState::Strained;
            debug!("In hysteresis cooldown. Remaining in Strained mode.");
        } else {
            determined_state = ConcurrencyState::Nominal;
            if last_state != ConcurrencyState::Nominal {
                info!(
                    "SQS Global Queue Depth ({}) is below threshold. Switching back to Nominal mode.",
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

        // Memory Pressure Check
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
            // Redis is still used for Artifact Caching (Correct)
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