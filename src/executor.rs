use anyhow::Result;
use futures_util::{stream, StreamExt};
use hex::ToHex;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{Instant, Duration};
use sysinfo::{SystemExt, System};
use tracing::{debug, error, info, warn};
use crate::caching::redis_client::RedisClient;
use crate::models::request::ExecutionRequest;
use crate::models::response::EvaluationResult;
use crate::sandbox::isolate::IsolateSandbox;
use crate::types::index::CodeExecutor;

impl CodeExecutor {

    // warm up for java 
    async fn warmup_java_environment(&self) -> Result<()> {
        let warmup_code = r#"
        public class Warmup {
            public static void main(String[] args) {
                System.out.println("Warmup completed");
            }
        }
        "#.to_string();
        
        let sandbox = crate::sandbox::isolate::IsolateSandbox;
        let result = sandbox.compile("java", &warmup_code).await;
        
        match result {
            Ok(_) => info!("Java environment warm-up completed successfully"),
            Err(e) => warn!("Java warm-up failed: {}, but continuing...", e),
        }
        
        Ok(())
    }

    /// ADDED: Calculates the optimal number of parallel tasks for a batch.
    /// It sets limits based on language and applies progressive backoff if memory is high.
    async fn calculate_optimal_concurrency(&self, language: &str, batch_size: usize) -> usize {
        // Base limits tuned for a dedicated deployment system.
        let base_limit = match language {
            "java" | "java11" | "java21" => 10,
            "c" | "cpp" => 20,
            "python" | "javascript" => 15,
            _ => 10, // A conservative default for any other language.
        };
        let limit = base_limit.min(batch_size);

        // Progressive Backoff: Check system memory usage.
        let mut sys = self.system.lock().await;
        sys.refresh_memory();
        let total_mem = sys.total_memory();
        let used_mem = sys.used_memory();
        
        // Ensure total_mem is not zero to avoid division by zero.
        if total_mem == 0 {
            return limit;
        }

        let mem_usage_percent = (used_mem as f64 / total_mem as f64) * 100.0;

        if mem_usage_percent > 80.0 {
            warn!(
                "High memory usage ({:.2}%) detected. Reducing batch concurrency.",
                mem_usage_percent
            );
            // Reduce concurrency by half, but ensure at least 1 task runs.
            (limit / 2).max(1)
        } else {
            limit
        }
    }

    /// Executes a batch of requests using the high-speed Isolate sandbox.
    /// This is the sole, unified execution function for the entire application.
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
        let base_code = first_request.code.as_ref().unwrap().trim().to_string();
        
        // Java warm-up system - trigger warm-up if Java and last warm-up was >2 minutes ago
        if base_language.starts_with("java") {
            let now = Instant::now();
            let last_warmup = *self.last_java_warmup.read().unwrap();
            if now.duration_since(last_warmup) > Duration::from_secs(120) {
                info!("Triggering Java environment warm-up");
                let executor_clone = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = executor_clone.warmup_java_environment().await {
                        warn!("Background Java warm-up failed: {}", e);
                    } else {
                        *executor_clone.last_java_warmup.write().unwrap() = Instant::now();
                    }
                });
            }
        }
        
        info!(
            "Executing Isolate batch for language: {}, with {} test cases",
            base_language,
            requests.len()
        );
        let is_compiled = matches!(base_language.as_str(), "c" | "cpp" | "java" | "java11" | "java21");
        let sandbox = IsolateSandbox;
        let mut compile_time = 0.0;
        let artifact: Vec<u8>;

        if is_compiled {
            let code_hash = Sha256::digest(base_code.as_bytes()).encode_hex::<String>();
            let artifact_key = format!("evalx:artifact:isolate:{}:{}", &base_language, &code_hash);
            if let Some(cached_artifact) = redis_client.get_artifact_async(&artifact_key).await? {
                debug!("Using cached compilation artifact for {}", code_hash);
                artifact = cached_artifact.binary;
            } else {
                debug!("Compiling code with Isolate for language: {}", base_language);
                let compile_result = sandbox
                 .compile(&base_language, &base_code)
                 .await?;
                compile_time = compile_result.compile_time;
                if!compile_result.success {
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
                redis_client.set_artifact_async(&artifact_key, &base_code, &artifact, 3600).await?;
            }
        } else {
            artifact = base_code.as_bytes().to_vec();
        }

        let shared_artifact = Arc::new(artifact);
        
        // MODIFIED: Intelligent Concurrency Control
        let concurrency_limit = self.calculate_optimal_concurrency(&base_language, requests.len()).await;
        info!("Running batch with concurrency limit of {}", concurrency_limit);

        let results_stream = stream::iter(requests).map(|request| {
            let lang_clone = base_language.clone();
            let artifact_clone = Arc::clone(&shared_artifact);
            
            async move {
                let sandbox = IsolateSandbox;
                match sandbox.run(&lang_clone, &artifact_clone, &request.stdin).await {
                    Ok(run_result) => Ok(EvaluationResult {
                        compile_time: 0.0, // This will be set later
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
                    EvaluationResult::error_result(format!("Execution failed: {}", e))
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
        Ok(results)
    }
}