use anyhow::Result;
use futures_util::future::join_all;
use hex::ToHex;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Instant;
use tokio::task;
use tracing::{debug, error, info, warn};
use crate::caching::redis_client::RedisClient;
use crate::models::request::ExecutionRequest;
use crate::models::response::EvaluationResult;
use crate::sandbox::isolate::IsolateSandbox;
use crate::types::index::CodeExecutor;

impl CodeExecutor {
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
        info!(
            "Executing Isolate batch for language: {}, with {} test cases",
            base_language,
            requests.len()
        );
        let is_compiled = matches!(base_language.as_str(), "c" | "cpp" | "java" | "java11" | "java21");
        let timeout_s = first_request.timeout.unwrap_or(10) as f64;
        let memory_limit_kb = 256 * 1024; // Default 256MB
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
                 .compile(&base_language, &base_code, 10, 512 * 1024)
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
        let mut tasks = Vec::new();
        for request in requests {
            let lang_clone = base_language.clone();
            let artifact_clone = Arc::clone(&shared_artifact);
            let task = task::spawn(async move {
                let sandbox = IsolateSandbox;
                let run_result = sandbox
                 .run(
                        &lang_clone,
                        &artifact_clone,
                        &request.stdin,
                        timeout_s,
                        memory_limit_kb,
                    )
                 .await?;
                Ok::<_, anyhow::Error>(EvaluationResult {
                    compile_time: 0.0,
                    stdout: run_result.stdout,
                    stderr: if run_result.stderr.is_empty() { None } else { Some(run_result.stderr) },
                    exit_code: run_result.exit_code,
                    run_time: run_result.run_time,
                    space_consumed: format!("{} KB", run_result.memory_used_kb),
                })
            });
            tasks.push(task);
        }

        let results: Vec<EvaluationResult> = join_all(tasks)
         .await
         .into_iter()
         .map(|res| match res {
                Ok(Ok(mut result)) => {
                    result.compile_time = compile_time;
                    result
                }
                Ok(Err(e)) => {
                    error!("Isolate task execution failed: {}", e);
                    EvaluationResult::error_result(format!("Execution failed: {}", e))
                }
                Err(e) => {
                    error!("Isolate task panicked: {}", e);
                    EvaluationResult::error_result(format!("Task panic: {}", e))
                }
            })
         .collect();

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