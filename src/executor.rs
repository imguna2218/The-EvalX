use anyhow::Result;
use anyhow::anyhow;
use bollard::container::{RemoveContainerOptions, StatsOptions, UploadToContainerOptions};
use bollard::exec::{CreateExecOptions, StartExecResults};
use futures_util::future::join_all;
use futures_util::stream::StreamExt;
use hex::ToHex;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tar::Builder;
use tokio::task;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::caching::redis_client::RedisClient;
use crate::compilers::artifact_handlers::{handle_c_artifact, handle_cpp_artifact, handle_java_artifact};
use crate::container_management::container_pool::{get_container, return_container};
use crate::models::request::ExecutionRequest;
use crate::models::response::EvaluationResult;
use crate::types::index::CodeExecutor;

impl CodeExecutor {
    pub async fn execute(
        self: Arc<Self>,
        request: ExecutionRequest,
        redis_client: RedisClient,
    ) -> Result<EvaluationResult> {
        let _permit = self.semaphore.acquire().await?;
        let _start_time = Instant::now();  // Prefixed to suppress warning

        let language = request.language.unwrap_or("".to_string()).trim().to_string();
        let version = request.version.unwrap_or("".to_string()).trim().to_string();
        let code = request.code.unwrap_or("".to_string()).trim().to_string();

        let is_compiled_language = matches!(language.as_str(), "java" | "java11" | "c" | "cpp");

        let language_config = self.language_configs.get(&language).ok_or_else(|| {
            anyhow!("Unsupported language: {}", language)
        })?.clone();

        let timeout_secs = request.timeout.unwrap_or(10) as f64;
        let timeout_duration = Duration::from_secs_f64(timeout_secs);

        let memory_limit = request.memory_limit.unwrap_or_else(|| "512m".to_string());
        let memory_bytes = parse_memory_limit(&memory_limit).unwrap_or(
            language_config.resource_limits.memory.parse::<i64>().unwrap_or(64 * 1024 * 1024)
        );

        let filename = match language.as_str() {
            "c" => "main.c",
            "cpp" => "main.cpp",
            "java" | "java11" => "Main.java",
            "python" => "main.py",
            "javascript" => "main.js",
            _ => "main",
        };

        let mut redis_client = redis_client;
        let (compile_time, run_cmd, compile_stderr, container_id) = if is_compiled_language {
            let container_id = get_container(&self, &language, &version)
                .await?
                .ok_or_else(|| anyhow!("No available container for {}:{}", language, version))?;
            let code_hash = Sha256::digest(&code).encode_hex::<String>();
            let artifact_key = format!("evalx:artifact:{}:{}:{}", &language, &version, &code_hash);
            let (compile_time, run_cmd, compile_stderr) = match language.as_str() {
                "c" => handle_c_artifact(&self, &container_id, filename, &code, &artifact_key, &mut redis_client, memory_bytes).await?,
                "cpp" => handle_cpp_artifact(&self, &container_id, filename, &code, &artifact_key, &mut redis_client, memory_bytes).await?,
                "java" | "java11" => handle_java_artifact(&self, &container_id, filename, &code, &artifact_key, &mut redis_client, memory_bytes).await?,
                _ => unreachable!(),
            };
            (compile_time, run_cmd, compile_stderr, container_id)
        } else {
            let container_id = get_container(&self, &language, &version)
                .await?
                .ok_or_else(|| anyhow!("No available container for {}:{}", language, version))?;
            let run_cmd = match language.as_str() {
                "python" => vec!["python".to_string(), "/app/main.py".to_string()],
                "javascript" => vec!["node".to_string(), "/app/main.js".to_string()],
                _ => language_config.command_format.clone(),
            };
            (0.0, run_cmd, Vec::new(), container_id)
        };

        let container_id_clone = container_id.clone();  // For logging after potential returns

        if !compile_stderr.is_empty() {
            let error_message = String::from_utf8_lossy(&compile_stderr).to_string();
            return_container(&self, &language, &version, &container_id).await;
            return Ok(EvaluationResult {
                compile_time,
                stdout: error_message.clone(),
                stderr: Some(error_message),
                exit_code: 1,
                run_time: 0.0,
                space_consumed: format!("{:.2} MB", memory_bytes as f64 / (1024.0 * 1024.0)),
            });
        }

        let result_key = format!(
            "evalx:exec:{}:{}:{}:{}:{}",
            &language,
            &version,
            timeout_secs,
            Sha256::digest(&code).encode_hex::<String>(),
            Sha256::digest(&request.stdin).encode_hex::<String>()
        );

        if let Ok(Some(cached_result)) = redis_client.get_from_cache(&result_key) {
            debug!("Result cache hit for key: {}", result_key);
            return_container(&self, &language, &version, &container_id).await;
            return Ok(EvaluationResult { compile_time: if is_compiled_language { compile_time } else { 0.0 }, ..cached_result });
        }

        if !is_compiled_language {
            let write_cmd = vec![
                "sh".to_string(),
                "-c".to_string(),
                format!("echo '{}' > /app/{}", code.replace("'", "'\\''"), filename),
            ];
            let write_exec = self.docker.create_exec(
                &container_id,
                CreateExecOptions { cmd: Some(write_cmd), attach_stdout: Some(true), attach_stderr: Some(true), ..Default::default() },
            ).await?;
            match timeout(Duration::from_secs(1), self.docker.start_exec(&write_exec.id, None)).await {
                Ok(Ok(_)) => debug!("Code written to /app/{}", filename),
                Ok(Err(e)) => {
                    return_container(&self, &language, &version, &container_id).await;
                    return Err(anyhow!("Failed to write code: {}", e));
                },
                Err(_) => {
                    return_container(&self, &language, &version, &container_id).await;
                    return Err(anyhow!("Timeout writing code"));
                },
            };
        }

        let run_start = Instant::now();
        let mut final_cmd = run_cmd.clone();

        debug!("Piping stdin for {}: '{}'", language, request.stdin);
        // Safe escaping for shell metachars
        let escaped_stdin = request.stdin.replace("\\", "\\\\").replace("$", "\\$").replace("`", "\\`").replace("\"", "\\\"").replace("'", "'\\''");
        final_cmd = vec![
            "sh".to_string(),
            "-c".to_string(),
            format!("printf '%s' '{}' | {}", escaped_stdin, run_cmd.join(" ")),
        ];

        debug!("Executing command: {:?}", final_cmd);
        let exec = self.docker.create_exec(
            &container_id,
            CreateExecOptions { cmd: Some(final_cmd), attach_stdout: Some(true), attach_stderr: Some(true), ..Default::default() },
        ).await?;

        let (stdout, stderr_data, exit_code) = match timeout(timeout_duration, async {
            let exec_result = self.docker.start_exec(&exec.id, None).await?;
            let mut output = Vec::new();
            let mut stderr = Vec::new();
            if let bollard::exec::StartExecResults::Attached { output: mut output_stream, .. } = exec_result {
                while let Some(Ok(log)) = output_stream.next().await {
                    match log {
                        bollard::container::LogOutput::StdOut { message } => output.extend_from_slice(&message),
                        bollard::container::LogOutput::StdErr { message } => stderr.extend_from_slice(&message),
                        _ => {}
                    }
                }
            }
            let inspect = self.docker.inspect_exec(&exec.id).await?;
            Ok::<_, anyhow::Error>((String::from_utf8_lossy(&output).to_string(), stderr, inspect.exit_code.unwrap_or(-1) as i64))
        }).await {
            Ok(Ok((stdout, stderr_data, exit_code))) => {
                debug!("Execution completed: stdout='{}', stderr='{}', exit_code={}", stdout, String::from_utf8_lossy(&stderr_data), exit_code);
                (stdout, stderr_data, exit_code)
            }
            Ok(Err(e)) => {
                return_container(&self, &language, &version, &container_id).await;
                return Err(anyhow!("Execution failed: {}", e));
            }
            Err(_) => {
                error!("Execution timed out");
                return_container(&self, &language, &version, &container_id).await;
                ("Time Limit Exceeded".to_string(), "Time Limit Exceeded".as_bytes().to_vec(), 124)
            }
        };
        let run_time = if exit_code == 124 { timeout_secs } else { run_start.elapsed().as_secs_f64() };

        // Get stats BEFORE final return_container
        let space_consumed = match self.docker.stats(&container_id, Some(StatsOptions { stream: false, one_shot: true })).next().await {
            Some(Ok(stats)) => format!("{:.2} MB", stats.memory_stats.usage.unwrap_or(0) as f64 / (1024.0 * 1024.0)),
            _ => format!("{:.2} MB", memory_bytes as f64 / (1024.0 * 1024.0)),
        };

        let result = EvaluationResult {
            compile_time: if is_compiled_language { compile_time } else { 0.0 },
            stdout,
            stderr: if stderr_data.is_empty() { None } else { Some(String::from_utf8_lossy(&stderr_data).to_string()) },
            exit_code,
            run_time,
            space_consumed,
        };

        if result.stderr.is_none() {
            if let Ok(_) = redis_client.set_in_cache(&result_key, &result, 3600) {
                debug!("Cached result for key: {}", result_key);
            }
        }

        return_container(&self, &language, &version, &container_id).await;
        Ok(result)
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

        let _permit = self.semaphore.acquire().await.map_err(|e| {
            error!("Failed to acquire semaphore: {}", e);
            anyhow!("Failed to acquire semaphore: {}", e)
        })?;
        let start_time = Instant::now();

        // --- 1. Validation and Setup ---
        let first_request = &requests[0];
        let base_language = first_request.language.as_ref().unwrap_or(&String::new()).trim().to_string();
        let base_version = first_request.version.as_ref().unwrap_or(&String::new()).trim().to_string();
        let base_code = first_request.code.as_ref().unwrap_or(&String::new()).trim().to_string();

        info!("Executing batch for language: {}, version: {}, with {} requests", base_language, base_version, requests.len());

        for (i, req) in requests.iter().enumerate() {
            if req.language.as_ref().unwrap_or(&String::new()).trim() != base_language
                || req.version.as_ref().unwrap_or(&String::new()).trim() != base_version
                || req.code.as_ref().unwrap_or(&String::new()).trim() != base_code
            {
                return Err(anyhow!("Mismatched language/version/code in batch at index {}", i));
            }
        }

        let is_compiled_language = matches!(base_language.as_str(), "java" | "java11" | "c" | "cpp");
        let language_config = self.language_configs.get(&base_language).ok_or_else(|| anyhow!("Unsupported language: {}", base_language))?.clone();
        let timeout_secs = first_request.timeout.unwrap_or(10) as f64;
        let timeout_duration = Duration::from_secs_f64(timeout_secs);
        let memory_limit = first_request.memory_limit.as_ref().unwrap_or(&"512m".to_string()).clone();
        let memory_bytes = parse_memory_limit(&memory_limit).unwrap_or(
            language_config.resource_limits.memory.parse::<i64>().unwrap_or(64 * 1024 * 1024)
        );
        let filename = match base_language.as_str() {
            "c" => "main.c", "cpp" => "main.cpp", "java" | "java11" => "Main.java",
            "python" => "main.py", "javascript" => "main.js", _ => "main",
        };
        let run_cmd = language_config.command_format.clone();


        // --- 2. Asynchronous Batch Cache Check ---
        let code_hash = Sha256::digest(&base_code).encode_hex::<String>();
        let full_batch_key = {
            let test_hashes: Vec<String> = requests.iter().map(|req| Sha256::digest(req.stdin.as_bytes()).encode_hex::<String>()).collect();
            let mut hasher = Sha256::new();
            for hash in &test_hashes { hasher.update(hash.as_bytes()); }
            let batch_key = hex::encode(hasher.finalize());
            format!("evalx:batch:{}:{}", code_hash, batch_key)
        };

        if let Some(cached_results) = redis_client.get_from_cache_async::<Vec<EvaluationResult>>(&full_batch_key).await? {
            debug!("Batch cache hit for key: {}", full_batch_key);
            redis_client.increment_cache_hit().await?;
            if let Some(token) = token {
                let result_key = format!("result:{}", token);
                if let Err(e) = redis_client.set_in_cache_async(&result_key, &cached_results, 3600).await {
                    warn!("Failed to store cached batch results for token {}: {}", token, e);
                }
            }
            return Ok(cached_results);
        }
        redis_client.increment_cache_miss().await?;

        // --- 3. Compile Once (if needed) and Fetch Artifact Before Parallel Loop ---
        let artifact_key = format!("evalx:artifact:{}:{}:{}", &base_language, &base_version, &code_hash);
        let mut compile_time = 0.0;
        let mut compile_stderr: Vec<u8> = Vec::new();
        
        let artifact_binary: Option<Arc<Vec<u8>>> = if is_compiled_language {
            if let Some(artifact) = redis_client.get_artifact_async(&artifact_key).await? {
                debug!("Using cached compilation artifact for {}", code_hash);
                Some(Arc::new(artifact.binary))
            } else {
                info!("Preparing to compile for language: {}, version: {}", base_language, base_version);
                let temp_id = get_container(&self, &base_language, &base_version).await?.ok_or_else(|| anyhow!("No available container for compilation"))?;
                
                let (ct, _, cse) = match base_language.as_str() {
                    "c" => handle_c_artifact(&self, &temp_id, filename, &base_code, &artifact_key, &redis_client, memory_bytes).await?,
                    "cpp" => handle_cpp_artifact(&self, &temp_id, filename, &base_code, &artifact_key, &redis_client, memory_bytes).await?,
                    "java" | "java11" => handle_java_artifact(&self, &temp_id, filename, &base_code, &artifact_key, &redis_client, memory_bytes).await?,
                    _ => unreachable!(),
                };

                compile_time = ct;
                compile_stderr = cse;
                
                return_container(&self, &base_language, &base_version, &temp_id).await;

                if !compile_stderr.is_empty() {
                    None 
                } else {
                    redis_client.get_artifact_async(&artifact_key).await?.map(|a| Arc::new(a.binary))
                }
            }
        } else {
            None 
        };
        
        // --- 4. Handle Compilation Errors ---
        if !compile_stderr.is_empty() {
            let error_message = String::from_utf8_lossy(&compile_stderr).to_string();
            let error_result = EvaluationResult {
                compile_time,
                stdout: error_message.clone(),
                stderr: Some(error_message),
                exit_code: 1,
                run_time: 0.0,
                space_consumed: format!("{:.2} MB", memory_bytes as f64 / (1024.0 * 1024.0)),
            };
            if let Some(token) = &token {
                let result_key = format!("result:{}", token);
                let _ = redis_client.set_in_cache_async(&result_key, &vec![error_result.clone()], 3600).await;
            }
            return Ok(vec![error_result; requests.len()]);
        }
        
        // --- 5. Parallel Execution of Test Cases ---
        let mut tasks = Vec::new();
        for request in requests {
            let self_clone = self.clone();
            let lang_clone = base_language.clone();
            let version_clone = base_version.clone();
            let code_clone = base_code.clone();
            let run_cmd_clone = run_cmd.clone();
            let filename_clone = filename.to_string();
            let artifact_binary_clone = artifact_binary.clone();

            let task = task::spawn(async move {
                let container_id = get_container(&self_clone, &lang_clone, &version_clone).await?
                    .ok_or_else(|| anyhow!("No available container for {}:{}", lang_clone, version_clone))?;

                if is_compiled_language {
                    if let Some(binary_data) = artifact_binary_clone {
                        let mut builder = Builder::new(Vec::new());
                        let mut header = tar::Header::new_gnu();
                        let class_filename = if lang_clone.starts_with("java") { "Main.class" } else { "main" };
                        header.set_path(class_filename)?;
                        header.set_size(binary_data.len() as u64);
                        header.set_mode(0o755);
                        header.set_cksum();
                        builder.append(&header, &binary_data[..])?;
                        builder.finish()?;
                        let tar_data = builder.into_inner()?;
                        self_clone.docker.upload_to_container(&container_id, Some(UploadToContainerOptions { path: "/app", ..Default::default() }), tar_data.into()).await?;
                    } else {
                        return_container(&self_clone, &lang_clone, &version_clone, &container_id).await;
                        return Err(anyhow!("Artifact binary was expected but not found"));
                    }
                } else { 
                    // **FIX:** Replaced the fragile `echo` command with the robust TAR upload method for interpreted languages.
                    let mut builder = Builder::new(Vec::new());
                    let mut header = tar::Header::new_gnu();
                    header.set_path(&filename_clone)?;
                    header.set_size(code_clone.as_bytes().len() as u64);
                    header.set_mode(0o755); // Make script executable
                    header.set_cksum();
                    builder.append(&header, code_clone.as_bytes())?;
                    builder.finish()?;
                    let tar_data = builder.into_inner()?;

                    self_clone.docker.upload_to_container(
                        &container_id,
                        Some(UploadToContainerOptions { path: "/app", ..Default::default() }),
                        tar_data.into(),
                    ).await?;
                }

                let run_start = Instant::now();
                let escaped_stdin = request.stdin.replace("\\", "\\\\").replace("$", "\\$").replace("`", "\\`").replace("\"", "\\\"").replace("'", "'\\''");
                let final_cmd = vec!["sh".to_string(), "-c".to_string(), format!("printf '%s' '{}' | {}", escaped_stdin, run_cmd_clone.join(" "))];
                let exec = self_clone.docker.create_exec(&container_id, CreateExecOptions { cmd: Some(final_cmd), attach_stdout: Some(true), attach_stderr: Some(true), ..Default::default() }).await?;
                
                let (stdout, stderr_data, exit_code) = match timeout(timeout_duration, async {
                    let mut output = Vec::new();
                    let mut stderr = Vec::new();
                    if let StartExecResults::Attached { output: mut stream, .. } = self_clone.docker.start_exec(&exec.id, None).await? {
                        while let Some(Ok(log)) = stream.next().await {
                            match log {
                                bollard::container::LogOutput::StdOut { message } => output.extend_from_slice(&message),
                                bollard::container::LogOutput::StdErr { message } => stderr.extend_from_slice(&message),
                                _ => {}
                            }
                        }
                    }
                    let inspect = self_clone.docker.inspect_exec(&exec.id).await?;
                    Ok::<_, anyhow::Error>((String::from_utf8_lossy(&output).to_string(), stderr, inspect.exit_code.unwrap_or(-1)))
                }).await {
                    Ok(Ok(res)) => res,
                    Ok(Err(e)) => return Err(e),
                    Err(_) => ("Time Limit Exceeded".to_string(), "Time Limit Exceeded".as_bytes().to_vec(), 124),
                };

                let run_time = if exit_code == 124 { timeout_secs } else { run_start.elapsed().as_secs_f64() };
                let space_consumed = match self_clone.docker.stats(&container_id, Some(StatsOptions { stream: false, one_shot: true })).next().await {
                    Some(Ok(stats)) => format!("{:.2} MB", stats.memory_stats.usage.unwrap_or(0) as f64 / (1024.0 * 1024.0)),
                    _ => format!("{:.2} MB", memory_bytes as f64 / (1024.0 * 1024.0)),
                };
                
                return_container(&self_clone, &lang_clone, &version_clone, &container_id).await;

                Ok(EvaluationResult {
                    compile_time: if is_compiled_language { compile_time } else { 0.0 },
                    stdout,
                    stderr: if stderr_data.is_empty() { None } else { Some(String::from_utf8_lossy(&stderr_data).to_string()) },
                    exit_code,
                    run_time,
                    space_consumed,
                })
            });
            tasks.push(task);
        }
        
        // --- 6. Aggregation and Final Caching ---
        let results = join_all(tasks).await.into_iter()
            .map(|res| match res {
                Ok(Ok(result)) => Ok(result),
                Ok(Err(e)) => { error!("Task execution failed: {}", e); Err(e) },
                Err(e) => { error!("Task panic: {}", e); Err(anyhow!("Task panic: {}", e)) },
            })
            .collect::<Result<Vec<EvaluationResult>>>()?;

        if results.iter().all(|r| r.stderr.is_none()) {
            if let Err(e) = redis_client.set_in_cache_async(&full_batch_key, &results, 3600).await {
                warn!("Failed to cache batch results for key {}: {}", full_batch_key, e);
            }
        }

        if let Some(token) = token {
            let result_key = format!("result:{}", token);
            if let Err(e) = redis_client.set_in_cache_async(&result_key, &results, 3600).await {
                warn!("Failed to store final batch results for token {}: {}", token, e);
            }
        }

        info!("Batch execution of {} requests completed in {:.2}s", results.len(), start_time.elapsed().as_secs_f64());
        
        if let Ok((hits, misses)) = redis_client.get_cache_stats_async().await {
            let total = hits + misses;
            let hit_rate = if total > 0 { (hits as f64 / total as f64) * 100.0 } else { 0.0 };
            info!("Cache statistics: {} hits, {} misses, {:.2}% hit rate", hits, misses, hit_rate);
        }

        Ok(results)
    }

    pub async fn cleanup(&self) -> Result<()> {
        let pool = self.container_pool.lock().await;
        for (_, containers) in pool.iter() {
            for container_id in containers {
                let _ = self.docker.remove_container(container_id, Some(RemoveContainerOptions { force: true, ..Default::default() })).await;
            }
        }
        info!("Cleaned up all containers");
        Ok(())
    }

    pub async fn execute_parallel(self: Arc<Self>, requests: Vec<ExecutionRequest>, redis_client: RedisClient) -> Vec<EvaluationResult> {
        let futures: Vec<_> = requests.into_iter().map(|req| {
            let req_clone = req.clone(); // Clone req to avoid partial move
            let executor_clone = self.clone();
            let mut redis_clone = redis_client.clone();
            async move {
                let cache_key = format!(
                    "evalx:exec:{}:{}:{}:{}:{}",
                    req_clone.language.as_ref().map_or("", String::as_str),
                    req_clone.version.as_ref().map_or("", String::as_str),
                    req_clone.timeout.unwrap_or(1),
                    Sha256::digest(req_clone.code.clone().unwrap_or_default().as_bytes()).encode_hex::<String>(),
                    Sha256::digest(&req_clone.stdin.as_bytes()).encode_hex::<String>()
                );
        
                match redis_clone.get_from_cache(&cache_key) {
                    Ok(Some(cached_result)) => cached_result,
                    Ok(None) => match executor_clone.execute(req_clone, redis_clone).await {
                        Ok(result) => result,
                        Err(e) => {
                            error!("Execution failed: {}", e);
                            EvaluationResult::error_result(e.to_string())
                        }
                    },
                    Err(e) => {
                        warn!("Redis error: {}", e);
                        match executor_clone.execute(req_clone, redis_clone).await {
                            Ok(result) => result,
                            Err(e) => {
                                error!("Execution failed: {}", e);
                                EvaluationResult::error_result(e.to_string())
                            }
                        }
                    }
                }
            }
        }).collect();
        
        futures_util::future::join_all(futures).await
    }

    pub async fn fetch_file_from_container(&self, container_id: &str, path: &str) -> Result<Vec<u8>> {
        let exec = self.docker.create_exec(container_id, CreateExecOptions { cmd: Some(vec!["cat".to_string(), path.to_string()]), attach_stdout: Some(true), attach_stderr: Some(true), ..Default::default() }).await?;
        let mut file_output = Vec::new();
        if let StartExecResults::Attached { mut output, .. } = self.docker.start_exec(&exec.id, None).await? {
            while let Some(Ok(msg)) = output.next().await {
                match msg {
                    bollard::container::LogOutput::StdOut { message } => file_output.extend_from_slice(&message),
                    bollard::container::LogOutput::StdErr { message } => return Err(anyhow::anyhow!("Error reading file {}: {}", path, String::from_utf8_lossy(&message))),
                    _ => {}
                }
            }
        }
        if file_output.is_empty() { return Err(anyhow::anyhow!("Failed to fetch file {}: empty output", path)); }
        Ok(file_output)
    }
}

pub async fn execute_command(executor: &CodeExecutor, container_id: &str, cmd: Vec<String>, _timeout_secs: u64) -> Result<(String, Vec<u8>, i64)> {
    let exec = executor.docker.create_exec(container_id, CreateExecOptions { cmd: Some(cmd.clone()), attach_stdout: Some(true), attach_stderr: Some(true), ..Default::default() }).await?;
    debug!("Created exec with command: {:?}", cmd);
    let exec_result = executor.docker.start_exec(&exec.id, None).await;

    match exec_result {
        Ok(StartExecResults::Attached { mut output, .. }) => {
            let mut stdout_data = Vec::new();
            let mut stderr_data = Vec::new();
            while let Some(result) = output.next().await {
                match result {
                    Ok(bollard::container::LogOutput::StdOut { message }) => stdout_data.extend_from_slice(&message),
                    Ok(bollard::container::LogOutput::StdErr { message }) => stderr_data.extend_from_slice(&message),
                    Ok(_) => {},
                    Err(e) => warn!("Error reading exec output: {}", e),
                }
            }
            let inspect = executor.docker.inspect_exec(&exec.id).await?;
            let exit_code = inspect.exit_code.unwrap_or(-1);
            debug!("Exec completed with exit_code: {}", exit_code);
            Ok((String::from_utf8_lossy(&stdout_data).to_string(), stderr_data, exit_code))
        }
        Ok(StartExecResults::Detached) => {
            error!("Execution detached unexpectedly for container {}", container_id);
            Ok((String::new(), "Execution detached unexpectedly".as_bytes().to_vec(), -1))
        }
        Err(e) => {
            error!("Docker exec failed for container {}: {}", container_id, e);
            Err(anyhow::anyhow!("Docker error: {}", e))
        }
    }
}

fn parse_memory_limit(limit: &str) -> Result<i64, anyhow::Error> {
    let trimmed = limit.trim().to_lowercase();
    let (value, unit) = trimmed.split_at(trimmed.len() - 1);
    let value: i64 = value.parse()?;
    match unit {
        "m" => Ok(value * 1024 * 1024),
        "g" => Ok(value * 1024 * 1024 * 1024),
        _ => Err(anyhow::anyhow!("Invalid memory unit: {}", unit)),
    }
}
