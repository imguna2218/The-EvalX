use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::Result;
use bollard::container::{RemoveContainerOptions, StatsOptions, UploadToContainerOptions};
use bollard::exec::{CreateExecOptions, StartExecResults};
use tokio::time::timeout;
use futures_util::stream::StreamExt;
use tar::Builder;
use std::io::Cursor;
use tracing::{debug, error, info, warn};
use sha2::{Sha256, Digest};
use hex::ToHex;
use std::env;
use crate::types::index::CodeExecutor;
use crate::models::request::ExecutionRequest;
use crate::models::response::EvaluationResult;
use crate::caching::redis_client::RedisClient;
use crate::container_management::container_pool::{get_container, return_container};
use crate::compilers::artifact_handlers::{handle_c_artifact, handle_cpp_artifact, handle_java_artifact};

impl CodeExecutor {
    pub async fn execute(
        self: Arc<Self>,
        request: ExecutionRequest,
        redis_client: RedisClient,
    ) -> Result<EvaluationResult> {
        let _permit = self.semaphore.acquire().await?;
        let start_time = Instant::now();
    
        let language = request.language.trim().to_string();
        let version = request.version.trim().to_string();
        let code = request.code.trim().to_string();
    
        let is_compiled_language = matches!(language.as_str(), "java" | "java11" | "c" | "cpp");
    
        let language_config = self.language_configs.get(&language).ok_or_else(|| {
            anyhow::anyhow!("Unsupported language: {}", language)
        })?.clone();
    
        let timeout_secs = request.timeout.unwrap_or_else(|| {
            env::var("DEFAULT_TIMEOUT")
                .unwrap_or_else(|_| "10.0".to_string())
                .parse::<f64>()
                .unwrap_or(10.0)
        });
        let timeout_duration = Duration::from_secs_f64(timeout_secs);
    
        let memory_limit = request.memory_limit.clone().unwrap_or_else(|| {
            env::var("DEFAULT_MEMORY").unwrap_or_else(|_| "512m".to_string())
        });
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
                .ok_or_else(|| anyhow::anyhow!("No available container for {}:{}", language, version))?;
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
                .ok_or_else(|| anyhow::anyhow!("No available container for {}:{}", language, version))?;
            let run_cmd = match language.as_str() {
                "python" => vec!["python".to_string(), "/app/main.py".to_string()],
                "javascript" => vec!["node".to_string(), "/app/main.js".to_string()],
                _ => language_config.command_format.clone(),
            };
            (0.0, run_cmd, Vec::new(), container_id)
        };
    
        if !compile_stderr.is_empty() {
            let error_message = String::from_utf8_lossy(&compile_stderr).to_string();
            return_container(&self, &language, &version, container_id).await;
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
            request.stdin.as_ref().map_or("".to_string(), |s| Sha256::digest(s.as_bytes()).encode_hex::<String>())
        );
    
        if let Ok(Some(cached_result)) = redis_client.get_from_cache(&result_key) {
            debug!("Result cache hit for key: {}", result_key);
            return_container(&self, &language, &version, container_id).await;
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
                    return_container(&self, &language, &version, container_id).await;
                    return Err(anyhow::anyhow!("Failed to write code: {}", e));
                },
                Err(_) => {
                    return_container(&self, &language, &version, container_id).await;
                    return Err(anyhow::anyhow!("Timeout writing code"));
                },
            };
        }
    
        let run_start = Instant::now();
        let mut final_cmd = run_cmd.clone();
    
        if let Some(stdin) = &request.stdin {
            debug!("Piping stdin for {}: '{}'", language, stdin);
            final_cmd = vec![
                "sh".to_string(),
                "-c".to_string(),
                format!("echo -n '{}' | {}", stdin.replace("'", "'\\''"), run_cmd.join(" ")),
            ];
        }
    
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
                return_container(&self, &language, &version, container_id).await;
                return Err(anyhow::anyhow!("Execution failed: {}", e));
            }
            Err(_) => {
                error!("Execution timed out");
                ("Time Limit Exceeded".to_string(), "Time Limit Exceeded".as_bytes().to_vec(), 124)
            }
        };
        let run_time = if exit_code == 124 { timeout_secs } else { run_start.elapsed().as_secs_f64() };
    
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
    
        return_container(&self, &language, &version, container_id).await;
        Ok(result)
    }

    pub async fn execute_batch(
        self: Arc<Self>,
        requests: Vec<ExecutionRequest>,
        redis_client: RedisClient,
    ) -> Result<Vec<EvaluationResult>> {
        if requests.is_empty() {
            return Ok(vec![]);
        }
    
        let _permit = self.semaphore.acquire().await?;
        let start_time = Instant::now();
    
        let first_request = &requests[0];
        let base_language = first_request.language.trim().to_string();
        let base_version = first_request.version.trim().to_string();
        let base_code = first_request.code.trim().to_string();
    
        let is_compiled_language = matches!(base_language.as_str(), "java" | "java11" | "c" | "cpp");
        let is_interpreted_language = matches!(base_language.as_str(), "python" | "javascript");
    
        if is_compiled_language {
            for (i, req) in requests[1..].iter().enumerate() {
                if !req.language.trim().is_empty() && req.language != base_language {
                    warn!("Ignoring language '{}' in request {} (using '{}')", req.language, i + 1, base_language);
                }
                if !req.version.trim().is_empty() && req.version != base_version {
                    warn!("Ignoring version '{}' in request {} (using '{}')", req.version, i + 1, base_version);
                }
                if !req.code.trim().is_empty() && req.code.trim() != base_code {
                    warn!("Ignoring code in request {} (using first request's code)", i + 1);
                }
                if req.timeout.is_some() && req.timeout != first_request.timeout {
                    warn!("Ignoring timeout in request {} (using first request's timeout)", i + 1);
                }
            }
        } else if is_interpreted_language {
            for (i, req) in requests[1..].iter().enumerate() {
                let req_language = req.language.trim();
                let req_version = req.version.trim();
                let req_code = req.code.trim();
                if !req_language.is_empty() && req_language != base_language {
                    return Err(anyhow::anyhow!(
                        "Batch execution requires all requests to have the same language or omit language after the first request (mismatch at index {})",
                        i + 1
                    ));
                }
                if !req_version.is_empty() && req_version != base_version {
                    return Err(anyhow::anyhow!(
                        "Batch execution requires all requests to have the same version or omit version after the first request (mismatch at index {})",
                        i + 1
                    ));
                }
                if !req_code.is_empty() && req_code != base_code {
                    warn!("Ignoring code '{}' in request {} for interpreted language (using first request's code: '{}')", req_code, i + 1, base_code);
                }
            }
        } else {
            return Err(anyhow::anyhow!("Unsupported language: {}", base_language));
        }
    
        let language_config = self.language_configs.get(&base_language).ok_or_else(|| {
            anyhow::anyhow!("Unsupported language: {}", base_language)
        })?.clone();
    
        let timeout_secs = first_request.timeout.unwrap_or_else(|| {
            env::var("DEFAULT_TIMEOUT")
                .unwrap_or_else(|_| "10.0".to_string())
                .parse::<f64>()
                .unwrap_or(10.0)
        });
        let timeout_duration = Duration::from_secs_f64(timeout_secs);
    
        let memory_limit = first_request.memory_limit.clone().unwrap_or_else(|| {
            env::var("DEFAULT_MEMORY").unwrap_or_else(|_| "512m".to_string())
        });
        let memory_bytes = parse_memory_limit(&memory_limit).unwrap_or(
            language_config.resource_limits.memory.parse::<i64>().unwrap_or(64 * 1024 * 1024)
        );
    
        let filename = match base_language.as_str() {
            "c" => "main.c",
            "cpp" => "main.cpp",
            "java" | "java11" => "Main.java",
            "python" => "main.py",
            "javascript" => "main.js",
            _ => "main",
        };
    
        let mut redis_client = redis_client;
        let (compile_time, run_cmd, compile_stderr, initial_container_id) = if is_compiled_language {
            let container_id = get_container(&self, &base_language, &base_version)
                .await?
                .ok_or_else(|| anyhow::anyhow!("No available container for {}:{}", base_language, base_version))?;
            let code_hash = Sha256::digest(&base_code).encode_hex::<String>();
            let artifact_key = format!("evalx:artifact:{}:{}:{}", &base_language, &base_version, &code_hash);
            let (compile_time, run_cmd, compile_stderr) = match base_language.as_str() {
                "c" => handle_c_artifact(&self, &container_id, filename, &base_code, &artifact_key, &mut redis_client, memory_bytes).await?,
                "cpp" => handle_cpp_artifact(&self, &container_id, filename, &base_code, &artifact_key, &mut redis_client, memory_bytes).await?,
                "java" | "java11" => handle_java_artifact(&self, &container_id, filename, &base_code, &artifact_key, &mut redis_client, memory_bytes).await?,
                _ => unreachable!(),
            };
            (compile_time, run_cmd, compile_stderr, container_id)
        } else {
            let container_id = get_container(&self, &base_language, &base_version)
                .await?
                .ok_or_else(|| anyhow::anyhow!("No available container for {}:{}", base_language, base_version))?;
            let run_cmd = match base_language.as_str() {
                "python" => vec!["python".to_string(), "/app/main.py".to_string()],
                "javascript" => vec!["node".to_string(), "/app/main.js".to_string()],
                _ => language_config.command_format.clone(),
            };
            (0.0, run_cmd, Vec::new(), container_id)
        };
    
        let mut results = Vec::with_capacity(requests.len());
    
        // Execute the first request and check for errors
        let first_result_key = format!(
            "evalx:exec:{}:{}:{}:{}:{}",
            &base_language,
            &base_version,
            timeout_secs,
            Sha256::digest(&base_code).encode_hex::<String>(),
            first_request.stdin.as_ref().map_or("".to_string(), |s| Sha256::digest(s.as_bytes()).encode_hex::<String>())
        );
    
        let first_result = if let Ok(Some(cached_result)) = redis_client.get_from_cache(&first_result_key) {
            debug!("Result cache hit for key: {}", first_result_key);
            cached_result
        } else {
            if is_interpreted_language {
                let write_cmd = vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    format!("echo '{}' > /app/{}", base_code.replace("'", "'\\''"), filename),
                ];
                let write_exec = self.docker.create_exec(
                    &initial_container_id,
                    CreateExecOptions { cmd: Some(write_cmd), attach_stdout: Some(true), attach_stderr: Some(true), ..Default::default() },
                ).await?;
                match timeout(Duration::from_secs(1), self.docker.start_exec(&write_exec.id, None)).await {
                    Ok(Ok(_)) => debug!("Code written to /app/{}", filename),
                    Ok(Err(e)) => {
                        return_container(&self, &base_language, &base_version, initial_container_id).await;
                        return Err(anyhow::anyhow!("Failed to write code: {}", e));
                    },
                    Err(_) => {
                        return_container(&self, &base_language, &base_version, initial_container_id).await;
                        return Err(anyhow::anyhow!("Timeout writing code"));
                    },
                };
            }
    
            let run_start = Instant::now();
            let mut final_cmd = run_cmd.clone();
    
            if let Some(stdin) = &first_request.stdin {
                debug!("Piping stdin for {}: '{}'", base_language, stdin);
                final_cmd = vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    format!("echo -n '{}' | {}", stdin.replace("'", "'\\''"), run_cmd.join(" ")),
                ];
            }
    
            debug!("Executing command: {:?}", final_cmd);
            let exec = self.docker.create_exec(
                &initial_container_id,
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
                Ok(Ok((stdout, stderr_data, exit_code))) => (stdout, stderr_data, exit_code),
                Ok(Err(e)) => {
                    return_container(&self, &base_language, &base_version, initial_container_id).await;
                    return Err(anyhow::anyhow!("Execution failed: {}", e));
                },
                Err(_) => ("Time Limit Exceeded".to_string(), "Time Limit Exceeded".as_bytes().to_vec(), 124),
            };
    
            let run_time = if exit_code == 124 { timeout_secs } else { run_start.elapsed().as_secs_f64() };
            let space_consumed = match self.docker.stats(&initial_container_id, Some(StatsOptions { stream: false, one_shot: true })).next().await {
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
                if let Ok(_) = redis_client.set_in_cache(&first_result_key, &result, 3600) {
                    debug!("Cached result for key: {}", first_result_key);
                }
            }
            result
        };
    
        results.push(first_result.clone());
    
        // Check for errors in the first result
        if !compile_stderr.is_empty() || first_result.exit_code != 0 {
            return_container(&self, &base_language, &base_version, initial_container_id).await;
            if !compile_stderr.is_empty() {
                let error_message = String::from_utf8_lossy(&compile_stderr).to_string();
                results[0] = EvaluationResult {
                    compile_time,
                    stdout: error_message.clone(),
                    stderr: Some(error_message),
                    exit_code: 1,
                    run_time: 0.0,
                    space_consumed: format!("{:.2} MB", memory_bytes as f64 / (1024.0 * 1024.0)),
                };
            }
            for _ in 1..requests.len() {
                results.push(EvaluationResult {
                    compile_time: 0.0,
                    stdout: String::new(),
                    stderr: None,
                    exit_code: 0,
                    run_time: 0.0,
                    space_consumed: format!("{:.2} MB", memory_bytes as f64 / (1024.0 * 1024.0)),
                });
            }
            return Ok(results);
        }
    
        // Prepare containers for parallel execution
        let max_parallel = self.semaphore.available_permits().min(requests.len() - 1);
        let mut containers = vec![initial_container_id];
    
        if is_compiled_language {
            let code_hash = Sha256::digest(&base_code).encode_hex::<String>();
            let artifact_key = format!("evalx:artifact:{}:{}:{}", &base_language, &base_version, &code_hash);
            for _ in 1..max_parallel {
                if let Ok(Some(container_id)) = get_container(&self, &base_language, &base_version).await {
                    if let Ok(Some(artifact)) = redis_client.get_artifact(&artifact_key) {
                        let mut archive = Builder::new(Vec::new());
                        let mut header = tar::Header::new_gnu();
                        let target_file = match base_language.as_str() {
                            "c" | "cpp" => "main",
                            "java" | "java11" => "Main.class",
                            _ => unreachable!(),
                        };
                        header.set_path(target_file)?;
                        header.set_size(artifact.binary.len() as u64);
                        header.set_mode(if base_language == "c" || base_language == "cpp" { 0o755 } else { 0o644 });
                        header.set_cksum();
                        archive.append(&header, Cursor::new(artifact.binary))?;
                        let tar_data = archive.into_inner()?;
                        self.docker
                            .upload_to_container(
                                &container_id,
                                Some(UploadToContainerOptions { path: "/app/", no_overwrite_dir_non_dir: "false" }),
                                tar_data.into(),
                            )
                            .await?;
                    }
                    containers.push(container_id);
                }
            }
        } else if is_interpreted_language {
            // For interpreted languages, write the base code to all containers
            for _ in 1..max_parallel {
                if let Ok(Some(container_id)) = get_container(&self, &base_language, &base_version).await {
                    let write_cmd = vec![
                        "sh".to_string(),
                        "-c".to_string(),
                        format!("echo '{}' > /app/{}", base_code.replace("'", "'\\''"), filename),
                    ];
                    let write_exec = self.docker.create_exec(
                        &container_id,
                        CreateExecOptions { cmd: Some(write_cmd), attach_stdout: Some(true), attach_stderr: Some(true), ..Default::default() },
                    ).await?;
                    match timeout(Duration::from_secs(1), self.docker.start_exec(&write_exec.id, None)).await {
                        Ok(Ok(_)) => debug!("Code written to /app/{} in container {}", filename, container_id),
                        Ok(Err(e)) => warn!("Failed to write code to container {}: {}", container_id, e),
                        Err(_) => warn!("Timeout writing code to container {}", container_id),
                    };
                    containers.push(container_id);
                }
            }
        }
    
        // Execute remaining requests in parallel
        let remaining_requests = &requests[1..];
        let mut request_chunks = remaining_requests.chunks(containers.len()).peekable();
        while let Some(chunk) = request_chunks.next() {
            let futures: Vec<_> = chunk
                .iter()
                .enumerate()
                .map(|(i, request)| {
                    let container_id = containers[i % containers.len()].clone();
                    let run_cmd = run_cmd.clone();
                    let mut redis_client = redis_client.clone();
                    let base_language = base_language.clone();
                    let base_version = base_version.clone();
                    let base_code = base_code.clone();
                    let timeout_duration = timeout_duration;
                    let memory_bytes = memory_bytes;
                    let self_clone = self.clone();
                    async move {
                        let effective_language = base_language.clone();
                        let effective_version = base_version.clone();
                        let normalized_code = if is_compiled_language || !is_interpreted_language {
                            base_code.clone()
                        } else {
                            base_code.clone() // Always use base_code for interpreted languages
                        };
    
                        let result_key = format!(
                            "evalx:exec:{}:{}:{}:{}:{}",
                            &effective_language,
                            &effective_version,
                            timeout_secs,
                            Sha256::digest(&normalized_code).encode_hex::<String>(),
                            request.stdin.as_ref().map_or("".to_string(), |s| Sha256::digest(s.as_bytes()).encode_hex::<String>())
                        );
    
                        if let Ok(Some(cached_result)) = redis_client.get_from_cache(&result_key) {
                            debug!("Result cache hit for key: {}", result_key);
                            return Ok(EvaluationResult { compile_time: if is_compiled_language { compile_time } else { 0.0 }, ..cached_result });
                        }
    
                        let run_start = Instant::now();
                        let mut final_cmd = run_cmd.clone();
    
                        if let Some(stdin) = &request.stdin {
                            debug!("Piping stdin for {}: '{}'", effective_language, stdin);
                            final_cmd = vec![
                                "sh".to_string(),
                                "-c".to_string(),
                                format!("echo -n '{}' | {}", stdin.replace("'", "'\\''"), run_cmd.join(" ")),
                            ];
                        }
    
                        debug!("Executing command: {:?}", final_cmd);
                        let exec = self_clone.docker.create_exec(
                            &container_id,
                            CreateExecOptions { cmd: Some(final_cmd), attach_stdout: Some(true), attach_stderr: Some(true), ..Default::default() },
                        ).await?;
    
                        let (stdout, stderr_data, exit_code) = match timeout(timeout_duration, async {
                            let exec_result = self_clone.docker.start_exec(&exec.id, None).await?;
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
                            let inspect = self_clone.docker.inspect_exec(&exec.id).await?;
                            Ok::<_, anyhow::Error>((String::from_utf8_lossy(&output).to_string(), stderr, inspect.exit_code.unwrap_or(-1) as i64))
                        }).await {
                            Ok(Ok((stdout, stderr_data, exit_code))) => {
                                debug!("Execution completed: stdout='{}', stderr='{}', exit_code={}", stdout, String::from_utf8_lossy(&stderr_data), exit_code);
                                (stdout, stderr_data, exit_code)
                            }
                            Ok(Err(e)) => {
                                error!("Execution failed: {}", e);
                                return Err(anyhow::anyhow!("Execution failed: {}", e));
                            }
                            Err(_) => {
                                error!("Execution timed out");
                                ("Time Limit Exceeded".to_string(), "Time Limit Exceeded".as_bytes().to_vec(), 124)
                            }
                        };
                        let run_time = if exit_code == 124 { timeout_secs } else { run_start.elapsed().as_secs_f64() };
    
                        let space_consumed = match self_clone.docker.stats(&container_id, Some(StatsOptions { stream: false, one_shot: true })).next().await {
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
                        Ok(result)
                    }
                })
                .collect();
    
            let chunk_results = futures_util::future::join_all(futures).await.into_iter().collect::<Result<Vec<_>>>()?;
            results.extend(chunk_results);
    
            if request_chunks.peek().is_some() {
                for container_id in containers.drain(..) {
                    return_container(&self, &base_language, &base_version, container_id).await;
                }
                if is_compiled_language {
                    let code_hash = Sha256::digest(&base_code).encode_hex::<String>();
                    let artifact_key = format!("evalx:artifact:{}:{}:{}", &base_language, &base_version, &code_hash);
                    for _ in 0..max_parallel {
                        if let Ok(Some(container_id)) = get_container(&self, &base_language, &base_version).await {
                            if let Ok(Some(artifact)) = redis_client.get_artifact(&artifact_key) {
                                let mut archive = Builder::new(Vec::new());
                                let mut header = tar::Header::new_gnu();
                                let target_file = match base_language.as_str() {
                                    "c" | "cpp" => "main",
                                    "java" | "java11" => "Main.class",
                                    _ => unreachable!(),
                                };
                                header.set_path(target_file)?;
                                header.set_size(artifact.binary.len() as u64);
                                header.set_mode(if base_language == "c" || base_language == "cpp" { 0o755 } else { 0o644 });
                                header.set_cksum();
                                archive.append(&header, Cursor::new(artifact.binary))?;
                                let tar_data = archive.into_inner()?;
                                self.docker.upload_to_container(&container_id, Some(UploadToContainerOptions { path: "/app/", no_overwrite_dir_non_dir: "false" }), tar_data.into()).await?;
                            }
                            containers.push(container_id);
                        }
                    }
                } else if is_interpreted_language {
                    for _ in 0..max_parallel {
                        if let Ok(Some(container_id)) = get_container(&self, &base_language, &base_version).await {
                            let write_cmd = vec![
                                "sh".to_string(),
                                "-c".to_string(),
                                format!("echo '{}' > /app/{}", base_code.replace("'", "'\\''"), filename),
                            ];
                            let write_exec = self.docker.create_exec(
                                &container_id,
                                CreateExecOptions { cmd: Some(write_cmd), attach_stdout: Some(true), attach_stderr: Some(true), ..Default::default() },
                            ).await?;
                            match timeout(Duration::from_secs(1), self.docker.start_exec(&write_exec.id, None)).await {
                                Ok(Ok(_)) => debug!("Code written to /app/{} in container {}", filename, container_id),
                                Ok(Err(e)) => warn!("Failed to write code to container {}: {}", container_id, e),
                                Err(_) => warn!("Timeout writing code to container {}", container_id),
                            };
                            containers.push(container_id);
                        }
                    }
                }
            }
        }
    
        for container_id in containers {
            return_container(&self, &base_language, &base_version, container_id).await;
        }
    
        info!("Batch execution completed in {:.2}s", start_time.elapsed().as_secs_f64());
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
            let executor_clone = self.clone();
            let mut redis_clone = redis_client.clone();
            async move {
                let cache_key = format!(
                    "evalx:exec:{}:{}:{}:{}:{}",
                    req.language,
                    req.version,
                    req.timeout.unwrap_or(1.0),
                    Sha256::digest(&req.code).encode_hex::<String>(),
                    req.stdin.as_ref().map_or("".to_string(), |s| Sha256::digest(s.as_bytes()).encode_hex::<String>())
                );
        
                match redis_clone.get_from_cache(&cache_key) {
                    Ok(Some(cached_result)) => cached_result,
                    Ok(None) => match executor_clone.execute(req, redis_clone).await {
                        Ok(result) => result,
                        Err(e) => {
                            error!("Execution failed: {}", e);
                            EvaluationResult::error_result(e.to_string())
                        }
                    },
                    Err(e) => {
                        warn!("Redis error: {}", e);
                        match executor_clone.execute(req, redis_clone).await {
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
