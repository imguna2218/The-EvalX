// src/sandbox/isolate.rs

use anyhow::{anyhow, Result};
use redis::AsyncCommands;
use std::os::unix::fs::PermissionsExt;
use std::time::Instant;
use tokio::fs;
use tokio::process::Command;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, warn};

use crate::caching::redis_client::RedisClient;
use crate::languages::config::LanguageConfig;

pub struct CompilationResult {
    pub success: bool,
    pub compile_time: f64,
    pub binary: Option<Vec<u8>>,
    pub stderr: String,
}

pub struct RunResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i64,
    pub run_time: f64,
    pub wall_time: f64,
    pub memory_used_kb: u64,
    pub status: String,
}

pub struct IsolateSandbox;

impl IsolateSandbox {
    pub async fn compile(
        &self,
        config: &LanguageConfig,
        code: &str,
        redis_client: RedisClient,
    ) -> Result<CompilationResult> {
        const POOL_KEY: &str = "evalx:sandbox_ids:available";

        // 1. ACQUIRE sandbox ID with a dedicated connection that we keep alive
        let mut dedicated_conn = redis_client.get_multiplexed_async_connection().await?;
        let box_id: u16 = loop {
            let id_opt: Option<u16> = dedicated_conn.spop(POOL_KEY).await?;
            if let Some(id) = id_opt {
                break id;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };

        // 2. EXECUTE the compilation
        let execution_result = async {
            let start_time = Instant::now();
            self.init_box(box_id).await?;

            let limits = config.compile.limits.clone().unwrap_or_default();
            let time_limit_s = limits.time_s;
            let mem_limit_kb = limits.memory_kb;
            let process_count = limits.processes;
            let box_path = format!("/var/local/lib/isolate/{}/box", box_id);
            fs::write(format!("{}/{}", box_path, &config.source_filename), code).await?;

            let mut cmd = self.build_base_command(box_id, config, time_limit_s, mem_limit_kb, process_count);
            cmd.arg("--run").arg("--").args(&config.compile.command);
            debug!("Executing compile command: {:?}", cmd);

            let child = cmd.spawn()?;
            let child_id = child.id().unwrap_or(0);
            let execution_future = child.wait_with_output();
            let timeout_duration = Duration::from_secs(time_limit_s + 2);

            let output = match tokio::time::timeout(timeout_duration, execution_future).await {
                Ok(Ok(output)) => output,
                Ok(Err(e)) => return Err(anyhow!("Compile process failed: {}", e)),
                Err(_) => {
                    error!("Compile command for '{}' (PID: {}) timed out. Attempting to kill.", config.name, child_id);
                    if let Err(e) = Command::new("kill").arg("-9").arg(child_id.to_string()).status().await {
                        error!("Failed to kill runaway compile process with PID {}: {}", child_id, e);
                    }
                    return Err(anyhow!("Compiler process hung and was terminated."));
                }
            };

            let compile_time = start_time.elapsed().as_secs_f64();
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();

            if output.status.success() || !config.is_compiled {
                let binary = if !config.is_compiled {
                    code.as_bytes().to_vec()
                } else {
                    fs::read(format!("{}/{}", box_path, &config.executable_filename)).await?
                };
                Ok(CompilationResult { success: true, compile_time, binary: Some(binary), stderr })
            } else {
                Ok(CompilationResult { success: false, compile_time, binary: None, stderr })
            }
        }.await;

        // 3. CRITICAL FIX: Return the ID using the SAME connection
        let return_result = dedicated_conn.sadd::<_, _, ()>(POOL_KEY, box_id).await;
        if let Err(e) = return_result {
            error!("CRITICAL: Failed to return sandbox ID {} to pool: {}", box_id, e);
            // Even if return fails, try emergency recovery
            let _ = redis_client.emergency_pool_recovery().await;
        } else {
            debug!("Successfully returned sandbox ID {} to pool", box_id);
        }

        // 4. Cleanup is less critical than returning the ID
        let cleanup_res = self.cleanup_box(box_id).await;
        if let Err(e) = cleanup_res {
            error!(box_id, "Non-fatal error during sandbox cleanup: {}", e);
        }

        execution_result
    }

    pub async fn run(
        &self,
        config: &LanguageConfig,
        code_or_binary: &[u8],
        stdin: &str,
        time_limit_s: u64,
        redis_client: RedisClient,
    ) -> Result<RunResult> {
        const POOL_KEY: &str = "evalx:sandbox_ids:available";
        
        // 1. ACQUIRE sandbox ID with a dedicated connection that we keep alive
        let mut dedicated_conn = redis_client.get_multiplexed_async_connection().await?;
        let box_id: u16 = loop {
            let id_opt: Option<u16> = dedicated_conn.spop(POOL_KEY).await?;
            if let Some(id) = id_opt {
                break id;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };

        // 2. EXECUTE the run operation
        let execution_result = async {
            self.init_box(box_id).await?;
            let limits = config.run.limits.clone().unwrap_or_default();
            let mem_limit_kb = limits.memory_kb;
            let process_count = limits.processes;

            let box_path = format!("/var/local/lib/isolate/{}/box", box_id);
            let file_to_write_path = format!("{}/{}", box_path, &config.executable_filename);
            fs::write(&file_to_write_path, code_or_binary).await?;
            
            if config.is_compiled {
                fs::set_permissions(&file_to_write_path, std::fs::Permissions::from_mode(0o755)).await?;
            }
            fs::write(format!("{}/stdin.txt", box_path), stdin).await?;

            let meta_file_path = format!("/tmp/isolate_{}.txt", box_id);
            let mut cmd = self.build_base_command(box_id, config, time_limit_s, mem_limit_kb, process_count);
            cmd.arg("--stdin=stdin.txt")
            .arg(format!("--meta={}", meta_file_path))
            .arg("--run")
            .arg("--")
            .args(&config.run.command);
            debug!("Executing run command: {:?}", cmd);

            let child = cmd.spawn()?;
            let child_id = child.id().unwrap_or(0);
            let execution_future = child.wait_with_output();
            let timeout_duration = Duration::from_secs(time_limit_s + 2);
            
            let output = match tokio::time::timeout(timeout_duration, execution_future).await {
                Ok(Ok(output)) => output,
                Ok(Err(e)) => return Err(anyhow!("Run process failed: {}", e)),
                Err(_) => {
                    error!("Run command for '{}' (PID: {}) timed out. Attempting to kill.", config.name, child_id);
                    if let Err(e) = Command::new("kill").arg("-9").arg(child_id.to_string()).status().await {
                        error!("Failed to kill runaway run process with PID {}: {}", child_id, e);
                    }
                    return Ok(RunResult {
                        stdout: String::new(),
                        stderr: "Time Limit Exceeded".to_string(),
                        exit_code: -1,
                        run_time: time_limit_s as f64,
                        wall_time: 0.0,
                        memory_used_kb: 0,
                        status: "TO".to_string(),
                    });
                }
            };

            let program_stdout = fs::read_to_string(format!("{}/stdout.txt", box_path)).await.unwrap_or_default();
            let program_stderr = fs::read_to_string(format!("{}/stderr.txt", box_path)).await.unwrap_or_else(|_| "".to_string());
            let meta_content = fs::read_to_string(&meta_file_path).await.unwrap_or_else(|_| "status:XX".to_string());
            let _ = fs::remove_file(&meta_file_path).await;

            let mut run_time = 0.0;
            let mut wall_time = 0.0;
            let mut memory_used_kb = 0;
            let mut status = "Unknown".to_string();
            for line in meta_content.lines() {
                if let Some((key, value)) = line.split_once(':') {
                    match key {
                        "time" => run_time = value.parse().unwrap_or(0.0),
                        "wall-time" => wall_time = value.parse().unwrap_or(0.0),
                        "max-rss" => memory_used_kb = value.parse().unwrap_or(0),
                        "status" => status = value.to_string(),
                        _ => {}
                    }
                }
            }
    
            if status == "TO" {
                run_time = time_limit_s as f64;
            }

            let isolate_stderr = String::from_utf8_lossy(&output.stderr).to_string();
            let final_stderr = if status == "TO" {
                "Time Limit Exceeded".to_string()
            } else if !program_stderr.is_empty() {
                program_stderr
            } else {
                isolate_stderr
            };
            Ok(RunResult {
                stdout: program_stdout,
                stderr: final_stderr,
                exit_code: output.status.code().unwrap_or(-1) as i64,
                run_time,
                wall_time,
                memory_used_kb,
                status,
            })
        }.await;

        // 3. CRITICAL FIX: Return the ID using the SAME connection
        let return_result = dedicated_conn.sadd::<_, _, ()>(POOL_KEY, box_id).await;
        if let Err(e) = return_result {
            error!("CRITICAL: Failed to return sandbox ID {} to pool: {}", box_id, e);
            // Even if return fails, try emergency recovery
            let _ = redis_client.emergency_pool_recovery().await;
        } else {
            debug!("Successfully returned sandbox ID {} to pool", box_id);
        }

        // 4. Cleanup is less critical than returning the ID
        let cleanup_res = self.cleanup_box(box_id).await;
        if let Err(e) = cleanup_res {
            error!(box_id, "Non-fatal error during sandbox cleanup: {}", e);
        }

        execution_result
    }

    fn build_base_command(
        &self,
        box_id: u16,
        config: &LanguageConfig,
        time: u64,
        mem: u64,
        proc: u64,
    ) -> Command {
        let mut cmd = Command::new("isolate");
        
        if let Some(chroot_path) = &config.chroot_path {
            cmd.arg("--env=PATH=/usr/bin:/bin");
            let dirs_to_bind = ["bin", "etc", "lib", "lib64", "usr", "sbin"];
            for dir in dirs_to_bind {
                let source_path = std::path::Path::new(chroot_path).join(dir);
                if source_path.exists() {
                    cmd.arg(format!("--dir=/{}={}", dir, source_path.display()));
                }
            }
        } else {
            cmd.arg("--full-env")
                .arg("--env=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
                .arg("--dir=/etc")
                .arg("--dir=/bin")
                .arg("--dir=/usr/bin")
                .arg("--dir=/usr/local/bin")
                .arg("--dir=/lib")
                .arg("--dir=/lib64")
                .arg("--dir=/usr/lib")
                .arg("--dir=/usr/include");
            for path in &config.mount_paths {
                cmd.arg(format!("--dir={}", path));
            }
        }

        cmd.arg("--cg")
            .arg("--proc")
            .arg(format!("--box-id={}", box_id))
            .arg("--stdout=stdout.txt")
            .arg("--stderr=stderr.txt")
            .arg(format!("--time={}", time))
            .arg(format!("--wall-time={}", time * 2))
            .arg(format!("--fsize={}", 102400))
            .arg(format!("--mem={}", mem))
            .arg(format!("--processes={}", proc));
        
        for (key, val) in &config.env_vars {
            cmd.arg(format!("--env={}={}", key, val));
        }

        cmd
    }

    async fn init_box(&self, box_id: u16) -> Result<()> {
        let output = Command::new("isolate")
            .arg("--cg")
            .arg(format!("--box-id={}", box_id))
            .arg("--init")
            .output()
            .await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!("Failed to initialize isolate box {}: {}", box_id, stderr);
            return Err(anyhow!("Failed to initialize isolate box {}: {}", box_id, stderr));
        }
        Ok(())
    }

    async fn cleanup_box(&self, box_id: u16) -> Result<()> {
        let output = Command::new("isolate")
            .arg("--cg")
            .arg(format!("--box-id={}", box_id))
            .arg("--cleanup")
            .output()
            .await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stderr.contains("No such file or directory") {
                error!("Failed to cleanup isolate box {}: {}", box_id, stderr);
            }
        }
        Ok(())
    }
}