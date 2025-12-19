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
use tokio::time::timeout;
use crate::info;
use crate::sandbox::local_pool::LocalSandboxPool;
use std::sync::Arc;

pub struct PrewarmedSandbox {
    pub box_id: u16,
    pool: Arc<LocalSandboxPool>,
}

impl PrewarmedSandbox {
    // This function gets a ready box from the pool. It will wait up to 10 seconds.
    pub async fn new(pool: Arc<LocalSandboxPool>) -> Result<Self> {
        // Acquire from the local pool, potentially waiting
        match pool.acquire().await {
             Ok(box_id) => {
                 debug!("Acquired local sandbox ID {}", box_id);
                 Ok(Self { box_id, pool })
             },
             Err(e) => {
                 error!("Failed to acquire a sandbox box ID from the local pool: {}", e);
                 // Convert error type if necessary, or return directly if acquire returns anyhow::Error
                 Err(anyhow!("Failed to acquire a sandbox box ID from local pool: {}", e))
             }
        }
    }
}

// This is the magic. This `drop` function is GUARANTEED by Rust to run
// when the PrewarmedSandbox is destroyed, no matter what.
impl Drop for PrewarmedSandbox {
    fn drop(&mut self) {
        debug!("Local Sandbox {} is finished. Releasing to cleanup.", self.box_id);
        let pool_clone = self.pool.clone();
        let id = self.box_id;
        // Spawn a task to release the ID back to the pool asynchronously
        // This avoids blocking the drop handler if the channel is full (though it shouldn't be)
        tokio::spawn(async move {
            pool_clone.release(id).await;
        });
    }
}
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

    pub async fn run_in_existing_box(
        &self,
        box_id: u16,
        config: &LanguageConfig,
        code_or_binary: &[u8],
        stdin: &str,
        time_limit_s: u64,
    ) -> Result<RunResult> {
        let execution_result = async {
            let limits = config.run.limits.clone().unwrap_or_default();
            let mem_limit_kb = limits.memory_kb;
            let process_count = limits.processes;

            let box_path = format!("/var/local/lib/isolate/{}/box", box_id);
            let file_to_write_path = format!("{}/{}", box_path, &config.executable_filename);
            fs::write(&file_to_write_path, code_or_binary).await?;
            if config.is_compiled {
                let mut perms = fs::metadata(&file_to_write_path).await?.permissions();
                perms.set_mode(0o755);
                fs::set_permissions(&file_to_write_path, perms).await?;
            }
            fs::write(format!("{}/stdin.txt", box_path), stdin).await?;
            let meta_file_path = format!("/tmp/isolate_{}.txt", box_id);

            let mut cmd =
                self.build_base_command(box_id, config, time_limit_s, mem_limit_kb, process_count);
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
            let output_result = tokio::time::timeout(timeout_duration, execution_future).await;
            let output = match output_result {
                 Ok(Ok(output)) => {
                    let isolate_stderr_str = String::from_utf8_lossy(&output.stderr);
                    if !isolate_stderr_str.is_empty() {
                        warn!("Isolate stderr (run, box {}): {}", box_id, isolate_stderr_str);
                    }
                    output
                 }
                 Ok(Err(e)) => return Err(anyhow!("Run process failed: {}", e)),
                 Err(_) => {
                     error!(
                        "Run command for '{}' (PID: {}) timed out after {:?}. Attempting to kill.",
                        config.name, child_id, timeout_duration
                    );
                     if let Err(e) = Command::new("kill")
                       .arg("-9")
                       .arg(child_id.to_string())
                       .status()
                       .await
                     {
                         error!("Failed to kill runaway run process with PID {}: {}", child_id, e);
                     }
                     tokio::time::sleep(Duration::from_millis(50)).await;
                     return Ok(RunResult {
                         stdout: String::new(),
                         stderr: "Time Limit Exceeded".to_string(),
                         exit_code: -1,
                         run_time: time_limit_s as f64,
                         wall_time: (time_limit_s * 2) as f64,
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
            let final_stderr = match status.as_str() {
                "TO" => "Time Limit Exceeded".to_string(),
                "SG" | "RE" | "XX" => {
                    if !program_stderr.is_empty() {
                        program_stderr
                    } else if !isolate_stderr.is_empty() {
                        format!("Runtime Error (isolate: {})", isolate_stderr)
                    } else {
                        format!("Runtime Error (status: {})", status)
                    }
                }
                _ => {
                    if !program_stderr.is_empty() {
                        program_stderr
                    } else {
                        "".to_string()
                    }
                }
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
        }
      .await;
        execution_result
    }


    pub async fn compile(
        &self,
        config: &LanguageConfig,
        code: &str,
        pool: Arc<LocalSandboxPool>,
    ) -> Result<CompilationResult> {
        let sandbox = PrewarmedSandbox::new(pool).await?;
        let box_id = sandbox.box_id;

        // 1. Define paths
        let box_path = format!("/var/local/lib/isolate/{}/box", box_id);
        let source_file_host_path = format!("{}/{}", box_path, &config.source_filename);
        let executable_file_host_path = format!("{}/{}", box_path, &config.executable_filename);
        let stderr_file_host_path = format!("{}/stderr.txt", box_path);

        // 2. Write the source code from the HOST (Safe & Reliable)
        fs::write(&source_file_host_path, code).await?;

        let execution_result = async {
            let start_time = Instant::now();
            let limits = config.compile.limits.clone().unwrap_or_default();
            let time_limit_s = limits.time_s;
            let mem_limit_kb = limits.memory_kb;
            let process_count = limits.processes;

            // 3. Build Command (No more printf!)
            let mut cmd = self.build_base_command(box_id, config, time_limit_s, mem_limit_kb, process_count);

            // Construct the compile command to run INSIDE the sandbox
            // We redirect stderr to stderr.txt to capture compiler warnings/errors
            let compile_cmd = config.compile.command.join(" ");
            let bash_command = format!("{} 2> stderr.txt", compile_cmd);

            cmd.arg("--run").arg("--")
                .arg("/bin/bash")
                .arg("-c")
                .arg(bash_command);

            debug!("Executing compile command: {:?}", cmd);

            let child = cmd.spawn()?;
            let child_id = child.id().unwrap_or(0);
            let execution_future = child.wait_with_output();
            let timeout_duration = Duration::from_secs(time_limit_s + 2);

            let output = match tokio::time::timeout(timeout_duration, execution_future).await {
                Ok(Ok(output)) => output,
                Ok(Err(e)) => return Err(anyhow!("Compile process failed to execute: {}", e)),
                Err(_) => {
                    error!("Compile timed out. Killing PID {}", child_id);
                    let _ = Command::new("kill").arg("-9").arg(child_id.to_string()).status().await;
                    sleep(Duration::from_millis(50)).await;
                    return Ok(CompilationResult {
                        success: false,
                        compile_time: time_limit_s as f64,
                        binary: None,
                        stderr: "Compilation timed out.".to_string(),
                    });
                }
            };

            let compile_time = start_time.elapsed().as_secs_f64();

            if output.status.success() {
                // Success: Read binary and cleanup
                let binary = fs::read(&executable_file_host_path).await?;
                let compiler_warnings = fs::read_to_string(&stderr_file_host_path).await.unwrap_or_default();
                let _ = fs::remove_file(&stderr_file_host_path).await;

                Ok(CompilationResult {
                    success: true,
                    compile_time,
                    binary: Some(binary),
                    stderr: compiler_warnings,
                })
            } else {
                // Failure: Read error logs
                let compiler_error = fs::read_to_string(&stderr_file_host_path).await.unwrap_or_default();
                let isolate_stderr = String::from_utf8_lossy(&output.stderr).to_string();
                let _ = fs::remove_file(&stderr_file_host_path).await;

                let final_stderr = if !compiler_error.is_empty() {
                    compiler_error
                } else {
                    format!("Compilation failed (Exit {}). Isolate: {}", output.status.code().unwrap_or(-1), isolate_stderr)
                };

                Ok(CompilationResult {
                    success: false,
                    compile_time,
                    binary: None,
                    stderr: final_stderr,
                })
            }
        }.await;

        // Cleanup source file
        let _ = fs::remove_file(&source_file_host_path).await;

        execution_result
    }


    pub async fn run(
        &self,
        config: &LanguageConfig,
        code_or_binary: &[u8],
        stdin: &str,
        time_limit_s: u64,
        pool: Arc<LocalSandboxPool>,
    ) -> Result<RunResult> {
        let sandbox = PrewarmedSandbox::new(pool).await?;
        let box_id = sandbox.box_id;

        let execution_result = async {
            let limits = config.run.limits.clone().unwrap_or_default();
            let mem_limit_kb = limits.memory_kb;
            let process_count = limits.processes;

            let box_path = format!("/var/local/lib/isolate/{}/box", box_id);
            let file_to_write_path = format!("{}/{}", box_path, &config.executable_filename);
            fs::write(&file_to_write_path, code_or_binary).await?;
            if config.is_compiled {
                let mut perms = fs::metadata(&file_to_write_path).await?.permissions();
                perms.set_mode(0o755);
                fs::set_permissions(&file_to_write_path, perms).await?;
            }
            fs::write(format!("{}/stdin.txt", box_path), stdin).await?;

            let meta_file_path = format!("/tmp/isolate_{}.txt", box_id);

            let mut cmd =
                self.build_base_command(box_id, config, time_limit_s, mem_limit_kb, process_count);
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

            // --- Start Change 3: Log isolate's errors during run ---
            // Store the result of the timeout first
            let output_result = tokio::time::timeout(timeout_duration, execution_future).await;

            // Now match on the timeout result
            let output = match output_result {
                 Ok(Ok(output)) => {
                    // Log isolate's own stderr IF IT'S NOT EMPTY
                    let isolate_stderr_str = String::from_utf8_lossy(&output.stderr);
                    if !isolate_stderr_str.is_empty() {
                        // Use warn level, might not be a fatal error for isolate itself
                        warn!("Isolate stderr (run, box {}): {}", box_id, isolate_stderr_str);
                    }
                    output // Continue with the output
                 }
                 Ok(Err(e)) => return Err(anyhow!("Run process failed: {}", e)),
                 Err(_) => { // Timeout occurred
                    error!(
                        "Run command for '{}' (PID: {}) timed out after {:?}. Attempting to kill.",
                        config.name, child_id, timeout_duration // Added timeout duration to log
                    );
                     // Kill process logic... (keep as is)
                     if let Err(e) = Command::new("kill")
                       .arg("-9")
                       .arg(child_id.to_string())
                       .status()
                       .await
                     {
                         error!("Failed to kill runaway run process with PID {}: {}", child_id, e);
                     }
                     tokio::time::sleep(Duration::from_millis(50)).await;
                     // Return timeout result... (keep most fields, update runtime/walltime)
                     return Ok(RunResult {
                         stdout: String::new(),
                         stderr: "Time Limit Exceeded".to_string(),
                         exit_code: -1, // Consistent exit code for TLE
                         run_time: time_limit_s as f64, // Use the limit as runtime on TO
                         wall_time: (time_limit_s * 2) as f64, // Reflect wall time limit approx
                         memory_used_kb: 0, // No reliable memory usage on TO
                         status: "TO".to_string(),
                     });
                 }
            };
            // --- End Change 3 (Part 1/2) ---

            // Read program outputs and metadata (keep as is)
            let program_stdout = fs::read_to_string(format!("{}/stdout.txt", box_path)).await.unwrap_or_default();
            let program_stderr = fs::read_to_string(format!("{}/stderr.txt", box_path)).await.unwrap_or_else(|_| "".to_string());
            let meta_content = fs::read_to_string(&meta_file_path).await.unwrap_or_else(|_| "status:XX".to_string());
            let _ = fs::remove_file(&meta_file_path).await;

            // Parse meta (keep as is)
            let mut run_time = 0.0;
            let mut wall_time = 0.0;
            let mut memory_used_kb = 0;
            let mut status = "Unknown".to_string();
            for line in meta_content.lines() { /* ... keep parsing logic ... */
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


            // If status is Time Out, ensure runtime reflects the limit (keep as is)
            if status == "TO" {
                run_time = time_limit_s as f64;
            }

            // --- Start Change 3: Improve final stderr ---
            // Get isolate's own stderr output
            let isolate_stderr = String::from_utf8_lossy(&output.stderr).to_string();

            // Determine the final stderr message based on status and available outputs
            let final_stderr = match status.as_str() {
                "TO" => "Time Limit Exceeded".to_string(), // Explicit TLE message
                "SG" | "RE" | "XX" => { // Signal, Runtime Error, Internal Error from isolate
                    // Prioritize program's stderr if it exists
                    if !program_stderr.is_empty() {
                        program_stderr
                    // Otherwise, use isolate's stderr if it exists
                    } else if !isolate_stderr.is_empty() {
                        format!("Runtime Error (isolate: {})", isolate_stderr)
                    // Fallback to just reporting the status code
                    } else {
                        format!("Runtime Error (status: {})", status)
                    }
                }
                // For any other status (like normal exit "OK")
                _ => {
                    // Still return program stderr if it exists (e.g., warnings printed)
                    if !program_stderr.is_empty() {
                        program_stderr
                    // Otherwise, return an empty string for successful runs without stderr
                    } else {
                        // Optionally include isolate_stderr even on success if needed for deep debugging
                        // isolate_stderr // <-- Uncomment this if you ALWAYS want to see isolate's stderr output
                        "".to_string() // <-- Keep it clean for normal successful runs
                    }
                }
            };
            // --- End Change 3 (Part 2/2) ---


            Ok(RunResult {
                stdout: program_stdout,
                stderr: final_stderr, 
                exit_code: output.status.code().unwrap_or(-1) as i64,
                run_time,
                wall_time,
                memory_used_kb,
                status, 
            })
        }
      .await;
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
        cmd.arg("--cg").arg(format!("--box-id={}", box_id));
        cmd.arg("--open-files=2048");

        if let Some(chroot_path) = &config.chroot_path {
            for path in &config.mount_paths {
                if path == "dev" {
                    cmd.arg(format!("--dir=/{0}={1}/{0}:dev:rw", path, chroot_path));
                } else if path == "go_cache" {
                     cmd.arg(format!("--dir=/{0}={1}/{0}:rw", path, chroot_path));
                } else {
                    cmd.arg(format!("--dir=/{0}={1}/{0}", path, chroot_path));
                }
            }
        }

        for (key, val) in &config.env_vars {
            cmd.arg(format!("--env={}={}", key, val));
        }

        cmd.arg("--dir=/tmp:tmp");
        cmd.arg("--dir=/proc=proc:fs");

        cmd.arg("--stdout=stdout.txt")
        .arg("--stderr=stderr.txt")
        .arg(format!("--time={}", time))
        .arg(format!("--wall-time={}", time * 2))
        .arg(format!("--cg-mem={}", mem))
        .arg("--mem=0")
        // .arg(format!("--fsize={}", 1024 * 1024))
        .arg(format!("--processes={}", proc));
        cmd
    }
}
