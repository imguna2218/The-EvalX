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

// REPLACED: This is the new "smart key" for the pre-warmed pool.
// It gets a ready-to-use box ID from the pool.
// When it's destroyed (function ends or panics), the `drop` code runs automatically
// and sends the used box ID to the cleanup queue.
pub struct PrewarmedSandbox {
    pub box_id: u16,
    redis_client: RedisClient,
}

impl PrewarmedSandbox {
    // This function gets a ready box from the pool. It will wait up to 10 seconds.
    pub async fn new(redis_client: RedisClient) -> Result<Self> {
        const READY_POOL_KEY: &str = "evalx:sandboxes:ready";
        let mut conn = redis_client.get_multiplexed_async_connection().await?;

        // Use LPOP instead of BRPOP for immediate availability check
        let box_id: Option<u16> = conn.lpop(READY_POOL_KEY, None).await?;

        match box_id {
            Some(box_id) => {
                debug!("Acquired pre-warmed sandbox ID {}", box_id);
                Ok(Self { box_id, redis_client })
            }
            None => Err(anyhow!("No sandbox IDs available in pool")),
        }
    }
}

// This is the magic. This `drop` function is GUARANTEED by Rust to run
// when the PrewarmedSandbox is destroyed, no matter what.
impl Drop for PrewarmedSandbox {
    fn drop(&mut self) {
        const CLEANUP_POOL_KEY: &str = "evalx:sandboxes:cleanup";
        debug!("Sandbox {} is finished. Sending to cleanup queue.", self.box_id);
        let client = self.redis_client.clone();
        let id = self.box_id;
        tokio::spawn(async move {
            let mut conn = match client.get_multiplexed_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    error!("CRITICAL LEAK: Could not get Redis connection to send box #{} for cleanup: {}", id, e);
                    return;
                }
            };
            if let Err(e) = conn.rpush::<_, _, ()>(CLEANUP_POOL_KEY, id).await {
                error!("CRITICAL LEAK: Failed to send sandbox ID {} to cleanup queue: {}", id, e);
            }
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
pub async fn compile(
    &self,
    config: &LanguageConfig,
    code: &str,
    redis_client: RedisClient,
) -> Result<CompilationResult> {
    let sandbox = PrewarmedSandbox::new(redis_client).await?;
    let box_id = sandbox.box_id;

    // --- DEFINE box_path HERE ---
    let box_path = format!("/var/local/lib/isolate/{}/box", box_id);

    let execution_result = async {
        let start_time = Instant::now();
        let limits = config.compile.limits.clone().unwrap_or_default();
        let time_limit_s = limits.time_s;
        let mem_limit_kb = limits.memory_kb;
        let process_count = limits.processes;

        // --- Use box_path defined outside ---
        // let box_path = format!("/var/local/lib/isolate/{}/box", box_id); // REMOVE THIS LINE

        let source_file_sandbox_path = format!("/box/{}", &config.source_filename);
        let stderr_file_sandbox_path = "/box/stderr.txt";
        let executable_file_sandbox_path = format!("/box/{}", &config.executable_filename);

        // --- Debug --version check (optional) ---
        // ... (rest of the debug block, uses box_path) ...

        let mut cmd =
            self.build_base_command(box_id, config, time_limit_s, mem_limit_kb, process_count);

        let compile_command_str = config.compile.command.join(" ");
        let bash_command = format!(
            "printf '%s' \"$1\" > \"{}\" && cd /box && {} 2> \"{}\"; exit $?",
            source_file_sandbox_path,
            compile_command_str,
            stderr_file_sandbox_path
        );

        cmd.arg("--run").arg("--")
           .arg("/bin/bash")
           .arg("-c")
           .arg(bash_command)
           .arg("bash")
           .arg(code);

        debug!("Executing compile command via bash -c (writing file inside): {:?}", cmd);

        let child = cmd.spawn()?;
        let child_id = child.id().unwrap_or(0);
        let execution_future = child.wait_with_output();
        let timeout_duration = Duration::from_secs(time_limit_s + 2);

        // --- Timeout logic ---
        let output = match tokio::time::timeout(timeout_duration, execution_future).await {
            // ... (timeout handling as before, uses box_id) ...
             Ok(Ok(output)) => {
                 let isolate_stderr_str = String::from_utf8_lossy(&output.stderr);
                 if !isolate_stderr_str.is_empty() {
                    warn!("Isolate stderr (compile, box {}): {}", box_id, isolate_stderr_str);
                 }
                 output
            }
             Ok(Err(e)) => return Err(anyhow!("Compile process failed to execute: {}", e)),
             Err(_) => {
                 error!(
                     "Compile command for '{}' (PID: {}) timed out after {:?}. Killing.",
                     config.name, child_id, timeout_duration
                 );
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

        // --- Use box_path defined outside ---
        let stderr_file_host_path = format!("{}/stderr.txt", box_path);
        let executable_file_host_path = format!("{}/{}", box_path, &config.executable_filename);

        if output.status.success() {
            let binary = fs::read(&executable_file_host_path).await?;
            let compiler_warnings = fs::read_to_string(&stderr_file_host_path)
                                     .await
                                     .unwrap_or_default();
            let _ = fs::remove_file(&stderr_file_host_path).await; // Cleanup stderr file
            Ok(CompilationResult {
                success: true,
                compile_time,
                binary: Some(binary),
                stderr: compiler_warnings,
            })
        } else {
            let compiler_error = fs::read_to_string(&stderr_file_host_path)
                                     .await
                                     .unwrap_or_else(|e| format!("Failed to read compiler stderr.txt: {}", e));
            let _ = fs::remove_file(&stderr_file_host_path).await; // Cleanup stderr file
            let isolate_stderr = String::from_utf8_lossy(&output.stderr).to_string();

            let final_stderr = if !compiler_error.is_empty() {
                compiler_error
            } else if !isolate_stderr.is_empty() {
                format!("Compilation failed. Isolate stderr: {}", isolate_stderr)
            } else {
                format!("Compilation failed with exit code {:?} but no error message captured.", output.status.code())
            };

            error!("Compilation failed for '{}' (Box {}). Exit code: {:?}. Captured stderr: {}",
                   config.name, box_id, output.status.code(), final_stderr);

            Ok(CompilationResult {
                success: false,
                compile_time,
                binary: None,
                stderr: final_stderr,
            })
        }
    }
    .await; // End of the async block

    // --- Use box_path defined outside for cleanup ---
    let source_file_host_path = format!("{}/{}", box_path, &config.source_filename);
    let _ = fs::remove_file(&source_file_host_path).await; // Cleanup source file

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
        let sandbox = PrewarmedSandbox::new(redis_client).await?;
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
                stderr: final_stderr, // Use the new improved final_stderr
                exit_code: output.status.code().unwrap_or(-1) as i64, // Use isolate's exit code
                run_time,
                wall_time,
                memory_used_kb,
                status, // Include the raw isolate status code
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

    // Mount chroot directories based on language type
    if let Some(chroot_path) = &config.chroot_path {
        match config.name.as_str() {
            "java11" | "java21" | "java" => {
                // Java: Mount entire chroot as root + /etc
                cmd.arg(format!("--dir={}=/", chroot_path));
                cmd.arg("--dir=/etc=/etc");
                
                // Java environment variables
                let java_version = if config.name == "java11" { "11" } else { "21" };
                let java_home = format!("/usr/lib/jvm/java-{}-openjdk-amd64", java_version);
                cmd.arg(format!("--env=LD_LIBRARY_PATH={}/lib/jli:{}/lib/server:{}/lib:/lib/x86_64-linux-gnu:/usr/lib/x86_64-linux-gnu", java_home, java_home, java_home));
                cmd.arg(format!("--env=JAVA_HOME={}", java_home));
                cmd.arg(format!("--env=PATH={}/bin:/usr/bin:/bin", java_home));
            }
            "python" => {
                // Python: Mount specific directories
                cmd.arg(format!("--dir={}/usr=/usr", chroot_path));
                cmd.arg(format!("--dir={}/lib=/lib", chroot_path));
                cmd.arg(format!("--dir={}/lib64=/lib64", chroot_path));
                cmd.arg(format!("--dir={}/bin=/bin", chroot_path));
                cmd.arg(format!("--dir={}/etc=/etc", chroot_path));
                cmd.arg(format!("--dir={}/var=/var", chroot_path));
                
                // Python environment variables
                cmd.arg("--env=PATH=/usr/bin:/bin");
                cmd.arg("--env=PYTHONPATH=/usr/lib/python3.9:/usr/local/lib/python3.9/dist-packages");
            }
            "c" | "cpp" => {
                // C/C++: Mount specific directories
                cmd.arg(format!("--dir={}/usr=/usr", chroot_path));
                cmd.arg(format!("--dir={}/lib=/lib", chroot_path));
                cmd.arg(format!("--dir={}/lib64=/lib64", chroot_path));
                cmd.arg(format!("--dir={}/bin=/bin", chroot_path));
                cmd.arg(format!("--dir={}/etc=/etc", chroot_path));
                cmd.arg(format!("--dir={}/var=/var", chroot_path));
                
                // C/C++ environment variables
                cmd.arg("--env=PATH=/usr/bin:/bin");
            }
            _ => {
                // Default: Mount specific directories
                cmd.arg(format!("--dir={}/usr=/usr", chroot_path));
                cmd.arg(format!("--dir={}/lib=/lib", chroot_path));
                cmd.arg(format!("--dir={}/lib64=/lib64", chroot_path));
                cmd.arg(format!("--dir={}/bin=/bin", chroot_path));
                cmd.arg("--env=PATH=/usr/bin:/bin");
            }
        }
    }

    // Additional custom environment variables from config
    for (key, val) in &config.env_vars {
        cmd.arg(format!("--env={}={}", key, val));
    }

    cmd.arg("--dir=/tmp=/tmp:rw");

    // Resource limits and I/O redirection
    cmd.arg("--stdout=stdout.txt")
      .arg("--stderr=stderr.txt")
      .arg(format!("--time={}", time))
      .arg(format!("--wall-time={}", time * 2))
      .arg(format!("--cg-mem={}", mem))
      .arg(format!("--mem={}", mem))
      .arg(format!("--fsize={}", 1024 * 1024))
      .arg(format!("--processes={}", proc));

    cmd
}
}