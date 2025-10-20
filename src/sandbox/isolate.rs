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

        let execution_result = async {
            let start_time = Instant::now();

            

            let limits = config.compile.limits.clone().unwrap_or_default();
            let time_limit_s = limits.time_s;
            let mem_limit_kb = limits.memory_kb;
            let process_count = limits.processes;

            let box_path = format!("/var/local/lib/isolate/{}/box", box_id);
            fs::write(format!("{}/{}", box_path, &config.source_filename), code).await?;

            let mut cmd =
                self.build_base_command(box_id, config, time_limit_s, mem_limit_kb, process_count);
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
                    error!(
                        "Compile command for '{}' (PID: {}) timed out. Attempting to kill.",
                        config.name, child_id
                    );
                    if let Err(e) = Command::new("kill")
                      .arg("-9")
                      .arg(child_id.to_string())
                      .status()
                      .await
                    {
                        error!(
                            "Failed to kill runaway compile process with PID {}: {}",
                            child_id, e
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    return Ok(CompilationResult {
                        success: false,
                        compile_time: time_limit_s as f64,
                        binary: None,
                        stderr: "Compilation timed out.".to_string(),
                    });
                }
            };

            let compile_time = start_time.elapsed().as_secs_f64();
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();

            if output.status.success() ||!config.is_compiled {
                let binary = if!config.is_compiled {
                    code.as_bytes().to_vec()
                } else {
                    fs::read(format!("{}/{}", box_path, &config.executable_filename)).await?
                };
                Ok(CompilationResult {
                    success: true,
                    compile_time,
                    binary: Some(binary),
                    stderr,
                })
            } else {
                Ok(CompilationResult {
                    success: false,
                    compile_time,
                    binary: None,
                    stderr,
                })
            }
        }
      .await;
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

            let output = match tokio::time::timeout(timeout_duration, execution_future).await {
                Ok(Ok(output)) => output,
                Ok(Err(e)) => return Err(anyhow!("Run process failed: {}", e)),
                Err(_) => {
                    error!(
                        "Run command for '{}' (PID: {}) timed out. Attempting to kill.",
                        config.name, child_id
                    );
                    if let Err(e) = Command::new("kill")
                      .arg("-9")
                      .arg(child_id.to_string())
                      .status()
                      .await
                    {
                        error!(
                            "Failed to kill runaway run process with PID {}: {}",
                            child_id, e
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
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

            let program_stdout = fs::read_to_string(format!("{}/stdout.txt", box_path))
              .await
              .unwrap_or_default();
            let program_stderr = fs::read_to_string(format!("{}/stderr.txt", box_path))
              .await
              .unwrap_or_else(|_| "".to_string());
            let meta_content = fs::read_to_string(&meta_file_path)
              .await
              .unwrap_or_else(|_| "status:XX".to_string());
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
            } else if!program_stderr.is_empty() {
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

        let lang_chroot = format!("/opt/evalx/chroots/{}", config.name);
        cmd.arg(format!("--dir={}=/", lang_chroot));

        let essential_dirs = ["/dev", "/proc", "/sys", "/tmp", "/var", "/etc"];
        for dir in essential_dirs.iter() {
            cmd.arg(format!("--dir={}={}", dir, dir));
        }

        cmd.arg("--env=PATH=/bin:/usr/bin:/usr/local/bin:/usr/sbin");

        for (key, val) in &config.env_vars {
            cmd.arg(format!("--env={}={}", key, val));
        }

        match config.name.as_str() {
            "python" => {
                cmd.arg("--env=PYTHONPATH=/usr/lib/python3.9:/usr/local/lib/python3.9/dist-packages");
            }
            "javascript" => {
                cmd.arg("--env=NODE_PATH=/usr/lib/nodejs:/usr/local/lib/node_modules");
            }
            "java11" | "java21" => {
                cmd.arg("--env=JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64");
            }
            _ => {}
        }

        cmd.arg("--cg")
          .arg(format!("--cg-mem={}", mem))
          .arg(format!("--box-id={}", box_id))
          .arg("--stdout=stdout.txt")
          .arg("--stderr=stderr.txt")
          .arg(format!("--time={}", time))
          .arg(format!("--wall-time={}", time * 2))
          .arg(format!("--fsize={}", 1024 * 1024))
          .arg(format!("--mem={}", mem))
          .arg(format!("--processes={}", proc));

        cmd
    }
}