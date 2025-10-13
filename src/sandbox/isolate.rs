use anyhow::{anyhow, Result};
use lazy_static::lazy_static;
use std::sync::Arc;
use std::collections::VecDeque;
use std::os::unix::fs::PermissionsExt;
use std::time::Instant;
use tokio::fs;
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, warn};

use crate::languages::config::LanguageConfig;

// --- MODIFICATION START: Implement the Box ID Pool ---

lazy_static! {
    // This creates a shared, thread-safe queue containing all possible box IDs (0-999).
    static ref BOX_ID_POOL: Arc<Mutex<VecDeque<u16>>> = {
        let queue: VecDeque<u16> = (0..1000).collect();
        Arc::new(Mutex::new(queue))
    };
}

// A helper struct to ensure the box ID is returned to the pool even if errors occur.
struct BoxLease {
    id: u16,
}

impl BoxLease {
    // Acquires a box ID from the pool, waiting if none are available.
    async fn new() -> Result<Self> {
        let mut retries = 0;
        loop {
            let mut pool = BOX_ID_POOL.lock().await;
            if let Some(id) = pool.pop_front() {
                return Ok(Self { id });
            }
            // Drop the lock before sleeping to allow other threads to push IDs back.
            drop(pool);
            
            if retries >= 50 { // Wait for up to 5 seconds
                return Err(anyhow!("Failed to acquire a sandbox box ID: Pool is empty."));
            }
            warn!("Box ID pool is empty, waiting...");
            sleep(Duration::from_millis(100)).await;
            retries += 1;
        }
    }
}

// The `Drop` trait ensures that when a `BoxLease` goes out of scope,
// its ID is automatically returned to the pool.
impl Drop for BoxLease {
    fn drop(&mut self) {
        let pool = BOX_ID_POOL.clone();
        let id = self.id;
        tokio::spawn(async move {
            let mut pool = pool.lock().await;
            pool.push_back(id);
        });
    }
}

// --- MODIFICATION END ---


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
    // REMOVED: `generate_unique_box_id` is no longer needed.

    pub async fn compile(
        &self,
        config: &LanguageConfig,
        code: &str,
    ) -> Result<CompilationResult> {
        let start_time = Instant::now();
        
        // MODIFIED: Acquire a guaranteed-unique box ID from the pool.
        let lease = BoxLease::new().await?;
        let box_id = lease.id;

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

        // FIX: Spawn the process to get its PID for robust timeout handling.
        // FIX: Spawn the process to get its PID for robust timeout handling.
        let child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => return Err(anyhow!("Failed to spawn compile command: {}", e)),
        };
        let child_id = child.id().unwrap_or(0);

        let execution_future = child.wait_with_output();
        let timeout_duration = Duration::from_secs(time_limit_s + 2);

        let output = match tokio::time::timeout(timeout_duration, execution_future).await {
            // Process finished on its own.
            Ok(Ok(output)) => output,
            // Process failed immediately after spawning.
            Ok(Err(e)) => return Err(anyhow!("Compile process failed: {}", e)),
            // Our timeout was hit. The process is now a zombie.
            Err(_) => {
                error!(
                    "Compile command for '{}' (PID: {}) timed out. Attempting to kill.",
                    config.name, child_id
                );
                // Explicitly kill the runaway process.
                if let Err(e) = Command::new("kill").arg("-9").arg(child_id.to_string()).status().await {
                    error!("Failed to kill runaway compile process with PID {}: {}", child_id, e);
                }
                return Err(anyhow!("Compiler process hung and was terminated."));
            }
        };

        let compile_time = start_time.elapsed().as_secs_f64();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        let result = if output.status.success() || !config.is_compiled {
            let binary = if !config.is_compiled {
                code.as_bytes().to_vec()
            } else {
                fs::read(format!("{}/{}", box_path, &config.executable_filename)).await?
            };
            CompilationResult { success: true, compile_time, binary: Some(binary), stderr }
        } else {
            CompilationResult { success: false, compile_time, binary: None, stderr }
        };

        self.cleanup_box(box_id).await?;
        Ok(result)
    }
    
    // FIX: The function signature is changed to accept the final time limit.
    pub async fn run(
        &self,
        config: &LanguageConfig,
        code_or_binary: &[u8],
        stdin: &str,
        time_limit_s: u64, // The timeout is now passed in.
    ) -> Result<RunResult> {
        let lease = BoxLease::new().await?;
        let box_id = lease.id;

        self.init_box(box_id).await?;

        // FIX: Time limit is now from the argument. Memory/process limits are still from config.
        let limits = config.run.limits.clone().unwrap_or_default();
        let mem_limit_kb = limits.memory_kb;
        let process_count = limits.processes;

        let box_path = format!("/var/local/lib/isolate/{}/box", box_id);
        let file_to_write_path = format!("{}/{}", box_path, &config.executable_filename);
        
        fs::write(&file_to_write_path, code_or_binary).await?;
        
        // MODIFIED: Dynamically set permissions based on the config file.
        // This automatically handles any compiled language.
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

        // --- START OF DEFINITIVE FIX ---

        // 1. Spawn the process to get its PID for robust timeout handling.
        let child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => return Err(anyhow!("Failed to spawn run command: {}", e)),
        };
        let child_id = child.id().unwrap_or(0);

        let execution_future = child.wait_with_output();
        let timeout_duration = Duration::from_secs(time_limit_s + 2);

        // 2. Wait for the process to finish, with our timeout as a safety net.
        // 2. Wait for the process to finish, with our timeout as a safety net.
        let output = match tokio::time::timeout(timeout_duration, execution_future).await {
            Ok(Ok(output)) => {
                // The process finished on its own (successfully or with an error).
                output
            }
            Ok(Err(e)) => {
                // The process failed to even run properly.
                return Err(anyhow!("Run process failed: {}", e));
            }
            Err(_) => { // Our timeout was hit. The process is a zombie.
                error!(
                    "Run command for '{}' (PID: {}) timed out. Attempting to kill.",
                    config.name, child_id
                );
                // Explicitly kill the runaway process.
                if let Err(e) = Command::new("kill").arg("-9").arg(child_id.to_string()).status().await {
                    error!("Failed to kill runaway run process with PID {}: {}", child_id, e);
                }
                
                // THIS IS THE FIX: Instead of returning an error, we now
                // construct and return a proper Time Limit Exceeded result.
                let tle_result = RunResult {
                    stdout: String::new(),
                    stderr: "Time Limit Exceeded".to_string(),
                    exit_code: -1,
                    run_time: time_limit_s as f64,
                    wall_time: 0.0,
                    memory_used_kb: 0,
                    status: "TO".to_string(),
                };
                
                self.cleanup_box(box_id).await?;
                return Ok(tle_result); // Return the valid TLE result.
            }
        };

        // 3. NOW that the process is finished, read the output and metadata files.
        let program_stdout = fs::read_to_string(format!("{}/stdout.txt", box_path))
            .await
            .unwrap_or_default();
        let program_stderr = fs::read_to_string(format!("{}/stderr.txt", box_path))
            .await
            .unwrap_or_default();
        let meta_content = fs::read_to_string(&meta_file_path)
            .await
            .unwrap_or_else(|_| "status:XX".to_string());
        
        let _ = fs::remove_file(&meta_file_path).await;

        // --- END OF DEFINITIVE FIX ---

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
            // If isolate reported a Time Out, this is the only message that matters.
            "Time Limit Exceeded".to_string()
        } else if !program_stderr.is_empty() {
            // Otherwise, prioritize the program's own stderr (e.g., for runtime errors).
            program_stderr
        } else {
            // Finally, fall back to isolate's own stderr metadata.
            isolate_stderr
        };

        let result = RunResult {
            stdout: program_stdout,
            stderr: final_stderr,
            exit_code: output.status.code().unwrap_or(-1) as i64,
            run_time,
            wall_time,
            memory_used_kb,
            status,
        };
        
        self.cleanup_box(box_id).await?;
        Ok(result)
    }

    fn build_base_command(&self, box_id: u16, config: &LanguageConfig, time: u64, mem: u64, proc: u64) -> Command {
        // ... (this function's logic remains the same, but takes u16 for box_id)
        let mut cmd;
        if let Some(chroot_path) = &config.chroot_path {
            cmd = Command::new("setarch");
            cmd.arg(std::env::consts::ARCH).arg("-R").arg("isolate");
            cmd.arg("--cg")
               .arg(format!("--box-id={}", box_id));
            cmd.arg(format!("--dir={}", chroot_path));
            cmd.arg("--dir=/etc");
        } else {
            cmd = Command::new("isolate");
            cmd.arg("--cg")
               .arg(format!("--box-id={}", box_id))
               .arg("--full-env")
               .arg("--env=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
               .arg("--dir=/etc")
               .arg("--dir=/bin")
               .arg("--dir=/usr/bin")
         
               .arg("--dir=/usr/local/bin")
               .arg("--dir=/lib")
               .arg("--dir=/lib64")
               .arg("--dir=/usr/lib")
               .arg("--dir=/usr/include")
               .arg("--dir=/proc")
               .arg("--stdout=stdout.txt");
        
                for path in &config.mount_paths {
                    cmd.arg(format!("--dir={}", path));
                }

                
        }
                
        // Add stdout and stderr file redirection
        cmd.arg("--stdout=stdout.txt")
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
    
    // REMOVED: `get_clean_box` is replaced by the `BoxLease` mechanism.

    async fn init_box(&self, box_id: u16) -> Result<()> {
        let output = Command::new("isolate")
            .arg("--cg")
            .arg(format!("--box-id={}", box_id))
            .arg("--init")
            .output()
            .await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // This error is now more critical as collisions shouldn't happen.
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