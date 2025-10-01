// src/sandbox/isolate.rs
use anyhow::{anyhow, Result};
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::fs;
use tokio::process::Command;
use tracing::{debug, warn};

// A global, thread-safe counter to generate unique numeric box IDs for Isolate.
static BOX_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

// Represents the result of a compilation operation within the sandbox.
pub struct CompilationResult {
    pub success: bool,
    pub compile_time: f64,
    pub binary: Option<Vec<u8>>,
    pub stderr: String,
}

// Represents the result of a single run operation within the sandbox.
pub struct RunResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i64,
    pub run_time: f64,
    pub wall_time: f64,
    pub memory_used_kb: u64,
    pub status: String,
}

// The main struct for interacting with the Isolate sandbox.
pub struct IsolateSandbox;

impl IsolateSandbox {
    /// Manages the full lifecycle of compiling user code within a secure Isolate sandbox.
    pub async fn compile(
        &self,
        language: &str,
        code: &str,
        time_limit_s: u64,
        mem_limit_kb: u64,
    ) -> Result<CompilationResult> {
        let start_time = Instant::now();
        let box_id = BOX_ID_COUNTER.fetch_add(1, Ordering::Relaxed) % 1000;
        self.init_box(&box_id.to_string()).await?;

        let (source_filename, executable_filename, compile_command, java_home, node_path, python_path) = match language {
            "c" => ("main.c", "main", vec!["/usr/bin/gcc", "-o", "main", "main.c"], "".to_string(), "".to_string(), "".to_string()),
            "cpp" => ("main.cpp", "main", vec!["/usr/bin/g++", "-o", "main", "main.cpp"], "".to_string(), "".to_string(), "".to_string()),
            "java" | "java21" => ("Main.java", "Main.class", vec!["/usr/bin/javac", "Main.java"], "/usr/lib/jvm/java-21-openjdk-amd64".to_string(), "".to_string(), "".to_string()),
            "java11" => ("Main.java", "Main.class", vec!["/usr/bin/javac", "Main.java"], "/usr/lib/jvm/java-11-openjdk-amd64".to_string(), "".to_string(), "".to_string()),
            "python" => ("main.py", "main.py", vec!["echo", "no-compile"], "".to_string(), "".to_string(), "".to_string()),
            "javascript" => ("main.js", "main.js", vec!["echo", "no-compile"], "".to_string(), "/usr/lib/nodejs".to_string(), "".to_string()),
            _ => return Err(anyhow!("Unsupported compiled language: {}", language)),
        };

        let box_path = format!("/var/local/lib/isolate/{}/box", box_id);
        fs::write(format!("{}/{}", box_path, source_filename), code).await?;

        let mut cmd = Command::new("isolate");
        cmd.arg(format!("--box-id={}", box_id))
            .arg(format!("--time={}", time_limit_s))
            .arg(format!("--wall-time={}", time_limit_s * 2))
            .arg(format!("--mem={}", mem_limit_kb))
            .arg("--fsize=102400")
            .arg("--processes=10")
            // --- Enhanced environment propagation ---
            // Propagate the full host environment to ensure PATH and other vars are available.
            .arg("--full-env")
            // Explicitly ensure PATH is set to include all necessary directories.
            .arg("--env=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
            // --- Comprehensive directory mounting ---
            .arg("--dir=/etc:noexec")
            .arg("--dir=/bin")
            .arg("--dir=/usr/bin")
            .arg("--dir=/usr/local/bin")
            .arg("--dir=/lib")
            .arg("--dir=/lib64")
            .arg("--dir=/usr/lib")
            .arg("--dir=/usr/lib/gcc")
            .arg("--dir=/usr/lib/nodejs")
            .arg("--dir=/usr/lib/python3")
            .arg("--dir=/usr/lib/python3.10")
            .arg("--dir=/usr/lib/jvm")
            .arg("--dir=/lib/x86_64-linux-gnu")
            .arg("--dir=/usr/lib/x86_64-linux-gnu")
            .arg("--dir=/etc/alternatives")
            .arg("--dir=/usr/include")
            .arg("--dir=/usr/lib/gcc/x86_64-linux-gnu/11")
            .arg("--dir=/usr/lib/jvm/java-11-openjdk-amd64")
            .arg("--dir=/usr/lib/jvm/java-21-openjdk-amd64")
            .arg("--dir=/usr/share")
            .arg("--dir=/usr/share/java");

        // Set language-specific environment variables.
        if !java_home.is_empty() {
            cmd.arg(format!("--env=JAVA_HOME={}", java_home));
        }
        if !node_path.is_empty() {
            cmd.arg(format!("--env=NODE_PATH={}", node_path));
        }
        if !python_path.is_empty() {
            cmd.arg(format!("--env=PYTHONPATH={}", python_path));
        }

        cmd.arg("--run")
            .arg("--")
            .args(compile_command);

        debug!("Executing compile command: {:?}", cmd);
        let output = cmd.output().await?;
        let compile_time = start_time.elapsed().as_secs_f64();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        let result = if output.status.success() || matches!(language, "python" | "javascript") {
            let binary = if matches!(language, "python" | "javascript") {
                code.as_bytes().to_vec()
            } else {
                fs::read(format!("{}/{}", box_path, executable_filename)).await?
            };
            CompilationResult {
                success: true,
                compile_time,
                binary: Some(binary),
                stderr,
            }
        } else {
            CompilationResult {
                success: false,
                compile_time,
                binary: None,
                stderr,
            }
        };

        self.cleanup_box(&box_id.to_string()).await?;
        Ok(result)
    }

    /// Manages the full lifecycle of running code in a sandbox.
    pub async fn run(
        &self,
        language: &str,
        code_or_binary: &[u8],
        stdin: &str,
        time_limit_s: f64,
        mem_limit_kb: u64,
    ) -> Result<RunResult> {
        let box_id = BOX_ID_COUNTER.fetch_add(1, Ordering::Relaxed) % 1000;
        self.init_box(&box_id.to_string()).await?;

        let (filename_to_write, run_command, java_home, node_path, python_path) = match language {
            "c" | "cpp" => ("main", vec!["./main"], "".to_string(), "".to_string(), "".to_string()),
            "java" | "java21" => ("Main.class", vec!["/usr/bin/java", "Main"], "/usr/lib/jvm/java-21-openjdk-amd64".to_string(), "".to_string(), "".to_string()),
            "java11" => ("Main.class", vec!["/usr/bin/java", "Main"], "/usr/lib/jvm/java-11-openjdk-amd64".to_string(), "".to_string(), "".to_string()),
            "python" => ("main.py", vec!["/usr/bin/python3", "main.py"], "".to_string(), "".to_string(), "/usr/lib/python3.10".to_string()),
            "javascript" => ("main.js", vec!["/usr/bin/node", "main.js"], "".to_string(), "/usr/lib/nodejs".to_string(), "".to_string()),
            _ => return Err(anyhow!("Unsupported language for run: {}", language)),
        };

        let box_path = format!("/var/local/lib/isolate/{}/box", box_id);
        let file_path = format!("{}/{}", box_path, filename_to_write);
        fs::write(&file_path, code_or_binary).await?;
        if matches!(language, "c" | "cpp") {
            fs::set_permissions(&file_path, std::fs::Permissions::from_mode(0o755)).await?;
        }
        fs::write(format!("{}/stdin.txt", box_path), stdin).await?;

        let meta_file_path = format!("/tmp/isolate_{}.txt", box_id);
        let mut cmd = Command::new("isolate");
        cmd.arg(format!("--box-id={}", box_id))
            .arg(format!("--time={}", time_limit_s))
            .arg(format!("--wall-time={}", time_limit_s * 2.0))
            .arg(format!("--mem={}", mem_limit_kb))
            .arg("--fsize=10240")
            .arg("--processes=5")
             // --- Enhanced environment propagation ---
            .arg("--full-env")
            // Explicitly ensure PATH is set to include all necessary directories.
            .arg("--env=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
            // --- Comprehensive directory mounting for runtime ---
            .arg("--dir=/etc:noexec")
            .arg("--dir=/bin")
            .arg("--dir=/usr/bin")
            .arg("--dir=/usr/local/bin")
            .arg("--dir=/lib")
            .arg("--dir=/lib64")
            .arg("--dir=/usr/lib")
            .arg("--dir=/usr/lib/gcc")
            .arg("--dir=/usr/lib/nodejs")
            .arg("--dir=/usr/lib/python3")
            .arg("--dir=/usr/lib/python3.10")
            .arg("--dir=/usr/lib/jvm")
            .arg("--dir=/lib/x86_64-linux-gnu")
            .arg("--dir=/usr/lib/x86_64-linux-gnu")
            .arg("--dir=/etc/alternatives")
            .arg("--dir=/usr/lib/python3.10")
            .arg("--dir=/usr/lib/jvm/java-11-openjdk-amd64")
            .arg("--dir=/usr/lib/jvm/java-21-openjdk-amd64")
            .arg("--dir=/usr/share")
            .arg("--dir=/usr/share/java");

        // Set language-specific environment variables.
        if !java_home.is_empty() {
            cmd.arg(format!("--env=JAVA_HOME={}", java_home));
        }
        if !node_path.is_empty() {
            cmd.arg(format!("--env=NODE_PATH={}", node_path));
        }
        if !python_path.is_empty() {
            cmd.arg(format!("--env=PYTHONPATH={}", python_path));
        }

        cmd.arg("--stdin=stdin.txt")
            .arg(format!("--meta={}", meta_file_path))
            .arg("--run")
            .arg("--")
            .args(run_command);

        debug!("Executing run command: {:?}", cmd);
        let output = cmd.output().await?;
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
            run_time = time_limit_s;
        }

        let result = RunResult {
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            exit_code: output.status.code().unwrap_or(-1) as i64,
            run_time,
            wall_time,
            memory_used_kb,
            status,
        };
        self.cleanup_box(&box_id.to_string()).await?;
        Ok(result)
    }

    /// Initializes a new, empty sandbox directory.
    async fn init_box(&self, box_id: &str) -> Result<()> {
        let output = Command::new("isolate")
            .arg(format!("--box-id={}", box_id))
            .arg("--init")
            .output()
            .await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("Failed to initialize isolate box {}: {}", box_id, stderr));
        }
        Ok(())
    }

    /// Cleans up and removes the sandbox directory.
    async fn cleanup_box(&self, box_id: &str) -> Result<()> {
        let output = Command::new("isolate")
            .arg(format!("--box-id={}", box_id))
            .arg("--cleanup")
            .output()
            .await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!("Failed to cleanup isolate box {}: {}", box_id, stderr);
        }
        Ok(())
    }
}