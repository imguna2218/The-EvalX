// src/sandbox/isolate.rs
use anyhow::{anyhow, Result};
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::process::Command;
use tokio::time::{sleep, Duration};
use std::path::Path;
use tracing::{debug, error, warn};

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

// CHANGED: Use u16 instead of u32 since we only need 0-999 range
static BOX_ID_COUNTER: AtomicU16 = AtomicU16::new(0);

impl IsolateSandbox {
    /// ADDED: Returns the time limit in seconds and memory limit in kilobytes for a given language.
    fn get_language_limits(language: &str) -> (u64, u64) {
        const MB: u64 = 1024;
        match language {
            "java" | "java11" | "java21" => (8, 512 * MB),
            "c" | "cpp" => (10, 256 * MB),
            "python" | "javascript" => (5, 128 * MB),
            _ => (10, 256 * MB), // A sensible default for other potential languages
        }
    }
    
    // FIXED: Generate box ID within Isolate's allowed range (0-999)
    fn generate_unique_box_id() -> String {
        let counter = BOX_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        // Isolate only allows box IDs from 0-999
        // Use nanosecond timestamp to ensure uniqueness within the 0-999 range
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        
        // Combine timestamp with counter and modulo 1000 to stay within range
        let box_id = (time % 1000 + counter as u128) % 1000;
        format!("{}", box_id)
    }

    /// MODIFIED: Signature now only requires language and code. Limits are determined internally.
    pub async fn compile(
        &self,
        language: &str,
        code: &str,
    ) -> Result<CompilationResult> {
        let start_time = Instant::now();
        
        // FIXED: Add retry logic for box initialization with proper ID range
        let mut retries = 0;
        let max_retries = 5; // Increased retries for better reliability
        let mut box_id = Self::generate_unique_box_id();

        loop {
            match self.init_box(&box_id).await {
                Ok(()) => break,
                Err(e) if retries < max_retries => {
                    retries += 1;
                    warn!("Failed to init box {}, retry {}/{}: {}", box_id, retries, max_retries, e);
                    // Increase backoff time and generate new ID
                    tokio::time::sleep(tokio::time::Duration::from_millis(100 * retries as u64)).await;
                    box_id = Self::generate_unique_box_id();
                }
                Err(e) => return Err(anyhow!("Failed to initialize sandbox after {} retries: {}", max_retries, e)),
            }
        }

        // MODIFIED: Getting limits from the new centralized function.
        let (time_limit_s, mem_limit_kb) = Self::get_language_limits(language);

        let (source_filename, executable_filename, compile_command, java_home, node_path, python_path) = match language {
            "c" => (
                "main.c",
                "main",
                vec!["/usr/bin/gcc", "-o", "main", "main.c"],
                "".to_string(),
                "".to_string(),
                "".to_string(),
            ),
            "cpp" => (
                "main.cpp",
                "main",
                vec!["/usr/bin/g++", "-o", "main", "main.cpp"],
                "".to_string(),
                "".to_string(),
                "".to_string(),
            ),
            "java" | "java21" => {
                let v = vec![
                    "/usr/lib/jvm/java-21-openjdk-amd64/bin/javac",
                    "-J-XX:TieredStopAtLevel=1",
                    "-J-Xms512m",
                    "-J-Xmx512m",
                    "-J-XX:MaxMetaspaceSize=192m",
                    "-J-XX:ReservedCodeCacheSize=64m",
                    "-J-XX:+UseSerialGC",
                    "Main.java",
                ];
                (
                    "Main.java",
                    "Main.class",
                    v,
                    "/usr/lib/jvm/java-21-openjdk-amd64".to_string(),
                    "".to_string(),
                    "".to_string(),
                )
            },
            "java11" => {
                let v = vec![
                    "/usr/lib/jvm/java-11-openjdk-amd64/bin/javac",
                    "-J-XX:TieredStopAtLevel=1",
                    "-J-Xms512m",
                    "-J-Xmx512m",
                    "-J-XX:MaxMetaspaceSize=192m",
                    "-J-XX:ReservedCodeCacheSize=64m",
                    "-J-XX:+UseSerialGC",
                    "Main.java",
                ];
                (
                    "Main.java",
                    "Main.class",
                    v,
                    "/usr/lib/jvm/java-11-openjdk-amd64".to_string(),
                    "".to_string(),
                    "".to_string(),
                )
            },
            "python" => (
                "main.py",
                "main.py",
                vec!["echo", "no-compile"],
                "".to_string(),
                "".to_string(),
                "".to_string(),
            ),
            "javascript" => (
                "main.js",
                "main.js",
                vec!["echo", "no-compile"],
                "".to_string(),
                "/usr/lib/nodejs".to_string(),
                "".to_string(),
            ),
            _ => return Err(anyhow!("Unsupported compiled language: {}", language))
        };

        let box_path = format!("/var/local/lib/isolate/{}/box", box_id);
        fs::write(format!("{}/{}", box_path, source_filename), code).await?;

        let mut cmd;

        if language.starts_with("java") {
            // --- JAVA "CLEAN ROOM" CHROOT STRATEGY ---
            cmd = Command::new("setarch");
            cmd.arg(std::env::consts::ARCH).arg("-R").arg("isolate");

            // Add standard isolate args
            cmd.arg("--cg")
               .arg(format!("--box-id={}", box_id))
               .arg(format!("--time={}", time_limit_s))
               .arg(format!("--wall-time={}", time_limit_s * 2))
               .arg("--fsize=102400");

            // Mount the pre-built chroot environment as the sandbox root
            let chroot_path = if language == "java11" {
                std::env::var("JAVA11_CHROOT_PATH").unwrap_or_else(|_| "/opt/java11_chroot".to_string())
            } else { // Assumes java21 for "java" or "java21"
                std::env::var("JAVA21_CHROOT_PATH").unwrap_or_else(|_| "/opt/java21_chroot".to_string())
            };
            cmd.arg(format!("--dir={}={}", chroot_path, "/"));
            // Mount /etc for essential configs like resolv.conf as per the successful script
            cmd.arg("--dir=/etc");
        } else {
            // --- ORIGINAL STRATEGY FOR ALL OTHER LANGUAGES ---
            cmd = Command::new("isolate");
            cmd.arg("--cg")
               .arg(format!("--box-id={}", box_id))
               .arg(format!("--time={}", time_limit_s))
               .arg(format!("--wall-time={}", time_limit_s * 2))
               .arg("--fsize=102400");

            // Mount a wide range of host directories
            cmd.arg("--full-env")
                .arg("--env=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
                .arg("--dir=/etc")
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
                .arg("--dir=/proc");
        }

        // Set memory and process limits
        let compile_mem_limit_kb = match language {
            "java" | "java11" | "java21" => 1536000, // 1.5GB, as per successful script
            _ => mem_limit_kb,
        };
        cmd.arg(format!("--mem={}", compile_mem_limit_kb));

        let process_count = match language {
            "java" | "java11" | "java21" => 150,
            _ => 10,
        };
        cmd.arg(format!("--processes={}", process_count));

        if !java_home.is_empty() && !language.starts_with("java") {
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

        self.cleanup_box(&box_id).await?;
        sleep(Duration::from_millis(100)).await;
        Ok(result)
    }

    /// MODIFIED: Signature now only requires language, binary, and stdin. Limits are determined internally.
    pub async fn run(
        &self,
        language: &str,
        code_or_binary: &[u8],
        stdin: &str,
    ) -> Result<RunResult> {
        // FIXED: Add retry logic for box initialization with proper ID range
        let mut retries = 0;
        let max_retries = 5; // Increased retries for better reliability
        let mut box_id = Self::generate_unique_box_id();

        loop {
            match self.init_box(&box_id).await {
                Ok(()) => break,
                Err(e) if retries < max_retries => {
                    retries += 1;
                    warn!("Failed to init box {}, retry {}/{}: {}", box_id, retries, max_retries, e);
                    // Increase backoff time and generate new ID
                    tokio::time::sleep(tokio::time::Duration::from_millis(100 * retries as u64)).await;
                    box_id = Self::generate_unique_box_id();
                }
                Err(e) => return Err(anyhow!("Failed to initialize sandbox after {} retries: {}", max_retries, e)),
            }
        }

        // MODIFIED: Getting limits from the new centralized function.
        let (time_limit_s_u64, mem_limit_kb) = Self::get_language_limits(language);
        let time_limit_s = time_limit_s_u64 as f64;

        let (filename_to_write, run_command, java_home, node_path, python_path) = match language {
            "c" | "cpp" => (
                "main",
                vec!["./main".to_string()],
                "".to_string(),
                "".to_string(),
                "".to_string(),
            ),
            "java" | "java21" => {
                let v = vec![
                    "/usr/lib/jvm/java-21-openjdk-amd64/bin/java".to_string(),
                    "-Xms256m".to_string(), "-Xmx256m".to_string(),
                    "-XX:MaxMetaspaceSize=64m".to_string(),
                    "-XX:ReservedCodeCacheSize=32m".to_string(),
                    "-XX:+UseSerialGC".to_string(),
                    "Main".to_string(),
                ];
                (
                    "Main.class",
                    v,
                    "/usr/lib/jvm/java-21-openjdk-amd64".to_string(),
                    "".to_string(),
                    "".to_string(),
                )
            },
            "java11" => {
                let v = vec![
                    "/usr/lib/jvm/java-11-openjdk-amd64/bin/java".to_string(),
                    "-Xms256m".to_string(), "-Xmx256m".to_string(),
                    "-XX:MaxMetaspaceSize=64m".to_string(),
                    "-XX:ReservedCodeCacheSize=32m".to_string(),
                    "-XX:+UseSerialGC".to_string(),
                    "Main".to_string(),
                ];
                (
                    "Main.class",
                    v,
                    "/usr/lib/jvm/java-11-openjdk-amd64".to_string(),
                    "".to_string(),
                    "".to_string(),
                )
            },
            "python" => (
                "main.py",
                vec!["/usr/bin/python3".to_string(), "main.py".to_string()],
                "".to_string(),
                "".to_string(),
                "/usr/lib/python3.10".to_string(),
            ),
            "javascript" => (
                "main.js",
                vec!["/usr/bin/node".to_string(), "main.js".to_string()],
                "".to_string(),
                "/usr/lib/nodejs".to_string(),
                "".to_string(),
            ),
            _ => return Err(anyhow!("Unsupported language for run: {}", language)),
        };

        let box_path = format!("/var/local/lib/isolate/{}/box", box_id);
        let file_path = format!("{}/{}", box_path, filename_to_write);
        fs::write(&file_path, code_or_binary).await?;
        if matches!(language, "c" | "cpp") {
            fs::set_permissions(&file_path, std::fs::Permissions::from_mode(0o755)).await?;
        }
        fs::write(format!("{}/stdin.txt", box_path), stdin).await?;

        let mut path_retries = 0;
        while !Path::new(&box_path).exists() && path_retries < 10 {
            warn!("Box path {} not found, waiting...", box_path);
            sleep(Duration::from_millis(50)).await;
            path_retries += 1;
        }
        if !Path::new(&box_path).exists() {
            return Err(anyhow!("Box path not found after retries: {}", box_path));
        }

        let meta_file_path = format!("/tmp/isolate_{}.txt", box_id);
        let mut cmd;

        if language.starts_with("java") {
            // --- JAVA "CLEAN ROOM" CHROOT STRATEGY ---
            cmd = Command::new("setarch");
            cmd.arg(std::env::consts::ARCH).arg("-R").arg("isolate");

            cmd.arg("--cg")
               .arg(format!("--box-id={}", box_id))
               .arg(format!("--time={}", time_limit_s))
               .arg(format!("--wall-time={}", time_limit_s * 2.0))
               .arg("--fsize=10240");

            let chroot_path = if language == "java11" {
                std::env::var("JAVA11_CHROOT_PATH").unwrap_or_else(|_| "/opt/java11_chroot".to_string())
            } else {
                std::env::var("JAVA21_CHROOT_PATH").unwrap_or_else(|_| "/opt/java21_chroot".to_string())
            };
            cmd.arg(format!("--dir={}={}", chroot_path, "/"));
            cmd.arg("--dir=/etc");
        } else {
            // --- ORIGINAL STRATEGY FOR ALL OTHER LANGUAGES ---
            cmd = Command::new("isolate");
            cmd.arg("--cg")
               .arg(format!("--box-id={}", box_id))
               .arg(format!("--time={}", time_limit_s))
               .arg(format!("--wall-time={}", time_limit_s * 2.0))
               .arg("--fsize=10240");

            cmd.arg("--full-env")
                .arg("--env=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
                .arg("--dir=/etc")
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
                .arg("--dir=/proc");
        }

        let final_mem_limit_kb = match language {
            "java" | "java11" | "java21" => 786432, // 768MB, as per successful script
            _ => mem_limit_kb,
        };
        cmd.arg(format!("--mem={}", final_mem_limit_kb));

        let process_count = match language {
            "java" | "java11" | "java21" => 50,
            _ => 5,
        };
        cmd.arg(format!("--processes={}", process_count));

        if !java_home.is_empty() && !language.starts_with("java") {
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
        self.cleanup_box(&box_id).await?;
        sleep(Duration::from_millis(100)).await;
        Ok(result)
    }

    async fn init_box(&self, box_id: &str) -> Result<()> {
        let cgroup_path = format!("/sys/fs/cgroup/system.slice/isolate.service/box-{}", box_id);
        let mut retries = 0;
        while Path::new(&cgroup_path).exists() && retries < 20 {
            warn!("Box cgroup {} still exists, waiting for cleanup...", cgroup_path);
            sleep(Duration::from_millis(50)).await;
            retries += 1;
        }

        let output = Command::new("isolate")
            .arg("--cg")
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

    async fn cleanup_box(&self, box_id: &str) -> Result<()> {
        let output = Command::new("isolate")
            .arg("--cg")
            .arg(format!("--box-id={}", box_id))
            .arg("--cleanup")
            .output()
            .await?;
        
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Only log as error if it's not "box doesn't exist" error
            if !stderr.contains("No such file or directory") && !stderr.contains("does not exist") {
                error!("Failed to cleanup isolate box {}: {}", box_id, stderr);
                // Force cleanup using system commands as fallback
                let _ = tokio::process::Command::new("sh")
                    .arg("-c")
                    .arg(format!("rm -rf /var/local/lib/isolate/{} 2>/dev/null || true", box_id))
                    .output()
                    .await;
            } else {
                debug!("Box {} already cleaned up", box_id);
            }
        }
        Ok(())
    }
}