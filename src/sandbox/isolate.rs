// src/sandbox/isolate.rs
use anyhow::{anyhow, Result};
use std::os::unix::fs::PermissionsExt;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::process::Command;
use tokio::time::{sleep, Duration};
use std::path::Path;
use tracing::{debug, warn};

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
    // Generate a truly unique box ID using timestamp and counter
    fn generate_unique_box_id() -> String {
        use std::process;
        let pid = process::id();
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        // The box ID for isolate must be numeric. We combine pid and time and take the last few digits.
        format!("{}", (pid as u128 + time) % 1000)
    }

    pub async fn compile(
        &self,
        language: &str,
        code: &str,
        time_limit_s: u64,
        mem_limit_kb: u64,
    ) -> Result<CompilationResult> {
        let start_time = Instant::now();
        let box_id = Self::generate_unique_box_id();
        self.init_box(&box_id).await?;

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
            // CORRECTED: Use '=' instead of ':' to map the directory path
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

    pub async fn run(
        &self,
        language: &str,
        code_or_binary: &[u8],
        stdin: &str,
        time_limit_s: f64,
        mem_limit_kb: u64,
    ) -> Result<RunResult> {
        let box_id = Self::generate_unique_box_id();
        self.init_box(&box_id).await?;

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

        let mut retries = 0;
        while !Path::new(&box_path).exists() && retries < 10 {
            warn!("Box path {} not found, waiting...", box_path);
            sleep(Duration::from_millis(50)).await;
            retries += 1;
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
            // CORRECTED: Use '=' instead of ':' to map the directory path
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
            warn!("Failed to cleanup isolate box {}: {}", box_id, stderr);
        }
        Ok(())
    }
}