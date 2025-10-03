// src/sandbox/isolate.rs
use anyhow::{anyhow, Result};
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::process::Command;
use tokio::time::{sleep, Duration};
use std::path::Path;
use tracing::{debug, warn};

// A global, thread-safe counter to generate unique numeric box IDs for Isolate.
static BOX_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

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

const JAVA_JVM_FLAGS: &[&str] = &[
    "-Xms16m",                   // tiny initial heap
    "-Xmx128m",                  // smaller max heap
    "-XX:MaxMetaspaceSize=32m",  // cap metaspace
    "-XX:+UseSerialGC",          // single-threaded GC
    "-XX:CICompilerCount=1",     // single compiler thread
    "-XX:ParallelGCThreads=1",   // limit parallel GC threads
    "-XX:ConcGCThreads=1",       // limit concurrent GC threads
    "-XX:ThreadStackSize=128k",  // minimal stack per thread
];

impl IsolateSandbox {

    // Generate a truly unique box ID using timestamp and counter
    fn generate_unique_box_id() -> String {
        let counter = BOX_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        // Use a larger modulo to reduce collision chance while staying in range
        let box_id = (counter % 900) + 100; // Range: 100-999
        box_id.to_string()
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

        let (source_filename, executable_filename, mut compile_command, java_home, node_path, python_path) = match language {
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
                let mut v = vec!["/usr/lib/jvm/java-21-openjdk-amd64/bin/javac"];
                v.extend(JAVA_JVM_FLAGS.iter().map(|s| *s));
                v.extend(&["-d", ".", "Main.java"]);
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
                let mut v = vec!["/usr/lib/jvm/java-11-openjdk-amd64/bin/javac"];
                v.extend(JAVA_JVM_FLAGS.iter().map(|s| *s));
                v.extend(&["-d", ".", "Main.java"]);
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

        let mut cmd = Command::new("isolate");
        cmd.arg("--cg")
           .arg(format!("--box-id={}", box_id))
           //.arg("--cg-dir=/sys/fs/cgroup/isolate")
           .arg(format!("--time={}", time_limit_s))
           .arg(format!("--wall-time={}", time_limit_s * 2));
        let compile_mem_limit_kb = match language {
            "java" | "java11" | "java21" => 1048576,
            _ => mem_limit_kb,
        };
        cmd.arg(format!("--mem={}", compile_mem_limit_kb));
        cmd.arg("--fsize=102400");

        let process_count = match language {
            "java" | "java11" | "java21" => 50,
            _ => 10,
        };
        cmd.arg(format!("--processes={}", process_count));

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
           .arg("--dir=/usr/lib/jvm/java-11-openjdk-amd64")
           .arg("--dir=/usr/lib/jvm/java-21-openjdk-amd64")
           .arg("--dir=/usr/share")
           .arg("--dir=/usr/share/java")
           .arg("--dir=/proc");

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
                stderr
            }
        } else {
            CompilationResult {
                success: false,
                compile_time,
                binary: None,
                stderr
            }
        };

        self.cleanup_box(&box_id).await?;
        // Add delay to ensure cgroup cleanup completes
        sleep(Duration::from_millis(100)).await;
        Ok(result)
    }

    pub async fn run(
        &self,
        language: &str,
        code_or_binary: &[u8],
        stdin: &str,
        time_limit_s: f64,
        mem_limit_kb: u64
    ) -> Result<RunResult> {
        let box_id = Self::generate_unique_box_id();
        self.init_box(&box_id).await?;

        let (filename_to_write, mut run_command, java_home, node_path, python_path) = match language {
            "c" | "cpp" => (
                "main",
                vec!["./main"],
                "".to_string(),
                "".to_string(),
                "".to_string(),
            ),
            "java" | "java21" => {
                let mut v = vec!["/usr/lib/jvm/java-21-openjdk-amd64/bin/java"];
                v.extend(JAVA_JVM_FLAGS.iter().map(|s| *s));
                v.extend(&["-cp", ".", "Main"]);
                (
                    "Main.class",
                    v,
                    "/usr/lib/jvm/java-21-openjdk-amd64".to_string(),
                    "".to_string(),
                    "".to_string(),
                )
            },
            "java11" => {
                let mut v = vec!["/usr/lib/jvm/java-11-openjdk-amd64/bin/java"];
                v.extend(JAVA_JVM_FLAGS.iter().map(|s| *s));
                v.extend(&["-cp", ".", "Main"]);
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
                vec!["/usr/bin/python3", "main.py"],
                "".to_string(),
                "".to_string(),
                "/usr/lib/python3.10".to_string(),
            ),
            "javascript" => (
                "main.js",
                vec!["/usr/bin/node", "main.js"],
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

        // Wait for the box directory to exist before running isolate
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
        let mut cmd = Command::new("isolate");
        cmd.arg("--cg")
           .arg(format!("--box-id={}", box_id))
           //.arg("--cg-dir=/sys/fs/cgroup/isolate")
           .arg(format!("--time={}", time_limit_s))
           .arg(format!("--wall-time={}", time_limit_s * 2.0));
        let final_mem_limit_kb = match language {
            "java" | "java11" | "java21" => 1048576, // 1GB for Java runtime
            _ => mem_limit_kb,
        };
        cmd.arg(format!("--mem={}", final_mem_limit_kb));
        let process_count = match language {
            "java" | "java11" | "java21" => 50,
            _ => 5,
        };
        cmd.arg(format!("--processes={}", process_count));

        cmd.arg("--fsize=10240")
           .arg("--full-env")
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
           .arg("--dir=/usr/lib/jvm/java-11-openjdk-amd64")
           .arg("--dir=/usr/lib/jvm/java-21-openjdk-amd64")
           .arg("--dir=/usr/share")
           .arg("--dir=/usr/share/java")
           .arg("--dir=/proc");

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
        self.cleanup_box(&box_id).await?;
        // Add delay to ensure cgroup cleanup completes
        sleep(Duration::from_millis(100)).await;
        Ok(result)
    }

    async fn init_box(&self, box_id: &str) -> Result<()> {
        // Check if cgroup directory already exists and wait for cleanup
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
            // .arg("--cg-dir=/sys/fs/cgroup/isolate")
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
            //.arg("--cg-dir=/sys/fs/cgroup/isolate")
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
