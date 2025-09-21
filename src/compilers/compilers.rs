use anyhow::Result;
use std::time::Instant;
use tokio::time::{timeout, Duration};
use tracing::{warn, debug};
use crate::types::index::CodeExecutor;
use crate::caching::redis_client::RedisClient;
use crate::executor::execute_command; // Import directly from executor

pub async fn compile_c(
    executor: &CodeExecutor,
    container_id: &str,
    filename: &str,
    code: &str,
    artifact_key: &str,
    // CHANGED: Removed `mut` as async methods don't need it.
    redis_client: &RedisClient,
    _memory_bytes: i64,
) -> Result<(f64, Vec<String>, Vec<u8>)> {
    let compile_start = Instant::now();
    let write_cmd = vec![
        "sh".to_string(),
        "-c".to_string(),
        format!("cat <<'EOF' > {}\n{}\nEOF", filename, code),
    ];
    let write_exec = executor.docker.create_exec(
        container_id,
        bollard::exec::CreateExecOptions {
            cmd: Some(write_cmd),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            ..Default::default()
        },
    ).await?;
    timeout(Duration::from_secs(10), executor.docker.start_exec(&write_exec.id, None)).await??;

    let compile_cmd = vec!["gcc".to_string(), "-o".to_string(), "main".to_string(), filename.to_string()];
    let (_stdout, compile_stderr, compile_exit) = execute_command(executor, container_id, compile_cmd, 10).await?;
    if compile_exit != 0 {
        Ok((compile_start.elapsed().as_secs_f64(), vec![], compile_stderr))
    } else {
        let binary_data = executor.fetch_file_from_container(container_id, "/app/main").await?;
        // CHANGED: Switched to the non-blocking async version of set_artifact.
        if let Err(e) = redis_client.set_artifact_async(artifact_key, code, &binary_data, 120).await {
            warn!("Failed to cache artifact for key: {}: {}", artifact_key, e);
        } else {
            debug!("Cached artifact for key: {}, binary size: {}", artifact_key, binary_data.len());
        }
        Ok((compile_start.elapsed().as_secs_f64(), vec!["./main".to_string()], Vec::new()))
    }
}

pub async fn compile_cpp(
    executor: &CodeExecutor,
    container_id: &str,
    filename: &str,
    code: &str,
    artifact_key: &str,
    // CHANGED: Removed `mut` as async methods don't need it.
    redis_client: &RedisClient,
    _memory_bytes: i64,
) -> Result<(f64, Vec<String>, Vec<u8>)> {
    let compile_start = Instant::now();
    let write_cmd = vec![
        "sh".to_string(),
        "-c".to_string(),
        format!("cat <<'EOF' > {}\n{}\nEOF", filename, code),
    ];
    let write_exec = executor.docker.create_exec(
        container_id,
        bollard::exec::CreateExecOptions {
            cmd: Some(write_cmd),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            ..Default::default()
        },
    ).await?;
    timeout(Duration::from_secs(10), executor.docker.start_exec(&write_exec.id, None)).await??;

    let compile_cmd = vec!["g++".to_string(), "-o".to_string(), "main".to_string(), filename.to_string()];
    let (_stdout, compile_stderr, compile_exit) = execute_command(executor, container_id, compile_cmd, 10).await?;
    if compile_exit != 0 {
        warn!("Compilation failed for key: {}, stderr: {}", artifact_key, String::from_utf8_lossy(&compile_stderr));
        Ok((compile_start.elapsed().as_secs_f64(), vec![], compile_stderr))
    } else {
        let binary_data = executor.fetch_file_from_container(container_id, "/app/main").await?;
        // CHANGED: Switched to the non-blocking async version of set_artifact.
        if let Err(e) = redis_client.set_artifact_async(artifact_key, code, &binary_data, 120).await {
            warn!("Failed to cache artifact for key: {}: {}", artifact_key, e);
        } else {
            debug!("Cached artifact for key: {}, binary size: {}", artifact_key, binary_data.len());
        }
        Ok((compile_start.elapsed().as_secs_f64(), vec!["./main".to_string()], Vec::new()))
    }
}

pub async fn compile_java(
    executor: &CodeExecutor,
    container_id: &str,
    filename: &str,
    code: &str,
    artifact_key: &str,
    // CHANGED: Removed `mut` as async methods don't need it.
    redis_client: &RedisClient,
    _memory_bytes: i64,
) -> Result<(f64, Vec<String>, Vec<u8>)> {
    let compile_start = Instant::now();
    let write_cmd = vec![
        "sh".to_string(),
        "-c".to_string(),
        format!("cat <<'EOF' > {}\n{}\nEOF", filename, code),
    ];
    let write_exec = executor.docker.create_exec(
        container_id,
        bollard::exec::CreateExecOptions {
            cmd: Some(write_cmd),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            ..Default::default()
        },
    ).await?;
    timeout(Duration::from_secs(10), executor.docker.start_exec(&write_exec.id, None)).await??;

    let compile_cmd = vec!["javac".to_string(), filename.to_string()];
    let (_stdout, compile_stderr, compile_exit) = execute_command(executor, container_id, compile_cmd, 10).await?;
    if compile_exit != 0 {
        warn!("Compilation failed for key: {}, stderr: {}", artifact_key, String::from_utf8_lossy(&compile_stderr));
        Ok((compile_start.elapsed().as_secs_f64(), vec![], compile_stderr))
    } else {
        let class_data = executor.fetch_file_from_container(container_id, "/app/Main.class").await?;
        // CHANGED: Switched to the non-blocking async version of set_artifact.
        if let Err(e) = redis_client.set_artifact_async(artifact_key, code, &class_data, 120).await {
            warn!("Failed to cache artifact for key: {}: {}", artifact_key, e);
        } else {
            debug!("Cached artifact for key: {}, binary size: {}", artifact_key, class_data.len());
        }
        Ok((compile_start.elapsed().as_secs_f64(), vec!["java".to_string(), "Main".to_string()], Vec::new()))
    }
}
