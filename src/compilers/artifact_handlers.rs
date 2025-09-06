use anyhow::Result;
use tokio::time::{timeout, Duration};
use tracing::debug;
use crate::types::index::CodeExecutor;
use crate::caching::redis_client::RedisClient;
use crate::compilers::compilers::{compile_c, compile_cpp, compile_java};
use tar::Builder;
use std::io::Cursor;
use bollard::exec::CreateExecOptions;
use bollard::container::UploadToContainerOptions;

pub async fn handle_c_artifact(
    executor: &CodeExecutor,
    container_id: &str,
    filename: &str,
    code: &str,
    artifact_key: &str,
    redis_client: &mut RedisClient,
    memory_bytes: i64,
) -> Result<(f64, Vec<String>, Vec<u8>)> {
    match redis_client.get_artifact(artifact_key) {
        Ok(Some(artifact)) => {
            debug!("Artifact cache hit for key: {}", artifact_key);
            let mut archive = Builder::new(Vec::new());
            let mut header = tar::Header::new_gnu();
            header.set_path("main")?;
            header.set_size(artifact.binary.len() as u64);
            header.set_mode(0o755);
            header.set_cksum();
            archive.append(&header, Cursor::new(artifact.binary))?;
            let tar_data = archive.into_inner()?;
            executor.docker.upload_to_container(
                container_id,
                Some(UploadToContainerOptions { path: "/app/", no_overwrite_dir_non_dir: "false" }),
                tar_data.into(),
            ).await?;
            Ok((0.0, vec!["./main".to_string()], Vec::new()))
        }
        _ => {
            debug!("Artifact cache miss for key: {}, compiling", artifact_key);
            compile_c(executor, container_id, filename, code, artifact_key, redis_client, memory_bytes).await
        }
    }
}

pub async fn handle_cpp_artifact(
    executor: &CodeExecutor,
    container_id: &str,
    filename: &str,
    code: &str,
    artifact_key: &str,
    redis_client: &mut RedisClient,
    memory_bytes: i64,
) -> Result<(f64, Vec<String>, Vec<u8>)> {
    match redis_client.get_artifact(artifact_key) {
        Ok(Some(artifact)) => {
            debug!("Artifact cache hit for key: {}", artifact_key);
            let mut archive = Builder::new(Vec::new());
            let mut header = tar::Header::new_gnu();
            header.set_path("main")?;
            header.set_size(artifact.binary.len() as u64);
            header.set_mode(0o755);
            header.set_cksum();
            archive.append(&header, Cursor::new(artifact.binary))?;
            let tar_data = archive.into_inner()?;
            executor.docker.upload_to_container(
                container_id,
                Some(UploadToContainerOptions { path: "/app/", no_overwrite_dir_non_dir: "false" }),
                tar_data.into(),
            ).await?;
            Ok((0.0, vec!["./main".to_string()], Vec::new()))
        }
        _ => {
            debug!("Artifact cache miss for key: {}, compiling", artifact_key);
            compile_cpp(executor, container_id, filename, code, artifact_key, redis_client, memory_bytes).await
        }
    }
}

pub async fn handle_java_artifact(
    executor: &CodeExecutor,
    container_id: &str,
    filename: &str,
    code: &str,
    artifact_key: &str,
    redis_client: &mut RedisClient,
    memory_bytes: i64,
) -> Result<(f64, Vec<String>, Vec<u8>)> {
    match redis_client.get_artifact(artifact_key) {
        Ok(Some(artifact)) => {
            debug!("Artifact cache hit for key: {}", artifact_key);
            let mut archive = Builder::new(Vec::new());
            let mut header = tar::Header::new_gnu();
            header.set_path("Main.class")?;
            header.set_size(artifact.binary.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            archive.append(&header, Cursor::new(artifact.binary))?;
            let tar_data = archive.into_inner()?;
            executor.docker.upload_to_container(
                container_id,
                Some(UploadToContainerOptions { path: "/app/", no_overwrite_dir_non_dir: "false" }),
                tar_data.into(),
            ).await?;
            let write_cmd = vec![
                "sh".to_string(),
                "-c".to_string(),
                format!("cat <<'EOF' > {}\n{}\nEOF", filename, code),
            ];
            let write_exec = executor.docker.create_exec(
                container_id,
                CreateExecOptions { cmd: Some(write_cmd), attach_stdout: Some(true), attach_stderr: Some(true), ..Default::default() },
            ).await?;
            timeout(Duration::from_secs(10), executor.docker.start_exec(&write_exec.id, None)).await??;
            Ok((0.0, vec!["java".to_string(), "Main".to_string()], Vec::new()))
        }
        _ => {
            debug!("Artifact cache miss for key: {}, compiling", artifact_key);
            compile_java(executor, container_id, filename, code, artifact_key, redis_client, memory_bytes).await
        }
    }
}

pub async fn handle_python_artifact(
    executor: &CodeExecutor,
    container_id: &str,
    filename: &str,
    code: &str,
) -> Result<(f64, Vec<String>, Vec<u8>)> {
    // Write the Python code to main.py
    let write_cmd = vec![
        "sh".to_string(),
        "-c".to_string(),
        format!("cat <<'EOF' > {}\n{}\nEOF", filename, code),
    ];
    let write_exec = executor.docker.create_exec(
        container_id,
        CreateExecOptions { cmd: Some(write_cmd), attach_stdout: Some(true), attach_stderr: Some(true), ..Default::default() },
    ).await?;
    match timeout(Duration::from_secs(5), executor.docker.start_exec(&write_exec.id, None)).await {
        Ok(Ok(_)) => debug!("Python code written to {} in container {}", filename, container_id),
        Ok(Err(e)) => return Err(anyhow::anyhow!("Failed to write Python code: {}", e)),
        Err(_) => return Err(anyhow::anyhow!("Timeout writing Python code")),
    };
    // Return command to execute the script directly
    Ok((0.0, vec!["python".to_string(), filename.to_string()], Vec::new()))
}

pub async fn handle_javascript_artifact(
    executor: &CodeExecutor,
    container_id: &str,
    filename: &str,
    code: &str,
) -> Result<(f64, Vec<String>, Vec<u8>)> {
    // Write the JavaScript code to main.js
    let write_cmd = vec![
        "sh".to_string(),
        "-c".to_string(),
        format!("cat <<'EOF' > {}\n{}\nEOF", filename, code),
    ];
    let write_exec = executor.docker.create_exec(
        container_id,
        CreateExecOptions { cmd: Some(write_cmd), attach_stdout: Some(true), attach_stderr: Some(true), ..Default::default() },
    ).await?;
    match timeout(Duration::from_secs(5), executor.docker.start_exec(&write_exec.id, None)).await {
        Ok(Ok(_)) => debug!("JavaScript code written to {} in container {}", filename, container_id),
        Ok(Err(e)) => return Err(anyhow::anyhow!("Failed to write JavaScript code: {}", e)),
        Err(_) => return Err(anyhow::anyhow!("Timeout writing JavaScript code")),
    };
    // Return command to execute the script directly
    Ok((0.0, vec!["node".to_string(), filename.to_string()], Vec::new()))
}

pub async fn handle_default_artifact(
    executor: &CodeExecutor,
    container_id: &str,
    filename: &str,
    code: &str,
    command_format: &[String],
) -> Result<(f64, Vec<String>, Vec<u8>)> {
    let write_cmd = vec![
        "sh".to_string(),
        "-c".to_string(),
        format!("cat <<'EOF' > {}\n{}\nEOF", filename, code),
    ];
    let write_exec = executor.docker.create_exec(
        container_id,
        CreateExecOptions { cmd: Some(write_cmd), attach_stdout: Some(true), attach_stderr: Some(true), ..Default::default() },
    ).await?;
    timeout(Duration::from_secs(10), executor.docker.start_exec(&write_exec.id, None)).await??;
    Ok((0.0, command_format.to_vec(), Vec::new()))
}
