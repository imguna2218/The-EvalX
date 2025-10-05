use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Artifact {
    pub code: String,
    pub binary: Vec<u8>,
    /// ADDED: Flag to indicate if the binary is GZIP compressed.
    pub compressed: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EvaluationResult {
    pub compile_time: f64,
    pub stdout: String,
    pub stderr: Option<String>,
    pub exit_code: i64,
    pub run_time: f64,
    pub space_consumed: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SubmissionResponse {
    pub token: String,
    pub status: SubmissionStatus,
    // CHANGED: This now holds a vector of results to support batches.
    pub results: Option<Vec<EvaluationResult>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum SubmissionStatus {
    Queued,
    Processing,
    Completed,
    Failed,
}

impl EvaluationResult {
    /// Creates a new error result with the given error message
    pub fn error_result(error: impl Into<String>) -> Self {
        let error = error.into();
        Self {
            compile_time: 0.0,
            stdout: error.clone(),
            stderr: Some(error),
            exit_code: 1,
            run_time: 0.0,
            space_consumed: "0 MB".to_string(),
        }
    }

    /// Creates a success result with the given output
    pub fn success_result(stdout: impl Into<String>) -> Self {
        Self {
            compile_time: 0.0,
            stdout: stdout.into(),
            stderr: None,
            exit_code: 0,
            run_time: 0.0,
            space_consumed: "0 MB".to_string(),
        }
    }
}