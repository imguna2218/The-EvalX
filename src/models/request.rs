use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExecutionRequest {
    #[serde(default)] // Makes language optional, defaults to empty string
    pub language: String,
    #[serde(default)] // Makes version optional, defaults to empty string
    pub version: String,
    #[serde(default)] // Makes code optional, defaults to empty string
    pub code: String,
    #[serde(default)] // Makes stdin optional
    pub stdin: Option<String>,
    #[serde(default)] // Timeout in seconds (can be fractional, e.g., 0.6)
    pub timeout: Option<f64>,
    #[serde(default)] // Makes memory_limit optional
    pub memory_limit: Option<String>,
}