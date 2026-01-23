use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRequest {
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub code: Option<String>,
    pub stdin: String,
    #[serde(default)]
    pub timeout: Option<u64>,
    #[serde(default)]
    pub memory_limit: Option<String>,
}