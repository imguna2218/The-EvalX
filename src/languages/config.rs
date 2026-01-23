use serde::Deserialize;
use std::collections::HashMap;

/// Represents the full, validated configuration for a specific language version.
/// Represents the full, validated configuration for a specific language version.
#[derive(Debug, Clone)]
pub struct LanguageConfig {
    pub name: String,
    pub version: String,
    pub is_compiled: bool,
    pub source_filename: String,
    pub executable_filename: String,
    pub chroot_path: Option<String>,
    pub env_vars: HashMap<String, String>,
    pub mount_paths: Vec<String>, // ADDED: The missing field
    pub compile: CommandConfig,
    pub run: CommandConfig,
}

/// Represents the structure of the TOML files, used for deserialization.
#[derive(Deserialize, Debug, Clone)]
pub struct TomlConfig {
    pub is_compiled: bool,
    pub source_filename: String,
    pub executable_filename: String,
    #[serde(default)]
    pub chroot_path: Option<String>,
    #[serde(default)]
    pub env_vars: HashMap<String, String>,
    #[serde(default)]
    pub mount_paths: Vec<String>,
    pub compile: CommandConfig,
    pub run: CommandConfig,
}


#[derive(Deserialize, Debug, Clone)]
pub struct CommandConfig {
    pub command: Vec<String>,
    #[serde(default)]
    pub limits: Option<ResourceLimits>,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct ResourceLimits {
    pub time_s: u64,
    pub memory_kb: u64,
    pub processes: u64,
}