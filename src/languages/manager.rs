use crate::languages::config::{LanguageConfig, TomlConfig};
use anyhow::{anyhow, Context, Result};
use glob::glob;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tracing::{error, info, warn};

#[derive(Debug)]
pub struct LanguageRegistry {
    configs: HashMap<String, Arc<LanguageConfig>>,
}

impl LanguageRegistry {
    /// Loads and validates all language configurations from `config/languages/**/*.toml`.
    pub fn new() -> Result<Self> {
        let mut configs = HashMap::new();
        let config_pattern = "config/languages/*/*.toml";

        for entry in glob(config_pattern).context(format!("Failed to read glob pattern {}", config_pattern))? {
            match entry {
                Ok(path) => {
                    // 1. Parse name and version from path
                    let (lang_name, lang_version) = match Self::parse_path(&path) {
                        Some(result) => result,
                        None => {
                            warn!("Could not parse language/version from path: {:?}", path);
                            continue;
                        }
                    };
                    
                    // 2. Read and parse the TOML file content
                    let content = fs::read_to_string(&path)
                        .with_context(|| format!("Failed to read language config file: {:?}", path))?;
                    
                    let toml_config: TomlConfig = toml::from_str(&content)
                        .with_context(|| format!("Failed to parse TOML from file: {:?}", path))?;

                    // 3. ADDED: Validate the parsed configuration
                    if let Err(e) = Self::validate_config(&toml_config) {
                        error!("Invalid configuration in {:?}: {}. Skipping.", path, e);
                        continue;
                    }

                    // 4. Construct the final, in-memory LanguageConfig
                    let config = LanguageConfig {
                        name: lang_name.clone(),
                        version: lang_version.clone(),
                        is_compiled: toml_config.is_compiled,
                        source_filename: toml_config.source_filename,
                        executable_filename: toml_config.executable_filename,
                        chroot_path: toml_config.chroot_path,
                        env_vars: toml_config.env_vars,
                        compile: toml_config.compile,
                        run: toml_config.run,
                    };
                    
                    let key = format!("{}:{}", lang_name, lang_version);
                    info!("Loaded and validated language config for '{}' from {:?}", &key, path.file_name().unwrap_or_default());
                    configs.insert(key, Arc::new(config));
                }
                Err(e) => warn!("Failed to process a language config entry: {}", e),
            }
        }
        
        if configs.is_empty() {
            warn!("No language configurations found in '{}'. The application will not be able to execute any code.", config_pattern);
        }

        Ok(Self { configs })
    }

    /// ADDED: Private helper to validate required fields in the config.
    fn validate_config(config: &TomlConfig) -> Result<()> {
        if config.source_filename.is_empty() {
            return Err(anyhow!("'source_filename' cannot be empty."));
        }
        if config.run.command.is_empty() {
            return Err(anyhow!("'[run].command' array cannot be empty."));
        }
        if config.is_compiled {
            if config.executable_filename.is_empty() {
                return Err(anyhow!("'executable_filename' cannot be empty for a compiled language."));
            }
            if config.compile.command.is_empty() {
                return Err(anyhow!("'[compile].command' array cannot be empty for a compiled language."));
            }
        }
        Ok(())
    }

    /// Parses a file path to extract the language name and version.
    fn parse_path(path: &Path) -> Option<(String, String)> {
        let version_str = path.file_stem()?.to_str()?.strip_prefix('v')?.to_string();
        let lang_dir_name = path.parent()?.file_name()?.to_str()?;
        let lang_name = lang_dir_name.strip_suffix("_language")?.to_string();
        
        // Handle special cases from your old setup.rs to maintain API compatibility
        let final_lang_name = if lang_name == "java" && version_str == "11" {
            "java11".to_string()
        } else {
            lang_name
        };

        Some((final_lang_name, version_str))
    }
    
    /// Retrieves a language configuration by its key (e.g., "python:3.9").
    pub fn get(&self, key: &str) -> Option<Arc<LanguageConfig>> {
        self.configs.get(key).cloned()
    }

    /// Returns a list of all loaded language configurations.
    pub fn list_all(&self) -> Vec<Arc<LanguageConfig>> {
        self.configs.values().cloned().collect()
    }
}