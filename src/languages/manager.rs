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
                    let (lang_name, lang_version) = match Self::parse_path(&path) {
                        Some(result) => result,
                        None => {
                            warn!("Could not parse language/version from path: {:?}", path);
                            continue;
                        }
                    };
                    
                    let content = fs::read_to_string(&path)
                        .with_context(|| format!("Failed to read language config file: {:?}", path))?;
                    
                    let toml_config: TomlConfig = toml::from_str(&content)
                        .with_context(|| format!("Failed to parse TOML from file: {:?}", path))?;

                    if let Err(e) = Self::validate_config(&toml_config) {
                        error!("Invalid configuration in {:?}: {}. Skipping.", path, e);
                        continue;
                    }
                    
                    // --- ADDED: Startup Path Validation ---
                    // This is a critical pre-flight check. It ensures the application doesn't start
                    // with a broken configuration that would lead to runtime errors.
                    Self::validate_paths(&path, &toml_config)?;
                    // --- END OF ADDITION ---

                    let config = LanguageConfig {
                        name: lang_name.clone(),
                        version: lang_version.clone(),
                        is_compiled: toml_config.is_compiled,
                        source_filename: toml_config.source_filename,
                        executable_filename: toml_config.executable_filename,
                        chroot_path: toml_config.chroot_path,
                        env_vars: toml_config.env_vars,
                        mount_paths: toml_config.mount_paths,
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

    /// ADDED: Validates that all executable paths and mount paths exist on the host filesystem.
    /// Panics if any path is not found, preventing runtime errors.
    fn validate_paths(config_path: &Path, config: &TomlConfig) -> Result<()> {
        let check = |p: &String| {
        // Only validate absolute paths. Ignore relative paths like "./main".
            if p.starts_with('/') {
                if !Path::new(p).exists() {
                        // This is a fatal configuration error. The application cannot run correctly.
                    panic!(
                        "FATAL CONFIG ERROR in \"{}\": Path '{}' does not exist on the host machine. The application cannot start.",
                        config_path.display(), p
                    );
                }
            }
        };

        // The first element of a command is the executable.
        if let Some(exe) = config.compile.command.get(0) {
            if exe != "echo" { // Ignore placeholder commands
                check(exe);
            }
        }
        if let Some(exe) = config.run.command.get(0) {
            check(exe);
        }

        for path in &config.mount_paths {
            check(path);
        }
        
        if let Some(chroot) = &config.chroot_path {
            check(chroot);
        }

        Ok(())
    }

    fn parse_path(path: &Path) -> Option<(String, String)> {
        let version_str = path.file_stem()?.to_str()?.strip_prefix('v')?.to_string();
        let lang_dir_name = path.parent()?.file_name()?.to_str()?;
        let lang_name = lang_dir_name.strip_suffix("_language")?.to_string();
        
        let final_lang_name = if lang_name == "java" && version_str == "11" {
            "java11".to_string()
        } else {
            lang_name
        };

        Some((final_lang_name, version_str))
    }
    
    pub fn get(&self, key: &str) -> Option<Arc<LanguageConfig>> {
        self.configs.get(key).cloned()
    }

    pub fn list_all(&self) -> Vec<Arc<LanguageConfig>> {
        self.configs.values().cloned().collect()
    }
}
