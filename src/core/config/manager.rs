/*
 * Copyright 2025-2026 EventFlux.io
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Configuration Manager
//!
//! Loads configuration from every registered source, merges it in priority order and
//! validates the result.
//!
//! ## Configuration Reload Strategy
//!
//! **Important**: Configuration cannot be reloaded while an application runs.
//! To apply changes:
//!
//! 1. **Ensure State Persistence**: Turn on state persistence before stopping the application
//! 2. **Graceful Shutdown**: Stop the current EventFlux application runtime cleanly
//! 3. **Update Configuration**: Change your configuration files or environment variables
//! 4. **Restart with New Config**: Start a new EventFlux application runtime with the new
//!    configuration
//! 5. **State Recovery**: Restore the last persisted revision, for example with
//!    `EventFluxManager::restore_all_last_state()`, so processing continues where it
//!    stopped. A new runtime does not do this by itself.
//!
//! Restarting keeps the data consistent and avoids running with a half applied
//! configuration.

use crate::core::config::{
    loader::{
        ConfigLoader, EnvironmentConfigLoader, EnvironmentConfigLoaderWithPriority,
        TomlConfigLoader, YamlConfigLoader,
    },
    ConfigError, ConfigResult, EventFluxConfig,
};

#[cfg(feature = "kubernetes")]
use crate::core::config::loader::KubernetesConfigMapLoader;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Configuration Manager
///
/// Loads configuration from several sources and merges them so that a higher priority
/// source wins. Validates the result unless validation is turned off.
#[derive(Debug)]
pub struct ConfigManager {
    /// Registered loaders, sorted by priority from lowest to highest
    loaders: Vec<Box<dyn ConfigLoader>>,

    /// Last loaded configuration
    cached_config: Arc<RwLock<Option<EventFluxConfig>>>,

    /// Whether loaded configuration is validated. Defaults to true.
    validation_enabled: bool,
}

impl ConfigManager {
    /// Create a manager with the default loaders. See `register_default_loaders`.
    pub fn new() -> Self {
        let mut manager = Self {
            loaders: Vec::new(),
            cached_config: Arc::new(RwLock::new(None)),
            validation_enabled: true,
        };

        manager.register_default_loaders();
        manager
    }

    /// Create a manager for one required YAML file.
    ///
    /// `EVENTFLUX_` environment variables are also loaded, at priority 110, so they
    /// override the file.
    pub fn from_file<P: Into<PathBuf>>(path: P) -> Self {
        let mut manager = Self {
            loaders: Vec::new(),
            cached_config: Arc::new(RwLock::new(None)),
            validation_enabled: true,
        };

        manager.add_loader(Box::new(YamlConfigLoader::from_file(path)));

        // Above the explicit file (100) so the environment overrides it
        manager.add_loader(Box::new(EnvironmentConfigLoaderWithPriority::new(110)));

        manager
    }

    /// Register the default loaders.
    ///
    /// These are a TOML search (45), a YAML search (50), a Kubernetes ConfigMap named
    /// `eventflux-config` when built with the `kubernetes` feature and running in a pod
    /// (70), and `EVENTFLUX_` environment variables (80).
    fn register_default_loaders(&mut self) {
        // add_loader sorts by priority, so the order here does not matter

        // Its load() always returns None. Stream and table TOML configs are read
        // through TomlConfigLoader directly.
        self.add_loader(Box::new(TomlConfigLoader::new()));

        self.add_loader(Box::new(YamlConfigLoader::new()));

        #[cfg(feature = "kubernetes")]
        {
            if self.is_kubernetes_environment() {
                self.add_loader(Box::new(
                    KubernetesConfigMapLoader::new("eventflux-config")
                        .with_key("eventflux-config.yaml"),
                ));
            }
        }

        self.add_loader(Box::new(EnvironmentConfigLoader::new()));
    }

    /// Add a loader and keep the list sorted by priority.
    pub fn add_loader(&mut self, loader: Box<dyn ConfigLoader>) {
        self.loaders.push(loader);
        // Lowest first, so the highest priority loader merges last and wins
        self.loaders.sort_by_key(|loader| loader.priority());
    }

    /// Enable or disable configuration validation
    pub fn set_validation_enabled(&mut self, enabled: bool) {
        self.validation_enabled = enabled;
    }

    /// Check if running inside a Kubernetes pod
    #[allow(dead_code)]
    fn is_kubernetes_environment(&self) -> bool {
        std::env::var("KUBERNETES_SERVICE_HOST").is_ok()
    }

    /// Load and merge configuration from every available loader.
    ///
    /// Starts from `EventFluxConfig::default()` and merges each loader from lowest to
    /// highest priority. A loader that fails to load or merge is skipped and its error
    /// is printed as a warning at the end. Caches the result.
    ///
    /// # Errors
    ///
    /// Returns `InternalError` if nothing loaded and at least one loader failed, and
    /// `ValidationFailed` if validation is on and the merged config is invalid.
    pub async fn load_unified_config(&self) -> ConfigResult<EventFluxConfig> {
        let mut final_config = EventFluxConfig::default();
        let mut loaded_any = false;
        let mut errors = Vec::new();

        for loader in &self.loaders {
            if !loader.is_available() {
                continue;
            }

            match loader.load().await {
                Ok(Some(config)) => {
                    if let Err(e) = final_config.merge(config) {
                        errors.push(format!(
                            "Failed to merge config from {}: {}",
                            loader.description(),
                            e
                        ));
                        continue;
                    }
                    loaded_any = true;
                    println!("Loaded configuration from: {}", loader.description());
                }
                Ok(None) => {
                    // This source has nothing to add
                    continue;
                }
                Err(e) => {
                    errors.push(format!(
                        "Failed to load config from {}: {}",
                        loader.description(),
                        e
                    ));
                    continue;
                }
            }
        }

        // With no source loaded and no errors, the defaults are used as is
        if !loaded_any && !errors.is_empty() {
            return Err(ConfigError::internal_error(format!(
                "No configuration could be loaded. Errors: {}",
                errors.join("; ")
            )));
        }

        if self.validation_enabled {
            self.validate_config(&final_config)?;
        }

        {
            let mut cache = self.cached_config.write().await;
            *cache = Some(final_config.clone());
        }

        println!(
            "Configuration loaded successfully: {}",
            final_config.summary()
        );
        if !errors.is_empty() {
            println!("Configuration warnings: {}", errors.join("; "));
        }

        Ok(final_config)
    }

    /// Load one YAML file, ignoring the registered loaders.
    ///
    /// `EVENTFLUX_` environment variables are still merged on top. Caches the result.
    ///
    /// # Errors
    ///
    /// Returns the YAML loading errors, any environment loader error, `MergeConflict`
    /// if the merge fails, and `ValidationFailed` if validation is on and fails.
    pub async fn load_from_file<P: AsRef<Path>>(&self, path: P) -> ConfigResult<EventFluxConfig> {
        let path = path.as_ref();
        let yaml_loader = YamlConfigLoader::from_file(path);

        let mut config = yaml_loader.load_file(path).await?;

        let env_loader = EnvironmentConfigLoader::new();
        if let Some(env_config) = env_loader.load().await? {
            config
                .merge(env_config)
                .map_err(|e| ConfigError::MergeConflict { message: e })?;
        }

        if self.validation_enabled {
            self.validate_config(&config)?;
        }

        {
            let mut cache = self.cached_config.write().await;
            *cache = Some(config.clone());
        }

        println!("Configuration loaded from file: {}", path.display());

        Ok(config)
    }

    /// Get a copy of the last loaded configuration, if any
    pub async fn get_cached_config(&self) -> Option<EventFluxConfig> {
        let cache = self.cached_config.read().await;
        cache.clone()
    }

    /// Clear configuration cache
    pub async fn clear_cache(&self) {
        let mut cache = self.cached_config.write().await;
        *cache = None;
    }

    /// Run `EventFluxConfig::validate` and wrap each message as a `CustomValidation`
    /// error on the field "config".
    fn validate_config(&self, config: &EventFluxConfig) -> ConfigResult<()> {
        if let Err(errors) = config.validate() {
            return Err(ConfigError::validation_failed(
                errors
                    .into_iter()
                    .map(|e| crate::core::config::ValidationError::custom_validation("config", e))
                    .collect(),
            ));
        }

        // Not done yet: checking that external services are reachable, that resources
        // exist, and that dependencies line up

        Ok(())
    }

    /// List every registered loader with its priority and whether it is available,
    /// lowest priority first
    pub fn list_loaders(&self) -> Vec<LoaderInfo> {
        self.loaders
            .iter()
            .map(|loader| LoaderInfo {
                description: loader.description(),
                priority: loader.priority(),
                available: loader.is_available(),
            })
            .collect()
    }

    /// Start a builder that registers only the loaders you choose
    pub fn builder() -> ConfigManagerBuilder {
        ConfigManagerBuilder::new()
    }
}

impl Default for ConfigManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Information about a configuration loader
#[derive(Debug, Clone)]
pub struct LoaderInfo {
    /// Description of the loader
    pub description: String,

    /// Priority of the loader
    pub priority: u32,

    /// Whether the loader is available
    pub available: bool,
}

/// Configuration Manager Builder
///
/// Starts with no loaders and validation on.
#[derive(Debug)]
pub struct ConfigManagerBuilder {
    loaders: Vec<Box<dyn ConfigLoader>>,
    validation_enabled: bool,
}

impl ConfigManagerBuilder {
    /// Create a builder with no loaders and validation on
    pub fn new() -> Self {
        Self {
            loaders: Vec::new(),
            validation_enabled: true,
        }
    }

    /// Add a configuration loader
    pub fn add_loader(mut self, loader: Box<dyn ConfigLoader>) -> Self {
        self.loaders.push(loader);
        self
    }

    /// Add a loader for one required YAML file
    pub fn with_yaml_file<P: Into<PathBuf>>(self, path: P) -> Self {
        self.add_loader(Box::new(YamlConfigLoader::from_file(path)))
    }

    /// Add a YAML loader that searches the default paths
    pub fn with_yaml_search(self) -> Self {
        self.add_loader(Box::new(YamlConfigLoader::new()))
    }

    /// Add a loader for one required TOML file. Its `load` adds nothing to the merged
    /// config, see [`TomlConfigLoader`].
    pub fn with_toml_file<P: Into<PathBuf>>(self, path: P) -> Self {
        self.add_loader(Box::new(TomlConfigLoader::from_file(path)))
    }

    /// Add a TOML loader that searches the default paths. Its `load` adds nothing to
    /// the merged config, see [`TomlConfigLoader`].
    pub fn with_toml_search(self) -> Self {
        self.add_loader(Box::new(TomlConfigLoader::new()))
    }

    /// Add a loader for `EVENTFLUX_` environment variables
    pub fn with_environment_vars(self) -> Self {
        self.add_loader(Box::new(EnvironmentConfigLoader::new()))
    }

    /// Add environment variable loader with custom prefix
    pub fn with_environment_vars_prefix(self, prefix: impl Into<String>) -> Self {
        self.add_loader(Box::new(EnvironmentConfigLoader::with_prefix(prefix)))
    }

    #[cfg(feature = "kubernetes")]
    /// Add a Kubernetes ConfigMap loader with the default namespace and key
    pub fn with_kubernetes_configmap(self, name: impl Into<String>) -> Self {
        self.add_loader(Box::new(KubernetesConfigMapLoader::new(name)))
    }

    #[cfg(feature = "kubernetes")]
    /// Add Kubernetes ConfigMap loader with custom namespace and key
    pub fn with_kubernetes_configmap_detailed(
        self,
        name: impl Into<String>,
        namespace: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        self.add_loader(Box::new(
            KubernetesConfigMapLoader::new(name)
                .with_namespace(namespace)
                .with_key(key),
        ))
    }

    /// Enable or disable validation
    pub fn validation_enabled(mut self, enabled: bool) -> Self {
        self.validation_enabled = enabled;
        self
    }

    /// Build the configuration manager
    pub fn build(self) -> ConfigManager {
        let mut manager = ConfigManager {
            loaders: self.loaders,
            cached_config: Arc::new(RwLock::new(None)),
            validation_enabled: self.validation_enabled,
        };

        // Lowest first, same order as ConfigManager::add_loader keeps
        manager.loaders.sort_by_key(|loader| loader.priority());

        manager
    }
}

impl Default for ConfigManagerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_config_manager_default() {
        let manager = ConfigManager::new();
        assert!(!manager.loaders.is_empty());
        assert!(manager.validation_enabled);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_config_manager_from_file() {
        // Clean up all EVENTFLUX_ environment variables that might interfere
        let eventflux_vars: Vec<String> = std::env::vars()
            .filter_map(|(key, _)| {
                if key.starts_with("EVENTFLUX_") {
                    Some(key)
                } else {
                    None
                }
            })
            .collect();
        for key in eventflux_vars {
            std::env::remove_var(&key);
        }

        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("test-config.yaml");

        let yaml_content = r#"
apiVersion: eventflux.io/v1
kind: EventFluxConfig
metadata:
  name: test-config

eventflux:
  runtime:
    mode: single-node
    performance:
      thread_pool_size: 8
      event_buffer_size: 1024
"#;

        let mut file = File::create(&config_path).unwrap();
        file.write_all(yaml_content.as_bytes()).unwrap();

        let manager = ConfigManager::from_file(&config_path);
        let config = manager.load_unified_config().await.unwrap();

        assert_eq!(config.metadata.name, Some("test-config".to_string()));
        assert_eq!(config.eventflux.runtime.performance.thread_pool_size, 8);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_config_manager_environment_override() {
        // Clean up all EVENTFLUX_ environment variables before setting our test var
        let eventflux_vars: Vec<String> = std::env::vars()
            .filter_map(|(key, _)| {
                if key.starts_with("EVENTFLUX_") {
                    Some(key)
                } else {
                    None
                }
            })
            .collect();
        for key in eventflux_vars {
            std::env::remove_var(&key);
        }

        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("base-config.yaml");

        let yaml_content = r#"
apiVersion: eventflux.io/v1
kind: EventFluxConfig
metadata:
  name: base-config

eventflux:
  runtime:
    mode: single-node
    performance:
      thread_pool_size: 4
      event_buffer_size: 1024
"#;

        let mut file = File::create(&config_path).unwrap();
        file.write_all(yaml_content.as_bytes()).unwrap();

        // Set environment variable override
        std::env::set_var("EVENTFLUX_RUNTIME_PERFORMANCE_THREAD_POOL_SIZE", "16");

        let manager = ConfigManager::from_file(&config_path);
        let config = manager.load_unified_config().await.unwrap();

        assert_eq!(config.metadata.name, Some("base-config".to_string()));
        // Environment variable should override YAML value
        assert_eq!(config.eventflux.runtime.performance.thread_pool_size, 16);

        // Cleanup
        std::env::remove_var("EVENTFLUX_RUNTIME_PERFORMANCE_THREAD_POOL_SIZE");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_config_manager_cache() {
        // Ensure clean environment for this test
        std::env::remove_var("EVENTFLUX_RUNTIME_MODE");

        let manager = ConfigManager::new();

        // Initially no cache
        assert!(manager.get_cached_config().await.is_none());

        // Load config (will cache it)
        let _config = manager.load_unified_config().await.unwrap();

        // Should now have cached config
        assert!(manager.get_cached_config().await.is_some());

        // Clear cache
        manager.clear_cache().await;
        assert!(manager.get_cached_config().await.is_none());
    }

    #[tokio::test]
    async fn test_config_manager_builder() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("builder-config.yaml");

        let yaml_content = r#"
apiVersion: eventflux.io/v1
kind: EventFluxConfig
metadata:
  name: builder-test

eventflux:
  runtime:
    mode: single-node
    performance:
      thread_pool_size: 4
      event_buffer_size: 1024
"#;

        let mut file = File::create(&config_path).unwrap();
        file.write_all(yaml_content.as_bytes()).unwrap();

        let manager = ConfigManager::builder()
            .with_yaml_file(&config_path)
            .with_environment_vars()
            .validation_enabled(true)
            .build();

        let config = manager.load_unified_config().await.unwrap();
        assert_eq!(config.metadata.name, Some("builder-test".to_string()));
    }

    #[test]
    fn test_loader_info() {
        let manager = ConfigManager::new();
        let loaders = manager.list_loaders();

        assert!(!loaders.is_empty());

        // The environment loader is always registered, even when no variables are set
        assert!(loaders
            .iter()
            .any(|l| l.description.contains("Environment variables")));

        // Check priority ordering
        for i in 1..loaders.len() {
            assert!(loaders[i - 1].priority <= loaders[i].priority);
        }
    }

    #[tokio::test]
    async fn test_config_manager_validation_disabled() {
        let mut manager = ConfigManager::new();
        manager.set_validation_enabled(false);

        // Loading must succeed with validation off
        let config = manager.load_unified_config().await.unwrap();
        assert!(!config.api_version.is_empty());
    }
}
