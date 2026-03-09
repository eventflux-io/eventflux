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

//! Configuration Loaders
//!
//! Implements various configuration loaders for different sources (YAML files, Kubernetes, etc.).

use crate::core::config::{ConfigError, ConfigResult, EventFluxConfig};
use async_trait::async_trait;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Trait for configuration loaders
#[async_trait]
pub trait ConfigLoader: Send + Sync + std::fmt::Debug {
    /// Load configuration from the source
    async fn load(&self) -> ConfigResult<Option<EventFluxConfig>>;

    /// Check if this loader is available/configured
    fn is_available(&self) -> bool;

    /// Get the priority of this loader (higher = more priority)
    fn priority(&self) -> u32;

    /// Get a description of this loader
    fn description(&self) -> String;
}

/// YAML file configuration loader
#[derive(Debug, Clone)]
pub struct YamlConfigLoader {
    /// Paths to search for configuration files
    search_paths: Vec<PathBuf>,

    /// Specific file path (takes precedence over search paths)
    file_path: Option<PathBuf>,

    /// Whether to fail if no configuration file is found
    required: bool,
}

impl YamlConfigLoader {
    /// Create a new YAML loader with default search paths
    pub fn new() -> Self {
        let mut search_paths = vec![
            PathBuf::from("."),      // Current directory
            PathBuf::from("config"), // ./config directory
        ];

        // Add user config directory if available
        if let Some(config_dir) = dirs::config_dir() {
            search_paths.push(config_dir.join("eventflux"));
        }

        // Add system config directories
        search_paths.push(PathBuf::from("/etc/eventflux"));
        search_paths.push(PathBuf::from("/usr/local/etc/eventflux"));

        Self {
            search_paths,
            file_path: None,
            required: false,
        }
    }

    /// Create a YAML loader for a specific file
    pub fn from_file<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            search_paths: vec![],
            file_path: Some(path.into()),
            required: true,
        }
    }

    /// Set whether configuration file is required
    pub fn required(mut self, required: bool) -> Self {
        self.required = required;
        self
    }

    /// Add a search path
    pub fn add_search_path<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.search_paths.push(path.into());
        self
    }

    /// Find configuration file in search paths
    fn find_config_file(&self) -> Option<PathBuf> {
        if let Some(ref path) = self.file_path {
            if path.exists() {
                return Some(path.clone());
            }
            return None;
        }

        let filenames = [
            "eventflux-config.yaml",
            "eventflux-config.yml",
            "eventflux.yaml",
            "eventflux.yml",
            "config.yaml",
            "config.yml",
        ];

        for search_path in &self.search_paths {
            for filename in &filenames {
                let path = search_path.join(filename);
                if path.exists() && path.is_file() {
                    return Some(path);
                }
            }
        }

        None
    }

    /// Load configuration from a specific file
    pub async fn load_file<P: AsRef<Path>>(&self, path: P) -> ConfigResult<EventFluxConfig> {
        let path = path.as_ref();
        let content = fs::read_to_string(path).map_err(|e| ConfigError::IoError(e))?;

        self.parse_yaml_content(&content, path.to_string_lossy().as_ref())
    }

    /// Parse YAML content into configuration
    fn parse_yaml_content(&self, content: &str, source: &str) -> ConfigResult<EventFluxConfig> {
        // Perform environment variable substitution
        let expanded_content = self.expand_environment_variables(content)?;

        // Parse YAML
        serde_yaml::from_str(&expanded_content)
            .map_err(|e| ConfigError::yaml_parse_error(source, e.to_string()))
    }

    /// Expand environment variables in YAML content
    /// Supports ${VAR}, ${VAR:-default}, and ${VAR:default} formats
    fn expand_environment_variables(&self, content: &str) -> ConfigResult<String> {
        use regex::Regex;

        let re = Regex::new(r"\$\{([^}]+)\}").unwrap();
        let mut result = content.to_string();

        for captures in re.captures_iter(content) {
            let full_match = &captures[0];
            let var_expr = &captures[1];

            let replacement = self.resolve_env_variable(var_expr)?;
            result = result.replace(full_match, &replacement);
        }

        Ok(result)
    }

    /// Resolve a single environment variable expression
    fn resolve_env_variable(&self, expr: &str) -> ConfigResult<String> {
        // Parse different formats:
        // VAR
        // VAR:-default
        // VAR:default

        if let Some(pos) = expr.find(":-") {
            // Format: VAR:-default
            let var_name = &expr[..pos];
            let default_value = &expr[pos + 2..];

            Ok(std::env::var(var_name).unwrap_or_else(|_| default_value.to_string()))
        } else if let Some(pos) = expr.find(':') {
            // Format: VAR:default
            let var_name = &expr[..pos];
            let default_value = &expr[pos + 1..];

            let env_value = std::env::var(var_name).unwrap_or_else(|_| default_value.to_string());
            if env_value.is_empty() {
                Ok(default_value.to_string())
            } else {
                Ok(env_value)
            }
        } else {
            // Format: VAR
            std::env::var(expr).map_err(|_| ConfigError::env_var_not_found(expr))
        }
    }
}

impl Default for YamlConfigLoader {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ConfigLoader for YamlConfigLoader {
    async fn load(&self) -> ConfigResult<Option<EventFluxConfig>> {
        if let Some(path) = self.find_config_file() {
            let config = self.load_file(&path).await?;
            Ok(Some(config))
        } else if self.required {
            let search_info = if let Some(ref path) = self.file_path {
                format!("file: {}", path.display())
            } else {
                format!(
                    "search paths: {}",
                    self.search_paths
                        .iter()
                        .map(|p| p.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            };
            Err(ConfigError::file_not_found(format!(
                "No configuration file found in {}",
                search_info
            )))
        } else {
            Ok(None)
        }
    }

    fn is_available(&self) -> bool {
        self.find_config_file().is_some() || !self.required
    }

    fn priority(&self) -> u32 {
        if self.file_path.is_some() {
            100 // Explicit file path has highest priority
        } else {
            50 // Search path has medium priority
        }
    }

    fn description(&self) -> String {
        if let Some(ref path) = self.file_path {
            format!("YAML file: {}", path.display())
        } else {
            format!("YAML file search in {} paths", self.search_paths.len())
        }
    }
}

/// Environment variable configuration loader
#[derive(Debug, Clone)]
pub struct EnvironmentConfigLoader {
    /// Prefix for environment variables
    prefix: String,
}

impl EnvironmentConfigLoader {
    /// Create a new environment variable loader
    pub fn new() -> Self {
        Self {
            prefix: "EVENTFLUX_".to_string(),
        }
    }

    /// Create with custom prefix
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    /// Load environment variables into a configuration overlay
    fn load_env_vars(&self) -> HashMap<String, String> {
        std::env::vars()
            .filter(|(key, _)| key.starts_with(&self.prefix))
            .map(|(key, value)| {
                // Remove prefix and convert to lowercase with underscores to dots
                let config_key = key[self.prefix.len()..].to_lowercase().replace('_', ".");
                (config_key, value)
            })
            .collect()
    }

    /// Convert environment variables to partial configuration
    async fn env_vars_to_config(
        &self,
        env_vars: HashMap<String, String>,
    ) -> ConfigResult<Option<EventFluxConfig>> {
        if env_vars.is_empty() {
            return Ok(None);
        }

        // This is a simplified implementation that handles common configuration overrides
        // A full implementation would need to handle the complete configuration structure

        let mut config = EventFluxConfig::default();
        let mut has_changes = false;

        for (key, value) in env_vars {
            has_changes = true;

            match key.as_str() {
                "runtime.mode" => {
                    let mode = match value.to_lowercase().as_str() {
                        "single-node" | "singlenode" => crate::core::config::RuntimeMode::SingleNode,
                        "distributed" => crate::core::config::RuntimeMode::Distributed,
                        "hybrid" => crate::core::config::RuntimeMode::Hybrid,
                        _ => return Err(ConfigError::invalid_environment_variable(&key,
                            format!("Invalid runtime mode: {}. Valid values: single-node, distributed, hybrid", value))),
                    };
                    config.eventflux.runtime.mode = mode;
                }
                "runtime.performance.thread.pool.size" => {
                    let size: usize = value.parse().map_err(|_| {
                        ConfigError::invalid_environment_variable(
                            &key,
                            format!("Invalid thread pool size: {}", value),
                        )
                    })?;
                    config.eventflux.runtime.performance.thread_pool_size = size;
                }
                "runtime.performance.event.buffer.size" => {
                    let size: usize = value.parse().map_err(|_| {
                        ConfigError::invalid_environment_variable(
                            &key,
                            format!("Invalid event buffer size: {}", value),
                        )
                    })?;
                    config.eventflux.runtime.performance.event_buffer_size = size;
                }
                "runtime.performance.batch.processing" => {
                    let enabled: bool = value.parse().map_err(|_| {
                        ConfigError::invalid_environment_variable(
                            &key,
                            format!("Invalid batch processing value: {}", value),
                        )
                    })?;
                    config.eventflux.runtime.performance.batch_processing = enabled;
                }
                "observability.metrics.enabled" => {
                    let enabled: bool = value.parse().map_err(|_| {
                        ConfigError::invalid_environment_variable(
                            &key,
                            format!("Invalid metrics enabled value: {}", value),
                        )
                    })?;

                    if config.eventflux.observability.is_none() {
                        config.eventflux.observability =
                            Some(crate::core::config::ObservabilityConfig {
                                metrics: None,
                                tracing: None,
                                logging: None,
                            });
                    }

                    if let Some(ref mut obs) = config.eventflux.observability {
                        if obs.metrics.is_none() {
                            obs.metrics = Some(crate::core::config::MetricsConfig::default());
                        }
                        if let Some(ref mut metrics) = obs.metrics {
                            metrics.enabled = enabled;
                        }
                    }
                }
                _ => {
                    // For unknown keys, we could log a warning but continue
                    eprintln!(
                        "Warning: Unknown configuration environment variable: {}",
                        key
                    );
                }
            }
        }

        if has_changes {
            Ok(Some(config))
        } else {
            Ok(None)
        }
    }
}

impl Default for EnvironmentConfigLoader {
    fn default() -> Self {
        Self::new()
    }
}

/// Environment Configuration Loader with custom priority
#[derive(Debug, Clone)]
pub struct EnvironmentConfigLoaderWithPriority {
    base: EnvironmentConfigLoader,
    priority: u32,
}

impl EnvironmentConfigLoaderWithPriority {
    /// Create a new environment loader with custom priority
    pub fn new(priority: u32) -> Self {
        Self {
            base: EnvironmentConfigLoader::new(),
            priority,
        }
    }
}

#[async_trait]
impl ConfigLoader for EnvironmentConfigLoaderWithPriority {
    async fn load(&self) -> ConfigResult<Option<EventFluxConfig>> {
        self.base.load().await
    }

    fn is_available(&self) -> bool {
        self.base.is_available()
    }

    fn priority(&self) -> u32 {
        self.priority
    }

    fn description(&self) -> String {
        self.base.description()
    }
}

#[async_trait]
impl ConfigLoader for EnvironmentConfigLoader {
    async fn load(&self) -> ConfigResult<Option<EventFluxConfig>> {
        let env_vars = self.load_env_vars();
        self.env_vars_to_config(env_vars).await
    }

    fn is_available(&self) -> bool {
        !self.load_env_vars().is_empty()
    }

    fn priority(&self) -> u32 {
        80 // Environment variables have high priority
    }

    fn description(&self) -> String {
        format!("Environment variables with prefix '{}'", self.prefix)
    }
}

/// Kubernetes ConfigMap loader
#[cfg(feature = "kubernetes")]
#[derive(Debug, Clone)]
pub struct KubernetesConfigMapLoader {
    namespace: Option<String>,
    config_map_name: String,
    key: String,
}

#[cfg(feature = "kubernetes")]
impl KubernetesConfigMapLoader {
    /// Create a new Kubernetes ConfigMap loader
    pub fn new(config_map_name: impl Into<String>) -> Self {
        Self {
            namespace: None,
            config_map_name: config_map_name.into(),
            key: "eventflux-config.yaml".to_string(),
        }
    }

    /// Set the namespace
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Set the key within the ConfigMap
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = key.into();
        self
    }

    /// Load configuration from Kubernetes ConfigMap
    async fn load_from_k8s(&self) -> ConfigResult<Option<String>> {
        use k8s_openapi::api::core::v1::ConfigMap;
        use kube::{Api, Client};

        let client = Client::try_default()
            .await
            .map_err(|e| ConfigError::KubernetesError {
                message: format!("Failed to create Kubernetes client: {}", e),
            })?;

        let namespace = self.namespace.as_deref().unwrap_or("default");
        let config_maps: Api<ConfigMap> = Api::namespaced(client, namespace);

        match config_maps.get(&self.config_map_name).await {
            Ok(config_map) => {
                if let Some(data) = config_map.data {
                    if let Some(content) = data.get(&self.key) {
                        Ok(Some(content.clone()))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
            Err(kube::Error::Api(kube::error::ErrorResponse { code: 404, .. })) => {
                // ConfigMap not found
                Ok(None)
            }
            Err(e) => Err(ConfigError::KubernetesError {
                message: format!("Failed to get ConfigMap '{}': {}", self.config_map_name, e),
            }),
        }
    }
}

#[cfg(feature = "kubernetes")]
#[async_trait]
impl ConfigLoader for KubernetesConfigMapLoader {
    async fn load(&self) -> ConfigResult<Option<EventFluxConfig>> {
        if let Some(content) = self.load_from_k8s().await? {
            let loader = YamlConfigLoader::new();
            let config = loader.parse_yaml_content(
                &content,
                &format!(
                    "ConfigMap {}/{}",
                    self.namespace.as_deref().unwrap_or("default"),
                    self.config_map_name
                ),
            )?;
            Ok(Some(config))
        } else {
            Ok(None)
        }
    }

    fn is_available(&self) -> bool {
        // Check if we're running in Kubernetes
        std::env::var("KUBERNETES_SERVICE_HOST").is_ok()
    }

    fn priority(&self) -> u32 {
        70 // Kubernetes ConfigMaps have high priority in containerized environments
    }

    fn description(&self) -> String {
        format!(
            "Kubernetes ConfigMap '{}/{}[{}]'",
            self.namespace.as_deref().unwrap_or("default"),
            self.config_map_name,
            self.key
        )
    }
}

/// TOML file configuration loader
///
/// This loader reads TOML configuration files and does NOT load them into EventFluxConfig.
/// Instead, it provides stream-level FlatConfig instances that can be merged with SQL WITH clauses.
///
/// **Note**: This loader is different from other loaders. It returns None from the ConfigLoader trait
/// because TOML configs are stream-specific and need to be accessed differently through
/// the `load_toml_config()` function.
#[derive(Debug, Clone)]
pub struct TomlConfigLoader {
    /// Paths to search for TOML configuration files
    search_paths: Vec<PathBuf>,

    /// Specific file path (takes precedence over search paths)
    file_path: Option<PathBuf>,

    /// Whether to fail if no configuration file is found
    required: bool,
}

impl TomlConfigLoader {
    /// Create a new TOML loader with default search paths
    pub fn new() -> Self {
        let mut search_paths = vec![
            PathBuf::from("."),      // Current directory
            PathBuf::from("config"), // ./config directory
        ];

        // Add user config directory if available
        if let Some(config_dir) = dirs::config_dir() {
            search_paths.push(config_dir.join("eventflux"));
        }

        // Add system config directories
        search_paths.push(PathBuf::from("/etc/eventflux"));
        search_paths.push(PathBuf::from("/usr/local/etc/eventflux"));

        Self {
            search_paths,
            file_path: None,
            required: false,
        }
    }

    /// Create a TOML loader for a specific file
    pub fn from_file<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            search_paths: vec![],
            file_path: Some(path.into()),
            required: true,
        }
    }

    /// Set whether configuration file is required
    pub fn required(mut self, required: bool) -> Self {
        self.required = required;
        self
    }

    /// Add a search path
    pub fn add_search_path<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.search_paths.push(path.into());
        self
    }

    /// Find TOML configuration file in search paths
    fn find_config_file(&self) -> Option<PathBuf> {
        if let Some(ref path) = self.file_path {
            if path.exists() {
                return Some(path.clone());
            }
            return None;
        }

        let filenames = ["eventflux.toml", "eventflux-config.toml", "config.toml"];

        for search_path in &self.search_paths {
            for filename in &filenames {
                let path = search_path.join(filename);
                if path.exists() && path.is_file() {
                    return Some(path);
                }
            }
        }

        None
    }

    /// Load TOML configuration and return stream configs
    ///
    /// This returns a HashMap of stream names to their FlatConfig instances.
    /// Tables are loaded separately via `load_toml_table_configs()`.
    pub fn load_toml_stream_configs(
        &self,
    ) -> ConfigResult<Option<HashMap<String, crate::core::config::FlatConfig>>> {
        if let Some(path) = self.find_config_file() {
            let loaded_config = crate::core::config::toml_config::load_toml_config(
                path.to_str()
                    .ok_or_else(|| ConfigError::internal_error("Invalid TOML path"))?,
            )
            .map_err(|e| ConfigError::internal_error(format!("TOML loading failed: {}", e)))?;

            // Return only stream configurations (tables are separate)
            Ok(Some(loaded_config.streams))
        } else if self.required {
            let search_info = if let Some(ref path) = self.file_path {
                format!("file: {}", path.display())
            } else {
                format!(
                    "search paths: {}",
                    self.search_paths
                        .iter()
                        .map(|p| p.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            };
            Err(ConfigError::file_not_found(format!(
                "No TOML configuration file found in {}",
                search_info
            )))
        } else {
            Ok(None)
        }
    }

    /// Load TOML configuration and return table configs
    ///
    /// This returns a HashMap of table names to their FlatConfig instances.
    /// Streams are loaded separately via `load_toml_stream_configs()`.
    pub fn load_toml_table_configs(
        &self,
    ) -> ConfigResult<Option<HashMap<String, crate::core::config::FlatConfig>>> {
        if let Some(path) = self.find_config_file() {
            let loaded_config = crate::core::config::toml_config::load_toml_config(
                path.to_str()
                    .ok_or_else(|| ConfigError::internal_error("Invalid TOML path"))?,
            )
            .map_err(|e| ConfigError::internal_error(format!("TOML loading failed: {}", e)))?;

            // Return only table configurations (streams are separate)
            Ok(Some(loaded_config.tables))
        } else if self.required {
            let search_info = if let Some(ref path) = self.file_path {
                format!("file: {}", path.display())
            } else {
                format!(
                    "search paths: {}",
                    self.search_paths
                        .iter()
                        .map(|p| p.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            };
            Err(ConfigError::file_not_found(format!(
                "No TOML configuration file found in {}",
                search_info
            )))
        } else {
            Ok(None)
        }
    }
}

impl Default for TomlConfigLoader {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ConfigLoader for TomlConfigLoader {
    /// TOML configs are stream-specific and don't load into EventFluxConfig
    ///
    /// Use `load_toml_stream_configs()` instead to get stream-level configurations.
    async fn load(&self) -> ConfigResult<Option<EventFluxConfig>> {
        // TOML configurations are stream-specific and handled separately
        // from the main EventFluxConfig structure
        Ok(None)
    }

    fn is_available(&self) -> bool {
        self.find_config_file().is_some() || !self.required
    }

    fn priority(&self) -> u32 {
        if self.file_path.is_some() {
            95 // Slightly lower than explicit YAML files but higher than search paths
        } else {
            45 // Medium priority for search paths
        }
    }

    fn description(&self) -> String {
        if let Some(ref path) = self.file_path {
            format!("TOML file: {}", path.display())
        } else {
            format!("TOML file search in {} paths", self.search_paths.len())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_yaml_loader_file_not_found() {
        let loader = YamlConfigLoader::from_file("/nonexistent/config.yaml");
        let result = loader.load().await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ConfigError::FileNotFound { .. }
        ));
    }

    #[tokio::test]
    async fn test_yaml_loader_basic() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("eventflux-config.yaml");

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

        let loader = YamlConfigLoader::from_file(&config_path);
        let result = loader.load().await;

        assert!(result.is_ok(), "Failed to load YAML config: {:?}", result);
        let config = result.unwrap().unwrap();
        assert_eq!(config.metadata.name, Some("test-config".to_string()));
        assert_eq!(config.eventflux.runtime.performance.thread_pool_size, 8);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_yaml_loader_environment_substitution() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("config-with-env.yaml");

        let yaml_content = r#"
apiVersion: eventflux.io/v1
kind: EventFluxConfig
metadata:
  name: ${APP_NAME:-default-app}
  namespace: ${NAMESPACE}
  
eventflux:
  runtime:
    mode: single-node
    performance:
      thread_pool_size: ${THREAD_POOL_SIZE:4}
      event_buffer_size: 1024
"#;

        let mut file = File::create(&config_path).unwrap();
        file.write_all(yaml_content.as_bytes()).unwrap();

        // Set environment variables
        std::env::set_var("APP_NAME", "test-app");
        std::env::set_var("NAMESPACE", "production");
        std::env::set_var("THREAD_POOL_SIZE", "16");

        let loader = YamlConfigLoader::from_file(&config_path);
        let result = loader.load().await;

        assert!(result.is_ok());
        let config = result.unwrap().unwrap();
        assert_eq!(config.metadata.name, Some("test-app".to_string()));
        assert_eq!(config.metadata.namespace, Some("production".to_string()));
        assert_eq!(config.eventflux.runtime.performance.thread_pool_size, 16);

        // Cleanup
        std::env::remove_var("APP_NAME");
        std::env::remove_var("NAMESPACE");
        std::env::remove_var("THREAD_POOL_SIZE");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_yaml_loader_environment_defaults() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("config-with-defaults.yaml");

        let yaml_content = r#"
apiVersion: eventflux.io/v1
kind: EventFluxConfig
metadata:
  name: ${MISSING_VAR:-fallback-name}
  namespace: ${EMPTY_VAR:default-namespace}

eventflux:
  runtime:
    mode: single-node
    performance:
      thread_pool_size: 4
      event_buffer_size: 1024
"#;

        let mut file = File::create(&config_path).unwrap();
        file.write_all(yaml_content.as_bytes()).unwrap();

        // Set an empty environment variable
        std::env::set_var("EMPTY_VAR", "");

        let loader = YamlConfigLoader::from_file(&config_path);
        let result = loader.load().await;

        assert!(result.is_ok());
        let config = result.unwrap().unwrap();
        assert_eq!(config.metadata.name, Some("fallback-name".to_string()));
        assert_eq!(
            config.metadata.namespace,
            Some("default-namespace".to_string())
        );

        // Cleanup
        std::env::remove_var("EMPTY_VAR");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_environment_config_loader() {
        // Set test environment variables
        std::env::set_var("EVENTFLUX_RUNTIME_MODE", "distributed");
        std::env::set_var("EVENTFLUX_RUNTIME_PERFORMANCE_THREAD_POOL_SIZE", "12");
        std::env::set_var("EVENTFLUX_RUNTIME_PERFORMANCE_BATCH_PROCESSING", "false");

        let loader = EnvironmentConfigLoader::new();
        let result = loader.load().await;

        assert!(result.is_ok());
        let config = result.unwrap().unwrap();
        assert_eq!(
            config.eventflux.runtime.mode,
            crate::core::config::RuntimeMode::Distributed
        );
        assert_eq!(config.eventflux.runtime.performance.thread_pool_size, 12);
        assert!(!config.eventflux.runtime.performance.batch_processing);

        // Cleanup
        std::env::remove_var("EVENTFLUX_RUNTIME_MODE");
        std::env::remove_var("EVENTFLUX_RUNTIME_PERFORMANCE_THREAD_POOL_SIZE");
        std::env::remove_var("EVENTFLUX_RUNTIME_PERFORMANCE_BATCH_PROCESSING");
    }

    #[test]
    fn test_yaml_loader_priority() {
        let file_loader = YamlConfigLoader::from_file("config.yaml");
        let search_loader = YamlConfigLoader::new();

        assert!(file_loader.priority() > search_loader.priority());
    }

    #[test]
    #[serial_test::serial]
    fn test_environment_loader_available() {
        // Clean up any existing EVENTFLUX_ variables first
        let existing_vars: Vec<String> = std::env::vars()
            .filter_map(|(key, _)| {
                if key.starts_with("EVENTFLUX_") {
                    Some(key)
                } else {
                    None
                }
            })
            .collect();

        for var in &existing_vars {
            std::env::remove_var(var);
        }

        let loader = EnvironmentConfigLoader::new();
        // Should not be available without EVENTFLUX_ env vars
        assert!(!loader.is_available());

        std::env::set_var("EVENTFLUX_TEST", "value");
        assert!(loader.is_available());

        std::env::remove_var("EVENTFLUX_TEST");
        assert!(!loader.is_available());

        // Restore existing variables if any (though we can't get their values back)
    }
}
