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

//! Main EventFlux Configuration Type
//!
//! Defines the top-level configuration structure that combines all configuration components.

use super::{ApplicationConfig, ConfigMetadata, EventFluxGlobalConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Top-level EventFlux configuration structure
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventFluxConfig {
    /// API version for configuration compatibility
    #[serde(rename = "apiVersion")]
    pub api_version: String,

    /// Kind of configuration object
    pub kind: String,

    /// Configuration metadata following Kubernetes API conventions
    pub metadata: ConfigMetadata,

    /// Global EventFlux runtime configuration
    pub eventflux: EventFluxGlobalConfig,

    /// Application-specific configurations
    #[serde(default)]
    pub applications: HashMap<String, ApplicationConfig>,
}

impl EventFluxConfig {
    /// Create a new EventFlux configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new EventFlux configuration with a specific name
    pub fn with_name(name: impl Into<String>) -> Self {
        Self {
            api_version: "eventflux.io/v1".to_string(),
            kind: "EventFluxConfig".to_string(),
            metadata: ConfigMetadata::new(name),
            eventflux: EventFluxGlobalConfig::default(),
            applications: HashMap::new(),
        }
    }

    /// Set the namespace for this configuration
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.metadata.namespace = Some(namespace.into());
        self
    }

    /// Set the environment for this configuration
    pub fn with_environment(mut self, environment: impl Into<String>) -> Self {
        self.metadata.environment = Some(environment.into());
        self
    }

    /// Set the version for this configuration
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.metadata.version = Some(version.into());
        self
    }

    /// Add a label to this configuration
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.labels.insert(key.into(), value.into());
        self
    }

    /// Add an annotation to this configuration
    pub fn with_annotation(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.annotations.insert(key.into(), value.into());
        self
    }

    /// Add an application configuration
    pub fn with_application(mut self, name: impl Into<String>, config: ApplicationConfig) -> Self {
        self.applications.insert(name.into(), config);
        self
    }

    /// Get an application configuration by name
    pub fn get_application(&self, name: &str) -> Option<&ApplicationConfig> {
        self.applications.get(name)
    }

    /// Get a mutable reference to an application configuration
    pub fn get_application_mut(&mut self, name: &str) -> Option<&mut ApplicationConfig> {
        self.applications.get_mut(name)
    }

    /// Add or update an application configuration
    pub fn set_application(&mut self, name: impl Into<String>, config: ApplicationConfig) {
        self.applications.insert(name.into(), config);
    }

    /// Remove an application configuration
    pub fn remove_application(&mut self, name: &str) -> Option<ApplicationConfig> {
        self.applications.remove(name)
    }

    /// List all application names
    pub fn application_names(&self) -> Vec<&String> {
        self.applications.keys().collect()
    }

    /// Check if configuration is valid for single-node mode
    pub fn is_single_node_mode(&self) -> bool {
        match self.eventflux.runtime.mode {
            super::global_config::RuntimeMode::SingleNode => true,
            _ => false,
        }
    }

    /// Check if configuration is for distributed mode
    pub fn is_distributed_mode(&self) -> bool {
        match self.eventflux.runtime.mode {
            super::global_config::RuntimeMode::Distributed
            | super::global_config::RuntimeMode::Hybrid => true,
            _ => false,
        }
    }

    /// Get the effective environment name
    pub fn effective_environment(&self) -> String {
        self.metadata
            .environment
            .as_ref()
            .unwrap_or(&"default".to_string())
            .clone()
    }

    /// Get the effective namespace
    pub fn effective_namespace(&self) -> String {
        self.metadata
            .namespace
            .as_ref()
            .unwrap_or(&"default".to_string())
            .clone()
    }

    /// Merge another configuration into this one
    ///
    /// This performs a deep merge where the other configuration takes precedence
    /// for conflicting values.
    pub fn merge(&mut self, other: EventFluxConfig) -> Result<(), String> {
        // Merge metadata (other takes precedence for non-None values)
        if let Some(name) = other.metadata.name {
            self.metadata.name = Some(name);
        }
        if let Some(namespace) = other.metadata.namespace {
            self.metadata.namespace = Some(namespace);
        }
        if let Some(environment) = other.metadata.environment {
            self.metadata.environment = Some(environment);
        }
        if let Some(version) = other.metadata.version {
            self.metadata.version = Some(version);
        }

        // Merge labels and annotations
        self.metadata.labels.extend(other.metadata.labels);
        self.metadata.annotations.extend(other.metadata.annotations);

        // Deep merge the eventflux global configuration
        self.eventflux.merge(other.eventflux);

        // Merge applications
        self.applications.extend(other.applications);

        Ok(())
    }

    /// Create a copy with sensitive information redacted
    pub fn redact_sensitive(&self) -> Self {
        // Implementation pending: Comprehensive secret redaction by walking configuration tree
        // and redacting fields that contain sensitive information
        let mut redacted = self.clone();

        // For now, just clear potentially sensitive annotations
        redacted.metadata.annotations.retain(|key, _| {
            !key.to_lowercase().contains("password")
                && !key.to_lowercase().contains("secret")
                && !key.to_lowercase().contains("token")
                && !key.to_lowercase().contains("key")
        });

        redacted
    }

    /// Validate the configuration structure
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        // Validate metadata
        if let Err(e) = self.metadata.validate() {
            errors.push(format!("Metadata validation failed: {}", e));
        }

        // Validate that distributed mode has required configuration
        if self.is_distributed_mode() && self.eventflux.distributed.is_none() {
            errors.push("Distributed mode requires distributed configuration".to_string());
        }

        // Validate application names are not empty
        for (name, _) in &self.applications {
            if name.is_empty() {
                errors.push("Application name cannot be empty".to_string());
            }
            if !name
                .chars()
                .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
            {
                errors.push(format!("Invalid application name '{}': must contain only alphanumeric characters, hyphens, and underscores", name));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Get configuration summary for logging
    pub fn summary(&self) -> String {
        let app_count = self.applications.len();
        let mode = match self.eventflux.runtime.mode {
            super::global_config::RuntimeMode::SingleNode => "single-node",
            super::global_config::RuntimeMode::Distributed => "distributed",
            super::global_config::RuntimeMode::Hybrid => "hybrid",
        };

        format!(
            "EventFluxConfig(name={}, mode={}, apps={}, env={})",
            self.metadata
                .name
                .as_ref()
                .unwrap_or(&"unnamed".to_string()),
            mode,
            app_count,
            self.effective_environment()
        )
    }

    /// Convert to YAML string
    pub fn to_yaml(&self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(self)
    }

    /// Parse from YAML string
    pub fn from_yaml(yaml: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Parse from JSON string
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Update timestamp
    pub fn touch(&mut self) {
        self.metadata.touch();
    }

    /// Generate unique configuration ID
    pub fn generate_id(&self) -> String {
        let metadata_id = self.metadata.generate_id();
        format!("{}/{}", self.api_version, metadata_id)
    }
}

impl Default for EventFluxConfig {
    fn default() -> Self {
        Self {
            api_version: "eventflux.io/v1".to_string(),
            kind: "EventFluxConfig".to_string(),
            metadata: ConfigMetadata::default(),
            eventflux: EventFluxGlobalConfig::default(),
            applications: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::types::{application_config::*, global_config::RuntimeMode};

    #[test]
    fn test_eventflux_config_default() {
        let config = EventFluxConfig::default();
        assert_eq!(config.api_version, "eventflux.io/v1");
        assert_eq!(config.kind, "EventFluxConfig");
        assert_eq!(config.eventflux.runtime.mode, RuntimeMode::SingleNode);
        assert!(config.applications.is_empty());
    }

    #[test]
    fn test_eventflux_config_builder() {
        let config = EventFluxConfig::with_name("test-config")
            .with_namespace("production")
            .with_environment("prod")
            .with_version("1.0.0")
            .with_label("team", "platform")
            .with_annotation("description", "Test configuration");

        assert_eq!(config.metadata.name, Some("test-config".to_string()));
        assert_eq!(config.metadata.namespace, Some("production".to_string()));
        assert_eq!(config.metadata.environment, Some("prod".to_string()));
        assert_eq!(config.metadata.version, Some("1.0.0".to_string()));
        assert_eq!(
            config.metadata.get_label("team"),
            Some(&"platform".to_string())
        );
        assert_eq!(
            config.metadata.get_annotation("description"),
            Some(&"Test configuration".to_string())
        );
    }

    #[test]
    fn test_application_management() {
        let mut config = EventFluxConfig::new();
        let app_config = ApplicationConfig::default();

        config.set_application("test-app", app_config.clone());
        assert_eq!(config.applications.len(), 1);
        assert!(config.get_application("test-app").is_some());

        let names = config.application_names();
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], &"test-app".to_string());

        let removed = config.remove_application("test-app");
        assert!(removed.is_some());
        assert_eq!(config.applications.len(), 0);
    }

    #[test]
    fn test_mode_detection() {
        let mut config = EventFluxConfig::default();
        assert!(config.is_single_node_mode());
        assert!(!config.is_distributed_mode());

        config.eventflux.runtime.mode = RuntimeMode::Distributed;
        assert!(!config.is_single_node_mode());
        assert!(config.is_distributed_mode());

        config.eventflux.runtime.mode = RuntimeMode::Hybrid;
        assert!(!config.is_single_node_mode());
        assert!(config.is_distributed_mode());
    }

    #[test]
    fn test_effective_values() {
        let config = EventFluxConfig::default();
        assert_eq!(config.effective_environment(), "default");
        assert_eq!(config.effective_namespace(), "default");

        let config = EventFluxConfig::with_name("test")
            .with_namespace("prod")
            .with_environment("production");

        assert_eq!(config.effective_environment(), "production");
        assert_eq!(config.effective_namespace(), "prod");
    }

    #[test]
    fn test_merge() {
        let mut config1 = EventFluxConfig::with_name("config1").with_label("env", "dev");

        let config2 = EventFluxConfig::with_name("config2")
            .with_label("team", "platform")
            .with_environment("production");

        config1.merge(config2).unwrap();

        assert_eq!(config1.metadata.name, Some("config2".to_string()));
        assert_eq!(config1.metadata.environment, Some("production".to_string()));
        assert_eq!(config1.metadata.labels.len(), 2);
        assert_eq!(config1.metadata.get_label("env"), Some(&"dev".to_string()));
        assert_eq!(
            config1.metadata.get_label("team"),
            Some(&"platform".to_string())
        );
    }

    #[test]
    fn test_validation() {
        let config = EventFluxConfig::default();
        assert!(config.validate().is_ok());

        let mut config = EventFluxConfig::default();
        config.eventflux.runtime.mode = RuntimeMode::Distributed;
        // Should fail because distributed mode requires distributed config
        assert!(config.validate().is_err());

        let mut config = EventFluxConfig::default();
        config
            .applications
            .insert("".to_string(), ApplicationConfig::default());
        // Should fail because application name is empty
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.contains("Application name cannot be empty")));
    }

    #[test]
    fn test_serialization() {
        let config = EventFluxConfig::with_name("test-config")
            .with_namespace("default")
            .with_application("test-app", ApplicationConfig::default());

        let yaml = config.to_yaml().unwrap();
        let deserialized = EventFluxConfig::from_yaml(&yaml).unwrap();

        assert_eq!(config.metadata.name, deserialized.metadata.name);
        assert_eq!(config.metadata.namespace, deserialized.metadata.namespace);
        assert_eq!(config.applications.len(), deserialized.applications.len());

        let json = config.to_json().unwrap();
        let deserialized = EventFluxConfig::from_json(&json).unwrap();

        assert_eq!(config.metadata.name, deserialized.metadata.name);
        assert_eq!(config.applications.len(), deserialized.applications.len());
    }

    #[test]
    fn test_summary() {
        let config = EventFluxConfig::with_name("test-config")
            .with_environment("production")
            .with_application("app1", ApplicationConfig::default())
            .with_application("app2", ApplicationConfig::default());

        let summary = config.summary();
        assert!(summary.contains("test-config"));
        assert!(summary.contains("single-node"));
        assert!(summary.contains("apps=2"));
        assert!(summary.contains("production"));
    }

    #[test]
    fn test_redact_sensitive() {
        let mut config = EventFluxConfig::with_name("test");
        config
            .metadata
            .annotations
            .insert("database.password".to_string(), "secret123".to_string());
        config
            .metadata
            .annotations
            .insert("api.key".to_string(), "key123".to_string());
        config
            .metadata
            .annotations
            .insert("description".to_string(), "Safe annotation".to_string());

        let redacted = config.redact_sensitive();

        assert!(!redacted
            .metadata
            .annotations
            .contains_key("database.password"));
        assert!(!redacted.metadata.annotations.contains_key("api.key"));
        assert!(redacted.metadata.annotations.contains_key("description"));
    }

    #[test]
    fn test_generate_id() {
        let config = EventFluxConfig::with_name("test-app")
            .with_namespace("production")
            .with_version("1.2.3");

        let id = config.generate_id();
        assert_eq!(id, "eventflux.io/v1/production/test-app:1.2.3");
    }
}
