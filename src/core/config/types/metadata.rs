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

//! Configuration Metadata Types
//!
//! Defines metadata structures for configuration objects following
//! Kubernetes-style API conventions.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration metadata following Kubernetes API conventions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ConfigMetadata {
    /// Name of the configuration
    pub name: Option<String>,

    /// Namespace for logical grouping of configurations
    pub namespace: Option<String>,

    /// Configuration version
    pub version: Option<String>,

    /// Environment identifier
    pub environment: Option<String>,

    /// Custom labels for organization and selection
    #[serde(default)]
    pub labels: HashMap<String, String>,

    /// Custom annotations for additional metadata
    #[serde(default)]
    pub annotations: HashMap<String, String>,

    /// Creation timestamp (ISO 8601)
    #[serde(rename = "createdAt", skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,

    /// Last update timestamp (ISO 8601)
    #[serde(rename = "updatedAt", skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

impl ConfigMetadata {
    /// Create a new metadata instance with required fields
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ..Default::default()
        }
    }

    /// Set the namespace
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Set the environment
    pub fn with_environment(mut self, environment: impl Into<String>) -> Self {
        self.environment = Some(environment.into());
        self
    }

    /// Set the version
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    /// Add a label
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    /// Add multiple labels
    pub fn with_labels(mut self, labels: HashMap<String, String>) -> Self {
        self.labels.extend(labels);
        self
    }

    /// Add an annotation
    pub fn with_annotation(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.annotations.insert(key.into(), value.into());
        self
    }

    /// Get a label value by key
    pub fn get_label(&self, key: &str) -> Option<&String> {
        self.labels.get(key)
    }

    /// Get an annotation value by key
    pub fn get_annotation(&self, key: &str) -> Option<&String> {
        self.annotations.get(key)
    }

    /// Check if this configuration matches the given selectors
    pub fn matches_labels(&self, selectors: &HashMap<String, String>) -> bool {
        selectors
            .iter()
            .all(|(key, value)| self.labels.get(key) == Some(value))
    }

    /// Generate a unique identifier for this configuration
    pub fn generate_id(&self) -> String {
        let name = self.name.as_deref().unwrap_or("unnamed");
        let namespace = self.namespace.as_deref().unwrap_or("default");
        let version = self.version.as_deref().unwrap_or("latest");

        format!("{}/{}:{}", namespace, name, version)
    }

    /// Update the timestamp to current time
    pub fn touch(&mut self) {
        let now = chrono::Utc::now().to_rfc3339();
        self.updated_at = Some(now.clone());
        if self.created_at.is_none() {
            self.created_at = Some(now);
        }
    }

    /// Validate the metadata fields
    pub fn validate(&self) -> Result<(), String> {
        // Validate name format if provided
        if let Some(ref name) = self.name {
            if name.is_empty() {
                return Err("Name cannot be empty".to_string());
            }
            if !name
                .chars()
                .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
            {
                return Err(
                    "Name must contain only alphanumeric characters, hyphens, and underscores"
                        .to_string(),
                );
            }
        }

        // Validate namespace format if provided
        if let Some(ref namespace) = self.namespace {
            if namespace.is_empty() {
                return Err("Namespace cannot be empty".to_string());
            }
            if !namespace.chars().all(|c| c.is_alphanumeric() || c == '-') {
                return Err(
                    "Namespace must contain only alphanumeric characters and hyphens".to_string(),
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_default() {
        let metadata = ConfigMetadata::default();
        // api_version and kind are now in EventFluxConfig, not ConfigMetadata
        assert!(metadata.name.is_none());
        assert!(metadata.labels.is_empty());
    }

    #[test]
    fn test_metadata_builder() {
        let metadata = ConfigMetadata::new("test-app")
            .with_namespace("production")
            .with_environment("prod")
            .with_version("1.0.0")
            .with_label("team", "platform")
            .with_label("tier", "backend");

        assert_eq!(metadata.name, Some("test-app".to_string()));
        assert_eq!(metadata.namespace, Some("production".to_string()));
        assert_eq!(metadata.environment, Some("prod".to_string()));
        assert_eq!(metadata.version, Some("1.0.0".to_string()));
        assert_eq!(metadata.get_label("team"), Some(&"platform".to_string()));
        assert_eq!(metadata.get_label("tier"), Some(&"backend".to_string()));
    }

    #[test]
    fn test_metadata_validation() {
        let mut metadata = ConfigMetadata::default();
        assert!(metadata.validate().is_ok());

        // api_version and kind validation is now handled at EventFluxConfig level
        metadata.name = Some("invalid name!".to_string());
        assert!(metadata.validate().is_err());

        metadata.name = Some("valid-name_123".to_string());
        assert!(metadata.validate().is_ok());
    }

    #[test]
    fn test_label_matching() {
        let metadata = ConfigMetadata::new("test")
            .with_label("env", "production")
            .with_label("tier", "backend");

        let mut selectors = HashMap::new();
        selectors.insert("env".to_string(), "production".to_string());
        assert!(metadata.matches_labels(&selectors));

        selectors.insert("tier".to_string(), "frontend".to_string());
        assert!(!metadata.matches_labels(&selectors));

        selectors.insert("tier".to_string(), "backend".to_string());
        assert!(metadata.matches_labels(&selectors));
    }

    #[test]
    fn test_generate_id() {
        let metadata = ConfigMetadata::new("my-app")
            .with_namespace("prod")
            .with_version("1.2.3");

        let id = metadata.generate_id();
        assert_eq!(id, "prod/my-app:1.2.3");
    }

    #[test]
    fn test_serde() {
        let metadata = ConfigMetadata::new("test-config")
            .with_namespace("default")
            .with_label("app", "eventflux");

        let yaml = serde_yaml::to_string(&metadata).unwrap();
        let deserialized: ConfigMetadata = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(metadata.name, deserialized.name);
        assert_eq!(metadata.namespace, deserialized.namespace);
        assert_eq!(metadata.labels, deserialized.labels);
    }
}
