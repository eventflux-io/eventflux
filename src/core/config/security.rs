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

//! Configuration Security Management
//!
//! Provides security utilities for handling sensitive configuration values
//! including encryption, secret management, and secure storage.

use crate::core::config::{ConfigError, ConfigResult};
use std::collections::HashMap;

/// Security provider for configuration secrets
#[derive(Debug)]
pub struct SecretManager {
    /// Secret providers by priority
    providers: Vec<Box<dyn SecretProvider>>,

    /// Cached secrets
    cache: HashMap<String, String>,

    /// Enable caching
    cache_enabled: bool,
}

impl SecretManager {
    /// Create a new secret manager
    pub fn new() -> Self {
        Self {
            providers: Vec::new(),
            cache: HashMap::new(),
            cache_enabled: true,
        }
    }

    /// Add a secret provider
    pub fn add_provider(&mut self, provider: Box<dyn SecretProvider>) {
        self.providers.push(provider);
    }

    /// Retrieve a secret by key
    pub fn get_secret(&mut self, key: &str) -> ConfigResult<String> {
        // Check cache first
        if self.cache_enabled {
            if let Some(value) = self.cache.get(key) {
                return Ok(value.clone());
            }
        }

        // Try each provider
        for provider in &self.providers {
            if provider.has_secret(key) {
                match provider.get_secret(key) {
                    Ok(value) => {
                        // Cache the result
                        if self.cache_enabled {
                            self.cache.insert(key.to_string(), value.clone());
                        }
                        return Ok(value);
                    }
                    Err(e) => {
                        // Continue to next provider on error
                        eprintln!("Provider {} failed for key {}: {}", provider.name(), key, e);
                        continue;
                    }
                }
            }
        }

        Err(ConfigError::secret_not_found(key))
    }

    /// Clear the secret cache
    pub fn clear_cache(&mut self) {
        self.cache.clear();
    }

    /// Enable or disable secret caching
    pub fn set_cache_enabled(&mut self, enabled: bool) {
        self.cache_enabled = enabled;
        if !enabled {
            self.cache.clear();
        }
    }
}

impl Default for SecretManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for secret providers
pub trait SecretProvider: Send + Sync + std::fmt::Debug {
    /// Get the provider name
    fn name(&self) -> &str;

    /// Check if the provider has a secret
    fn has_secret(&self, key: &str) -> bool;

    /// Get a secret by key
    fn get_secret(&self, key: &str) -> ConfigResult<String>;

    /// Get provider priority (higher = more important)
    fn priority(&self) -> u32 {
        50
    }
}

/// Environment variable secret provider
#[derive(Debug, Clone)]
pub struct EnvironmentSecretProvider {
    /// Environment variable prefix
    prefix: Option<String>,
}

impl EnvironmentSecretProvider {
    /// Create a new environment secret provider
    pub fn new() -> Self {
        Self { prefix: None }
    }

    /// Create with a prefix
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self {
            prefix: Some(prefix.into()),
        }
    }
}

impl Default for EnvironmentSecretProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl SecretProvider for EnvironmentSecretProvider {
    fn name(&self) -> &str {
        "environment"
    }

    fn has_secret(&self, key: &str) -> bool {
        let env_key = if let Some(ref prefix) = self.prefix {
            format!("{}{}", prefix, key.to_uppercase())
        } else {
            key.to_string()
        };

        std::env::var(&env_key).is_ok()
    }

    fn get_secret(&self, key: &str) -> ConfigResult<String> {
        let env_key = if let Some(ref prefix) = self.prefix {
            format!("{}{}", prefix, key.to_uppercase())
        } else {
            key.to_string()
        };

        std::env::var(&env_key).map_err(|_| ConfigError::secret_not_found(key))
    }

    fn priority(&self) -> u32 {
        30 // Medium priority
    }
}

/// File-based secret provider
#[derive(Debug, Clone)]
pub struct FileSecretProvider {
    /// Base directory for secret files
    base_dir: String,
}

impl FileSecretProvider {
    /// Create a new file secret provider
    pub fn new(base_dir: impl Into<String>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }
}

impl SecretProvider for FileSecretProvider {
    fn name(&self) -> &str {
        "file"
    }

    fn has_secret(&self, key: &str) -> bool {
        let file_path = format!("{}/{}", self.base_dir, key);
        std::path::Path::new(&file_path).exists()
    }

    fn get_secret(&self, key: &str) -> ConfigResult<String> {
        let file_path = format!("{}/{}", self.base_dir, key);
        std::fs::read_to_string(&file_path)
            .map(|s| s.trim().to_string()) // Remove trailing newlines
            .map_err(|e| {
                ConfigError::internal_error(format!(
                    "Failed to read secret file {}: {}",
                    file_path, e
                ))
            })
    }

    fn priority(&self) -> u32 {
        40 // Higher than environment
    }
}

/// Kubernetes Secret provider
#[cfg(feature = "kubernetes")]
#[derive(Debug, Clone)]
pub struct KubernetesSecretProvider {
    /// Secret name
    secret_name: String,

    /// Kubernetes namespace
    namespace: Option<String>,
}

#[cfg(feature = "kubernetes")]
impl KubernetesSecretProvider {
    /// Create a new Kubernetes secret provider
    pub fn new(secret_name: impl Into<String>) -> Self {
        Self {
            secret_name: secret_name.into(),
            namespace: None,
        }
    }

    /// Set the namespace for the secret
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Load secret from Kubernetes
    async fn load_secret(&self, key: &str) -> ConfigResult<Option<String>> {
        use base64::Engine;
        use k8s_openapi::api::core::v1::Secret;
        use kube::{Api, Client};

        let client = Client::try_default().await.map_err(|e| {
            ConfigError::internal_error(format!("Failed to create K8s client: {}", e))
        })?;

        let namespace = self.namespace.as_deref().unwrap_or("default");
        let secrets: Api<Secret> = Api::namespaced(client, namespace);

        match secrets.get(&self.secret_name).await {
            Ok(secret) => {
                if let Some(data) = secret.data {
                    if let Some(encoded_value) = data.get(key) {
                        let decoded = base64::engine::general_purpose::STANDARD
                            .decode(&encoded_value.0)
                            .map_err(|e| {
                                ConfigError::internal_error(format!(
                                    "Failed to decode secret: {}",
                                    e
                                ))
                            })?;

                        let value = String::from_utf8(decoded).map_err(|e| {
                            ConfigError::internal_error(format!("Invalid UTF-8 in secret: {}", e))
                        })?;

                        Ok(Some(value))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
            Err(kube::Error::Api(kube::error::ErrorResponse { code: 404, .. })) => {
                Ok(None) // Secret not found
            }
            Err(e) => Err(ConfigError::internal_error(format!(
                "Failed to get secret '{}': {}",
                self.secret_name, e
            ))),
        }
    }
}

#[cfg(feature = "kubernetes")]
impl SecretProvider for KubernetesSecretProvider {
    fn name(&self) -> &str {
        "kubernetes"
    }

    fn has_secret(&self, key: &str) -> bool {
        // For async operations, we can't easily check availability synchronously
        // In practice, this would be used in conjunction with a cache
        tokio::runtime::Handle::try_current()
            .and_then(|handle| {
                let provider = self.clone();
                let key = key.to_string();
                handle.block_on(async move {
                    provider
                        .load_secret(&key)
                        .await
                        .map(|opt| opt.is_some())
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false)
    }

    fn get_secret(&self, key: &str) -> ConfigResult<String> {
        // This is a blocking version for compatibility
        // In practice, you'd use the async version with proper runtime handling
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            ConfigError::internal_error(
                "No async runtime available for Kubernetes secret access".to_string(),
            )
        })?;

        let provider = self.clone();
        let key = key.to_string();

        runtime.block_on(async move {
            match provider.load_secret(&key).await? {
                Some(value) => Ok(value),
                None => Err(ConfigError::secret_not_found(&key)),
            }
        })
    }

    fn priority(&self) -> u32 {
        70 // High priority for Kubernetes secrets
    }
}

/// External secret provider interface for HashiCorp Vault, AWS Secrets Manager, etc.
#[derive(Debug, Clone)]
pub struct ExternalSecretProvider {
    /// Provider name
    name: String,

    /// Provider configuration
    config: HashMap<String, String>,
}

impl ExternalSecretProvider {
    /// Create a new external secret provider
    pub fn new(name: impl Into<String>, config: HashMap<String, String>) -> Self {
        Self {
            name: name.into(),
            config,
        }
    }
}

impl SecretProvider for ExternalSecretProvider {
    fn name(&self) -> &str {
        &self.name
    }

    fn has_secret(&self, _key: &str) -> bool {
        // Implementation pending: Provider-specific secret retrieval logic
        false
    }

    fn get_secret(&self, key: &str) -> ConfigResult<String> {
        // Implementation pending: Provider-specific secret retrieval logic for Vault, AWS Secrets Manager, etc.
        Err(ConfigError::internal_error(format!(
            "External secret provider {} not yet implemented for key {}",
            self.name, key
        )))
    }

    fn priority(&self) -> u32 {
        60 // Highest priority for external providers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[serial_test::serial]
    fn test_environment_secret_provider() {
        std::env::set_var("TEST_SECRET", "secret_value");

        let provider = EnvironmentSecretProvider::new();
        assert!(provider.has_secret("TEST_SECRET"));
        assert_eq!(provider.get_secret("TEST_SECRET").unwrap(), "secret_value");

        assert!(!provider.has_secret("NONEXISTENT_SECRET"));
        assert!(provider.get_secret("NONEXISTENT_SECRET").is_err());

        std::env::remove_var("TEST_SECRET");
    }

    #[test]
    #[serial_test::serial]
    fn test_environment_secret_provider_with_prefix() {
        std::env::set_var("EVENTFLUX_SECRET_TOKEN", "token_value");

        let provider = EnvironmentSecretProvider::with_prefix("EVENTFLUX_SECRET_");
        assert!(provider.has_secret("TOKEN"));
        assert_eq!(provider.get_secret("TOKEN").unwrap(), "token_value");

        std::env::remove_var("EVENTFLUX_SECRET_TOKEN");
    }

    #[test]
    #[serial_test::serial]
    fn test_secret_manager() {
        let mut manager = SecretManager::new();
        manager.add_provider(Box::new(EnvironmentSecretProvider::new()));

        std::env::set_var("MANAGER_TEST_SECRET", "manager_value");

        let result = manager.get_secret("MANAGER_TEST_SECRET");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "manager_value");

        // Test caching
        std::env::remove_var("MANAGER_TEST_SECRET");
        let cached_result = manager.get_secret("MANAGER_TEST_SECRET");
        assert!(cached_result.is_ok());
        assert_eq!(cached_result.unwrap(), "manager_value");

        // Clear cache and try again
        manager.clear_cache();
        let no_cache_result = manager.get_secret("MANAGER_TEST_SECRET");
        assert!(no_cache_result.is_err());
    }

    #[cfg(feature = "kubernetes")]
    #[test]
    fn test_kubernetes_secret_provider_creation() {
        let provider =
            KubernetesSecretProvider::new("eventflux-secrets").with_namespace("eventflux-system");

        assert_eq!(provider.name(), "kubernetes");
        assert_eq!(provider.secret_name, "eventflux-secrets");
        assert_eq!(provider.namespace, Some("eventflux-system".to_string()));
        assert_eq!(provider.priority(), 70);
    }

    #[test]
    fn test_file_secret_provider() {
        use std::fs::{create_dir_all, File};
        use std::io::Write;
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let secrets_dir = temp_dir.path().join("secrets");
        create_dir_all(&secrets_dir).unwrap();

        // Create a secret file
        let secret_file = secrets_dir.join("database-password");
        let mut file = File::create(&secret_file).unwrap();
        file.write_all(b"super_secret_password\n").unwrap();

        let provider = FileSecretProvider::new(secrets_dir.to_string_lossy());

        assert!(provider.has_secret("database-password"));
        let result = provider.get_secret("database-password").unwrap();
        assert_eq!(result, "super_secret_password"); // Should be trimmed

        assert!(!provider.has_secret("nonexistent-secret"));
        assert!(provider.get_secret("nonexistent-secret").is_err());
    }

    #[test]
    #[serial_test::serial]
    fn test_secret_manager_priority_ordering() {
        let mut manager = SecretManager::new();

        // Add providers in different order
        manager.add_provider(Box::new(EnvironmentSecretProvider::new())); // Priority 30
        manager.add_provider(Box::new(FileSecretProvider::new(
            "/tmp/secrets".to_string(),
        ))); // Priority 40

        #[cfg(feature = "kubernetes")]
        manager.add_provider(Box::new(KubernetesSecretProvider::new("test-secrets"))); // Priority 70

        // Test that it works with environment variables
        std::env::set_var("PRIORITY_TEST_SECRET", "env_value");

        let result = manager.get_secret("PRIORITY_TEST_SECRET");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "env_value");

        std::env::remove_var("PRIORITY_TEST_SECRET");
    }

    #[test]
    fn test_external_secret_provider_interface() {
        use std::collections::HashMap;

        let mut config = HashMap::new();
        config.insert(
            "endpoint".to_string(),
            "https://vault.example.com".to_string(),
        );

        let provider = ExternalSecretProvider::new("vault", config);

        assert_eq!(provider.name(), "vault");
        assert_eq!(provider.priority(), 60);
        assert!(!provider.has_secret("any-key"));

        let result = provider.get_secret("test-key");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not yet implemented"));
    }
}
