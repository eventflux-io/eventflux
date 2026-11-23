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

//! Configuration Validation
//!
//! Checks an EventFlux configuration for structural problems, bad values and custom
//! rules, and collects the findings in a [`ValidationResult`].

use crate::core::config::{EventFluxConfig, ValidationError, ValidationResult};

/// Configuration validator
#[derive(Debug)]
pub struct ConfigValidator {
    /// Strict mode flag. Defaults to true. `validate` does not read it yet.
    strict_mode: bool,

    /// Custom rules, run in the order they were added
    custom_rules: Vec<Box<dyn ValidationRule>>,

    /// Run the connectivity checks. Defaults to false.
    connectivity_validation: bool,

    /// Run the resource checks. Defaults to false.
    resource_validation: bool,
}

impl ConfigValidator {
    /// Create a validator with strict mode on and the optional checks off
    pub fn new() -> Self {
        Self {
            strict_mode: true,
            custom_rules: Vec::new(),
            connectivity_validation: false,
            resource_validation: false,
        }
    }

    /// Enable or disable strict validation mode
    pub fn set_strict_mode(&mut self, strict: bool) {
        self.strict_mode = strict;
    }

    /// Add a custom rule. Rules run in the order added. `priority()` is not used.
    pub fn add_rule(&mut self, rule: Box<dyn ValidationRule>) {
        self.custom_rules.push(rule);
    }

    /// Turn on the connectivity checks
    pub fn enable_connectivity_validation(&mut self) {
        self.connectivity_validation = true;
    }

    /// Turn on the resource checks
    pub fn enable_resource_validation(&mut self) {
        self.resource_validation = true;
    }

    /// Validate an EventFlux configuration.
    ///
    /// Runs the structure checks, the value checks, each custom rule, and then the
    /// connectivity and resource checks when they are turned on. Returns every finding.
    pub async fn validate(&self, config: &EventFluxConfig) -> ValidationResult {
        let mut result = ValidationResult::new();

        result.merge(self.validate_structure(config));

        result.merge(self.validate_semantics(config));

        for rule in &self.custom_rules {
            result.merge(rule.validate(config));
        }

        if self.connectivity_validation {
            let connectivity_result = self.validate_connectivity(config).await;
            result.merge(connectivity_result);
        }

        if self.resource_validation {
            result.merge(self.validate_resources(config));
        }

        result
    }

    /// Check the metadata, that distributed or hybrid mode has a `distributed`
    /// section, and that application names are valid identifiers
    fn validate_structure(&self, config: &EventFluxConfig) -> ValidationResult {
        let mut result = ValidationResult::new();

        if let Err(e) = config.metadata.validate() {
            result.add_error(ValidationError::custom_validation("metadata", e));
        }

        match config.eventflux.runtime.mode {
            crate::core::config::RuntimeMode::Distributed
            | crate::core::config::RuntimeMode::Hybrid => {
                if config.eventflux.distributed.is_none() {
                    result.add_error(ValidationError::missing_required_field(
                        "eventflux.distributed (required for distributed/hybrid mode)",
                    ));
                }
            }
            _ => {}
        }

        for name in config.applications.keys() {
            if name.is_empty() {
                result.add_error(ValidationError::invalid_field_value(
                    "applications.<name>",
                    "Application name cannot be empty",
                ));
            } else if !self.is_valid_identifier(name) {
                result.add_error(ValidationError::invalid_format(
                    "applications.<name>",
                    "alphanumeric characters, hyphens, and underscores",
                    name.clone(),
                ));
            }
        }

        result
    }

    /// Check performance values and the distributed section.
    ///
    /// A thread pool or event buffer of 0 is an error. A thread pool over 1000 or an
    /// event buffer under 1000 is a warning.
    fn validate_semantics(&self, config: &EventFluxConfig) -> ValidationResult {
        let mut result = ValidationResult::new();

        let thread_pool_size = config.eventflux.runtime.performance.thread_pool_size;
        if thread_pool_size == 0 {
            result.add_error(ValidationError::invalid_field_value(
                "eventflux.runtime.performance.thread_pool_size",
                "Thread pool size must be greater than 0",
            ));
        } else if thread_pool_size > 1000 {
            result.add_warning(ValidationError::invalid_field_value(
                "eventflux.runtime.performance.thread_pool_size",
                "Thread pool size is very large and may cause resource issues",
            ));
        }

        let buffer_size = config.eventflux.runtime.performance.event_buffer_size;
        if buffer_size == 0 {
            result.add_error(ValidationError::invalid_field_value(
                "eventflux.runtime.performance.event_buffer_size",
                "Event buffer size must be greater than 0",
            ));
        } else if buffer_size < 1000 {
            result.add_warning(ValidationError::invalid_field_value(
                "eventflux.runtime.performance.event_buffer_size",
                "Event buffer size is very small and may cause performance issues",
            ));
        }

        if let Some(ref distributed) = config.eventflux.distributed {
            result.merge(self.validate_distributed_config(distributed));
        }

        result
    }

    /// Check the distributed section.
    ///
    /// Errors when `min_nodes` is 0, when `expected_size` is below `min_nodes`, or when
    /// the transport pool size is 0. Warns when there are no seed nodes.
    fn validate_distributed_config(
        &self,
        distributed: &crate::core::config::DistributedConfig,
    ) -> ValidationResult {
        let mut result = ValidationResult::new();

        if let Some(ref cluster) = distributed.cluster {
            if cluster.min_nodes == 0 {
                result.add_error(ValidationError::invalid_field_value(
                    "eventflux.distributed.cluster.min_nodes",
                    "Minimum nodes must be greater than 0",
                ));
            }

            if cluster.expected_size < cluster.min_nodes {
                result.add_error(ValidationError::inconsistency(
                    "Expected cluster size is less than minimum required nodes",
                ));
            }

            if cluster.seed_nodes.is_empty() {
                result.add_warning(ValidationError::custom_validation(
                    "eventflux.distributed.cluster.seed_nodes",
                    "No seed nodes specified for cluster discovery",
                ));
            }
        }

        let pool_size = distributed.transport.pool_size;
        if pool_size == 0 {
            result.add_error(ValidationError::invalid_field_value(
                "eventflux.distributed.transport.pool_size",
                "Transport pool size must be greater than 0",
            ));
        }

        result
    }

    /// Check that external services can be reached. Not built yet, so it always
    /// returns an empty result.
    async fn validate_connectivity(&self, _config: &EventFluxConfig) -> ValidationResult {
        // The plan is to test database connections, Kafka brokers, Redis and other
        // external endpoints

        ValidationResult::new()
    }

    /// Check resource settings. Only the format of `memory.max_heap` is checked today.
    fn validate_resources(&self, config: &EventFluxConfig) -> ValidationResult {
        let mut result = ValidationResult::new();

        // Not done yet: checking that the memory, CPU, storage and bandwidth limits
        // are workable
        if let Some(ref resources) = config.eventflux.runtime.resources {
            if let Some(ref memory) = resources.memory {
                if let Err(e) = self.validate_memory_spec(&memory.max_heap) {
                    result.add_error(ValidationError::invalid_format(
                        "eventflux.runtime.resources.memory.max_heap",
                        "valid memory specification (e.g., '2Gi', '512Mi')",
                        e,
                    ));
                }
            }
        }

        result
    }

    /// Check a memory size such as "2Gi", "512Mi" or "1.5G".
    ///
    /// Accepts a number, with an optional fraction, followed by K, M, G or T and an
    /// optional `i`. Returns the reason as text when the format is wrong.
    fn validate_memory_spec(&self, spec: &str) -> Result<(), String> {
        if spec.is_empty() {
            return Err("empty specification".to_string());
        }

        let re = regex::Regex::new(r"^\d+(\.\d+)?[KMGT]i?$").unwrap();
        if !re.is_match(spec) {
            return Err(format!("invalid format: {}", spec));
        }

        Ok(())
    }

    /// Check that a name is not empty, has only letters, digits, `-` and `_`, and does
    /// not start or end with `-`
    fn is_valid_identifier(&self, name: &str) -> bool {
        !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
            && !name.starts_with('-')
            && !name.ends_with('-')
    }
}

impl Default for ConfigValidator {
    fn default() -> Self {
        Self::new()
    }
}

/// A custom check for [`ConfigValidator`]
pub trait ValidationRule: Send + Sync + std::fmt::Debug {
    /// Check a configuration and return the findings
    fn validate(&self, config: &EventFluxConfig) -> ValidationResult;

    /// Get a description of this validation rule
    fn description(&self) -> String;

    /// Rule priority, where a higher number means more important. Defaults to 50.
    /// [`ConfigValidator`] does not use it yet.
    fn priority(&self) -> u32 {
        50
    }
}

/// Built in rule that checks each application.
///
/// Warns when an application has no definitions and errors on an empty definition
/// name.
#[derive(Debug)]
pub struct ApplicationValidationRule;

impl ValidationRule for ApplicationValidationRule {
    fn validate(&self, config: &EventFluxConfig) -> ValidationResult {
        let mut result = ValidationResult::new();

        for (app_name, app_config) in &config.applications {
            if app_config.definitions.is_empty() {
                result.add_warning(ValidationError::custom_validation(
                    format!("applications.{}.definitions", app_name),
                    "Application has no definitions configured",
                ));
            }

            for (def_name, def_config) in &app_config.definitions {
                result.merge(self.validate_definition(app_name, def_name, def_config));
            }
        }

        result
    }

    fn description(&self) -> String {
        "Application-specific validation rules".to_string()
    }

    fn priority(&self) -> u32 {
        60
    }
}

impl ApplicationValidationRule {
    /// Check one definition. Only the name is checked today.
    fn validate_definition(
        &self,
        app_name: &str,
        def_name: &str,
        _def_config: &crate::core::config::DefinitionConfig,
    ) -> ValidationResult {
        let mut result = ValidationResult::new();

        if def_name.is_empty() {
            result.add_error(ValidationError::invalid_field_value(
                format!("applications.{}.definitions.<name>", app_name),
                "Definition name cannot be empty",
            ));
        }

        // Not done yet: checks specific to each definition type

        result
    }
}

/// Built in rule for TLS settings.
///
/// Warns when TLS is off in distributed or hybrid mode, and when TLS is on without
/// peer checks. Does nothing when there is no TLS section.
#[derive(Debug)]
pub struct SecurityValidationRule;

impl ValidationRule for SecurityValidationRule {
    fn validate(&self, config: &EventFluxConfig) -> ValidationResult {
        let mut result = ValidationResult::new();

        if let Some(ref security) = config.eventflux.security {
            if let Some(ref tls) = security.tls {
                if !tls.enabled && config.is_distributed_mode() {
                    result.add_warning(ValidationError::custom_validation(
                        "eventflux.security.tls.enabled",
                        "TLS is disabled in distributed mode, which may be insecure",
                    ));
                }

                if tls.enabled && !tls.verify_peer {
                    result.add_warning(ValidationError::custom_validation(
                        "eventflux.security.tls.verify_peer",
                        "TLS peer verification is disabled, which reduces security",
                    ));
                }
            }
        }

        result
    }

    fn description(&self) -> String {
        "Security configuration validation".to_string()
    }

    fn priority(&self) -> u32 {
        80 // Above the application rule
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::RuntimeMode;

    #[tokio::test]
    async fn test_basic_validation() {
        let validator = ConfigValidator::new();
        let config = EventFluxConfig::default();

        let result = validator.validate(&config).await;
        assert!(result.is_valid());
    }

    #[tokio::test]
    async fn test_invalid_thread_pool_size() {
        let validator = ConfigValidator::new();
        let mut config = EventFluxConfig::default();
        config.eventflux.runtime.performance.thread_pool_size = 0;

        let result = validator.validate(&config).await;
        assert!(!result.is_valid());
        assert!(result
            .errors
            .iter()
            .any(|e| e.to_string().contains("Thread pool size")));
    }

    #[tokio::test]
    async fn test_distributed_mode_validation() {
        let validator = ConfigValidator::new();
        let mut config = EventFluxConfig::default();
        config.eventflux.runtime.mode = RuntimeMode::Distributed;
        // Missing distributed configuration

        let result = validator.validate(&config).await;
        assert!(!result.is_valid());
        assert!(result
            .errors
            .iter()
            .any(|e| e.to_string().contains("distributed")));
    }

    #[tokio::test]
    async fn test_application_validation_rule() {
        let rule = ApplicationValidationRule;
        let mut config = EventFluxConfig::default();

        // Add an empty application
        config.applications.insert(
            "test-app".to_string(),
            crate::core::config::ApplicationConfig::default(),
        );

        let result = rule.validate(&config);
        // Should warn about empty definitions
        assert!(result.has_warnings());
        assert!(result
            .warnings
            .iter()
            .any(|w| w.to_string().contains("no definitions")));
    }

    #[tokio::test]
    async fn test_custom_validation_rule() {
        #[derive(Debug)]
        struct CustomRule;
        impl ValidationRule for CustomRule {
            fn validate(&self, config: &EventFluxConfig) -> ValidationResult {
                let mut result = ValidationResult::new();
                if config.metadata.name.is_none() {
                    result.add_error(ValidationError::missing_required_field("metadata.name"));
                }
                result
            }

            fn description(&self) -> String {
                "Custom test rule".to_string()
            }
        }

        let mut validator = ConfigValidator::new();
        validator.add_rule(Box::new(CustomRule));

        let config = EventFluxConfig::default(); // Has no name
        let result = validator.validate(&config).await;

        assert!(!result.is_valid());
        assert!(result
            .errors
            .iter()
            .any(|e| e.to_string().contains("metadata.name")));
    }

    #[test]
    fn test_memory_spec_validation() {
        let validator = ConfigValidator::new();

        assert!(validator.validate_memory_spec("2Gi").is_ok());
        assert!(validator.validate_memory_spec("512Mi").is_ok());
        assert!(validator.validate_memory_spec("1G").is_ok());
        assert!(validator.validate_memory_spec("100M").is_ok());

        assert!(validator.validate_memory_spec("").is_err());
        assert!(validator.validate_memory_spec("invalid").is_err());
        assert!(validator.validate_memory_spec("2X").is_err());
    }

    #[test]
    fn test_identifier_validation() {
        let validator = ConfigValidator::new();

        assert!(validator.is_valid_identifier("valid-name"));
        assert!(validator.is_valid_identifier("valid_name"));
        assert!(validator.is_valid_identifier("validname123"));
        assert!(validator.is_valid_identifier("a"));

        assert!(!validator.is_valid_identifier(""));
        assert!(!validator.is_valid_identifier("-invalid"));
        assert!(!validator.is_valid_identifier("invalid-"));
        assert!(!validator.is_valid_identifier("invalid name"));
        assert!(!validator.is_valid_identifier("invalid@name"));
    }
}
