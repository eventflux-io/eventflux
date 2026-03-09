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

//! Configuration Error Types
//!
//! Comprehensive error handling for configuration management operations.

use thiserror::Error;

/// Result type for configuration operations
pub type ConfigResult<T> = Result<T, ConfigError>;

/// Comprehensive configuration error types
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Configuration file not found: {path}")]
    FileNotFound { path: String },

    #[error("Invalid YAML syntax in {file}: {message}")]
    YamlParseError { file: String, message: String },

    #[error("JSON parsing error: {message}")]
    JsonParseError { message: String },

    #[error("Configuration validation failed: {errors:?}")]
    ValidationFailed { errors: Vec<ValidationError> },

    #[error("Secret resolution failed for '{reference}': {message}")]
    SecretResolutionFailed { reference: String, message: String },

    #[error("Secret '{name}' not found in any provider")]
    SecretNotFound { name: String },

    #[error("External service '{service}' unreachable: {message}")]
    ConnectivityError { service: String, message: String },

    #[error("Environment variable '{variable}' not found")]
    EnvironmentVariableNotFound { variable: String },

    #[error("Invalid environment variable value for '{variable}': {message}")]
    InvalidEnvironmentVariable { variable: String, message: String },

    #[error("Configuration version '{version}' is incompatible with current version '{current}'")]
    VersionMismatch { version: String, current: String },

    #[error("Kubernetes API error: {message}")]
    KubernetesError { message: String },

    #[error("Consul error: {message}")]
    ConsulError { message: String },

    #[error("Vault error: {message}")]
    VaultError { message: String },

    #[error("Etcd error: {message}")]
    EtcdError { message: String },

    #[error("Configuration merge conflict: {message}")]
    MergeConflict { message: String },

    #[error("Invalid configuration path: {path}")]
    InvalidPath { path: String },

    #[error("Permission denied accessing configuration: {path}")]
    PermissionDenied { path: String },

    #[error("Configuration schema validation failed: {message}")]
    SchemaValidation { message: String },

    #[error("Timeout occurred during configuration operation: {operation}")]
    Timeout { operation: String },

    #[error("Serialization error: {message}")]
    SerializationError { message: String },

    #[error("Deserialization error: {message}")]
    DeserializationError { message: String },

    #[error("Watch error: {message}")]
    WatchError { message: String },

    #[error("Encryption error: {message}")]
    EncryptionError { message: String },

    #[error("Decryption error: {message}")]
    DecryptionError { message: String },

    #[error("Network error: {message}")]
    NetworkError { message: String },

    #[error("Authentication failed: {message}")]
    AuthenticationFailed { message: String },

    #[error("Authorization failed: {message}")]
    AuthorizationFailed { message: String },

    #[error("Configuration not found: {name}")]
    ConfigurationNotFound { name: String },

    #[error("Multiple configurations found for: {name}")]
    MultipleConfigurationsFound { name: String },

    #[error("Configuration locked by another process: {name}")]
    ConfigurationLocked { name: String },

    #[error("Internal error: {message}")]
    InternalError { message: String },

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("YAML error: {0}")]
    YamlError(#[from] serde_yaml::Error),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
}

/// Configuration validation error details
#[derive(Error, Debug, Clone, PartialEq)]
pub enum ValidationError {
    #[error("Required field '{field}' is missing")]
    MissingRequiredField { field: String },

    #[error("Invalid value for field '{field}': {message}")]
    InvalidFieldValue { field: String, message: String },

    #[error("Field '{field}' has invalid format: expected {expected}, got {actual}")]
    InvalidFormat {
        field: String,
        expected: String,
        actual: String,
    },

    #[error("Value '{value}' is out of range for field '{field}': min={min}, max={max}")]
    OutOfRange {
        field: String,
        value: String,
        min: String,
        max: String,
    },

    #[error("Unknown field '{field}' in {context}")]
    UnknownField { field: String, context: String },

    #[error("Invalid enum value '{value}' for field '{field}': valid values are {valid_values:?}")]
    InvalidEnum {
        field: String,
        value: String,
        valid_values: Vec<String>,
    },

    #[error("Field '{field}' cannot be empty")]
    EmptyField { field: String },

    #[error("Dependency validation failed: {message}")]
    DependencyValidation { message: String },

    #[error("Custom validation failed for '{field}': {message}")]
    CustomValidation { field: String, message: String },

    #[error("Configuration inconsistency: {message}")]
    Inconsistency { message: String },
}

/// Configuration validation result
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// List of validation errors
    pub errors: Vec<ValidationError>,

    /// List of validation warnings
    pub warnings: Vec<ValidationError>,

    /// Additional validation context
    pub context: String,
}

impl ValidationResult {
    /// Create a new validation result
    pub fn new() -> Self {
        Self {
            errors: Vec::new(),
            warnings: Vec::new(),
            context: String::new(),
        }
    }

    /// Create a successful validation result
    pub fn success() -> Self {
        Self::new()
    }

    /// Create a validation result with errors
    pub fn with_errors(errors: Vec<ValidationError>) -> Self {
        Self {
            errors,
            warnings: Vec::new(),
            context: String::new(),
        }
    }

    /// Add an error to the validation result
    pub fn add_error(&mut self, error: ValidationError) {
        self.errors.push(error);
    }

    /// Add a warning to the validation result
    pub fn add_warning(&mut self, warning: ValidationError) {
        self.warnings.push(warning);
    }

    /// Check if the validation was successful (no errors)
    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }

    /// Check if there are any warnings
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }

    /// Get the total number of issues (errors + warnings)
    pub fn issue_count(&self) -> usize {
        self.errors.len() + self.warnings.len()
    }

    /// Merge another validation result into this one
    pub fn merge(&mut self, other: ValidationResult) {
        self.errors.extend(other.errors);
        self.warnings.extend(other.warnings);
        if !other.context.is_empty() {
            if self.context.is_empty() {
                self.context = other.context;
            } else {
                self.context = format!("{}, {}", self.context, other.context);
            }
        }
    }

    /// Convert to a Result type
    pub fn into_result(self) -> Result<(), ConfigError> {
        if self.is_valid() {
            Ok(())
        } else {
            Err(ConfigError::ValidationFailed {
                errors: self.errors,
            })
        }
    }

    /// Format validation result as a human-readable string
    pub fn format_summary(&self) -> String {
        let mut parts = Vec::new();

        if !self.errors.is_empty() {
            parts.push(format!("{} error(s)", self.errors.len()));
        }

        if !self.warnings.is_empty() {
            parts.push(format!("{} warning(s)", self.warnings.len()));
        }

        if parts.is_empty() {
            "Configuration is valid".to_string()
        } else {
            format!("Configuration validation: {}", parts.join(", "))
        }
    }

    /// Get detailed error messages
    pub fn error_messages(&self) -> Vec<String> {
        self.errors.iter().map(|e| e.to_string()).collect()
    }

    /// Get detailed warning messages  
    pub fn warning_messages(&self) -> Vec<String> {
        self.warnings.iter().map(|e| e.to_string()).collect()
    }
}

impl Default for ValidationResult {
    fn default() -> Self {
        Self::new()
    }
}

// Conversion implementations for common error types
impl From<std::env::VarError> for ConfigError {
    fn from(err: std::env::VarError) -> Self {
        match err {
            std::env::VarError::NotPresent => ConfigError::EnvironmentVariableNotFound {
                variable: "unknown".to_string(),
            },
            std::env::VarError::NotUnicode(_) => ConfigError::InvalidEnvironmentVariable {
                variable: "unknown".to_string(),
                message: "Contains invalid unicode".to_string(),
            },
        }
    }
}

#[cfg(feature = "kubernetes")]
impl From<kube::Error> for ConfigError {
    fn from(err: kube::Error) -> Self {
        ConfigError::KubernetesError {
            message: err.to_string(),
        }
    }
}

// Custom error creation helpers
impl ConfigError {
    /// Create a file not found error
    pub fn file_not_found(path: impl Into<String>) -> Self {
        Self::FileNotFound { path: path.into() }
    }

    /// Create a YAML parse error
    pub fn yaml_parse_error(file: impl Into<String>, message: impl Into<String>) -> Self {
        Self::YamlParseError {
            file: file.into(),
            message: message.into(),
        }
    }

    /// Create a validation error
    pub fn validation_failed(errors: Vec<ValidationError>) -> Self {
        Self::ValidationFailed { errors }
    }

    /// Create a secret resolution error
    pub fn secret_resolution_failed(
        reference: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::SecretResolutionFailed {
            reference: reference.into(),
            message: message.into(),
        }
    }

    /// Create a connectivity error
    pub fn connectivity_error(service: impl Into<String>, message: impl Into<String>) -> Self {
        Self::ConnectivityError {
            service: service.into(),
            message: message.into(),
        }
    }

    /// Create an environment variable not found error
    pub fn env_var_not_found(variable: impl Into<String>) -> Self {
        Self::EnvironmentVariableNotFound {
            variable: variable.into(),
        }
    }

    /// Create a secret not found error
    pub fn secret_not_found(name: impl Into<String>) -> Self {
        Self::SecretNotFound { name: name.into() }
    }

    /// Create an internal error
    pub fn internal_error(message: impl Into<String>) -> Self {
        Self::InternalError {
            message: message.into(),
        }
    }

    /// Create an invalid environment variable error
    pub fn invalid_environment_variable(
        variable: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::InvalidEnvironmentVariable {
            variable: variable.into(),
            message: message.into(),
        }
    }

    /// Create a validation error
    pub fn validation_error(errors: Vec<ValidationError>) -> Self {
        Self::ValidationFailed { errors }
    }

    /// Create an IO error wrapper
    pub fn io_error(error: std::io::Error) -> Self {
        Self::IoError(error)
    }

    /// Create a serialization error
    pub fn serialization_error(message: impl Into<String>) -> Self {
        Self::SerializationError {
            message: message.into(),
        }
    }
}

// Validation error creation helpers
impl ValidationError {
    /// Create a missing required field error
    pub fn missing_required_field(field: impl Into<String>) -> Self {
        Self::MissingRequiredField {
            field: field.into(),
        }
    }

    /// Create an invalid field value error
    pub fn invalid_field_value(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self::InvalidFieldValue {
            field: field.into(),
            message: message.into(),
        }
    }

    /// Create an invalid format error
    pub fn invalid_format(
        field: impl Into<String>,
        expected: impl Into<String>,
        actual: impl Into<String>,
    ) -> Self {
        Self::InvalidFormat {
            field: field.into(),
            expected: expected.into(),
            actual: actual.into(),
        }
    }

    /// Create an out of range error
    pub fn out_of_range(
        field: impl Into<String>,
        value: impl Into<String>,
        min: impl Into<String>,
        max: impl Into<String>,
    ) -> Self {
        Self::OutOfRange {
            field: field.into(),
            value: value.into(),
            min: min.into(),
            max: max.into(),
        }
    }

    /// Create an unknown field error
    pub fn unknown_field(field: impl Into<String>, context: impl Into<String>) -> Self {
        Self::UnknownField {
            field: field.into(),
            context: context.into(),
        }
    }

    /// Create an invalid enum error
    pub fn invalid_enum(
        field: impl Into<String>,
        value: impl Into<String>,
        valid_values: Vec<String>,
    ) -> Self {
        Self::InvalidEnum {
            field: field.into(),
            value: value.into(),
            valid_values,
        }
    }

    /// Create a custom validation error
    pub fn custom_validation(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self::CustomValidation {
            field: field.into(),
            message: message.into(),
        }
    }

    /// Create an inconsistency validation error
    pub fn inconsistency(message: impl Into<String>) -> Self {
        Self::Inconsistency {
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_result() {
        let mut result = ValidationResult::new();
        assert!(result.is_valid());
        assert!(!result.has_warnings());

        result.add_error(ValidationError::missing_required_field("test_field"));
        assert!(!result.is_valid());
        assert_eq!(result.errors.len(), 1);

        result.add_warning(ValidationError::invalid_field_value(
            "warning_field",
            "test",
        ));
        assert!(result.has_warnings());
        assert_eq!(result.warnings.len(), 1);
    }

    #[test]
    fn test_validation_result_merge() {
        let mut result1 = ValidationResult::new();
        result1.add_error(ValidationError::missing_required_field("field1"));

        let mut result2 = ValidationResult::new();
        result2.add_error(ValidationError::missing_required_field("field2"));
        result2.add_warning(ValidationError::invalid_field_value(
            "warning_field",
            "test",
        ));

        result1.merge(result2);

        assert_eq!(result1.errors.len(), 2);
        assert_eq!(result1.warnings.len(), 1);
        assert!(!result1.is_valid());
        assert!(result1.has_warnings());
    }

    #[test]
    fn test_config_error_creation() {
        let error = ConfigError::file_not_found("config.yaml");
        assert!(matches!(error, ConfigError::FileNotFound { .. }));

        let error =
            ConfigError::validation_failed(vec![ValidationError::missing_required_field("test")]);
        assert!(matches!(error, ConfigError::ValidationFailed { .. }));
    }

    #[test]
    fn test_validation_error_creation() {
        let error = ValidationError::missing_required_field("test_field");
        assert!(matches!(
            error,
            ValidationError::MissingRequiredField { .. }
        ));

        let error = ValidationError::invalid_enum(
            "enum_field",
            "invalid_value",
            vec!["valid1".to_string(), "valid2".to_string()],
        );
        assert!(matches!(error, ValidationError::InvalidEnum { .. }));
    }

    #[test]
    fn test_validation_result_into_result() {
        let valid_result = ValidationResult::success();
        assert!(valid_result.into_result().is_ok());

        let invalid_result =
            ValidationResult::with_errors(vec![ValidationError::missing_required_field("test")]);
        assert!(invalid_result.into_result().is_err());
    }

    #[test]
    fn test_error_display() {
        let error = ConfigError::file_not_found("test.yaml");
        let display = format!("{}", error);
        assert!(display.contains("Configuration file not found"));
        assert!(display.contains("test.yaml"));
    }
}
