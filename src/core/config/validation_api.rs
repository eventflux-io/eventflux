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

//! Configuration Validation API
//!
//! Provides comprehensive validation capabilities for EventFlux configuration,
//! including custom validation rules, schema validation, and enterprise
//! validation patterns for production deployments.

use crate::core::config::{ConfigResult, EventFluxConfig};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Comprehensive validation result with detailed feedback
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationReport {
    /// Overall validation status
    pub is_valid: bool,

    /// Validation errors (blocking issues)
    pub errors: Vec<ValidationError>,

    /// Validation warnings (non-blocking issues)
    pub warnings: Vec<ValidationWarning>,

    /// Validation information (advisory messages)
    pub info: Vec<ValidationInfo>,

    /// Validation score (0.0 to 1.0)
    pub validation_score: f64,

    /// Validation metadata
    pub metadata: HashMap<String, String>,

    /// Validation duration
    pub validation_duration: std::time::Duration,

    /// Validation timestamp
    pub validated_at: chrono::DateTime<chrono::Utc>,
}

impl ValidationReport {
    /// Create a new validation report
    pub fn new() -> Self {
        Self {
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
            info: Vec::new(),
            validation_score: 1.0,
            metadata: HashMap::new(),
            validation_duration: std::time::Duration::from_millis(0),
            validated_at: chrono::Utc::now(),
        }
    }

    /// Add an error to the report
    pub fn add_error(&mut self, error: ValidationError) {
        self.is_valid = false;
        self.errors.push(error);
        self.recalculate_score();
    }

    /// Add a warning to the report
    pub fn add_warning(&mut self, warning: ValidationWarning) {
        self.warnings.push(warning);
        self.recalculate_score();
    }

    /// Add informational message to the report
    pub fn add_info(&mut self, info: ValidationInfo) {
        self.info.push(info);
    }

    /// Add metadata to the report
    pub fn add_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    /// Set validation duration
    pub fn set_duration(&mut self, duration: std::time::Duration) {
        self.validation_duration = duration;
    }

    /// Recalculate validation score based on errors and warnings
    fn recalculate_score(&mut self) {
        let total_issues = self.errors.len() + self.warnings.len();
        if total_issues == 0 {
            self.validation_score = 1.0;
        } else {
            // Errors count more heavily than warnings
            let error_weight = 2.0;
            let warning_weight = 1.0;
            let total_weight = (self.errors.len() as f64 * error_weight)
                + (self.warnings.len() as f64 * warning_weight);

            // Score decreases with more issues
            self.validation_score = (1.0 - (total_weight / (total_weight + 10.0))).max(0.0);
        }
    }

    /// Get total issue count
    pub fn total_issues(&self) -> usize {
        self.errors.len() + self.warnings.len()
    }

    /// Get summary text
    pub fn summary(&self) -> String {
        if self.is_valid {
            if self.warnings.is_empty() && self.info.is_empty() {
                "✅ Configuration is valid with no issues".to_string()
            } else if self.warnings.is_empty() {
                format!(
                    "✅ Configuration is valid with {} informational message(s)",
                    self.info.len()
                )
            } else {
                format!(
                    "✅ Configuration is valid with {} warning(s) and {} info message(s)",
                    self.warnings.len(),
                    self.info.len()
                )
            }
        } else {
            format!("❌ Configuration is invalid with {} error(s), {} warning(s), and {} info message(s)", 
                   self.errors.len(), self.warnings.len(), self.info.len())
        }
    }
}

impl Default for ValidationReport {
    fn default() -> Self {
        Self::new()
    }
}

/// Validation error with detailed context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    /// Error code for programmatic handling
    pub code: String,

    /// Human-readable error message
    pub message: String,

    /// Configuration path where the error occurred
    pub path: String,

    /// Severity level of the error
    pub severity: ErrorSeverity,

    /// Suggested fix for the error
    pub suggestion: Option<String>,

    /// Additional context
    pub context: HashMap<String, String>,
}

impl ValidationError {
    /// Create a new validation error
    pub fn new(code: String, message: String, path: String) -> Self {
        Self {
            code,
            message,
            path,
            severity: ErrorSeverity::Error,
            suggestion: None,
            context: HashMap::new(),
        }
    }

    /// Create a critical validation error
    pub fn critical(code: String, message: String, path: String) -> Self {
        Self {
            code,
            message,
            path,
            severity: ErrorSeverity::Critical,
            suggestion: None,
            context: HashMap::new(),
        }
    }

    /// Add a suggestion to the error
    pub fn with_suggestion(mut self, suggestion: String) -> Self {
        self.suggestion = Some(suggestion);
        self
    }

    /// Add context to the error
    pub fn with_context(mut self, key: String, value: String) -> Self {
        self.context.insert(key, value);
        self
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] {}: {} (at {})",
            self.severity, self.code, self.message, self.path
        )
    }
}

/// Validation warning with advisory information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationWarning {
    /// Warning code
    pub code: String,

    /// Warning message
    pub message: String,

    /// Configuration path
    pub path: String,

    /// Warning category
    pub category: WarningCategory,

    /// Recommended action
    pub recommendation: Option<String>,
}

impl ValidationWarning {
    pub fn new(code: String, message: String, path: String) -> Self {
        Self {
            code,
            message,
            path,
            category: WarningCategory::General,
            recommendation: None,
        }
    }

    pub fn performance(code: String, message: String, path: String) -> Self {
        Self {
            code,
            message,
            path,
            category: WarningCategory::Performance,
            recommendation: None,
        }
    }

    pub fn security(code: String, message: String, path: String) -> Self {
        Self {
            code,
            message,
            path,
            category: WarningCategory::Security,
            recommendation: None,
        }
    }

    pub fn with_recommendation(mut self, recommendation: String) -> Self {
        self.recommendation = Some(recommendation);
        self
    }
}

impl fmt::Display for ValidationWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{:?}] {}: {} (at {})",
            self.category, self.code, self.message, self.path
        )
    }
}

/// Validation informational message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationInfo {
    /// Info code
    pub code: String,

    /// Info message
    pub message: String,

    /// Configuration path
    pub path: String,

    /// Info category
    pub category: InfoCategory,
}

impl ValidationInfo {
    pub fn new(code: String, message: String, path: String) -> Self {
        Self {
            code,
            message,
            path,
            category: InfoCategory::General,
        }
    }

    pub fn optimization(code: String, message: String, path: String) -> Self {
        Self {
            code,
            message,
            path,
            category: InfoCategory::Optimization,
        }
    }

    pub fn recommendation(code: String, message: String, path: String) -> Self {
        Self {
            code,
            message,
            path,
            category: InfoCategory::Recommendation,
        }
    }
}

impl fmt::Display for ValidationInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{:?}] {}: {} (at {})",
            self.category, self.code, self.message, self.path
        )
    }
}

/// Error severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorSeverity {
    /// Critical error that must be fixed
    Critical,
    /// Error that should be fixed
    Error,
    /// Minor error that can be ignored in some cases
    Minor,
}

impl fmt::Display for ErrorSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorSeverity::Critical => write!(f, "CRITICAL"),
            ErrorSeverity::Error => write!(f, "ERROR"),
            ErrorSeverity::Minor => write!(f, "MINOR"),
        }
    }
}

/// Warning categories
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WarningCategory {
    /// General warnings
    General,
    /// Performance-related warnings
    Performance,
    /// Security-related warnings
    Security,
    /// Compatibility warnings
    Compatibility,
    /// Configuration best practices
    BestPractice,
}

/// Information categories
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InfoCategory {
    /// General information
    General,
    /// Optimization suggestions
    Optimization,
    /// Configuration recommendations
    Recommendation,
    /// Usage statistics
    Statistics,
}

/// Validation rule trait for custom validation logic
#[async_trait]
pub trait ValidationRule: Send + Sync + std::fmt::Debug {
    /// Get the rule name
    fn name(&self) -> &str;

    /// Get the rule description
    fn description(&self) -> &str;

    /// Get the rule priority (higher = runs first)
    fn priority(&self) -> u32 {
        50
    }

    /// Check if the rule applies to the given configuration
    fn applies_to(&self, config: &EventFluxConfig) -> bool;

    /// Validate the configuration
    async fn validate(
        &self,
        config: &EventFluxConfig,
        report: &mut ValidationReport,
    ) -> ConfigResult<()>;

    /// Get rule category
    fn category(&self) -> ValidationRuleCategory {
        ValidationRuleCategory::General
    }
}

/// Validation rule categories
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationRuleCategory {
    /// Schema validation rules
    Schema,
    /// Business logic validation
    Business,
    /// Performance validation
    Performance,
    /// Security validation
    Security,
    /// Compatibility validation
    Compatibility,
    /// General validation
    General,
}

/// Comprehensive configuration validator
pub struct ConfigurationValidator {
    /// Registered validation rules
    rules: Vec<Arc<dyn ValidationRule>>,

    /// Validation configuration
    config: ValidationConfig,
}

/// Validation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationConfig {
    /// Enable strict validation (treat warnings as errors)
    pub strict_mode: bool,

    /// Maximum validation time before timeout
    pub timeout: std::time::Duration,

    /// Enable performance validation
    pub enable_performance_checks: bool,

    /// Enable security validation
    pub enable_security_checks: bool,

    /// Enable compatibility validation
    pub enable_compatibility_checks: bool,

    /// Fail fast on first error
    pub fail_fast: bool,

    /// Minimum validation score to pass (0.0 to 1.0)
    pub minimum_score: f64,

    /// Custom validation parameters
    pub custom_params: HashMap<String, String>,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            strict_mode: false,
            timeout: std::time::Duration::from_secs(30),
            enable_performance_checks: true,
            enable_security_checks: true,
            enable_compatibility_checks: true,
            fail_fast: false,
            minimum_score: 0.7,
            custom_params: HashMap::new(),
        }
    }
}

impl ValidationConfig {
    /// Create production validation configuration
    pub fn production() -> Self {
        Self {
            strict_mode: true,
            timeout: std::time::Duration::from_secs(60),
            enable_performance_checks: true,
            enable_security_checks: true,
            enable_compatibility_checks: true,
            fail_fast: false,
            minimum_score: 0.9,
            custom_params: HashMap::new(),
        }
    }

    /// Create development validation configuration
    pub fn development() -> Self {
        Self {
            strict_mode: false,
            timeout: std::time::Duration::from_secs(10),
            enable_performance_checks: false,
            enable_security_checks: false,
            enable_compatibility_checks: false,
            fail_fast: true,
            minimum_score: 0.5,
            custom_params: HashMap::new(),
        }
    }

    /// Create Kubernetes validation configuration
    pub fn kubernetes() -> Self {
        Self {
            strict_mode: true,
            timeout: std::time::Duration::from_secs(45),
            enable_performance_checks: true,
            enable_security_checks: true,
            enable_compatibility_checks: true,
            fail_fast: false,
            minimum_score: 0.8,
            custom_params: {
                let mut params = HashMap::new();
                params.insert("kubernetes_validation".to_string(), "true".to_string());
                params.insert("resource_limits_required".to_string(), "true".to_string());
                params
            },
        }
    }
}

impl ConfigurationValidator {
    /// Create a new validator
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            config: ValidationConfig::default(),
        }
    }

    /// Create a validator with custom configuration
    pub fn with_config(config: ValidationConfig) -> Self {
        Self {
            rules: Vec::new(),
            config,
        }
    }

    /// Add a validation rule
    pub fn add_rule(&mut self, rule: Arc<dyn ValidationRule>) {
        self.rules.push(rule);

        // Sort rules by priority (highest first)
        self.rules.sort_by_key(|b| std::cmp::Reverse(b.priority()));
    }

    /// Add multiple validation rules
    pub fn add_rules(&mut self, rules: Vec<Arc<dyn ValidationRule>>) {
        for rule in rules {
            self.add_rule(rule);
        }
    }

    /// Validate configuration
    pub async fn validate(&self, config: &EventFluxConfig) -> ConfigResult<ValidationReport> {
        let start_time = std::time::Instant::now();
        let mut report = ValidationReport::new();

        // Set up timeout
        let timeout_duration = self.config.timeout;

        // Run validation with timeout
        let validation_result = tokio::time::timeout(timeout_duration, async {
            self.run_validation(config, &mut report).await
        })
        .await;

        match validation_result {
            Ok(result) => {
                result?;
            }
            Err(_) => {
                report.add_error(ValidationError::critical(
                    "VALIDATION_TIMEOUT".to_string(),
                    format!("Validation exceeded timeout of {:?}", timeout_duration),
                    "global".to_string(),
                ));
            }
        }

        // Set validation duration
        let duration = start_time.elapsed();
        report.set_duration(duration);

        // Apply strict mode
        if self.config.strict_mode && !report.warnings.is_empty() {
            for warning in report.warnings.clone() {
                report.add_error(ValidationError::new(
                    warning.code,
                    format!("Strict mode: {}", warning.message),
                    warning.path,
                ));
            }
            report.warnings.clear();
        }

        // Check minimum score
        if report.validation_score < self.config.minimum_score {
            report.add_error(ValidationError::new(
                "VALIDATION_SCORE_TOO_LOW".to_string(),
                format!(
                    "Validation score {} is below minimum required score {}",
                    report.validation_score, self.config.minimum_score
                ),
                "global".to_string(),
            ));
        }

        // Add metadata
        report.add_metadata("rules_executed".to_string(), self.rules.len().to_string());
        report.add_metadata(
            "validation_duration_ms".to_string(),
            duration.as_millis().to_string(),
        );
        report.add_metadata(
            "strict_mode".to_string(),
            self.config.strict_mode.to_string(),
        );

        Ok(report)
    }

    /// Run the actual validation
    async fn run_validation(
        &self,
        config: &EventFluxConfig,
        report: &mut ValidationReport,
    ) -> ConfigResult<()> {
        let mut executed_rules = 0;

        for rule in &self.rules {
            // Check if rule applies
            if !rule.applies_to(config) {
                continue;
            }

            // Check category filters
            match rule.category() {
                ValidationRuleCategory::Performance if !self.config.enable_performance_checks => {
                    continue
                }
                ValidationRuleCategory::Security if !self.config.enable_security_checks => continue,
                ValidationRuleCategory::Compatibility
                    if !self.config.enable_compatibility_checks =>
                {
                    continue
                }
                _ => {}
            }

            // Run the validation rule
            if let Err(e) = rule.validate(config, report).await {
                report.add_error(ValidationError::new(
                    "RULE_EXECUTION_FAILED".to_string(),
                    format!("Rule '{}' failed to execute: {}", rule.name(), e),
                    "global".to_string(),
                ));
            }

            executed_rules += 1;

            // Fail fast if enabled and we have errors
            if self.config.fail_fast && !report.errors.is_empty() {
                break;
            }
        }

        report.add_metadata("executed_rules".to_string(), executed_rules.to_string());

        Ok(())
    }

    /// Get registered rules
    pub fn get_rules(&self) -> &[Arc<dyn ValidationRule>] {
        &self.rules
    }

    /// Get validation configuration
    pub fn get_config(&self) -> &ValidationConfig {
        &self.config
    }
}

impl Default for ConfigurationValidator {
    fn default() -> Self {
        Self::new()
    }
}

// Built-in validation rules

/// Schema validation rule
#[derive(Debug)]
pub struct SchemaValidationRule;

#[async_trait]
impl ValidationRule for SchemaValidationRule {
    fn name(&self) -> &str {
        "schema_validation"
    }

    fn description(&self) -> &str {
        "Validates configuration against the expected schema"
    }

    fn priority(&self) -> u32 {
        100 // High priority
    }

    fn applies_to(&self, _config: &EventFluxConfig) -> bool {
        true // Always applies
    }

    async fn validate(
        &self,
        config: &EventFluxConfig,
        report: &mut ValidationReport,
    ) -> ConfigResult<()> {
        // Validate basic schema requirements
        if config.eventflux.runtime.performance.thread_pool_size == 0 {
            report.add_error(
                ValidationError::new(
                    "INVALID_THREAD_POOL_SIZE".to_string(),
                    "Thread pool size must be greater than 0".to_string(),
                    "eventflux.runtime.performance.thread_pool_size".to_string(),
                )
                .with_suggestion(
                    "Set thread_pool_size to a positive integer (recommended: number of CPU cores)"
                        .to_string(),
                ),
            );
        }

        if config.eventflux.runtime.performance.event_buffer_size == 0 {
            report.add_error(
                ValidationError::new(
                    "INVALID_EVENT_BUFFER_SIZE".to_string(),
                    "Event buffer size must be greater than 0".to_string(),
                    "eventflux.runtime.performance.event_buffer_size".to_string(),
                )
                .with_suggestion(
                    "Set event_buffer_size to a positive integer (recommended: 1000-100000)"
                        .to_string(),
                ),
            );
        }

        Ok(())
    }

    fn category(&self) -> ValidationRuleCategory {
        ValidationRuleCategory::Schema
    }
}

/// Performance validation rule
#[derive(Debug)]
pub struct PerformanceValidationRule;

#[async_trait]
impl ValidationRule for PerformanceValidationRule {
    fn name(&self) -> &str {
        "performance_validation"
    }

    fn description(&self) -> &str {
        "Validates configuration for optimal performance"
    }

    fn priority(&self) -> u32 {
        60
    }

    fn applies_to(&self, _config: &EventFluxConfig) -> bool {
        true
    }

    async fn validate(
        &self,
        config: &EventFluxConfig,
        report: &mut ValidationReport,
    ) -> ConfigResult<()> {
        let thread_pool_size = config.eventflux.runtime.performance.thread_pool_size;
        let cpu_count = num_cpus::get();

        // Check thread pool size vs CPU count
        if thread_pool_size > cpu_count * 4 {
            report.add_warning(
                ValidationWarning::performance(
                    "EXCESSIVE_THREAD_POOL_SIZE".to_string(),
                    format!(
                        "Thread pool size {} is much larger than available CPUs ({})",
                        thread_pool_size, cpu_count
                    ),
                    "eventflux.runtime.performance.thread_pool_size".to_string(),
                )
                .with_recommendation(
                    "Consider reducing thread pool size to 2-4x CPU count for optimal performance"
                        .to_string(),
                ),
            );
        }

        if thread_pool_size == 1 && cpu_count > 1 {
            report.add_warning(ValidationWarning::performance(
                "UNDERUTILIZED_CPU".to_string(),
                format!("Single thread pool size on {} CPU system may underutilize resources", cpu_count),
                "eventflux.runtime.performance.thread_pool_size".to_string(),
            ).with_recommendation("Consider increasing thread pool size to match CPU count for better parallelism".to_string()));
        }

        // Check event buffer size
        let buffer_size = config.eventflux.runtime.performance.event_buffer_size;
        if buffer_size < 1000 {
            report.add_warning(
                ValidationWarning::performance(
                    "SMALL_EVENT_BUFFER".to_string(),
                    format!(
                        "Event buffer size {} may be too small for high throughput",
                        buffer_size
                    ),
                    "eventflux.runtime.performance.event_buffer_size".to_string(),
                )
                .with_recommendation(
                    "Consider increasing event_buffer_size to at least 10000 for better throughput"
                        .to_string(),
                ),
            );
        }

        if buffer_size > 1000000 {
            report.add_warning(
                ValidationWarning::performance(
                    "LARGE_EVENT_BUFFER".to_string(),
                    format!(
                        "Event buffer size {} may consume excessive memory",
                        buffer_size
                    ),
                    "eventflux.runtime.performance.event_buffer_size".to_string(),
                )
                .with_recommendation(
                    "Consider reducing event_buffer_size to balance memory usage and performance"
                        .to_string(),
                ),
            );
        }

        // Check batch processing configuration
        if config.eventflux.runtime.performance.batch_processing {
            if let Some(batch_size) = config.eventflux.runtime.performance.batch_size {
                if batch_size > 10000 {
                    report.add_warning(ValidationWarning::performance(
                        "LARGE_BATCH_SIZE".to_string(),
                        format!("Batch size {} may cause latency issues", batch_size),
                        "eventflux.runtime.performance.batch_size".to_string(),
                    ).with_recommendation("Consider reducing batch size to 1000-5000 for balanced throughput and latency".to_string()));
                }
            }
        }

        Ok(())
    }

    fn category(&self) -> ValidationRuleCategory {
        ValidationRuleCategory::Performance
    }
}

/// Security validation rule
#[derive(Debug)]
pub struct SecurityValidationRule;

#[async_trait]
impl ValidationRule for SecurityValidationRule {
    fn name(&self) -> &str {
        "security_validation"
    }

    fn description(&self) -> &str {
        "Validates configuration for security best practices"
    }

    fn priority(&self) -> u32 {
        80
    }

    fn applies_to(&self, _config: &EventFluxConfig) -> bool {
        true
    }

    async fn validate(
        &self,
        config: &EventFluxConfig,
        report: &mut ValidationReport,
    ) -> ConfigResult<()> {
        // Check if security configuration is present in distributed mode
        if matches!(
            config.eventflux.runtime.mode,
            crate::core::config::types::RuntimeMode::Distributed
        ) && config.eventflux.security.is_none()
        {
            report.add_error(ValidationError::new(
                    "MISSING_SECURITY_CONFIG".to_string(),
                    "Security configuration is required in distributed mode".to_string(),
                    "eventflux.security".to_string(),
                ).with_suggestion("Add security configuration with appropriate authentication and authorization settings".to_string()));
        }

        // Check for monitoring configuration in production
        if config.eventflux.monitoring.is_none() {
            report.add_warning(
                ValidationWarning::security(
                    "NO_MONITORING_CONFIG".to_string(),
                    "No monitoring configuration found - this reduces security observability"
                        .to_string(),
                    "eventflux.monitoring".to_string(),
                )
                .with_recommendation(
                    "Add monitoring configuration for better security visibility".to_string(),
                ),
            );
        }

        Ok(())
    }

    fn category(&self) -> ValidationRuleCategory {
        ValidationRuleCategory::Security
    }
}

/// Kubernetes validation rule
#[derive(Debug)]
pub struct KubernetesValidationRule;

#[async_trait]
impl ValidationRule for KubernetesValidationRule {
    fn name(&self) -> &str {
        "kubernetes_validation"
    }

    fn description(&self) -> &str {
        "Validates configuration for Kubernetes deployment"
    }

    fn priority(&self) -> u32 {
        70
    }

    fn applies_to(&self, _config: &EventFluxConfig) -> bool {
        // Check if running in Kubernetes environment
        std::env::var("KUBERNETES_SERVICE_HOST").is_ok()
    }

    async fn validate(
        &self,
        config: &EventFluxConfig,
        report: &mut ValidationReport,
    ) -> ConfigResult<()> {
        // Check for required resources configuration
        if config.eventflux.runtime.resources.is_none() {
            report.add_warning(
                ValidationWarning::new(
                    "NO_RESOURCE_LIMITS".to_string(),
                    "No resource limits configured for Kubernetes deployment".to_string(),
                    "eventflux.runtime.resources".to_string(),
                )
                .with_recommendation(
                    "Add resource limits for memory and CPU to prevent resource starvation"
                        .to_string(),
                ),
            );
        }

        // Check monitoring endpoint configuration
        if let Some(monitoring) = &config.eventflux.monitoring {
            if monitoring.endpoints.health_endpoint != "/healthz" {
                report.add_info(ValidationInfo::recommendation(
                    "K8S_HEALTH_ENDPOINT".to_string(),
                    "Consider using /healthz for health checks in Kubernetes".to_string(),
                    "eventflux.monitoring.endpoints.health_endpoint".to_string(),
                ));
            }
        }

        Ok(())
    }

    fn category(&self) -> ValidationRuleCategory {
        ValidationRuleCategory::Compatibility
    }
}

/// Create a default validator with built-in rules
pub fn create_default_validator() -> ConfigurationValidator {
    let mut validator = ConfigurationValidator::new();

    // Add built-in rules
    validator.add_rule(Arc::new(SchemaValidationRule));
    validator.add_rule(Arc::new(PerformanceValidationRule));
    validator.add_rule(Arc::new(SecurityValidationRule));
    validator.add_rule(Arc::new(KubernetesValidationRule));

    validator
}

/// Create a production validator with strict settings
pub fn create_production_validator() -> ConfigurationValidator {
    let mut validator = ConfigurationValidator::with_config(ValidationConfig::production());

    // Add built-in rules
    validator.add_rule(Arc::new(SchemaValidationRule));
    validator.add_rule(Arc::new(PerformanceValidationRule));
    validator.add_rule(Arc::new(SecurityValidationRule));
    validator.add_rule(Arc::new(KubernetesValidationRule));

    validator
}

/// Create a development validator with lenient settings
pub fn create_development_validator() -> ConfigurationValidator {
    let mut validator = ConfigurationValidator::with_config(ValidationConfig::development());

    // Add basic rules only
    validator.add_rule(Arc::new(SchemaValidationRule));

    validator
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_validation_report_creation() {
        let mut report = ValidationReport::new();

        assert!(report.is_valid);
        assert_eq!(report.errors.len(), 0);
        assert_eq!(report.warnings.len(), 0);
        assert_eq!(report.info.len(), 0);
        assert_eq!(report.validation_score, 1.0);

        // Add an error
        report.add_error(ValidationError::new(
            "TEST_ERROR".to_string(),
            "Test error message".to_string(),
            "test.path".to_string(),
        ));

        assert!(!report.is_valid);
        assert_eq!(report.errors.len(), 1);
        assert!(report.validation_score < 1.0);
    }

    #[tokio::test]
    async fn test_validation_error_creation() {
        let error = ValidationError::new(
            "INVALID_CONFIG".to_string(),
            "Configuration value is invalid".to_string(),
            "eventflux.runtime.mode".to_string(),
        )
        .with_suggestion("Use 'SingleNode' or 'Distributed'".to_string())
        .with_context("current_value".to_string(), "Invalid".to_string());

        assert_eq!(error.code, "INVALID_CONFIG");
        assert_eq!(error.message, "Configuration value is invalid");
        assert_eq!(error.path, "eventflux.runtime.mode");
        assert_eq!(
            error.suggestion,
            Some("Use 'SingleNode' or 'Distributed'".to_string())
        );
        assert_eq!(
            error.context.get("current_value"),
            Some(&"Invalid".to_string())
        );
    }

    #[tokio::test]
    async fn test_schema_validation_rule() {
        let rule = SchemaValidationRule;
        let mut config = EventFluxConfig::default();
        let mut report = ValidationReport::new();

        // Test with invalid thread pool size
        config.eventflux.runtime.performance.thread_pool_size = 0;

        rule.validate(&config, &mut report).await.unwrap();

        assert!(!report.is_valid);
        assert_eq!(report.errors.len(), 1);
        assert_eq!(report.errors[0].code, "INVALID_THREAD_POOL_SIZE");
    }

    #[tokio::test]
    async fn test_performance_validation_rule() {
        let rule = PerformanceValidationRule;
        let mut config = EventFluxConfig::default();
        let mut report = ValidationReport::new();

        // Set extreme values to trigger warnings
        config.eventflux.runtime.performance.thread_pool_size = 1000; // Excessive
        config.eventflux.runtime.performance.event_buffer_size = 100; // Too small

        rule.validate(&config, &mut report).await.unwrap();

        assert!(report.is_valid); // Warnings don't invalidate
        assert!(!report.warnings.is_empty()); // Should have warnings
    }

    #[tokio::test]
    async fn test_security_validation_rule() {
        let rule = SecurityValidationRule;
        let mut config = EventFluxConfig::default();
        let mut report = ValidationReport::new();

        // Set distributed mode without security config
        config.eventflux.runtime.mode = crate::core::config::types::RuntimeMode::Distributed;
        config.eventflux.security = None;

        rule.validate(&config, &mut report).await.unwrap();

        assert!(!report.is_valid);
        assert!(report
            .errors
            .iter()
            .any(|e| e.code == "MISSING_SECURITY_CONFIG"));
    }

    #[tokio::test]
    async fn test_configuration_validator() {
        let mut validator = ConfigurationValidator::new();
        validator.add_rule(Arc::new(SchemaValidationRule));
        validator.add_rule(Arc::new(PerformanceValidationRule));

        let mut config = EventFluxConfig::default();
        config.eventflux.runtime.performance.thread_pool_size = 0; // Invalid

        let report = validator.validate(&config).await.unwrap();

        assert!(!report.is_valid);
        assert!(!report.errors.is_empty());
        // Validation duration should be set (could be 0 for very fast validation)
        // validation_duration is always >= 0 for Duration
        let _ = report.validation_duration;
        assert!(!report.summary().contains("✅"));
    }

    #[test]
    fn test_validation_config_presets() {
        let dev_config = ValidationConfig::development();
        assert!(!dev_config.strict_mode);
        assert!(dev_config.fail_fast);
        assert_eq!(dev_config.minimum_score, 0.5);

        let prod_config = ValidationConfig::production();
        assert!(prod_config.strict_mode);
        assert!(!prod_config.fail_fast);
        assert_eq!(prod_config.minimum_score, 0.9);

        let k8s_config = ValidationConfig::kubernetes();
        assert!(k8s_config.enable_security_checks);
        assert!(k8s_config
            .custom_params
            .contains_key("kubernetes_validation"));
    }

    #[tokio::test]
    async fn test_default_validator_creation() {
        let validator = create_default_validator();
        assert_eq!(validator.get_rules().len(), 4);

        let config = EventFluxConfig::default();
        let report = validator.validate(&config).await.unwrap();

        // Default config should be valid
        assert!(report.is_valid);
    }

    #[tokio::test]
    async fn test_production_validator() {
        let validator = create_production_validator();
        assert!(validator.get_config().strict_mode);

        let mut config = EventFluxConfig::default();
        config.eventflux.runtime.performance.thread_pool_size = 1000; // Will cause warnings

        let report = validator.validate(&config).await.unwrap();

        // In strict mode, warnings become errors
        assert!(!report.is_valid);
    }

    #[tokio::test]
    async fn test_validation_report_summary() {
        let mut report = ValidationReport::new();
        assert!(report.summary().contains("✅"));

        report.add_warning(ValidationWarning::new(
            "TEST_WARNING".to_string(),
            "Test warning".to_string(),
            "test.path".to_string(),
        ));
        assert!(report.summary().contains("✅"));
        assert!(report.summary().contains("warning"));

        report.add_error(ValidationError::new(
            "TEST_ERROR".to_string(),
            "Test error".to_string(),
            "test.path".to_string(),
        ));
        assert!(report.summary().contains("❌"));
        assert!(report.summary().contains("error"));
    }
}
