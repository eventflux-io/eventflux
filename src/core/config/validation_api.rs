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
//! Checks an EventFlux configuration against a set of rules and returns a report of
//! errors, warnings and notes with a score. Comes with built in rules for schema,
//! performance, security and Kubernetes, and accepts custom rules.

use crate::core::config::{ConfigResult, EventFluxConfig};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Result of a validation run
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationReport {
    /// False once any error is added
    pub is_valid: bool,

    /// Problems that make the configuration invalid
    pub errors: Vec<ValidationError>,

    /// Problems that are reported but do not make it invalid
    pub warnings: Vec<ValidationWarning>,

    /// Notes and suggestions. They do not affect the score.
    pub info: Vec<ValidationInfo>,

    /// Score from 0.0 to 1.0. See `recalculate_score`.
    pub validation_score: f64,

    /// Extra details about the run, such as rule counts and duration
    pub metadata: HashMap<String, String>,

    /// How long the run took
    pub validation_duration: std::time::Duration,

    /// When the report was created, in UTC
    pub validated_at: chrono::DateTime<chrono::Utc>,
}

impl ValidationReport {
    /// Create an empty, valid report with a score of 1.0
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

    /// Add an error. Marks the report invalid and lowers the score.
    pub fn add_error(&mut self, error: ValidationError) {
        self.is_valid = false;
        self.errors.push(error);
        self.recalculate_score();
    }

    /// Add a warning. Lowers the score but keeps the report valid.
    pub fn add_warning(&mut self, warning: ValidationWarning) {
        self.warnings.push(warning);
        self.recalculate_score();
    }

    /// Add a note. Does not change the score.
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

    /// Work out the score from the errors and warnings.
    ///
    /// Each error weighs 2 and each warning 1. The score is `1 - w / (w + 10)` for the
    /// total weight `w`, so it is 1.0 with no issues and falls toward 0.0 as they add up.
    fn recalculate_score(&mut self) {
        let total_issues = self.errors.len() + self.warnings.len();
        if total_issues == 0 {
            self.validation_score = 1.0;
        } else {
            let error_weight = 2.0;
            let warning_weight = 1.0;
            let total_weight = (self.errors.len() as f64 * error_weight)
                + (self.warnings.len() as f64 * warning_weight);

            self.validation_score = (1.0 - (total_weight / (total_weight + 10.0))).max(0.0);
        }
    }

    /// Number of errors plus warnings. Notes are not counted.
    pub fn total_issues(&self) -> usize {
        self.errors.len() + self.warnings.len()
    }

    /// One line summary with the counts, for people to read
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

/// One validation error with its context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    /// Stable error code that code can match on
    pub code: String,

    /// Error message for people to read
    pub message: String,

    /// Configuration path where the error was found
    pub path: String,

    /// How serious the error is
    pub severity: ErrorSeverity,

    /// Suggested fix
    pub suggestion: Option<String>,

    /// Extra key and value details
    pub context: HashMap<String, String>,
}

impl ValidationError {
    /// Create an error with `Error` severity
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

    /// Create an error with `Critical` severity
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

/// One validation warning. Strict mode turns warnings into errors.
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

/// One validation note, such as a suggestion
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
    /// Performance warnings
    Performance,
    /// Security warnings
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

/// A validation check that can be added to [`ConfigurationValidator`]
#[async_trait]
pub trait ValidationRule: Send + Sync + std::fmt::Debug {
    /// Get the rule name
    fn name(&self) -> &str;

    /// Get the rule description
    fn description(&self) -> &str;

    /// Rule priority. A higher number runs first. Defaults to 50.
    fn priority(&self) -> u32 {
        50
    }

    /// Check if the rule should run for this configuration
    fn applies_to(&self, config: &EventFluxConfig) -> bool;

    /// Check the configuration and add any findings to `report`.
    ///
    /// An `Err` is recorded in the report as a `RULE_EXECUTION_FAILED` error and the
    /// other rules still run.
    async fn validate(
        &self,
        config: &EventFluxConfig,
        report: &mut ValidationReport,
    ) -> ConfigResult<()>;

    /// Rule category, used to switch groups of rules off. Defaults to `General`.
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

/// Runs a set of validation rules over a configuration
pub struct ConfigurationValidator {
    /// Registered rules, highest priority first
    rules: Vec<Arc<dyn ValidationRule>>,

    /// Validation configuration
    config: ValidationConfig,
}

/// Settings for a validation run
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationConfig {
    /// Treat warnings as errors. Defaults to false.
    pub strict_mode: bool,

    /// Longest the whole run may take. Defaults to 30 seconds.
    pub timeout: std::time::Duration,

    /// Run performance rules. Defaults to true.
    pub enable_performance_checks: bool,

    /// Run security rules. Defaults to true.
    pub enable_security_checks: bool,

    /// Run compatibility rules. Defaults to true.
    pub enable_compatibility_checks: bool,

    /// Stop after the first rule that leaves an error. Defaults to false.
    pub fail_fast: bool,

    /// Lowest score that passes, from 0.0 to 1.0. Defaults to 0.7.
    pub minimum_score: f64,

    /// Free form parameters for rules. Not read by the built in rules.
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
    /// Preset for production. Strict, 60 second timeout, minimum score 0.9.
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

    /// Preset for development. Not strict, fails fast, skips performance, security
    /// and compatibility rules, 10 second timeout, minimum score 0.5.
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

    /// Preset for Kubernetes. Strict, 45 second timeout, minimum score 0.8, with the
    /// `kubernetes_validation` and `resource_limits_required` parameters set.
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
    /// Create a validator with no rules and the default settings
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            config: ValidationConfig::default(),
        }
    }

    /// Create a validator with no rules and the given settings
    pub fn with_config(config: ValidationConfig) -> Self {
        Self {
            rules: Vec::new(),
            config,
        }
    }

    /// Add a rule and keep the list sorted by priority
    pub fn add_rule(&mut self, rule: Arc<dyn ValidationRule>) {
        self.rules.push(rule);

        // Highest priority first
        self.rules.sort_by_key(|b| std::cmp::Reverse(b.priority()));
    }

    /// Add multiple validation rules
    pub fn add_rules(&mut self, rules: Vec<Arc<dyn ValidationRule>>) {
        for rule in rules {
            self.add_rule(rule);
        }
    }

    /// Run every rule that applies and return the report.
    ///
    /// When the run times out, a critical `VALIDATION_TIMEOUT` error is added and the
    /// findings so far are kept. In strict mode each warning then becomes an error. A
    /// score below the minimum adds a `VALIDATION_SCORE_TOO_LOW` error. Problems are
    /// reported in the report, not as an `Err`.
    pub async fn validate(&self, config: &EventFluxConfig) -> ConfigResult<ValidationReport> {
        let start_time = std::time::Instant::now();
        let mut report = ValidationReport::new();

        let timeout_duration = self.config.timeout;

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

        let duration = start_time.elapsed();
        report.set_duration(duration);

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

        // The score already counts the warnings turned into errors above
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

        // "rules_executed" is the number of registered rules. The number that actually
        // ran is in "executed_rules".
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

    /// Run the rules in priority order, skipping rules that do not apply or whose
    /// category is switched off
    async fn run_validation(
        &self,
        config: &EventFluxConfig,
        report: &mut ValidationReport,
    ) -> ConfigResult<()> {
        let mut executed_rules = 0;

        for rule in &self.rules {
            if !rule.applies_to(config) {
                continue;
            }

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

            // A rule that fails is reported, and the other rules still run
            if let Err(e) = rule.validate(config, report).await {
                report.add_error(ValidationError::new(
                    "RULE_EXECUTION_FAILED".to_string(),
                    format!("Rule '{}' failed to execute: {}", rule.name(), e),
                    "global".to_string(),
                ));
            }

            executed_rules += 1;

            if self.config.fail_fast && !report.errors.is_empty() {
                break;
            }
        }

        report.add_metadata("executed_rules".to_string(), executed_rules.to_string());

        Ok(())
    }

    /// Get the registered rules, highest priority first
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

// Built in validation rules

/// Basic schema rule. Errors when the thread pool size or event buffer size is 0.
/// Priority 100, so it runs first.
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
        100 // Highest of the built in rules
    }

    fn applies_to(&self, _config: &EventFluxConfig) -> bool {
        true // Always applies
    }

    async fn validate(
        &self,
        config: &EventFluxConfig,
        report: &mut ValidationReport,
    ) -> ConfigResult<()> {
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

/// Performance rule. Warns when the thread pool is over 4 times the CPU count or is 1
/// on a machine with more CPUs, when the event buffer is under 1000 or over 1,000,000,
/// and when batching is on with a batch size over 10,000.
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

/// Security rule. Errors when the mode is `Distributed` with no security section, and
/// warns whenever there is no monitoring section. Hybrid mode is not checked.
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

        // Checked in every environment, not only production
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

/// Kubernetes rule. Runs only inside a pod. Warns when no resource limits are set and
/// suggests `/healthz` as the health endpoint. Its category is `Compatibility`.
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
        // Kubernetes sets this variable in every pod
        std::env::var("KUBERNETES_SERVICE_HOST").is_ok()
    }

    async fn validate(
        &self,
        config: &EventFluxConfig,
        report: &mut ValidationReport,
    ) -> ConfigResult<()> {
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

/// Create a validator with the default settings and all four built in rules
pub fn create_default_validator() -> ConfigurationValidator {
    let mut validator = ConfigurationValidator::new();

    validator.add_rule(Arc::new(SchemaValidationRule));
    validator.add_rule(Arc::new(PerformanceValidationRule));
    validator.add_rule(Arc::new(SecurityValidationRule));
    validator.add_rule(Arc::new(KubernetesValidationRule));

    validator
}

/// Create a validator with [`ValidationConfig::production`] and all four built in rules
pub fn create_production_validator() -> ConfigurationValidator {
    let mut validator = ConfigurationValidator::with_config(ValidationConfig::production());

    validator.add_rule(Arc::new(SchemaValidationRule));
    validator.add_rule(Arc::new(PerformanceValidationRule));
    validator.add_rule(Arc::new(SecurityValidationRule));
    validator.add_rule(Arc::new(KubernetesValidationRule));

    validator
}

/// Create a validator with [`ValidationConfig::development`] and only the schema rule
pub fn create_development_validator() -> ConfigurationValidator {
    let mut validator = ConfigurationValidator::with_config(ValidationConfig::development());

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
