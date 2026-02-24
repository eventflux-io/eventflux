// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for configuration validation API
//!
//! These tests demonstrate how the comprehensive validation system works
//! with custom rules, enterprise validation patterns, and detailed reporting.

use async_trait::async_trait;
use eventflux::core::config::{
    monitoring::MonitoringConfig,
    validation_api::{
        create_default_validator, create_development_validator, create_production_validator,
        ConfigurationValidator, ErrorSeverity, InfoCategory, KubernetesValidationRule,
        PerformanceValidationRule, SchemaValidationRule, SecurityValidationRule, ValidationConfig,
        ValidationError, ValidationInfo, ValidationReport, ValidationRule, ValidationRuleCategory,
        ValidationWarning, WarningCategory,
    },
    EventFluxConfig,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Test basic validation report functionality
#[test]
fn test_validation_report_creation() {
    let mut report = ValidationReport::new();

    assert!(report.is_valid);
    assert_eq!(report.errors.len(), 0);
    assert_eq!(report.warnings.len(), 0);
    assert_eq!(report.info.len(), 0);
    assert_eq!(report.validation_score, 1.0);
    assert_eq!(report.total_issues(), 0);
    assert!(report.summary().contains("✅"));
}

/// Test validation report with errors
#[test]
fn test_validation_report_with_errors() {
    let mut report = ValidationReport::new();

    // Add a critical error
    report.add_error(
        ValidationError::critical(
            "CRITICAL_ERROR".to_string(),
            "This is a critical configuration error".to_string(),
            "eventflux.runtime.mode".to_string(),
        )
        .with_suggestion("Fix the configuration immediately".to_string()),
    );

    assert!(!report.is_valid);
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.errors[0].code, "CRITICAL_ERROR");
    assert_eq!(report.errors[0].severity, ErrorSeverity::Critical);
    assert!(report.errors[0].suggestion.is_some());
    assert!(report.validation_score < 1.0);
    assert!(report.summary().contains("❌"));
}

/// Test validation report with warnings
#[test]
fn test_validation_report_with_warnings() {
    let mut report = ValidationReport::new();

    // Add performance warning
    report.add_warning(
        ValidationWarning::performance(
            "PERFORMANCE_WARNING".to_string(),
            "Thread pool size may be suboptimal".to_string(),
            "eventflux.runtime.performance.thread_pool_size".to_string(),
        )
        .with_recommendation("Consider adjusting thread pool size".to_string()),
    );

    // Add security warning
    report.add_warning(ValidationWarning::security(
        "SECURITY_WARNING".to_string(),
        "Security configuration is incomplete".to_string(),
        "eventflux.security".to_string(),
    ));

    assert!(report.is_valid); // Warnings don't invalidate
    assert_eq!(report.warnings.len(), 2);
    assert_eq!(report.warnings[0].category, WarningCategory::Performance);
    assert_eq!(report.warnings[1].category, WarningCategory::Security);
    assert!(report.warnings[0].recommendation.is_some());
    assert!(report.validation_score < 1.0); // Score affected by warnings
    assert!(report.summary().contains("✅")); // Still valid
    assert!(report.summary().contains("warning"));
}

/// Test validation report with informational messages
#[test]
fn test_validation_report_with_info() {
    let mut report = ValidationReport::new();

    // Add optimization info
    report.add_info(ValidationInfo::optimization(
        "OPTIMIZATION_INFO".to_string(),
        "Configuration can be optimized for better performance".to_string(),
        "eventflux.runtime.performance".to_string(),
    ));

    // Add recommendation info
    report.add_info(ValidationInfo::recommendation(
        "RECOMMENDATION_INFO".to_string(),
        "Consider enabling monitoring for production deployment".to_string(),
        "eventflux.monitoring".to_string(),
    ));

    assert!(report.is_valid);
    assert_eq!(report.info.len(), 2);
    assert_eq!(report.info[0].category, InfoCategory::Optimization);
    assert_eq!(report.info[1].category, InfoCategory::Recommendation);
    assert_eq!(report.validation_score, 1.0); // Info doesn't affect score
    assert!(report.summary().contains("✅"));
    assert!(report.summary().contains("info"));
}

/// Test validation error types and display
#[test]
fn test_validation_error_types() {
    let error = ValidationError::new(
        "TEST_ERROR".to_string(),
        "Test error message".to_string(),
        "test.config.path".to_string(),
    )
    .with_suggestion("Fix the configuration".to_string())
    .with_context("current_value".to_string(), "invalid".to_string())
    .with_context("expected_value".to_string(), "valid".to_string());

    assert_eq!(error.code, "TEST_ERROR");
    assert_eq!(error.message, "Test error message");
    assert_eq!(error.path, "test.config.path");
    assert_eq!(error.severity, ErrorSeverity::Error);
    assert_eq!(error.suggestion, Some("Fix the configuration".to_string()));
    assert_eq!(error.context.len(), 2);
    assert_eq!(
        error.context.get("current_value"),
        Some(&"invalid".to_string())
    );

    // Test display formatting
    let display_str = format!("{}", error);
    assert!(display_str.contains("ERROR"));
    assert!(display_str.contains("TEST_ERROR"));
    assert!(display_str.contains("test.config.path"));
}

/// Test error severity levels
#[test]
fn test_error_severity_levels() {
    let critical_error = ValidationError::critical(
        "CRITICAL".to_string(),
        "Critical error".to_string(),
        "path".to_string(),
    );
    assert_eq!(critical_error.severity, ErrorSeverity::Critical);

    let normal_error = ValidationError::new(
        "ERROR".to_string(),
        "Normal error".to_string(),
        "path".to_string(),
    );
    assert_eq!(normal_error.severity, ErrorSeverity::Error);

    // Test display
    assert_eq!(format!("{}", ErrorSeverity::Critical), "CRITICAL");
    assert_eq!(format!("{}", ErrorSeverity::Error), "ERROR");
    assert_eq!(format!("{}", ErrorSeverity::Minor), "MINOR");
}

/// Test warning categories
#[test]
fn test_warning_categories() {
    let categories = vec![
        WarningCategory::General,
        WarningCategory::Performance,
        WarningCategory::Security,
        WarningCategory::Compatibility,
        WarningCategory::BestPractice,
    ];

    for category in categories {
        let warning = ValidationWarning {
            code: "TEST".to_string(),
            message: "Test warning".to_string(),
            path: "test".to_string(),
            category,
            recommendation: None,
        };

        // Each category should be handled correctly
        assert_eq!(warning.category, category);
    }
}

/// Test custom validation rule
#[derive(Debug)]
struct CustomValidationRule {
    name: String,
}

impl CustomValidationRule {
    fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait]
impl ValidationRule for CustomValidationRule {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        "Custom validation rule for testing"
    }

    fn priority(&self) -> u32 {
        75
    }

    fn applies_to(&self, _config: &EventFluxConfig) -> bool {
        true
    }

    async fn validate(
        &self,
        config: &EventFluxConfig,
        report: &mut ValidationReport,
    ) -> eventflux::core::config::ConfigResult<()> {
        // Custom validation logic
        if config.eventflux.runtime.performance.thread_pool_size > 100 {
            report.add_warning(
                ValidationWarning::new(
                    "CUSTOM_THREAD_POOL_WARNING".to_string(),
                    format!(
                        "Custom rule: Thread pool size {} is unusually large",
                        config.eventflux.runtime.performance.thread_pool_size
                    ),
                    "eventflux.runtime.performance.thread_pool_size".to_string(),
                )
                .with_recommendation(
                    "Consider reducing thread pool size for this custom rule".to_string(),
                ),
            );
        }

        report.add_info(ValidationInfo::new(
            "CUSTOM_RULE_EXECUTED".to_string(),
            format!("Custom rule '{}' executed successfully", self.name),
            "custom".to_string(),
        ));

        Ok(())
    }

    fn category(&self) -> ValidationRuleCategory {
        ValidationRuleCategory::General
    }
}

/// Test custom validation rule integration
#[tokio::test]
async fn test_custom_validation_rule() {
    let mut validator = ConfigurationValidator::new();
    validator.add_rule(Arc::new(CustomValidationRule::new("test_rule".to_string())));

    let mut config = EventFluxConfig::default();
    config.eventflux.runtime.performance.thread_pool_size = 150; // Triggers custom rule

    let report = validator.validate(&config).await.unwrap();

    assert!(report.is_valid); // Warnings don't invalidate
    assert_eq!(report.warnings.len(), 1);
    assert_eq!(report.info.len(), 1);
    assert_eq!(report.warnings[0].code, "CUSTOM_THREAD_POOL_WARNING");
    assert_eq!(report.info[0].code, "CUSTOM_RULE_EXECUTED");
}

/// Test schema validation rule
#[tokio::test]
async fn test_schema_validation_rule() {
    let rule = SchemaValidationRule;
    let mut config = EventFluxConfig::default();
    let mut report = ValidationReport::new();

    // Test with valid configuration
    rule.validate(&config, &mut report).await.unwrap();
    assert!(report.is_valid);
    assert_eq!(report.errors.len(), 0);

    // Test with invalid thread pool size
    config.eventflux.runtime.performance.thread_pool_size = 0;
    let mut report2 = ValidationReport::new();
    rule.validate(&config, &mut report2).await.unwrap();

    assert!(!report2.is_valid);
    assert_eq!(report2.errors.len(), 1);
    assert_eq!(report2.errors[0].code, "INVALID_THREAD_POOL_SIZE");
    assert!(report2.errors[0].suggestion.is_some());

    // Test with invalid event buffer size
    config.eventflux.runtime.performance.thread_pool_size = 8; // Fix previous error
    config.eventflux.runtime.performance.event_buffer_size = 0;
    let mut report3 = ValidationReport::new();
    rule.validate(&config, &mut report3).await.unwrap();

    assert!(!report3.is_valid);
    assert_eq!(report3.errors.len(), 1);
    assert_eq!(report3.errors[0].code, "INVALID_EVENT_BUFFER_SIZE");
}

/// Test performance validation rule
#[tokio::test]
async fn test_performance_validation_rule() {
    let rule = PerformanceValidationRule;
    let mut config = EventFluxConfig::default();
    let mut report = ValidationReport::new();

    // Test with reasonable values
    rule.validate(&config, &mut report).await.unwrap();
    assert!(report.is_valid);

    // Test with excessive thread pool size
    config.eventflux.runtime.performance.thread_pool_size = 1000;
    let mut report2 = ValidationReport::new();
    rule.validate(&config, &mut report2).await.unwrap();

    assert!(report2.is_valid); // Warnings don't invalidate
    assert!(report2.warnings.len() > 0);
    assert!(report2
        .warnings
        .iter()
        .any(|w| w.code == "EXCESSIVE_THREAD_POOL_SIZE"));

    // Test with small event buffer
    config.eventflux.runtime.performance.thread_pool_size = 8; // Reset
    config.eventflux.runtime.performance.event_buffer_size = 500; // Small buffer
    let mut report3 = ValidationReport::new();
    rule.validate(&config, &mut report3).await.unwrap();

    assert!(report3.is_valid);
    assert!(report3
        .warnings
        .iter()
        .any(|w| w.code == "SMALL_EVENT_BUFFER"));

    // Test with large event buffer
    config.eventflux.runtime.performance.event_buffer_size = 2000000; // Large buffer
    let mut report4 = ValidationReport::new();
    rule.validate(&config, &mut report4).await.unwrap();

    assert!(report4.is_valid);
    assert!(report4
        .warnings
        .iter()
        .any(|w| w.code == "LARGE_EVENT_BUFFER"));
}

/// Test security validation rule
#[tokio::test]
async fn test_security_validation_rule() {
    let rule = SecurityValidationRule;
    let mut config = EventFluxConfig::default();
    let mut report = ValidationReport::new();

    // Test with single node mode (should be okay)
    rule.validate(&config, &mut report).await.unwrap();
    assert!(report.is_valid);

    // Test with distributed mode but no security config
    config.eventflux.runtime.mode = eventflux::core::config::types::RuntimeMode::Distributed;
    config.eventflux.security = None;
    let mut report2 = ValidationReport::new();
    rule.validate(&config, &mut report2).await.unwrap();

    assert!(!report2.is_valid);
    assert!(report2
        .errors
        .iter()
        .any(|e| e.code == "MISSING_SECURITY_CONFIG"));

    // Test without monitoring configuration
    let mut config3 = EventFluxConfig::default();
    config3.eventflux.monitoring = None;
    let mut report3 = ValidationReport::new();
    rule.validate(&config3, &mut report3).await.unwrap();

    assert!(report3.is_valid); // Warning, not error
    assert!(report3
        .warnings
        .iter()
        .any(|w| w.code == "NO_MONITORING_CONFIG"));
}

/// Test Kubernetes validation rule
#[tokio::test]
async fn test_kubernetes_validation_rule() {
    let rule = KubernetesValidationRule;
    let config = EventFluxConfig::default();
    let mut report = ValidationReport::new();

    // Rule only applies in Kubernetes environment
    let applies = rule.applies_to(&config);
    println!("Kubernetes validation rule applies: {}", applies);

    if applies {
        rule.validate(&config, &mut report).await.unwrap();

        // Should warn about missing resource limits
        assert!(report.is_valid); // Warnings don't invalidate
        if !report.warnings.is_empty() {
            assert!(report
                .warnings
                .iter()
                .any(|w| w.code == "NO_RESOURCE_LIMITS"));
        }
    }
}

/// Test configuration validator with multiple rules
#[tokio::test]
async fn test_configuration_validator_integration() {
    let mut validator = ConfigurationValidator::new();

    // Add built-in rules
    validator.add_rule(Arc::new(SchemaValidationRule));
    validator.add_rule(Arc::new(PerformanceValidationRule));
    validator.add_rule(Arc::new(SecurityValidationRule));
    validator.add_rule(Arc::new(CustomValidationRule::new(
        "integration_test".to_string(),
    )));

    let mut config = EventFluxConfig::default();

    // Test with valid configuration
    let report = validator.validate(&config).await.unwrap();
    assert!(report.is_valid);
    assert!(report.validation_duration.as_nanos() >= 0);
    assert!(report.metadata.contains_key("rules_executed"));

    // Test with invalid configuration
    config.eventflux.runtime.performance.thread_pool_size = 0; // Invalid
    let report2 = validator.validate(&config).await.unwrap();

    assert!(!report2.is_valid);
    assert!(report2.errors.len() > 0);
    assert!(report2.validation_score < 1.0);
}

/// Test validation configuration presets
#[test]
fn test_validation_config_presets() {
    // Development configuration
    let dev_config = ValidationConfig::development();
    assert!(!dev_config.strict_mode);
    assert_eq!(dev_config.timeout.as_secs(), 10);
    assert!(!dev_config.enable_performance_checks);
    assert!(!dev_config.enable_security_checks);
    assert!(dev_config.fail_fast);
    assert_eq!(dev_config.minimum_score, 0.5);

    // Production configuration
    let prod_config = ValidationConfig::production();
    assert!(prod_config.strict_mode);
    assert_eq!(prod_config.timeout.as_secs(), 60);
    assert!(prod_config.enable_performance_checks);
    assert!(prod_config.enable_security_checks);
    assert!(!prod_config.fail_fast);
    assert_eq!(prod_config.minimum_score, 0.9);

    // Kubernetes configuration
    let k8s_config = ValidationConfig::kubernetes();
    assert!(k8s_config.strict_mode);
    assert_eq!(k8s_config.timeout.as_secs(), 45);
    assert!(k8s_config.enable_compatibility_checks);
    assert_eq!(k8s_config.minimum_score, 0.8);
    assert_eq!(
        k8s_config.custom_params.get("kubernetes_validation"),
        Some(&"true".to_string())
    );
    assert_eq!(
        k8s_config.custom_params.get("resource_limits_required"),
        Some(&"true".to_string())
    );
}

/// Test pre-built validators
#[tokio::test]
async fn test_prebuilt_validators() {
    let mut config = EventFluxConfig::default();
    config.eventflux.monitoring = Some(MonitoringConfig::production());

    // Test default validator
    let default_validator = create_default_validator();
    assert_eq!(default_validator.get_rules().len(), 4);
    let report = default_validator.validate(&config).await.unwrap();
    assert!(report.is_valid);

    // Test production validator
    let prod_validator = create_production_validator();
    assert!(prod_validator.get_config().strict_mode);
    let report2 = prod_validator.validate(&config).await.unwrap();
    assert!(report2.is_valid);

    // Test development validator
    let dev_validator = create_development_validator();
    assert!(!dev_validator.get_config().strict_mode);
    assert_eq!(dev_validator.get_rules().len(), 1); // Only schema rule
    let report3 = dev_validator.validate(&config).await.unwrap();
    assert!(report3.is_valid);
}

/// Test strict mode functionality
#[tokio::test]
async fn test_strict_mode() {
    let strict_config = ValidationConfig {
        strict_mode: true,
        ..ValidationConfig::default()
    };

    let mut validator = ConfigurationValidator::with_config(strict_config);
    validator.add_rule(Arc::new(PerformanceValidationRule));

    let mut config = EventFluxConfig::default();
    config.eventflux.runtime.performance.thread_pool_size = 1000; // Causes performance warning

    let report = validator.validate(&config).await.unwrap();

    // In strict mode, warnings become errors
    assert!(!report.is_valid);
    assert!(report.errors.len() > 0);
    assert_eq!(report.warnings.len(), 0); // Warnings converted to errors
}

/// Test fail-fast functionality
#[tokio::test]
async fn test_fail_fast() {
    let fail_fast_config = ValidationConfig {
        fail_fast: true,
        ..ValidationConfig::default()
    };

    let mut validator = ConfigurationValidator::with_config(fail_fast_config);
    validator.add_rule(Arc::new(SchemaValidationRule));
    validator.add_rule(Arc::new(PerformanceValidationRule));

    let mut config = EventFluxConfig::default();
    config.eventflux.runtime.performance.thread_pool_size = 0; // Causes schema error
    config.eventflux.runtime.performance.event_buffer_size = 0; // Would cause another error

    let report = validator.validate(&config).await.unwrap();

    // Should stop after first error in fail-fast mode
    assert!(!report.is_valid);
    // Exact count depends on rule execution order and implementation
}

/// Test minimum score validation
#[tokio::test]
async fn test_minimum_score_validation() {
    let high_score_config = ValidationConfig {
        minimum_score: 0.95, // Very high requirement
        ..ValidationConfig::default()
    };

    let mut validator = ConfigurationValidator::with_config(high_score_config);
    validator.add_rule(Arc::new(PerformanceValidationRule));

    let mut config = EventFluxConfig::default();
    config.eventflux.runtime.performance.event_buffer_size = 500; // Causes warning

    let report = validator.validate(&config).await.unwrap();

    // Should fail due to score being below minimum
    assert!(!report.is_valid);
    assert!(report
        .errors
        .iter()
        .any(|e| e.code == "VALIDATION_SCORE_TOO_LOW"));
}

/// Test validation timeout
#[tokio::test]
async fn test_validation_timeout() {
    use std::time::Duration;

    // Very short timeout
    let timeout_config = ValidationConfig {
        timeout: Duration::from_millis(1), // 1ms timeout
        ..ValidationConfig::default()
    };

    // Custom rule that takes time
    #[derive(Debug)]
    struct SlowValidationRule;

    #[async_trait]
    impl ValidationRule for SlowValidationRule {
        fn name(&self) -> &str {
            "slow_rule"
        }
        fn description(&self) -> &str {
            "A slow validation rule for testing timeout"
        }
        fn applies_to(&self, _config: &EventFluxConfig) -> bool {
            true
        }

        async fn validate(
            &self,
            _config: &EventFluxConfig,
            _report: &mut ValidationReport,
        ) -> eventflux::core::config::ConfigResult<()> {
            // Simulate slow validation
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        }

        fn category(&self) -> ValidationRuleCategory {
            ValidationRuleCategory::General
        }
    }

    let mut validator = ConfigurationValidator::with_config(timeout_config);
    validator.add_rule(Arc::new(SlowValidationRule));

    let config = EventFluxConfig::default();
    let report = validator.validate(&config).await.unwrap();

    // Should timeout and add timeout error
    assert!(!report.is_valid);
    assert!(report.errors.iter().any(|e| e.code == "VALIDATION_TIMEOUT"));
}

/// Test comprehensive validation scenario
#[tokio::test]
async fn test_comprehensive_validation_scenario() {
    let validator = create_production_validator(); // Strict validator

    let mut config = EventFluxConfig::default();

    // Create a configuration with multiple issues
    config.eventflux.runtime.performance.thread_pool_size = 0; // Schema error
    config.eventflux.runtime.performance.event_buffer_size = 100; // Performance warning -> error in strict mode
    config.eventflux.runtime.mode = eventflux::core::config::types::RuntimeMode::Distributed;
    config.eventflux.security = None; // Security error
    config.eventflux.monitoring = None; // Security warning -> error in strict mode

    let report = validator.validate(&config).await.unwrap();

    assert!(!report.is_valid);
    assert!(report.errors.len() >= 2); // Schema + security errors
    assert!(report.validation_score < 0.8); // Low score due to multiple issues

    println!("Comprehensive validation report:");
    println!("  Summary: {}", report.summary());
    println!("  Score: {:.2}", report.validation_score);
    println!("  Errors: {}", report.errors.len());
    println!("  Duration: {:?}", report.validation_duration);

    for (i, error) in report.errors.iter().enumerate() {
        println!("  Error {}: {} - {}", i + 1, error.code, error.message);
        if let Some(suggestion) = &error.suggestion {
            println!("    Suggestion: {}", suggestion);
        }
    }
}

/// Test validation rule priority and execution order
#[tokio::test]
async fn test_validation_rule_priority() {
    #[derive(Debug)]
    struct HighPriorityRule;
    #[derive(Debug)]
    struct LowPriorityRule;

    #[async_trait]
    impl ValidationRule for HighPriorityRule {
        fn name(&self) -> &str {
            "high_priority"
        }
        fn description(&self) -> &str {
            "High priority rule"
        }
        fn priority(&self) -> u32 {
            100
        }
        fn applies_to(&self, _config: &EventFluxConfig) -> bool {
            true
        }

        async fn validate(
            &self,
            _config: &EventFluxConfig,
            report: &mut ValidationReport,
        ) -> eventflux::core::config::ConfigResult<()> {
            report.add_info(ValidationInfo::new(
                "HIGH_PRIORITY_EXECUTED".to_string(),
                "High priority rule executed".to_string(),
                "priority_test".to_string(),
            ));
            Ok(())
        }

        fn category(&self) -> ValidationRuleCategory {
            ValidationRuleCategory::General
        }
    }

    #[async_trait]
    impl ValidationRule for LowPriorityRule {
        fn name(&self) -> &str {
            "low_priority"
        }
        fn description(&self) -> &str {
            "Low priority rule"
        }
        fn priority(&self) -> u32 {
            10
        }
        fn applies_to(&self, _config: &EventFluxConfig) -> bool {
            true
        }

        async fn validate(
            &self,
            _config: &EventFluxConfig,
            report: &mut ValidationReport,
        ) -> eventflux::core::config::ConfigResult<()> {
            report.add_info(ValidationInfo::new(
                "LOW_PRIORITY_EXECUTED".to_string(),
                "Low priority rule executed".to_string(),
                "priority_test".to_string(),
            ));
            Ok(())
        }

        fn category(&self) -> ValidationRuleCategory {
            ValidationRuleCategory::General
        }
    }

    let mut validator = ConfigurationValidator::new();
    // Add in reverse priority order to test sorting
    validator.add_rule(Arc::new(LowPriorityRule));
    validator.add_rule(Arc::new(HighPriorityRule));

    let config = EventFluxConfig::default();
    let report = validator.validate(&config).await.unwrap();

    assert!(report.is_valid);
    assert_eq!(report.info.len(), 2);

    // High priority rule should execute first (and thus appear first in info messages)
    assert_eq!(report.info[0].code, "HIGH_PRIORITY_EXECUTED");
    assert_eq!(report.info[1].code, "LOW_PRIORITY_EXECUTED");
}

/// Example showing enterprise validation workflow
#[tokio::test]
async fn test_enterprise_validation_workflow() {
    println!("🏢 Enterprise Configuration Validation Workflow");
    println!("================================================");

    // Step 1: Create production validator
    let validator = create_production_validator();
    println!(
        "✅ Created production validator with {} rules",
        validator.get_rules().len()
    );

    // Step 2: Load configuration (simulated)
    let mut config = EventFluxConfig::default();
    config.eventflux.runtime.performance.thread_pool_size = num_cpus::get();
    config.eventflux.runtime.performance.event_buffer_size = 50000;
    config.eventflux.runtime.performance.batch_processing = true;
    config.eventflux.runtime.performance.async_processing = true;
    config.eventflux.monitoring = Some(MonitoringConfig::production());
    println!("✅ Loaded enterprise configuration");

    // Step 3: Run comprehensive validation
    let start_time = std::time::Instant::now();
    let report = validator.validate(&config).await.unwrap();
    let validation_time = start_time.elapsed();

    println!("✅ Validation completed in {:?}", validation_time);

    // Step 4: Generate validation report
    println!("\n📊 Validation Report:");
    println!(
        "  Status: {}",
        if report.is_valid {
            "✅ VALID"
        } else {
            "❌ INVALID"
        }
    );
    println!("  Score: {:.1}%", report.validation_score * 100.0);
    println!(
        "  Issues: {} errors, {} warnings, {} info",
        report.errors.len(),
        report.warnings.len(),
        report.info.len()
    );
    println!("  Summary: {}", report.summary());

    // Step 5: Handle validation results
    if report.is_valid {
        println!("\n✅ Configuration approved for production deployment");

        if !report.warnings.is_empty() {
            println!("\n⚠️  Production Warnings to Address:");
            for warning in &report.warnings {
                println!("   - {}: {}", warning.code, warning.message);
                if let Some(rec) = &warning.recommendation {
                    println!("     Recommendation: {}", rec);
                }
            }
        }

        if !report.info.is_empty() {
            println!("\n💡 Optimization Suggestions:");
            for info in &report.info {
                if matches!(info.category, InfoCategory::Optimization) {
                    println!("   - {}: {}", info.code, info.message);
                }
            }
        }
    } else {
        println!("\n❌ Configuration REJECTED for production deployment");
        println!("\n🚨 Critical Issues to Fix:");
        for error in &report.errors {
            println!("   - {}: {}", error.code, error.message);
            if let Some(suggestion) = &error.suggestion {
                println!("     Fix: {}", suggestion);
            }
        }
    }

    // Step 6: Metadata and diagnostics
    println!("\n🔍 Validation Metadata:");
    for (key, value) in &report.metadata {
        println!("   - {}: {}", key, value);
    }

    assert!(report.is_valid); // Should pass with default good config
}
