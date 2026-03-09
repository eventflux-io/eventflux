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

//! Error configuration system that integrates all error handling components.
//!
//! This module provides the main ErrorConfig structure that combines error strategy,
//! retry configuration, DLQ configuration, and logging settings into a unified
//! configuration system.

use crate::core::config::FlatConfig;

use super::dlq::DlqConfig;
use super::retry::RetryConfig;
use super::strategy::ErrorStrategy;

/// Log level for error reporting
///
/// Determines how errors are logged when they occur.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogLevel {
    /// Debug level - verbose logging for development
    Debug,
    /// Info level - informational messages
    Info,
    /// Warn level - warning messages (default)
    Warn,
    /// Error level - error messages
    Error,
}

impl LogLevel {
    /// Parse log level from string (case-insensitive)
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "debug" => Ok(LogLevel::Debug),
            "info" => Ok(LogLevel::Info),
            "warn" | "warning" => Ok(LogLevel::Warn),
            "error" => Ok(LogLevel::Error),
            _ => Err(format!(
                "Invalid log level '{}'. Valid values: 'debug', 'info', 'warn', 'error'",
                s
            )),
        }
    }

    /// Convert log level to string representation
    #[inline]
    pub const fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
        }
    }
}

impl Default for LogLevel {
    /// Default log level is Warn
    #[inline]
    fn default() -> Self {
        LogLevel::Warn
    }
}

/// Fail configuration (for ErrorStrategy::Fail)
///
/// Currently a placeholder for future configuration options.
#[derive(Debug, Clone)]
pub struct FailConfig {
    // Future: Could include options like:
    // - shutdown_timeout: Duration
    // - cleanup_handler: Option<Box<dyn FnOnce()>>
    // - send_alert: bool
}

impl FailConfig {
    /// Create default fail configuration
    pub fn new() -> Self {
        Self {}
    }

    /// Parse fail configuration from FlatConfig
    pub fn from_flat_config(_config: &FlatConfig) -> Result<Self, String> {
        // Currently no configuration options, but prepared for future extension
        Ok(Self {})
    }
}

impl Default for FailConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Comprehensive error handling configuration
///
/// Combines all error handling settings into a single configuration structure
/// that can be applied to sources, sinks, and processors.
///
/// # Configuration Precedence
/// The configuration is parsed from FlatConfig with the following structure:
/// - `error.strategy` - Main error handling strategy (drop/retry/dlq/fail)
/// - `error.log-level` - Logging level for errors
/// - `error.retry.*` - Retry configuration (if strategy=retry)
/// - `error.dlq.*` - DLQ configuration (if strategy=dlq)
/// - `error.fail.*` - Fail configuration (if strategy=fail)
#[derive(Debug, Clone)]
pub struct ErrorConfig {
    /// Primary error handling strategy
    pub strategy: ErrorStrategy,

    /// Log level for error messages
    pub log_level: LogLevel,

    /// Retry configuration (only if strategy is Retry)
    pub retry: Option<RetryConfig>,

    /// DLQ configuration (only if strategy is Dlq)
    pub dlq: Option<DlqConfig>,

    /// Fail configuration (only if strategy is Fail)
    pub fail: Option<FailConfig>,
}

impl ErrorConfig {
    /// Create a new error configuration
    pub fn new(
        strategy: ErrorStrategy,
        log_level: LogLevel,
        retry: Option<RetryConfig>,
        dlq: Option<DlqConfig>,
        fail: Option<FailConfig>,
    ) -> Result<Self, String> {
        // Validate that required configs are present for each strategy
        if strategy == ErrorStrategy::Retry && retry.is_none() {
            return Err("ErrorStrategy::Retry requires retry configuration".to_string());
        }

        if strategy == ErrorStrategy::Dlq && dlq.is_none() {
            return Err("ErrorStrategy::Dlq requires DLQ configuration".to_string());
        }

        Ok(Self {
            strategy,
            log_level,
            retry,
            dlq,
            fail,
        })
    }

    /// Parse error configuration from FlatConfig
    ///
    /// Extracts error handling settings from configuration with sensible defaults:
    /// - strategy: drop (default)
    /// - log-level: warn (default)
    /// - retry: Only parsed if strategy=retry
    /// - dlq: Only parsed if strategy=dlq
    /// - fail: Only parsed if strategy=fail
    ///
    /// # Example Configuration
    /// ```toml
    /// [application]
    /// error.strategy = "retry"
    /// error.log-level = "error"
    /// error.retry.max-attempts = 5
    /// error.retry.backoff = "exponential"
    /// error.retry.initial-delay = "100ms"
    /// error.retry.max-delay = "30s"
    /// ```
    ///
    /// # Example with DLQ
    /// ```toml
    /// [application]
    /// error.strategy = "dlq"
    /// error.dlq.stream = "ErrorStream"
    /// error.dlq.fallback-strategy = "log"
    /// ```
    pub fn from_flat_config(config: &FlatConfig) -> Result<Self, String> {
        let strategy = config
            .get("error.strategy")
            .map(|s| ErrorStrategy::from_str(s))
            .transpose()?
            .unwrap_or(ErrorStrategy::Drop); // Default: drop

        let log_level = config
            .get("error.log-level")
            .map(|s| LogLevel::from_str(s))
            .transpose()?
            .unwrap_or(LogLevel::Warn); // Default: warn

        let retry = if matches!(strategy, ErrorStrategy::Retry) {
            Some(RetryConfig::from_flat_config(config)?)
        } else {
            None
        };

        let dlq = if matches!(strategy, ErrorStrategy::Dlq) {
            Some(DlqConfig::from_flat_config(config)?)
        } else {
            None
        };

        let fail = if matches!(strategy, ErrorStrategy::Fail) {
            Some(FailConfig::from_flat_config(config)?)
        } else {
            None
        };

        Self::new(strategy, log_level, retry, dlq, fail)
    }

    /// Get retry configuration if available
    ///
    /// Returns None if strategy is not Retry or retry config is missing.
    #[inline]
    pub fn retry_config(&self) -> Option<&RetryConfig> {
        self.retry.as_ref()
    }

    /// Get DLQ configuration if available
    ///
    /// Returns None if strategy is not Dlq or DLQ config is missing.
    #[inline]
    pub fn dlq_config(&self) -> Option<&DlqConfig> {
        self.dlq.as_ref()
    }

    /// Get fail configuration if available
    ///
    /// Returns None if strategy is not Fail or fail config is missing.
    #[inline]
    pub fn fail_config(&self) -> Option<&FailConfig> {
        self.fail.as_ref()
    }

    /// Check if error logging is enabled at the given level
    pub fn should_log(&self, level: LogLevel) -> bool {
        // Simple level comparison (could be enhanced with proper ordering)
        match self.log_level {
            LogLevel::Debug => true, // Log everything
            LogLevel::Info => !matches!(level, LogLevel::Debug),
            LogLevel::Warn => matches!(level, LogLevel::Warn | LogLevel::Error),
            LogLevel::Error => matches!(level, LogLevel::Error),
        }
    }
}

impl Default for ErrorConfig {
    /// Default error configuration:
    /// - Strategy: Drop
    /// - Log level: Warn
    /// - No retry/dlq/fail config
    fn default() -> Self {
        Self {
            strategy: ErrorStrategy::Drop,
            log_level: LogLevel::Warn,
            retry: None,
            dlq: None,
            fail: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::PropertySource;

    // ========================================================================
    // LogLevel Tests
    // ========================================================================

    #[test]
    fn test_log_level_from_str() {
        assert_eq!(LogLevel::from_str("debug").unwrap(), LogLevel::Debug);
        assert_eq!(LogLevel::from_str("Debug").unwrap(), LogLevel::Debug);
        assert_eq!(LogLevel::from_str("info").unwrap(), LogLevel::Info);
        assert_eq!(LogLevel::from_str("warn").unwrap(), LogLevel::Warn);
        assert_eq!(LogLevel::from_str("warning").unwrap(), LogLevel::Warn);
        assert_eq!(LogLevel::from_str("error").unwrap(), LogLevel::Error);
        assert!(LogLevel::from_str("invalid").is_err());
    }

    #[test]
    fn test_log_level_as_str() {
        assert_eq!(LogLevel::Debug.as_str(), "debug");
        assert_eq!(LogLevel::Info.as_str(), "info");
        assert_eq!(LogLevel::Warn.as_str(), "warn");
        assert_eq!(LogLevel::Error.as_str(), "error");
    }

    #[test]
    fn test_log_level_default() {
        assert_eq!(LogLevel::default(), LogLevel::Warn);
    }

    // ========================================================================
    // FailConfig Tests
    // ========================================================================

    #[test]
    fn test_fail_config_new() {
        let config = FailConfig::new();
        // Just ensure it creates successfully
        assert!(format!("{:?}", config).contains("FailConfig"));
    }

    #[test]
    fn test_fail_config_from_flat_config() {
        let flat_config = FlatConfig::new();
        let fail_config = FailConfig::from_flat_config(&flat_config);
        assert!(fail_config.is_ok());
    }

    // ========================================================================
    // ErrorConfig Tests
    // ========================================================================

    #[test]
    fn test_error_config_new_drop() {
        let config = ErrorConfig::new(ErrorStrategy::Drop, LogLevel::Warn, None, None, None);
        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.strategy, ErrorStrategy::Drop);
        assert_eq!(config.log_level, LogLevel::Warn);
        assert!(config.retry.is_none());
        assert!(config.dlq.is_none());
    }

    #[test]
    fn test_error_config_new_retry_without_config() {
        let config = ErrorConfig::new(ErrorStrategy::Retry, LogLevel::Warn, None, None, None);
        assert!(config.is_err());
        assert!(config
            .unwrap_err()
            .contains("Retry requires retry configuration"));
    }

    #[test]
    fn test_error_config_new_dlq_without_config() {
        let config = ErrorConfig::new(ErrorStrategy::Dlq, LogLevel::Warn, None, None, None);
        assert!(config.is_err());
        assert!(config
            .unwrap_err()
            .contains("Dlq requires DLQ configuration"));
    }

    #[test]
    fn test_error_config_new_with_retry() {
        let retry = RetryConfig::default();
        let config = ErrorConfig::new(
            ErrorStrategy::Retry,
            LogLevel::Error,
            Some(retry),
            None,
            None,
        );
        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.strategy, ErrorStrategy::Retry);
        assert!(config.retry.is_some());
    }

    #[test]
    fn test_error_config_default() {
        let config = ErrorConfig::default();
        assert_eq!(config.strategy, ErrorStrategy::Drop);
        assert_eq!(config.log_level, LogLevel::Warn);
        assert!(config.retry.is_none());
        assert!(config.dlq.is_none());
    }

    #[test]
    fn test_error_config_from_flat_config_defaults() {
        let flat_config = FlatConfig::new();
        let error_config = ErrorConfig::from_flat_config(&flat_config).unwrap();

        assert_eq!(error_config.strategy, ErrorStrategy::Drop);
        assert_eq!(error_config.log_level, LogLevel::Warn);
        assert!(error_config.retry.is_none());
        assert!(error_config.dlq.is_none());
    }

    #[test]
    fn test_error_config_from_flat_config_drop() {
        let mut config = FlatConfig::new();
        config.set("error.strategy", "drop", PropertySource::SqlWith);
        config.set("error.log-level", "error", PropertySource::SqlWith);

        let error_config = ErrorConfig::from_flat_config(&config).unwrap();
        assert_eq!(error_config.strategy, ErrorStrategy::Drop);
        assert_eq!(error_config.log_level, LogLevel::Error);
    }

    #[test]
    fn test_error_config_from_flat_config_retry() {
        let mut config = FlatConfig::new();
        config.set("error.strategy", "retry", PropertySource::SqlWith);
        config.set("error.retry.max-attempts", "5", PropertySource::SqlWith);
        config.set(
            "error.retry.backoff",
            "exponential",
            PropertySource::SqlWith,
        );

        let error_config = ErrorConfig::from_flat_config(&config).unwrap();
        assert_eq!(error_config.strategy, ErrorStrategy::Retry);
        assert!(error_config.retry.is_some());

        let retry = error_config.retry.unwrap();
        assert_eq!(retry.max_attempts, 5);
    }

    #[test]
    fn test_error_config_from_flat_config_dlq() {
        let mut config = FlatConfig::new();
        config.set("error.strategy", "dlq", PropertySource::SqlWith);
        config.set("error.dlq.stream", "ErrorStream", PropertySource::SqlWith);
        config.set(
            "error.dlq.fallback-strategy",
            "log",
            PropertySource::SqlWith,
        );

        let error_config = ErrorConfig::from_flat_config(&config).unwrap();
        assert_eq!(error_config.strategy, ErrorStrategy::Dlq);
        assert!(error_config.dlq.is_some());

        let dlq = error_config.dlq.unwrap();
        assert_eq!(dlq.stream, "ErrorStream");
    }

    #[test]
    fn test_error_config_from_flat_config_fail() {
        let mut config = FlatConfig::new();
        config.set("error.strategy", "fail", PropertySource::SqlWith);

        let error_config = ErrorConfig::from_flat_config(&config).unwrap();
        assert_eq!(error_config.strategy, ErrorStrategy::Fail);
        assert!(error_config.fail.is_some());
    }

    #[test]
    fn test_error_config_retry_config_getter() {
        let retry = RetryConfig::default();
        let config = ErrorConfig::new(
            ErrorStrategy::Retry,
            LogLevel::Warn,
            Some(retry.clone()),
            None,
            None,
        )
        .unwrap();

        assert!(config.retry_config().is_some());
        assert_eq!(config.retry_config().unwrap().max_attempts, 3);
    }

    #[test]
    fn test_error_config_dlq_config_getter() {
        use super::super::dlq::{DlqConfig, DlqFallbackStrategy};

        let dlq =
            DlqConfig::new("ErrorStream".to_string(), DlqFallbackStrategy::Log, None).unwrap();
        let config =
            ErrorConfig::new(ErrorStrategy::Dlq, LogLevel::Warn, None, Some(dlq), None).unwrap();

        assert!(config.dlq_config().is_some());
        assert_eq!(config.dlq_config().unwrap().stream, "ErrorStream");
    }

    #[test]
    fn test_error_config_should_log() {
        let config = ErrorConfig {
            strategy: ErrorStrategy::Drop,
            log_level: LogLevel::Warn,
            retry: None,
            dlq: None,
            fail: None,
        };

        assert!(!config.should_log(LogLevel::Debug));
        assert!(!config.should_log(LogLevel::Info));
        assert!(config.should_log(LogLevel::Warn));
        assert!(config.should_log(LogLevel::Error));
    }

    #[test]
    fn test_error_config_should_log_debug() {
        let config = ErrorConfig {
            strategy: ErrorStrategy::Drop,
            log_level: LogLevel::Debug,
            retry: None,
            dlq: None,
            fail: None,
        };

        assert!(config.should_log(LogLevel::Debug));
        assert!(config.should_log(LogLevel::Info));
        assert!(config.should_log(LogLevel::Warn));
        assert!(config.should_log(LogLevel::Error));
    }

    #[test]
    fn test_error_config_should_log_error() {
        let config = ErrorConfig {
            strategy: ErrorStrategy::Drop,
            log_level: LogLevel::Error,
            retry: None,
            dlq: None,
            fail: None,
        };

        assert!(!config.should_log(LogLevel::Debug));
        assert!(!config.should_log(LogLevel::Info));
        assert!(!config.should_log(LogLevel::Warn));
        assert!(config.should_log(LogLevel::Error));
    }
}
