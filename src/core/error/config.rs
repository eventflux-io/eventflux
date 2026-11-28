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

//! Error settings.
//!
//! [`ErrorConfig`] gathers the strategy, retry, DLQ, fail and log level settings in
//! one place.

use std::str::FromStr;

use crate::core::config::FlatConfig;

use super::dlq::{DlqConfig, DLQ_FALLBACK_RETRY_KEY_PREFIX, DLQ_STREAM_KEY};
use super::retry::{RetryConfig, RETRY_KEY_PREFIX, RETRY_KEY_SUFFIXES};
use super::strategy::ErrorStrategy;

/// Lowest level of error handling messages to log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogLevel {
    /// Everything, for development.
    Debug,
    Info,
    /// Warnings and errors. The default.
    Warn,
    /// Errors only.
    Error,
}

impl std::str::FromStr for LogLevel {
    type Err = String;

    /// Parse a level name, ignoring case. Accepts `warning` as well as `warn`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
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
}

impl LogLevel {
    /// Returns the lowercase name, such as `"warn"`.
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
    /// Returns `Warn`.
    #[inline]
    fn default() -> Self {
        LogLevel::Warn
    }
}

/// Settings for `ErrorStrategy::Fail`.
///
/// Has no options yet.
#[derive(Debug, Clone)]
pub struct FailConfig {
    // Possible later options:
    // - shutdown_timeout: Duration
    // - cleanup_handler: Option<Box<dyn FnOnce()>>
    // - send_alert: bool
}

impl FailConfig {
    pub fn new() -> Self {
        Self {}
    }

    /// Parse from `error.fail.*` properties. There are none yet, so this always
    /// succeeds.
    pub fn from_flat_config(_config: &FlatConfig) -> Result<Self, String> {
        Ok(Self {})
    }
}

impl Default for FailConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// All error handling settings for a source.
///
/// # Properties
/// Read from these `FlatConfig` keys:
/// - `error.strategy` - `drop`, `retry`, `dlq` or `fail`.
/// - `error.log-level` - Lowest level to log.
/// - `error.retry.*` - Retry settings, read when the strategy is `retry`.
/// - `error.dlq.*` - DLQ settings, read when the strategy is `dlq`, or `retry`
///   with `error.dlq.stream` set.
/// - `error.fail.*` - Fail settings, read when the strategy is `fail`.
#[derive(Debug, Clone)]
pub struct ErrorConfig {
    pub strategy: ErrorStrategy,

    pub log_level: LogLevel,

    /// Set when the strategy is `Retry`.
    pub retry: Option<RetryConfig>,

    /// Set when the strategy is `Dlq`, or `Retry` with a DLQ stream that takes
    /// events whose retries ran out.
    pub dlq: Option<DlqConfig>,

    /// Set when the strategy is `Fail`.
    pub fail: Option<FailConfig>,
}

impl ErrorConfig {
    /// Build settings from parts.
    ///
    /// # Errors
    ///
    /// Returns an error message if the strategy is `Retry` without `retry`, or
    /// `Dlq` without `dlq`.
    pub fn new(
        strategy: ErrorStrategy,
        log_level: LogLevel,
        retry: Option<RetryConfig>,
        dlq: Option<DlqConfig>,
        fail: Option<FailConfig>,
    ) -> Result<Self, String> {
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

    /// Parse settings from `error.*` properties.
    ///
    /// - strategy defaults to `drop`.
    /// - log level defaults to `warn`.
    /// - retry is parsed only when the strategy is `retry`.
    /// - dlq is parsed when the strategy is `dlq`, or `retry` with
    ///   `error.dlq.stream` set. In the retry case, events whose retries run out go
    ///   to the DLQ.
    /// - fail is parsed only when the strategy is `fail`.
    ///
    /// # Errors
    ///
    /// Returns an error message if any value fails to parse, or if the parsed
    /// retry or DLQ settings are invalid.
    ///
    /// # Example configuration
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
    /// # Example with a DLQ
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
            .unwrap_or(ErrorStrategy::Drop);

        let log_level = config
            .get("error.log-level")
            .map(|s| LogLevel::from_str(s))
            .transpose()?
            .unwrap_or(LogLevel::Warn);

        let retry = if matches!(strategy, ErrorStrategy::Retry) {
            Some(RetryConfig::from_flat_config(config, RETRY_KEY_PREFIX)?)
        } else {
            None
        };

        // Always parse DLQ settings for strategy=dlq. For strategy=retry, parse
        // them only when a DLQ stream is named. That stream gets events whose
        // retries ran out.
        let dlq = match strategy {
            ErrorStrategy::Dlq => Some(DlqConfig::from_flat_config(config)?),
            ErrorStrategy::Retry if config.get(DLQ_STREAM_KEY).is_some() => {
                Some(DlqConfig::from_flat_config(config)?)
            }
            _ => None,
        };

        let fail = if matches!(strategy, ErrorStrategy::Fail) {
            Some(FailConfig::from_flat_config(config)?)
        } else {
            None
        };

        Self::new(strategy, log_level, retry, dlq, fail)
    }

    /// Returns the retry settings, or `None` if none are set.
    #[inline]
    pub fn retry_config(&self) -> Option<&RetryConfig> {
        self.retry.as_ref()
    }

    /// Returns the DLQ settings, or `None` if none are set. They can be set for
    /// the `Retry` strategy as well as `Dlq`.
    #[inline]
    pub fn dlq_config(&self) -> Option<&DlqConfig> {
        self.dlq.as_ref()
    }

    /// Returns the fail settings, or `None` if none are set.
    #[inline]
    pub fn fail_config(&self) -> Option<&FailConfig> {
        self.fail.as_ref()
    }

    /// Returns the `error.*` keys in `keys` that these settings never read, sorted.
    ///
    /// A key is unused when it is unknown (a typo such as
    /// `error.retry.max_attempts`), or when it belongs to a part the strategy does
    /// not use. For example `error.retry.*` without strategy `retry`, `error.dlq.*`
    /// when there is no DLQ, or `error.dlq.fallback-retry.*` without fallback
    /// strategy `retry`. Keys that do not start with `error.` are skipped.
    ///
    /// `keys` should be the keys these settings were parsed from.
    pub fn unused_keys<'a>(&self, keys: impl IntoIterator<Item = &'a String>) -> Vec<String> {
        let fallback_retry = self
            .dlq
            .as_ref()
            .and_then(|dlq| dlq.fallback_retry.as_ref());
        let is_used = |key: &str| -> bool {
            let retry_key = |prefix: &str| {
                key.strip_prefix(prefix)
                    .and_then(|rest| rest.strip_prefix('.'))
                    .map(|suffix| RETRY_KEY_SUFFIXES.contains(&suffix))
            };
            if let Some(known) = retry_key(DLQ_FALLBACK_RETRY_KEY_PREFIX) {
                return known && fallback_retry.is_some();
            }
            if let Some(known) = retry_key(RETRY_KEY_PREFIX) {
                return known && self.retry.is_some();
            }
            match key {
                "error.strategy" | "error.log-level" => true,
                DLQ_STREAM_KEY | "error.dlq.fallback-strategy" => self.dlq.is_some(),
                _ => false,
            }
        };

        let mut unused: Vec<String> = keys
            .into_iter()
            .filter(|key| key.starts_with("error.") && !is_used(key))
            .cloned()
            .collect();
        unused.sort();
        unused
    }

    /// Returns true if a message at `level` is at or above the configured level.
    pub fn should_log(&self, level: LogLevel) -> bool {
        match self.log_level {
            LogLevel::Debug => true,
            LogLevel::Info => !matches!(level, LogLevel::Debug),
            LogLevel::Warn => matches!(level, LogLevel::Warn | LogLevel::Error),
            LogLevel::Error => matches!(level, LogLevel::Error),
        }
    }
}

impl Default for ErrorConfig {
    /// Returns the `Drop` strategy at `Warn` level, with no retry, DLQ or fail
    /// settings.
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
    use crate::core::error::DlqFallbackStrategy;

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
        assert!(error_config.dlq.is_none(), "no DLQ stream named");
    }

    #[test]
    fn test_error_config_from_flat_config_retry_with_dlq_stream() {
        // With retry plus error.dlq.stream, events whose retries run out go to
        // the DLQ.
        let mut config = FlatConfig::new();
        config.set("error.strategy", "retry", PropertySource::SqlWith);
        config.set("error.dlq.stream", "ErrorStream", PropertySource::SqlWith);
        config.set(
            "error.dlq.fallback-strategy",
            "fail",
            PropertySource::SqlWith,
        );

        let error_config = ErrorConfig::from_flat_config(&config).unwrap();
        assert_eq!(error_config.strategy, ErrorStrategy::Retry);
        assert!(error_config.retry.is_some());
        let dlq = error_config.dlq.expect("DLQ parsed for retry + dlq stream");
        assert_eq!(dlq.stream, "ErrorStream");
        assert_eq!(dlq.fallback_strategy, DlqFallbackStrategy::Fail);
    }

    #[test]
    fn test_error_config_from_flat_config_drop_ignores_dlq_stream() {
        let mut config = FlatConfig::new();
        config.set("error.strategy", "drop", PropertySource::SqlWith);
        config.set("error.dlq.stream", "ErrorStream", PropertySource::SqlWith);

        let error_config = ErrorConfig::from_flat_config(&config).unwrap();
        assert!(error_config.dlq.is_none());
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

    /// Parses `pairs` and returns the keys the settings never read.
    fn unused(pairs: &[(&str, &str)]) -> Vec<String> {
        let mut config = FlatConfig::new();
        for (key, value) in pairs {
            config.set(*key, *value, PropertySource::SqlWith);
        }
        let keys: Vec<String> = pairs.iter().map(|(key, _)| key.to_string()).collect();
        ErrorConfig::from_flat_config(&config)
            .unwrap()
            .unused_keys(&keys)
    }

    #[test]
    fn test_unused_keys() {
        // Every key is read
        let all_read: &[(&str, &str)] = &[
            ("error.strategy", "retry"),
            ("error.log-level", "info"),
            ("error.retry.max-attempts", "2"),
            ("error.retry.backoff", "fixed"),
            ("error.retry.initial-delay", "10ms"),
            ("error.retry.max-delay", "1s"),
            ("error.dlq.stream", "E"),
            ("error.dlq.fallback-strategy", "retry"),
            ("error.dlq.fallback-retry.max-attempts", "2"),
            ("kafka.topic", "orders"),
        ];
        assert!(unused(all_read).is_empty());

        // Parts the strategy does not read, and unknown keys
        assert_eq!(
            unused(&[
                ("error.strategy", "drop"),
                ("error.retry.max-attempts", "2"),
                ("error.dlq.stream", "E"),
                ("error.dlq.fallback-strategy", "log"),
            ]),
            vec![
                "error.dlq.fallback-strategy",
                "error.dlq.stream",
                "error.retry.max-attempts"
            ]
        );
        assert_eq!(
            unused(&[
                ("error.strategy", "dlq"),
                ("error.dlq.stream", "E"),
                ("error.dlq.fallback-retry.max-attempts", "2"),
                ("error.retry.max_attempts", "2"),
                ("error.unknown", "x"),
            ]),
            vec![
                "error.dlq.fallback-retry.max-attempts",
                "error.retry.max_attempts",
                "error.unknown"
            ]
        );
        // Retry without a DLQ stream reads no DLQ keys
        assert_eq!(
            unused(&[
                ("error.strategy", "retry"),
                ("error.dlq.fallback-strategy", "log"),
            ]),
            vec!["error.dlq.fallback-strategy"]
        );
    }

    /// The retry keys `unused_keys` knows must match the keys sources list.
    #[test]
    fn test_retry_key_suffixes_match_source_error_parameters() {
        use crate::core::error::source_support::SOURCE_ERROR_PARAMETERS;

        for suffix in RETRY_KEY_SUFFIXES {
            let key = format!("{RETRY_KEY_PREFIX}.{suffix}");
            assert!(SOURCE_ERROR_PARAMETERS.contains(&key.as_str()), "{key}");
        }
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
