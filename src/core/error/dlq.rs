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

//! Dead-Letter Queue (DLQ) configuration and event handling.
//!
//! This module provides DLQ configuration with fallback strategies for scenarios
//! where the DLQ stream itself is unavailable or delivery fails.

use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::core::config::{FlatConfig, PropertySource};
use crate::core::event::{AttributeValue, Event};
use crate::core::exception::EventFluxError;

use super::retry::RetryConfig;

/// Fallback strategy when DLQ stream is unavailable or delivery fails
///
/// Determines what action to take when the DLQ itself cannot receive failed events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DlqFallbackStrategy {
    /// Log detailed error and discard event (default)
    ///
    /// The failed event is dropped after logging comprehensive error details.
    /// This is the safest option that prevents cascading failures.
    Log,

    /// Terminate application immediately
    ///
    /// The application crashes if DLQ delivery fails.
    /// Use when losing failed events is unacceptable.
    Fail,

    /// Retry DLQ delivery with backoff
    ///
    /// Attempts to deliver to DLQ with exponential backoff.
    /// Eventually falls back to logging after max retries.
    Retry,
}

impl std::str::FromStr for DlqFallbackStrategy {
    type Err = String;

    /// Parse fallback strategy from string (case-insensitive)
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "log" => Ok(DlqFallbackStrategy::Log),
            "fail" => Ok(DlqFallbackStrategy::Fail),
            "retry" => Ok(DlqFallbackStrategy::Retry),
            _ => Err(format!(
                "Invalid DLQ fallback strategy '{}'. Valid values: 'log', 'fail', 'retry'",
                s
            )),
        }
    }
}

impl DlqFallbackStrategy {
    /// Convert fallback strategy to string representation
    #[inline]
    pub const fn as_str(&self) -> &'static str {
        match self {
            DlqFallbackStrategy::Log => "log",
            DlqFallbackStrategy::Fail => "fail",
            DlqFallbackStrategy::Retry => "retry",
        }
    }
}

impl Default for DlqFallbackStrategy {
    /// Default fallback strategy is Log
    #[inline]
    fn default() -> Self {
        DlqFallbackStrategy::Log
    }
}

/// Dead-Letter Queue configuration
///
/// Specifies the DLQ stream name and fallback behavior when DLQ is unavailable.
#[derive(Debug, Clone)]
pub struct DlqConfig {
    /// Name of the DLQ stream to send failed events
    pub stream: String,

    /// Fallback strategy when DLQ delivery fails
    pub fallback_strategy: DlqFallbackStrategy,

    /// Retry configuration for DLQ fallback (only if fallback_strategy is Retry)
    pub fallback_retry: Option<RetryConfig>,
}

impl DlqConfig {
    /// Create a new DLQ configuration
    pub fn new(
        stream: String,
        fallback_strategy: DlqFallbackStrategy,
        fallback_retry: Option<RetryConfig>,
    ) -> Result<Self, String> {
        if stream.is_empty() {
            return Err("DLQ stream name cannot be empty".to_string());
        }

        // Validate that fallback_retry is provided if strategy is Retry
        if fallback_strategy == DlqFallbackStrategy::Retry && fallback_retry.is_none() {
            return Err(
                "DLQ fallback strategy 'retry' requires fallback_retry configuration".to_string(),
            );
        }

        Ok(Self {
            stream,
            fallback_strategy,
            fallback_retry,
        })
    }

    /// Parse DLQ configuration from FlatConfig
    ///
    /// Extracts DLQ settings from configuration:
    /// - error.dlq.stream (required)
    /// - error.dlq.fallback-strategy (default: log)
    /// - error.dlq.fallback-retry.* (if fallback-strategy is retry)
    pub fn from_flat_config(config: &FlatConfig) -> Result<Self, String> {
        let stream = config
            .get("error.dlq.stream")
            .ok_or("error.strategy='dlq' requires 'error.dlq.stream' property")?
            .clone();

        if stream.is_empty() {
            return Err("error.dlq.stream cannot be empty".to_string());
        }

        let fallback_strategy = config
            .get("error.dlq.fallback-strategy")
            .map(|s| DlqFallbackStrategy::from_str(s))
            .transpose()?
            .unwrap_or(DlqFallbackStrategy::Log); // Default: log

        let fallback_retry = if matches!(fallback_strategy, DlqFallbackStrategy::Retry) {
            // Parse fallback retry config with "dlq.fallback-retry" prefix
            // We need to transform keys like "error.dlq.fallback-retry.max-attempts"
            // into "error.retry.max-attempts" for RetryConfig::from_flat_config
            let mut fallback_retry_config = FlatConfig::new();

            for (key, value) in config.properties() {
                if let Some(suffix) = key.strip_prefix("error.dlq.fallback-retry.") {
                    fallback_retry_config.set(
                        format!("error.retry.{}", suffix),
                        value.clone(),
                        PropertySource::SqlWith,
                    );
                }
            }

            // Use defaults if no fallback retry config specified
            if fallback_retry_config.is_empty() {
                Some(RetryConfig::default())
            } else {
                Some(RetryConfig::from_flat_config(&fallback_retry_config)?)
            }
        } else {
            None
        };

        Self::new(stream, fallback_strategy, fallback_retry)
    }
}

/// Create DLQ event from failed event and error
///
/// Constructs a standardized DLQ event with exact schema match:
/// - originalEvent STRING (serialized original event as JSON)
/// - errorMessage STRING (human-readable error description)
/// - errorType STRING (EventFluxError variant name)
/// - timestamp BIGINT (milliseconds since epoch)
/// - attemptCount INT (number of retry attempts)
/// - streamName STRING (source stream name)
///
/// # Arguments
/// * `original_event` - The event that failed processing
/// * `error` - The error that caused the failure
/// * `attempt_count` - Number of retry attempts made
/// * `stream_name` - Name of the source stream
///
/// # Returns
/// A new Event with DLQ schema
pub fn create_dlq_event(
    original_event: &Event,
    error: &EventFluxError,
    attempt_count: usize,
    stream_name: &str,
) -> Event {
    let error_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Serialize original event to JSON
    let original_event_json = match serde_json::to_string(original_event) {
        Ok(json) => json,
        Err(e) => {
            eprintln!("Warning: Failed to serialize original event: {}", e);
            format!("{{\"error\": \"Failed to serialize: {}\"}}", e)
        }
    };

    // Get error type (variant name only, not full Debug)
    let error_type = match error {
        EventFluxError::EventFluxAppCreation { .. } => "EventFluxAppCreation",
        EventFluxError::EventFluxAppRuntime { .. } => "EventFluxAppRuntime",
        EventFluxError::QueryCreation { .. } => "QueryCreation",
        EventFluxError::QueryRuntime { .. } => "QueryRuntime",
        EventFluxError::OnDemandQueryCreation { .. } => "OnDemandQueryCreation",
        EventFluxError::OnDemandQueryRuntime { .. } => "OnDemandQueryRuntime",
        EventFluxError::StoreQuery { .. } => "StoreQuery",
        EventFluxError::DefinitionNotExist { .. } => "DefinitionNotExist",
        EventFluxError::QueryNotExist { .. } => "QueryNotExist",
        EventFluxError::NoSuchAttribute { .. } => "NoSuchAttribute",
        EventFluxError::ExtensionNotFound { .. } => "ExtensionNotFound",
        EventFluxError::ConnectionUnavailable { .. } => "ConnectionUnavailable",
        EventFluxError::DatabaseRuntime { .. } => "DatabaseRuntime",
        EventFluxError::QueryableRecordTable { .. } => "QueryableRecordTable",
        EventFluxError::PersistenceStore { .. } => "PersistenceStore",
        EventFluxError::CannotPersistState { .. } => "CannotPersistState",
        EventFluxError::CannotRestoreState { .. } => "CannotRestoreState",
        EventFluxError::CannotClearState { .. } => "CannotClearState",
        EventFluxError::OperationNotSupported { .. } => "OperationNotSupported",
        EventFluxError::TypeError { .. } => "TypeError",
        EventFluxError::InvalidParameter { .. } => "InvalidParameter",
        EventFluxError::MappingFailed { .. } => "MappingFailed",
        EventFluxError::Configuration { .. } => "Configuration",
        EventFluxError::CannotLoadClass { .. } => "CannotLoadClass",
        EventFluxError::ParseError { .. } => "ParseError",
        EventFluxError::SendError { .. } => "SendError",
        EventFluxError::ProcessorError { .. } => "ProcessorError",
        EventFluxError::Other(_) => "Other",
        EventFluxError::Sqlite(_) => "Sqlite",
        EventFluxError::Io(_) => "Io",
        EventFluxError::Serialization(_) => "Serialization",
    }
    .to_string();

    Event::new_with_data(
        error_timestamp, // Event arrival timestamp
        vec![
            AttributeValue::String(original_event_json), // originalEvent
            AttributeValue::String(error.to_string()),   // errorMessage
            AttributeValue::String(error_type),          // errorType
            AttributeValue::Long(error_timestamp),       // timestamp (BIGINT)
            AttributeValue::Int(attempt_count as i32),   // attemptCount (INT)
            AttributeValue::String(stream_name.to_string()), // streamName
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::PropertySource;

    // ========================================================================
    // DlqFallbackStrategy Tests
    // ========================================================================

    #[test]
    fn test_dlq_fallback_strategy_from_str() {
        assert_eq!(
            DlqFallbackStrategy::from_str("log").unwrap(),
            DlqFallbackStrategy::Log
        );
        assert_eq!(
            DlqFallbackStrategy::from_str("Log").unwrap(),
            DlqFallbackStrategy::Log
        );
        assert_eq!(
            DlqFallbackStrategy::from_str("fail").unwrap(),
            DlqFallbackStrategy::Fail
        );
        assert_eq!(
            DlqFallbackStrategy::from_str("retry").unwrap(),
            DlqFallbackStrategy::Retry
        );
        assert!(DlqFallbackStrategy::from_str("invalid").is_err());
    }

    #[test]
    fn test_dlq_fallback_strategy_as_str() {
        assert_eq!(DlqFallbackStrategy::Log.as_str(), "log");
        assert_eq!(DlqFallbackStrategy::Fail.as_str(), "fail");
        assert_eq!(DlqFallbackStrategy::Retry.as_str(), "retry");
    }

    #[test]
    fn test_dlq_fallback_strategy_default() {
        assert_eq!(DlqFallbackStrategy::default(), DlqFallbackStrategy::Log);
    }

    // ========================================================================
    // DlqConfig Tests
    // ========================================================================

    #[test]
    fn test_dlq_config_new() {
        let config = DlqConfig::new("ErrorStream".to_string(), DlqFallbackStrategy::Log, None);
        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.stream, "ErrorStream");
        assert_eq!(config.fallback_strategy, DlqFallbackStrategy::Log);
        assert!(config.fallback_retry.is_none());
    }

    #[test]
    fn test_dlq_config_new_empty_stream() {
        let config = DlqConfig::new(String::new(), DlqFallbackStrategy::Log, None);
        assert!(config.is_err());
        assert!(config.unwrap_err().contains("stream name cannot be empty"));
    }

    #[test]
    fn test_dlq_config_new_retry_without_config() {
        let config = DlqConfig::new("ErrorStream".to_string(), DlqFallbackStrategy::Retry, None);
        assert!(config.is_err());
        assert!(config
            .unwrap_err()
            .contains("requires fallback_retry configuration"));
    }

    #[test]
    fn test_dlq_config_new_with_retry() {
        let retry_config = RetryConfig::default();
        let config = DlqConfig::new(
            "ErrorStream".to_string(),
            DlqFallbackStrategy::Retry,
            Some(retry_config),
        );
        assert!(config.is_ok());
    }

    #[test]
    fn test_dlq_config_from_flat_config() {
        let mut config = FlatConfig::new();
        config.set("error.dlq.stream", "ErrorStream", PropertySource::SqlWith);
        config.set(
            "error.dlq.fallback-strategy",
            "log",
            PropertySource::SqlWith,
        );

        let dlq_config = DlqConfig::from_flat_config(&config).unwrap();
        assert_eq!(dlq_config.stream, "ErrorStream");
        assert_eq!(dlq_config.fallback_strategy, DlqFallbackStrategy::Log);
        assert!(dlq_config.fallback_retry.is_none());
    }

    #[test]
    fn test_dlq_config_from_flat_config_missing_stream() {
        let config = FlatConfig::new();
        let dlq_config = DlqConfig::from_flat_config(&config);
        assert!(dlq_config.is_err());
        assert!(dlq_config
            .unwrap_err()
            .contains("requires 'error.dlq.stream'"));
    }

    #[test]
    fn test_dlq_config_from_flat_config_empty_stream() {
        let mut config = FlatConfig::new();
        config.set("error.dlq.stream", "", PropertySource::SqlWith);

        let dlq_config = DlqConfig::from_flat_config(&config);
        assert!(dlq_config.is_err());
        assert!(dlq_config.unwrap_err().contains("stream cannot be empty"));
    }

    #[test]
    fn test_dlq_config_from_flat_config_with_fallback_retry() {
        let mut config = FlatConfig::new();
        config.set("error.dlq.stream", "ErrorStream", PropertySource::SqlWith);
        config.set(
            "error.dlq.fallback-strategy",
            "retry",
            PropertySource::SqlWith,
        );
        config.set(
            "error.dlq.fallback-retry.max-attempts",
            "5",
            PropertySource::SqlWith,
        );
        config.set(
            "error.dlq.fallback-retry.initial-delay",
            "200ms",
            PropertySource::SqlWith,
        );

        let dlq_config = DlqConfig::from_flat_config(&config).unwrap();
        assert_eq!(dlq_config.stream, "ErrorStream");
        assert_eq!(dlq_config.fallback_strategy, DlqFallbackStrategy::Retry);
        assert!(dlq_config.fallback_retry.is_some());

        let retry = dlq_config.fallback_retry.unwrap();
        assert_eq!(retry.max_attempts, 5);
        assert_eq!(retry.initial_delay, std::time::Duration::from_millis(200));
    }

    #[test]
    fn test_dlq_config_from_flat_config_retry_with_defaults() {
        let mut config = FlatConfig::new();
        config.set("error.dlq.stream", "ErrorStream", PropertySource::SqlWith);
        config.set(
            "error.dlq.fallback-strategy",
            "retry",
            PropertySource::SqlWith,
        );
        // No fallback-retry.* properties, should use defaults

        let dlq_config = DlqConfig::from_flat_config(&config).unwrap();
        assert!(dlq_config.fallback_retry.is_some());

        let retry = dlq_config.fallback_retry.unwrap();
        assert_eq!(retry.max_attempts, 3); // Default
        assert_eq!(retry.initial_delay, std::time::Duration::from_millis(100)); // Default
    }

    // ========================================================================
    // DLQ Event Creation Tests
    // ========================================================================

    #[test]
    fn test_create_dlq_event() {
        let original_event = Event::new_with_data(
            123,
            vec![
                AttributeValue::String("order-1".to_string()),
                AttributeValue::Double(100.0),
            ],
        );

        let error = EventFluxError::MappingFailed {
            message: "JSON parse error".to_string(),
            source: None,
        };

        let dlq_event = create_dlq_event(&original_event, &error, 3, "Orders");

        // Verify DLQ event structure
        assert_eq!(dlq_event.data.len(), 6);

        // originalEvent (serialized JSON)
        assert!(matches!(dlq_event.data[0], AttributeValue::String(_)));
        if let AttributeValue::String(json) = &dlq_event.data[0] {
            assert!(json.contains("order-1"));
        }

        // errorMessage
        assert!(matches!(dlq_event.data[1], AttributeValue::String(_)));
        if let AttributeValue::String(msg) = &dlq_event.data[1] {
            assert!(msg.contains("JSON parse error"));
        }

        // errorType
        assert!(matches!(dlq_event.data[2], AttributeValue::String(_)));
        if let AttributeValue::String(error_type) = &dlq_event.data[2] {
            assert!(error_type.contains("MappingFailed"));
        }

        // timestamp
        assert!(matches!(dlq_event.data[3], AttributeValue::Long(_)));

        // attemptCount
        assert_eq!(dlq_event.data[4], AttributeValue::Int(3));

        // streamName
        assert_eq!(
            dlq_event.data[5],
            AttributeValue::String("Orders".to_string())
        );
    }

    #[test]
    fn test_create_dlq_event_with_different_errors() {
        let original_event = Event::new_with_data(123, vec![AttributeValue::Int(42)]);

        // Test with ConnectionUnavailable error
        let error = EventFluxError::ConnectionUnavailable {
            message: "Database connection lost".to_string(),
            source: None,
        };

        let dlq_event = create_dlq_event(&original_event, &error, 1, "Readings");

        assert_eq!(dlq_event.data.len(), 6);
        assert_eq!(dlq_event.data[4], AttributeValue::Int(1));
        assert_eq!(
            dlq_event.data[5],
            AttributeValue::String("Readings".to_string())
        );
    }

    #[test]
    fn test_create_dlq_event_zero_attempts() {
        let original_event = Event::new_empty(0);
        let error = EventFluxError::Other("Unknown error".to_string());

        let dlq_event = create_dlq_event(&original_event, &error, 0, "TestStream");

        assert_eq!(dlq_event.data[4], AttributeValue::Int(0));
    }
}
