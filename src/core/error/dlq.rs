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

//! Dead letter queue (DLQ) settings and events.
//!
//! A DLQ is a stream that collects failed events. This module holds its settings,
//! the fallback used when the DLQ send itself fails, and the builder for DLQ
//! events.

use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::core::config::FlatConfig;
use crate::core::event::{AttributeValue, Event};
use crate::core::exception::EventFluxError;
use crate::query_api::definition::attribute::Type as AttributeType;

use super::retry::RetryConfig;

/// Stream property that names the DLQ stream for a source's failed events.
pub const DLQ_STREAM_KEY: &str = "error.dlq.stream";

/// Prefix of the keys that set how a failed DLQ send is retried, such as
/// `error.dlq.fallback-retry.max-attempts`. They take the same names as the
/// `error.retry.*` keys.
pub const DLQ_FALLBACK_RETRY_KEY_PREFIX: &str = "error.dlq.fallback-retry";

/// The DLQ event schema, in order.
///
/// [`create_dlq_event`] writes its values in exactly this order. Events are
/// positional, so a DLQ stream must declare exactly these attributes in this
/// order.
pub const DLQ_FIELDS: &[(&str, AttributeType)] = &[
    ("originalEvent", AttributeType::STRING),
    ("errorMessage", AttributeType::STRING),
    ("errorType", AttributeType::STRING),
    ("timestamp", AttributeType::LONG),
    ("attemptCount", AttributeType::INT),
    ("streamName", AttributeType::STRING),
];

/// What to do when an event cannot be sent to the DLQ.
///
/// Applies when the DLQ stream is missing or the send fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DlqFallbackStrategy {
    /// Log the details and discard the event. The default.
    ///
    /// Logs both errors and the event. The safest choice, since one failure
    /// cannot set off others.
    Log,

    /// Stop the source.
    ///
    /// Use when losing a failed event is not acceptable.
    Fail,

    /// Retry the DLQ send with backoff.
    ///
    /// Uses the `fallback_retry` settings, which default to exponential backoff.
    /// Logs and discards the event when the retries run out.
    Retry,
}

impl std::str::FromStr for DlqFallbackStrategy {
    type Err = String;

    /// Parse `log`, `fail` or `retry`, ignoring case.
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
    /// Returns the lowercase name, such as `"log"`.
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
    /// Returns `Log`.
    #[inline]
    fn default() -> Self {
        DlqFallbackStrategy::Log
    }
}

/// Dead letter queue settings.
#[derive(Debug, Clone)]
pub struct DlqConfig {
    /// Name of the stream that takes failed events.
    pub stream: String,

    /// What to do when the DLQ send fails.
    pub fallback_strategy: DlqFallbackStrategy,

    /// Retry settings for the DLQ send. Set when `fallback_strategy` is `Retry`.
    pub fallback_retry: Option<RetryConfig>,
}

impl DlqConfig {
    /// Build DLQ settings.
    ///
    /// # Errors
    ///
    /// Returns an error message if `stream` is empty, or `fallback_strategy` is
    /// `Retry` without `fallback_retry`.
    pub fn new(
        stream: String,
        fallback_strategy: DlqFallbackStrategy,
        fallback_retry: Option<RetryConfig>,
    ) -> Result<Self, String> {
        if stream.is_empty() {
            return Err("DLQ stream name cannot be empty".to_string());
        }

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

    /// Parse DLQ settings from `error.dlq.*` properties.
    ///
    /// - `error.dlq.stream` is required.
    /// - `error.dlq.fallback-strategy` defaults to `log`.
    /// - `error.dlq.fallback-retry.*` is read when the fallback is `retry`. It takes
    ///   the same keys as `error.retry.*`. With none set, the retry defaults apply.
    ///
    /// # Errors
    ///
    /// Returns an error message if the stream is missing or empty, or any value
    /// fails to parse.
    pub fn from_flat_config(config: &FlatConfig) -> Result<Self, String> {
        let stream = config
            .get(DLQ_STREAM_KEY)
            .ok_or("error.strategy='dlq' requires 'error.dlq.stream' property")?
            .clone();

        if stream.is_empty() {
            return Err("error.dlq.stream cannot be empty".to_string());
        }

        let fallback_strategy = config
            .get("error.dlq.fallback-strategy")
            .map(|s| DlqFallbackStrategy::from_str(s))
            .transpose()?
            .unwrap_or(DlqFallbackStrategy::Log);

        // With no fallback retry keys set, this gives the retry defaults.
        let fallback_retry = match fallback_strategy {
            DlqFallbackStrategy::Retry => Some(RetryConfig::from_flat_config(
                config,
                DLQ_FALLBACK_RETRY_KEY_PREFIX,
            )?),
            DlqFallbackStrategy::Log | DlqFallbackStrategy::Fail => None,
        };

        Self::new(stream, fallback_strategy, fallback_retry)
    }
}

/// Build a DLQ event for a failed event.
///
/// Values follow [`DLQ_FIELDS`]:
/// - originalEvent STRING. The original event serialized as JSON. If that fails, a
///   JSON object with an `error` field, and a warning on stderr.
/// - errorMessage STRING. The error's display text.
/// - errorType STRING. The `EventFluxError` variant name.
/// - timestamp BIGINT. The current time in milliseconds since the Unix epoch.
/// - attemptCount INT. `attempt_count`.
/// - streamName STRING. `stream_name`.
///
/// The event's own timestamp is the same current time.
///
/// # Arguments
/// * `original_event` - The event that failed.
/// * `error` - The error that caused the failure.
/// * `attempt_count` - Attempts made, counting the first one.
/// * `stream_name` - Name of the source stream.
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

    let original_event_json = match serde_json::to_string(original_event) {
        Ok(json) => json,
        Err(e) => {
            eprintln!("Warning: Failed to serialize original event: {}", e);
            format!("{{\"error\": \"Failed to serialize: {}\"}}", e)
        }
    };

    // Variant name only, not the full Debug output.
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
        EventFluxError::ShutdownRejected { .. } => "ShutdownRejected",
        EventFluxError::ProcessorError { .. } => "ProcessorError",
        EventFluxError::Other(_) => "Other",
        EventFluxError::Sqlite(_) => "Sqlite",
        EventFluxError::Io(_) => "Io",
        EventFluxError::Serialization(_) => "Serialization",
    }
    .to_string();

    // Values in DLQ_FIELDS order.
    Event::new_with_data(
        error_timestamp, // event timestamp
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
        // No fallback-retry.* properties, so the retry defaults apply.

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

    #[test]
    fn test_create_dlq_event_values_follow_dlq_fields_order() {
        // DLQ streams are checked against DLQ_FIELDS by position, so the values
        // written must match it type for type.
        let dlq_event = create_dlq_event(
            &Event::new_with_data(1, vec![AttributeValue::Int(1)]),
            &EventFluxError::Other("e".to_string()),
            1,
            "S",
        );

        let emitted: Vec<AttributeType> = dlq_event
            .data
            .iter()
            .map(|value| match value {
                AttributeValue::String(_) => AttributeType::STRING,
                AttributeValue::Long(_) => AttributeType::LONG,
                AttributeValue::Int(_) => AttributeType::INT,
                other => panic!("unexpected DLQ value {other:?}"),
            })
            .collect();
        let declared: Vec<AttributeType> = DLQ_FIELDS.iter().map(|(_, t)| *t).collect();
        assert_eq!(emitted, declared);
    }
}
