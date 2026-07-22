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

//! # Source Error Handling Support
//!
//! This module provides helper utilities for Source implementations to integrate
//! error handling with minimal boilerplate.
//!
//! ## Overview
//!
//! The `SourceErrorContext` encapsulates error handling state and provides
//! convenient methods for sources to handle errors in their event loops.
//!
//! ## Usage Example
//!
//! ```rust,ignore
//! pub struct MySource {
//!     error_ctx: Option<SourceErrorContext>,
//!     // ... other fields
//! }
//!
//! impl Source for MySource {
//!     fn start(&mut self, handler: Arc<Mutex<InputHandler>>) {
//!         loop {
//!             match self.fetch_event() {
//!                 Ok(event) => {
//!                     // Success - send event and reset error counter
//!                     handler.lock().unwrap().send_event(event).ok();
//!                     if let Some(ctx) = &mut self.error_ctx {
//!                         ctx.reset_errors();
//!                     }
//!                 }
//!                 Err(e) => {
//!                     // Error - use context to handle
//!                     if let Some(ctx) = &mut self.error_ctx {
//!                         if !ctx.handle_error(Some(&event), &e) {
//!                             return; // Stop source
//!                         }
//!                     }
//!                 }
//!             }
//!         }
//!     }
//! }
//! ```

use super::handler::{ErrorAction, ErrorHandler};
use crate::core::config::FlatConfig;
use crate::core::error::ErrorConfig;
use crate::core::event::Event;
use crate::core::exception::EventFluxError;

/// Optional configuration keys consumed by
/// [`SourceErrorContext::from_properties`] (via `ErrorConfig`). Source
/// factories append these to their `optional_parameters()` list, so the
/// error-handling surface is defined in exactly one place.
///
/// Delays are duration strings (`100ms`, `30s`); `error.retry.backoff` is a
/// strategy name (`exponential`/`linear`/`fixed`). The DLQ fallback retry
/// family (`error.dlq.fallback-retry.*`) is a prefix, represented here by
/// its strategy key.
pub const SOURCE_ERROR_PARAMETERS: &[&str] = &[
    "error.strategy",
    "error.log-level",
    "error.retry.max-attempts",
    "error.retry.backoff",
    "error.retry.initial-delay",
    "error.retry.max-delay",
    "error.dlq.stream",
    "error.dlq.fallback-strategy",
];
use crate::core::stream::input::input_handler::InputHandler;
use std::sync::{Arc, Mutex};
use std::thread;

/// Source error handling context
///
/// Encapsulates error handling state and provides convenient methods
/// for sources to integrate error handling.
pub struct SourceErrorContext {
    /// The error handler
    handler: ErrorHandler,
}

impl SourceErrorContext {
    /// Create a new source error context
    ///
    /// # Arguments
    /// * `error_config` - Error handling configuration
    /// * `dlq_junction` - Optional DLQ stream junction
    /// * `stream_name` - Name of the source stream
    pub fn new(
        error_config: ErrorConfig,
        dlq_junction: Option<Arc<Mutex<InputHandler>>>,
        stream_name: String,
    ) -> Self {
        Self {
            handler: ErrorHandler::new(error_config, dlq_junction, stream_name),
        }
    }

    /// Create from FlatConfig (convenience method)
    ///
    /// # Arguments
    /// * `config` - Flat configuration with error.* properties
    /// * `dlq_junction` - Optional DLQ stream junction
    /// * `stream_name` - Name of the source stream
    pub fn from_config(
        config: &FlatConfig,
        dlq_junction: Option<Arc<Mutex<InputHandler>>>,
        stream_name: String,
    ) -> Result<Self, String> {
        let error_config = ErrorConfig::from_flat_config(config)?;
        Ok(Self::new(error_config, dlq_junction, stream_name))
    }

    /// Build from a raw `WITH`-clause properties map (convenience method).
    ///
    /// Returns `Ok(None)` when no `error.*` properties are configured —
    /// exactly the optional-context shape every source stores. Collapses the
    /// ErrorConfigBuilder/FlatConfig boilerplate previously copy-pasted into
    /// each connector's `from_properties`.
    pub fn from_properties(
        properties: &std::collections::HashMap<String, String>,
        dlq_junction: Option<Arc<Mutex<InputHandler>>>,
        stream_name: &str,
    ) -> Result<Option<Self>, String> {
        if !ErrorConfigBuilder::from_properties(properties).is_configured() {
            return Ok(None);
        }

        use crate::core::config::PropertySource;
        let mut flat_config = FlatConfig::new();
        for (key, value) in properties {
            if key.starts_with("error.") {
                flat_config.set(key.clone(), value.clone(), PropertySource::SqlWith);
            }
        }

        Self::from_config(&flat_config, dlq_junction, stream_name.to_string()).map(Some)
    }

    /// Handle an error and return whether to continue processing
    ///
    /// This method applies the error handling strategy and performs any
    /// necessary actions (retry delays, DLQ sending, etc.).
    ///
    /// # Arguments
    /// * `event` - The event that failed (if available)
    /// * `error` - The error that occurred
    ///
    /// # Returns
    /// * `true` - Continue processing (error was handled, drop/retry/dlq)
    /// * `false` - Stop processing (fail strategy or unrecoverable error)
    pub fn handle_error(&mut self, event: Option<&Event>, error: &EventFluxError) -> bool {
        let action = self.handle_error_with_action(event, error);
        !matches!(action, ErrorAction::Fail)
    }

    /// Handle an error and return the action taken
    ///
    /// This method is similar to `handle_error` but returns the `ErrorAction`
    /// so callers can make decisions based on the specific action (e.g., whether
    /// to requeue a message in a message broker).
    ///
    /// # Arguments
    /// * `event` - The event that failed (if available)
    /// * `error` - The error that occurred
    ///
    /// # Returns
    /// The `ErrorAction` that was taken:
    /// - `Retry { delay }` - Internal retry with delay (delay already applied)
    /// - `Drop` - Event was dropped
    /// - `SendToDlq` - Event was sent to DLQ
    /// - `Fail` - Source should stop processing
    pub fn handle_error_with_action(
        &mut self,
        event: Option<&Event>,
        error: &EventFluxError,
    ) -> ErrorAction {
        let action = self.handler.handle_error(event, error);

        if let ErrorAction::Retry { delay } = &action {
            // Note: This runs on blocking source threads (spawn_blocking), not the async executor.
            // thread::sleep is intentional here — sources are synchronous.
            thread::sleep(*delay);
        }

        action
    }

    /// Reset error counters (call after successful event processing)
    #[inline]
    pub fn reset_errors(&mut self) {
        self.handler.reset_consecutive_errors();
    }

    /// Get consecutive error count
    #[inline]
    pub fn error_count(&self) -> usize {
        self.handler.consecutive_error_count()
    }

    /// Get the underlying error handler (for advanced use cases)
    #[inline]
    pub fn handler(&self) -> &ErrorHandler {
        &self.handler
    }

    /// Get mutable reference to error handler (for advanced use cases)
    #[inline]
    pub fn handler_mut(&mut self) -> &mut ErrorHandler {
        &mut self.handler
    }

    /// Set the DLQ junction for error routing
    ///
    /// This method allows setting the DLQ junction after context creation,
    /// which is needed when the factory creates a source before the DLQ
    /// junction is available (stream_initializer wires it later).
    ///
    /// # Arguments
    /// * `junction` - The InputHandler for the DLQ stream
    #[inline]
    pub fn set_dlq_junction(&mut self, junction: Arc<Mutex<InputHandler>>) {
        self.handler.set_dlq_junction(junction);
    }
}

impl std::fmt::Debug for SourceErrorContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceErrorContext")
            .field("handler", &self.handler)
            .finish()
    }
}

/// Builder for creating ErrorConfig from configuration properties
///
/// This provides a convenient way for factories to extract error configuration
/// during source/sink creation.
pub struct ErrorConfigBuilder {
    config: FlatConfig,
}

impl ErrorConfigBuilder {
    /// Create a new builder from properties
    pub fn from_properties(properties: &std::collections::HashMap<String, String>) -> Self {
        use crate::core::config::PropertySource;

        let mut config = FlatConfig::new();
        for (key, value) in properties {
            if key.starts_with("error.") {
                config.set(key.clone(), value.clone(), PropertySource::SqlWith);
            }
        }

        Self { config }
    }

    /// Check if error handling is configured
    pub fn is_configured(&self) -> bool {
        self.config.contains("error.strategy")
    }

    /// Build ErrorConfig (returns None if not configured)
    pub fn build(&self) -> Result<Option<ErrorConfig>, String> {
        if !self.is_configured() {
            return Ok(None);
        }

        let error_config = ErrorConfig::from_flat_config(&self.config)?;
        Ok(Some(error_config))
    }

    /// Build ErrorConfig with default if not configured
    pub fn build_or_default(&self) -> Result<ErrorConfig, String> {
        Ok(self.build()?.unwrap_or_default())
    }
}

/// Outcome of delivering one payload through the error-handling strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryVerdict {
    /// The pipeline accepted the payload
    Delivered,
    /// The error strategy disposed of it (drop or DLQ) — the record counts
    /// as consumed and may be acknowledged/committed
    Disposed,
    /// Unrecoverable (fail strategy) — stop the source; do NOT acknowledge
    /// or commit, so the record is redelivered after restart
    Fail,
}

/// Deliver one payload to the pipeline, applying the source's error strategy.
///
/// Encapsulates the retry state machine every source needs: retry until
/// success or a non-retry action, build a DLQ fallback event from the raw
/// bytes, reset the error counter on success. Transport-specific
/// acknowledgement stays with the caller, keyed off the verdict:
/// `Delivered`/`Disposed` → ack/commit, `Fail` → don't ack, stop the source.
pub fn deliver_with_error_handling(
    callback: &dyn crate::core::stream::input::source::SourceCallback,
    payload: &[u8],
    error_ctx: &mut Option<SourceErrorContext>,
    source_tag: &str,
) -> DeliveryVerdict {
    loop {
        match callback.on_data(payload) {
            Ok(()) => {
                if let Some(ctx) = error_ctx {
                    ctx.reset_errors();
                }
                return DeliveryVerdict::Delivered;
            }
            Err(e) => {
                let action = if let Some(ctx) = error_ctx {
                    // Fallback event from raw bytes for DLQ support — only
                    // built when a strategy exists to consume it
                    let fallback_event = Event::new_with_data(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or(0),
                        vec![crate::core::event::value::AttributeValue::Bytes(
                            payload.to_vec(),
                        )],
                    );
                    ctx.handle_error_with_action(Some(&fallback_event), &e)
                } else {
                    log::error!("[{source_tag}] Callback error: {e}");
                    ErrorAction::Drop
                };

                match action {
                    // Delay already applied by handle_error_with_action
                    ErrorAction::Retry { .. } => continue,
                    ErrorAction::Drop | ErrorAction::SendToDlq => return DeliveryVerdict::Disposed,
                    ErrorAction::Fail => return DeliveryVerdict::Fail,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::error::{BackoffStrategy, RetryConfig};
    use crate::core::event::AttributeValue;
    use std::collections::HashMap;

    fn create_test_event() -> Event {
        Event::new_with_data(
            123,
            vec![
                AttributeValue::String("test".to_string()),
                AttributeValue::Int(42),
            ],
        )
    }

    #[test]
    fn test_source_error_context_creation() {
        let error_config = ErrorConfig::default();
        let ctx = SourceErrorContext::new(error_config, None, "TestStream".to_string());

        assert_eq!(ctx.error_count(), 0);
    }

    #[test]
    fn test_source_error_context_handle_drop() {
        let error_config = ErrorConfig::default(); // Default is Drop
        let mut ctx = SourceErrorContext::new(error_config, None, "TestStream".to_string());

        let error = EventFluxError::Other("Test error".to_string());

        // Should continue processing (drop)
        assert!(ctx.handle_error(Some(&create_test_event()), &error));
    }

    #[test]
    fn test_source_error_context_handle_fail() {
        use crate::core::error::{ErrorStrategy, FailConfig, LogLevel};

        let error_config = ErrorConfig::new(
            ErrorStrategy::Fail,
            LogLevel::Error,
            None,
            None,
            Some(FailConfig::default()),
        )
        .unwrap();

        let mut ctx = SourceErrorContext::new(error_config, None, "TestStream".to_string());

        let error = EventFluxError::Other("Fatal error".to_string());

        // Should stop processing (fail)
        assert!(!ctx.handle_error(Some(&create_test_event()), &error));
    }

    #[test]
    fn test_source_error_context_reset() {
        use crate::core::error::{ErrorStrategy, LogLevel};

        let retry_config = RetryConfig {
            max_attempts: 3,
            backoff: BackoffStrategy::Fixed,
            initial_delay: std::time::Duration::from_millis(1),
            max_delay: std::time::Duration::from_secs(1),
        };

        let error_config = ErrorConfig::new(
            ErrorStrategy::Retry,
            LogLevel::Warn,
            Some(retry_config),
            None,
            None,
        )
        .unwrap();

        let mut ctx = SourceErrorContext::new(error_config, None, "TestStream".to_string());

        let error = EventFluxError::ConnectionUnavailable {
            message: "Test".to_string(),
            source: None,
        };

        // Trigger error
        ctx.handle_error(Some(&create_test_event()), &error);
        assert_eq!(ctx.error_count(), 1);

        // Reset
        ctx.reset_errors();
        assert_eq!(ctx.error_count(), 0);
    }

    #[test]
    fn test_error_config_builder_empty() {
        let properties = HashMap::new();
        let builder = ErrorConfigBuilder::from_properties(&properties);

        assert!(!builder.is_configured());
        assert!(builder.build().unwrap().is_none());
    }

    #[test]
    fn test_error_config_builder_with_config() {
        let mut properties = HashMap::new();
        properties.insert("error.strategy".to_string(), "drop".to_string());
        properties.insert("error.log-level".to_string(), "warn".to_string());

        let builder = ErrorConfigBuilder::from_properties(&properties);

        assert!(builder.is_configured());

        let config = builder.build().unwrap().unwrap();
        assert_eq!(config.strategy, crate::core::error::ErrorStrategy::Drop);
        assert_eq!(config.log_level, crate::core::error::LogLevel::Warn);
    }

    #[test]
    fn test_error_config_builder_default() {
        let properties = HashMap::new();
        let builder = ErrorConfigBuilder::from_properties(&properties);

        let config = builder.build_or_default().unwrap();
        assert_eq!(config.strategy, crate::core::error::ErrorStrategy::Drop);
    }

    #[test]
    fn test_from_config() {
        use crate::core::config::PropertySource;

        let mut config = FlatConfig::new();
        config.set("error.strategy", "retry", PropertySource::SqlWith);
        config.set("error.retry.max-attempts", "5", PropertySource::SqlWith);

        let ctx = SourceErrorContext::from_config(&config, None, "TestStream".to_string());
        assert!(ctx.is_ok());

        let ctx = ctx.unwrap();
        assert_eq!(ctx.error_count(), 0);
    }
}
