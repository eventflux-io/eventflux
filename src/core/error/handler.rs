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

//! # Error Handler - Runtime Error Processing Component
//!
//! This module provides the runtime error handling component that applies
//! configured error strategies during event processing.
//!
//! ## Overview
//!
//! The `ErrorHandler` is used by Source implementations to handle errors
//! according to the configured strategy (Drop, Retry, DLQ, Fail).
//!
//! ## Usage Example
//!
//! ```rust,ignore
//! let mut error_handler = ErrorHandler::new(
//!     error_config,
//!     Some(dlq_junction),
//!     "OrdersStream".to_string(),
//! );
//!
//! // In source event loop
//! match source.receive_event() {
//!     Ok(event) => { /* process */ },
//!     Err(e) => {
//!         match error_handler.handle_error(&event, &e) {
//!             ErrorAction::Retry { delay } => {
//!                 thread::sleep(delay);
//!                 continue; // Retry
//!             }
//!             ErrorAction::Drop => continue, // Skip event
//!             ErrorAction::SendToDlq => { /* already sent */ }
//!             ErrorAction::Fail => return, // Stop source
//!         }
//!     }
//! }
//! ```

use crate::core::error::{create_dlq_event, DlqFallbackStrategy, ErrorConfig, ErrorStrategy};
use crate::core::event::Event;
use crate::core::exception::EventFluxError;
use crate::core::stream::input::input_handler::InputHandler;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Action to take after error handling
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorAction {
    /// Retry the operation after the specified delay
    Retry { delay: Duration },

    /// Drop the event and continue processing
    Drop,

    /// Event was sent to DLQ, continue processing
    SendToDlq,

    /// Fail the source/sink (stop processing)
    Fail,
}

/// Runtime error handler that applies error strategies
///
/// This component encapsulates all error handling logic including:
/// - Retry with exponential backoff
/// - DLQ event delivery
/// - Fallback strategy application
/// - Consecutive error tracking
pub struct ErrorHandler {
    /// Error configuration
    config: ErrorConfig,

    /// DLQ stream junction (if error.strategy=dlq)
    dlq_junction: Option<Arc<Mutex<InputHandler>>>,

    /// Source stream name (for DLQ events)
    stream_name: String,

    /// Counter for consecutive errors (for retry logic)
    consecutive_errors: usize,
}

impl ErrorHandler {
    /// Create a new error handler
    ///
    /// # Arguments
    /// * `config` - Error handling configuration
    /// * `dlq_junction` - Optional DLQ stream junction (required if strategy=dlq)
    /// * `stream_name` - Name of the source stream
    pub fn new(
        config: ErrorConfig,
        dlq_junction: Option<Arc<Mutex<InputHandler>>>,
        stream_name: String,
    ) -> Self {
        Self {
            config,
            dlq_junction,
            stream_name,
            consecutive_errors: 0,
        }
    }

    /// Handle an error and return the action to take
    ///
    /// This is the main entry point for error handling. It applies the
    /// configured strategy and returns the appropriate action.
    ///
    /// # Arguments
    /// * `event` - The event that failed (may be None if error occurred before event creation)
    /// * `error` - The error that occurred
    ///
    /// # Returns
    /// `ErrorAction` indicating what the caller should do next
    pub fn handle_error(&mut self, event: Option<&Event>, error: &EventFluxError) -> ErrorAction {
        // Log error if configured
        if self.config.should_log(super::config::LogLevel::Error) {
            eprintln!(
                "[{}] Error (attempt {}): {} [category: {:?}, retriable: {}]",
                self.stream_name,
                self.consecutive_errors + 1,
                error,
                error.category(),
                error.is_retriable()
            );
        }

        // Apply strategy
        match self.config.strategy {
            ErrorStrategy::Drop => self.handle_drop(error),
            ErrorStrategy::Retry => self.handle_retry(error),
            ErrorStrategy::Dlq => self.handle_dlq(event, error),
            ErrorStrategy::Fail => self.handle_fail(error),
        }
    }

    /// Reset consecutive error counter (call after successful processing)
    pub fn reset_consecutive_errors(&mut self) {
        if self.consecutive_errors > 0 {
            self.consecutive_errors = 0;
        }
    }

    /// Get current consecutive error count
    pub fn consecutive_error_count(&self) -> usize {
        self.consecutive_errors
    }

    /// Set the DLQ junction for error routing
    ///
    /// This method allows setting the DLQ junction after handler creation,
    /// which is needed when the factory creates a source before the DLQ
    /// junction is available (stream_initializer wires it later).
    ///
    /// # Arguments
    /// * `junction` - The InputHandler for the DLQ stream
    pub fn set_dlq_junction(&mut self, junction: Arc<Mutex<InputHandler>>) {
        self.dlq_junction = Some(junction);
    }

    /// Check if error should be retried based on retry config
    fn should_retry(&self, error: &EventFluxError) -> bool {
        // Only retry if error is retriable
        if !error.is_retriable() {
            return false;
        }

        // Check if we have retry config
        if let Some(retry_config) = &self.config.retry {
            self.consecutive_errors < retry_config.max_attempts
        } else {
            false
        }
    }

    /// Handle Drop strategy
    fn handle_drop(&mut self, error: &EventFluxError) -> ErrorAction {
        if self.config.should_log(super::config::LogLevel::Warn) {
            eprintln!(
                "[{}] Dropping event due to error: {}",
                self.stream_name, error
            );
        }
        self.consecutive_errors = 0; // Reset for next event
        ErrorAction::Drop
    }

    /// Handle Retry strategy
    fn handle_retry(&mut self, error: &EventFluxError) -> ErrorAction {
        // Increment error counter
        self.consecutive_errors += 1;

        // Check if we should retry
        if self.should_retry(error) {
            // Calculate backoff delay
            let retry_config = self.config.retry.as_ref().unwrap();
            let delay = retry_config.calculate_delay(self.consecutive_errors);

            if self.config.should_log(super::config::LogLevel::Warn) {
                eprintln!(
                    "[{}] Retriable error (attempt {}/{}). Retrying in {:?}",
                    self.stream_name, self.consecutive_errors, retry_config.max_attempts, delay
                );
            }

            ErrorAction::Retry { delay }
        } else {
            // Max retries exceeded or error not retriable
            if error.is_retriable() {
                eprintln!(
                    "[{}] Max retries ({}) exceeded. Dropping event.",
                    self.stream_name,
                    self.config
                        .retry
                        .as_ref()
                        .map(|r| r.max_attempts)
                        .unwrap_or(0)
                );
            } else {
                eprintln!(
                    "[{}] Non-retriable error. Dropping event.",
                    self.stream_name
                );
            }

            self.consecutive_errors = 0; // Reset for next event
            ErrorAction::Drop
        }
    }

    /// Handle DLQ strategy
    fn handle_dlq(&mut self, event: Option<&Event>, error: &EventFluxError) -> ErrorAction {
        self.consecutive_errors += 1;

        // Need an event to send to DLQ
        let Some(event) = event else {
            eprintln!(
                "[{}] Cannot send to DLQ: no event available",
                self.stream_name
            );
            return ErrorAction::Drop;
        };

        // Send to DLQ
        match self.send_to_dlq(event, error) {
            Ok(()) => {
                if self.config.should_log(super::config::LogLevel::Info) {
                    eprintln!(
                        "[{}] Event sent to DLQ after {} attempts",
                        self.stream_name, self.consecutive_errors
                    );
                }
                self.consecutive_errors = 0; // Reset for next event
                ErrorAction::SendToDlq
            }
            Err(dlq_error) => {
                // DLQ delivery failed - apply fallback strategy
                eprintln!(
                    "[{}] DLQ delivery failed: {}. Applying fallback strategy.",
                    self.stream_name, dlq_error
                );
                self.apply_dlq_fallback(event, error, &dlq_error)
            }
        }
    }

    /// Handle Fail strategy
    fn handle_fail(&mut self, error: &EventFluxError) -> ErrorAction {
        eprintln!(
            "[{}] FATAL: Error occurred with strategy=fail. Stopping source: {}",
            self.stream_name, error
        );
        ErrorAction::Fail
    }

    /// Send event to DLQ stream
    fn send_to_dlq(&self, event: &Event, error: &EventFluxError) -> Result<(), EventFluxError> {
        let dlq_config = self
            .config
            .dlq
            .as_ref()
            .ok_or_else(|| EventFluxError::Configuration {
                message: "DLQ strategy configured but no DLQ config found".to_string(),
                config_key: Some("error.dlq".to_string()),
            })?;

        let dlq_junction = self.dlq_junction.as_ref().ok_or_else(|| {
            EventFluxError::Configuration {
                message: format!(
                    "DLQ stream '{}' junction not found. DLQ stream may not exist or not be initialized.",
                    dlq_config.stream
                ),
                config_key: Some("error.dlq.stream".to_string()),
            }
        })?;

        // Create DLQ event with exact schema
        let dlq_event = create_dlq_event(event, error, self.consecutive_errors, &self.stream_name);

        // Send to DLQ junction
        dlq_junction
            .lock()
            .unwrap()
            .send_single_event(dlq_event)
            .map_err(|e| EventFluxError::SendError {
                message: format!("Failed to send event to DLQ stream: {}", e),
            })
    }

    /// Apply DLQ fallback strategy when DLQ delivery fails
    fn apply_dlq_fallback(
        &mut self,
        event: &Event,
        original_error: &EventFluxError,
        dlq_error: &EventFluxError,
    ) -> ErrorAction {
        let dlq_config = self.config.dlq.as_ref().unwrap();

        match dlq_config.fallback_strategy {
            DlqFallbackStrategy::Log => {
                eprintln!(
                    "[{}] DLQ FALLBACK: Logging and dropping event",
                    self.stream_name
                );
                eprintln!("  Original error: {}", original_error);
                eprintln!("  DLQ error: {}", dlq_error);
                eprintln!("  Event: {:?}", event);
                self.consecutive_errors = 0;
                ErrorAction::Drop
            }

            DlqFallbackStrategy::Fail => {
                eprintln!(
                    "[{}] DLQ FALLBACK: Failing source (fallback-strategy=fail)",
                    self.stream_name
                );
                eprintln!("  Original error: {}", original_error);
                eprintln!("  DLQ error: {}", dlq_error);
                ErrorAction::Fail
            }

            DlqFallbackStrategy::Retry => {
                // Retry DLQ delivery with fallback retry config
                if let Some(fallback_retry) = &dlq_config.fallback_retry {
                    if self.consecutive_errors < fallback_retry.max_attempts {
                        let delay = fallback_retry.calculate_delay(self.consecutive_errors);
                        eprintln!(
                            "[{}] DLQ FALLBACK: Retrying DLQ delivery (attempt {}/{})",
                            self.stream_name, self.consecutive_errors, fallback_retry.max_attempts
                        );
                        return ErrorAction::Retry { delay };
                    }
                }

                // Max fallback retries exceeded
                eprintln!(
                    "[{}] DLQ FALLBACK: Max retries exceeded. Logging and dropping.",
                    self.stream_name
                );
                eprintln!("  Original error: {}", original_error);
                eprintln!("  DLQ error: {}", dlq_error);
                self.consecutive_errors = 0;
                ErrorAction::Drop
            }
        }
    }
}

impl std::fmt::Debug for ErrorHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErrorHandler")
            .field("strategy", &self.config.strategy)
            .field("log_level", &self.config.log_level)
            .field("stream_name", &self.stream_name)
            .field("consecutive_errors", &self.consecutive_errors)
            .field("has_dlq_junction", &self.dlq_junction.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::FlatConfig;
    use crate::core::error::{BackoffStrategy, RetryConfig};
    use crate::core::event::AttributeValue;

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
    fn test_drop_strategy() {
        let config = ErrorConfig::default(); // Default is Drop
        let mut handler = ErrorHandler::new(config, None, "TestStream".to_string());

        let error = EventFluxError::ConnectionUnavailable {
            message: "Test error".to_string(),
            source: None,
        };

        let action = handler.handle_error(Some(&create_test_event()), &error);
        assert_eq!(action, ErrorAction::Drop);
        assert_eq!(handler.consecutive_error_count(), 0); // Reset after drop
    }

    #[test]
    fn test_retry_strategy_retriable_error() {
        let retry_config = RetryConfig {
            max_attempts: 3,
            backoff: BackoffStrategy::Fixed,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
        };

        let config = ErrorConfig::new(
            ErrorStrategy::Retry,
            super::super::config::LogLevel::Warn,
            Some(retry_config),
            None,
            None,
        )
        .unwrap();

        let mut handler = ErrorHandler::new(config, None, "TestStream".to_string());

        let error = EventFluxError::ConnectionUnavailable {
            message: "Transient error".to_string(),
            source: None,
        };

        // First attempt - should retry
        let action1 = handler.handle_error(Some(&create_test_event()), &error);
        assert!(matches!(action1, ErrorAction::Retry { .. }));
        assert_eq!(handler.consecutive_error_count(), 1);

        // Second attempt - should retry
        let action2 = handler.handle_error(Some(&create_test_event()), &error);
        assert!(matches!(action2, ErrorAction::Retry { .. }));
        assert_eq!(handler.consecutive_error_count(), 2);

        // Third attempt - max retries (3) exceeded, should drop
        let action3 = handler.handle_error(Some(&create_test_event()), &error);
        assert_eq!(action3, ErrorAction::Drop);
        assert_eq!(handler.consecutive_error_count(), 0); // Reset after drop
    }

    #[test]
    fn test_retry_strategy_non_retriable_error() {
        let retry_config = RetryConfig::default();
        let config = ErrorConfig::new(
            ErrorStrategy::Retry,
            super::super::config::LogLevel::Warn,
            Some(retry_config),
            None,
            None,
        )
        .unwrap();

        let mut handler = ErrorHandler::new(config, None, "TestStream".to_string());

        let error = EventFluxError::Configuration {
            message: "Bad config".to_string(),
            config_key: None,
        };

        // Non-retriable error should drop immediately
        let action = handler.handle_error(Some(&create_test_event()), &error);
        assert_eq!(action, ErrorAction::Drop);
    }

    #[test]
    fn test_fail_strategy() {
        let config = ErrorConfig::new(
            ErrorStrategy::Fail,
            super::super::config::LogLevel::Error,
            None,
            None,
            Some(super::super::config::FailConfig::default()),
        )
        .unwrap();

        let mut handler = ErrorHandler::new(config, None, "TestStream".to_string());

        let error = EventFluxError::Other("Any error".to_string());

        let action = handler.handle_error(Some(&create_test_event()), &error);
        assert_eq!(action, ErrorAction::Fail);
    }

    #[test]
    fn test_reset_consecutive_errors() {
        let retry_config = RetryConfig::default();
        let config = ErrorConfig::new(
            ErrorStrategy::Retry,
            super::super::config::LogLevel::Warn,
            Some(retry_config),
            None,
            None,
        )
        .unwrap();

        let mut handler = ErrorHandler::new(config, None, "TestStream".to_string());

        let error = EventFluxError::ConnectionUnavailable {
            message: "Test".to_string(),
            source: None,
        };

        // Trigger some errors
        handler.handle_error(Some(&create_test_event()), &error);
        handler.handle_error(Some(&create_test_event()), &error);
        assert_eq!(handler.consecutive_error_count(), 2);

        // Reset
        handler.reset_consecutive_errors();
        assert_eq!(handler.consecutive_error_count(), 0);
    }

    #[test]
    fn test_is_retriable() {
        // Retriable errors
        assert!(EventFluxError::ConnectionUnavailable {
            message: "test".to_string(),
            source: None
        }
        .is_retriable());
        assert!(
            EventFluxError::Io(std::io::Error::from(std::io::ErrorKind::TimedOut)).is_retriable()
        );
        assert!(EventFluxError::SendError {
            message: "test".to_string()
        }
        .is_retriable());

        // Non-retriable errors
        assert!(!EventFluxError::Configuration {
            message: "test".to_string(),
            config_key: None
        }
        .is_retriable());
        assert!(!EventFluxError::InvalidParameter {
            message: "test".to_string(),
            parameter: None,
            expected: None
        }
        .is_retriable());
        assert!(!EventFluxError::MappingFailed {
            message: "test".to_string(),
            source: None
        }
        .is_retriable());
    }
}
