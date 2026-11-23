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

//! # Error handler
//!
//! Applies the configured error strategy while events are processed.
//!
//! ## Overview
//!
//! Sources use [`ErrorHandler`] to handle each failure with the configured
//! strategy (Drop, Retry, DLQ or Fail). It returns an [`ErrorAction`] that tells
//! the source what to do next.
//!
//! ## Usage example
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

/// What the caller should do after an error is handled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorAction {
    /// Wait `delay`, then retry the operation. The caller does the waiting.
    Retry { delay: Duration },

    /// Drop the event and carry on.
    Drop,

    /// The event was sent to the DLQ. Carry on.
    SendToDlq,

    /// Stop the source or sink.
    Fail,

    /// The worker was told to stop while the handler was retrying.
    ///
    /// Stop processing, but leave the current record unacknowledged and
    /// uncommitted so it is delivered again after a restart. Unlike [`Fail`],
    /// which marks the record as bad (RabbitMQ nacks it without requeue),
    /// `Cancelled` must never discard the record.
    ///
    /// [`Fail`]: ErrorAction::Fail
    Cancelled,
}

/// Applies the configured error strategy to each failure.
///
/// Handles retry with backoff, DLQ delivery, the DLQ fallback, and counts
/// failures in a row for the current event.
pub struct ErrorHandler {
    config: ErrorConfig,

    /// Input handler of the DLQ stream. Needed for DLQ delivery.
    dlq_junction: Option<Arc<Mutex<InputHandler>>>,

    /// Source stream name, written into DLQ events.
    stream_name: String,

    /// Failures in a row for the current event. Drives the retry limit and is
    /// reported as the DLQ `attemptCount`.
    consecutive_errors: usize,
}

impl ErrorHandler {
    /// Create an error handler.
    ///
    /// # Arguments
    /// * `config` - Error handling settings.
    /// * `dlq_junction` - Input handler of the DLQ stream. Needed for DLQ delivery.
    ///   Can also be set later with [`set_dlq_junction`](Self::set_dlq_junction).
    /// * `stream_name` - Name of the source stream.
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

    /// The error settings this handler was built from.
    pub fn config(&self) -> &ErrorConfig {
        &self.config
    }

    /// The DLQ input handler, if one is set.
    pub fn dlq_junction(&self) -> Option<Arc<Mutex<InputHandler>>> {
        self.dlq_junction.clone()
    }

    /// The source stream name written into DLQ events.
    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    /// Handle an error with the configured strategy and return what to do next.
    ///
    /// Blocks the thread with `std::thread::sleep` between DLQ send retries. Use
    /// [`handle_error_with_backoff`](Self::handle_error_with_backoff) to wait in a
    /// way that can be stopped.
    ///
    /// # Arguments
    /// * `event` - The event that failed. `None` if the error happened before an
    ///   event was built, in which case it cannot go to the DLQ and is dropped.
    /// * `error` - The error that happened.
    pub fn handle_error(&mut self, event: Option<&Event>, error: &EventFluxError) -> ErrorAction {
        self.handle_error_with_backoff(event, error, &mut |delay| {
            std::thread::sleep(delay);
            true
        })
    }

    /// [`Self::handle_error`] with a wait function supplied by the caller.
    ///
    /// `wait` sleeps between DLQ send retries (`fallback-strategy=retry`). It
    /// returns `false` if the caller was told to stop, which ends the retries with
    /// [`ErrorAction::Cancelled`]. Sources pass a wait that respects `stop()`.
    /// Source retries under the `Retry` strategy do not call `wait`. They come back
    /// as [`ErrorAction::Retry`] for the caller to wait on.
    pub fn handle_error_with_backoff(
        &mut self,
        event: Option<&Event>,
        error: &EventFluxError,
        wait: &mut dyn FnMut(Duration) -> bool,
    ) -> ErrorAction {
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

        match self.config.strategy {
            ErrorStrategy::Drop => self.handle_drop(error),
            ErrorStrategy::Retry => self.handle_retry(event, error, wait),
            ErrorStrategy::Dlq => self.handle_dlq(event, error, wait),
            ErrorStrategy::Fail => self.handle_fail(error),
        }
    }

    /// Reset the failure count. Call after an event succeeds.
    pub fn reset_consecutive_errors(&mut self) {
        if self.consecutive_errors > 0 {
            self.consecutive_errors = 0;
        }
    }

    /// Returns the failures in a row for the current event.
    pub fn consecutive_error_count(&self) -> usize {
        self.consecutive_errors
    }

    /// Set the DLQ input handler after the handler is built.
    ///
    /// Needed because a factory builds the source before the DLQ stream is
    /// available. The runtime wires it in when the source is attached.
    ///
    /// Also sets the stream name, because factories only know a connector tag
    /// such as a topic or file path. The stream name is what DLQ events report in
    /// their `streamName` field.
    ///
    /// # Arguments
    /// * `stream_name` - Name of the stream whose failed events go to the DLQ.
    /// * `junction` - The input handler of the DLQ stream.
    pub fn set_dlq_junction(&mut self, stream_name: &str, junction: Arc<Mutex<InputHandler>>) {
        self.stream_name = stream_name.to_string();
        self.dlq_junction = Some(junction);
    }

    /// Returns true if the error can be retried and the attempt limit is not
    /// reached yet. `max_attempts` counts the first attempt, so 3 allows 2
    /// retries.
    fn should_retry(&self, error: &EventFluxError) -> bool {
        if !error.is_retriable() {
            return false;
        }

        if let Some(retry_config) = &self.config.retry {
            self.consecutive_errors < retry_config.max_attempts
        } else {
            false
        }
    }

    fn handle_drop(&mut self, error: &EventFluxError) -> ErrorAction {
        if self.config.should_log(super::config::LogLevel::Warn) {
            eprintln!(
                "[{}] Dropping event due to error: {}",
                self.stream_name, error
            );
        }
        self.consecutive_errors = 0; // start fresh for the next event
        ErrorAction::Drop
    }

    /// Ask the caller to retry with a backoff delay while attempts remain.
    ///
    /// Once retries run out, or the error cannot be retried, the event goes to the
    /// DLQ when one is configured and is dropped otherwise.
    fn handle_retry(
        &mut self,
        event: Option<&Event>,
        error: &EventFluxError,
        wait: &mut dyn FnMut(Duration) -> bool,
    ) -> ErrorAction {
        self.consecutive_errors += 1;

        if self.should_retry(error) {
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
            let has_dlq = self.config.dlq.is_some();
            let outcome = if has_dlq {
                "Sending event to DLQ"
            } else {
                "Dropping event"
            };
            if error.is_retriable() {
                eprintln!(
                    "[{}] Max retries ({}) exceeded. {outcome}.",
                    self.stream_name,
                    self.config
                        .retry
                        .as_ref()
                        .map(|r| r.max_attempts)
                        .unwrap_or(0)
                );
            } else {
                eprintln!("[{}] Non-retriable error. {outcome}.", self.stream_name);
            }

            if has_dlq {
                return self.dead_letter(event, error, wait);
            }
            self.consecutive_errors = 0; // start fresh for the next event
            ErrorAction::Drop
        }
    }

    fn handle_dlq(
        &mut self,
        event: Option<&Event>,
        error: &EventFluxError,
        wait: &mut dyn FnMut(Duration) -> bool,
    ) -> ErrorAction {
        self.consecutive_errors += 1;
        self.dead_letter(event, error, wait)
    }

    /// Send a failed event to the DLQ, and apply the DLQ fallback if the send
    /// fails.
    ///
    /// `consecutive_errors` must already count this failure, because it is
    /// reported as the DLQ event's `attemptCount`. With no event, logs and drops.
    fn dead_letter(
        &mut self,
        event: Option<&Event>,
        error: &EventFluxError,
        wait: &mut dyn FnMut(Duration) -> bool,
    ) -> ErrorAction {
        let Some(event) = event else {
            eprintln!(
                "[{}] Cannot send to DLQ: no event available",
                self.stream_name
            );
            self.consecutive_errors = 0; // start fresh for the next event
            return ErrorAction::Drop;
        };

        match self.send_to_dlq(event, error) {
            Ok(()) => {
                if self.config.should_log(super::config::LogLevel::Info) {
                    eprintln!(
                        "[{}] Event sent to DLQ after {} attempts",
                        self.stream_name, self.consecutive_errors
                    );
                }
                self.consecutive_errors = 0; // start fresh for the next event
                ErrorAction::SendToDlq
            }
            Err(dlq_error) => {
                eprintln!(
                    "[{}] DLQ delivery failed: {}. Applying fallback strategy.",
                    self.stream_name, dlq_error
                );
                self.apply_dlq_fallback(event, error, &dlq_error, wait)
            }
        }
    }

    /// Log the error as fatal and tell the caller to stop the source.
    fn handle_fail(&mut self, error: &EventFluxError) -> ErrorAction {
        eprintln!(
            "[{}] FATAL: Error occurred with strategy=fail. Stopping source: {}",
            self.stream_name, error
        );
        ErrorAction::Fail
    }

    /// Build a DLQ event and send it to the DLQ stream.
    ///
    /// # Errors
    ///
    /// Returns `Configuration` if there are no DLQ settings or no DLQ input
    /// handler, and `SendError` if the send fails.
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
                config_key: Some(super::dlq::DLQ_STREAM_KEY.to_string()),
            }
        })?;

        let dlq_event = create_dlq_event(event, error, self.consecutive_errors, &self.stream_name);

        dlq_junction
            .lock()
            .unwrap()
            .send_single_event(dlq_event)
            .map_err(|e| EventFluxError::SendError {
                message: format!("Failed to send event to DLQ stream: {}", e),
            })
    }

    /// Apply the DLQ fallback after a failed DLQ send.
    ///
    /// Panics if there are no DLQ settings. Callers only reach this after a DLQ
    /// send, which needs them.
    fn apply_dlq_fallback(
        &mut self,
        event: &Event,
        original_error: &EventFluxError,
        dlq_error: &EventFluxError,
        wait: &mut dyn FnMut(Duration) -> bool,
    ) -> ErrorAction {
        let dlq_config = self.config.dlq.as_ref().unwrap();
        let fallback_retry = dlq_config.fallback_retry.clone();

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
                // Retry only the DLQ send, with its own attempt limit. The
                // source retry limit (`consecutive_errors`) may already be used
                // up, and running the source callback again would process an
                // event that already failed a second time.
                let retry = fallback_retry.unwrap_or_default();
                let mut last_error = dlq_error.to_string();

                // The failed first send counts as attempt 1.
                for attempt in 2..=retry.max_attempts {
                    let delay = retry.calculate_delay(attempt - 1);
                    eprintln!(
                        "[{}] DLQ FALLBACK: Retrying DLQ delivery in {:?} (attempt {}/{})",
                        self.stream_name, delay, attempt, retry.max_attempts
                    );
                    if !wait(delay) {
                        self.consecutive_errors = 0;
                        return ErrorAction::Cancelled;
                    }
                    match self.send_to_dlq(event, original_error) {
                        Ok(()) => {
                            self.consecutive_errors = 0;
                            return ErrorAction::SendToDlq;
                        }
                        Err(e) => last_error = e.to_string(),
                    }
                }

                eprintln!(
                    "[{}] DLQ FALLBACK: Max retries exceeded. Logging and dropping.",
                    self.stream_name
                );
                eprintln!("  Original error: {}", original_error);
                eprintln!("  DLQ error: {}", last_error);
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

    use crate::core::error::{BackoffStrategy, DlqConfig, RetryConfig};
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
        let config = ErrorConfig::default(); // Drop
        let mut handler = ErrorHandler::new(config, None, "TestStream".to_string());

        let error = EventFluxError::ConnectionUnavailable {
            message: "Test error".to_string(),
            source: None,
        };

        let action = handler.handle_error(Some(&create_test_event()), &error);
        assert_eq!(action, ErrorAction::Drop);
        assert_eq!(handler.consecutive_error_count(), 0); // reset after drop
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

        // First failure retries.
        let action1 = handler.handle_error(Some(&create_test_event()), &error);
        assert!(matches!(action1, ErrorAction::Retry { .. }));
        assert_eq!(handler.consecutive_error_count(), 1);

        // Second failure retries.
        let action2 = handler.handle_error(Some(&create_test_event()), &error);
        assert!(matches!(action2, ErrorAction::Retry { .. }));
        assert_eq!(handler.consecutive_error_count(), 2);

        // Third failure reaches max_attempts (3) and drops.
        let action3 = handler.handle_error(Some(&create_test_event()), &error);
        assert_eq!(action3, ErrorAction::Drop);
        assert_eq!(handler.consecutive_error_count(), 0); // reset after drop
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

        // An error that cannot be retried drops right away.
        let action = handler.handle_error(Some(&create_test_event()), &error);
        assert_eq!(action, ErrorAction::Drop);
    }

    /// Retry strategy whose events go, once retries run out, to a DLQ with no
    /// input handler set. `fallback` applies when that DLQ send fails. Reaching
    /// the fallback proves the event was routed to the DLQ, not dropped.
    fn retry_handler_with_dlq(max_attempts: usize, fallback: DlqFallbackStrategy) -> ErrorHandler {
        let fast_retry = |max_attempts| RetryConfig {
            max_attempts,
            backoff: BackoffStrategy::Fixed,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
        };
        let retry_config = fast_retry(max_attempts);
        let dlq_config = DlqConfig {
            stream: "ErrorStream".to_string(),
            fallback_strategy: fallback,
            // Same limit as the source retries. A shared counter would leave
            // no DLQ retries at all in this case.
            fallback_retry: (fallback == DlqFallbackStrategy::Retry)
                .then(|| fast_retry(max_attempts)),
        };
        let config = ErrorConfig::new(
            ErrorStrategy::Retry,
            super::super::config::LogLevel::Warn,
            Some(retry_config),
            Some(dlq_config),
            None,
        )
        .unwrap();
        ErrorHandler::new(config, None, "TestStream".to_string())
    }

    #[test]
    fn test_retry_strategy_exhausted_retries_go_to_dlq() {
        // max_attempts counts every attempt, so 3 attempts means 2 retries.
        let mut handler = retry_handler_with_dlq(3, DlqFallbackStrategy::Fail);
        let event = create_test_event();
        let error = EventFluxError::ConnectionUnavailable {
            message: "Test error".to_string(),
            source: None,
        };

        for _ in 0..2 {
            let action = handler.handle_error(Some(&event), &error);
            assert!(
                matches!(action, ErrorAction::Retry { .. }),
                "got {action:?}"
            );
        }

        // Retries used up. The event goes to the DLQ, whose send fails because
        // no input handler is set, so fallback-strategy=fail applies.
        let action = handler.handle_error(Some(&event), &error);
        assert_eq!(action, ErrorAction::Fail);
    }

    /// Run `handler` with a wait that counts its calls. Returns the action and
    /// how many DLQ retry waits happened.
    fn handle_counting_waits(
        handler: &mut ErrorHandler,
        error: &EventFluxError,
        allow: bool,
    ) -> (ErrorAction, usize) {
        let mut waits = 0;
        let action =
            handler.handle_error_with_backoff(Some(&create_test_event()), error, &mut |_| {
                waits += 1;
                allow
            });
        (action, waits)
    }

    #[test]
    fn test_dlq_fallback_retry_has_own_budget_after_source_retries() {
        let mut handler = retry_handler_with_dlq(3, DlqFallbackStrategy::Retry);
        let error = EventFluxError::ConnectionUnavailable {
            message: "Test error".to_string(),
            source: None,
        };

        // Use up the source retry limit. 3 attempts means 2 retries.
        for _ in 0..2 {
            let (action, waits) = handle_counting_waits(&mut handler, &error, true);
            assert!(
                matches!(action, ErrorAction::Retry { .. }),
                "got {action:?}"
            );
            assert_eq!(waits, 0, "source retries wait in the caller");
        }

        // Retries used up. The DLQ send always fails because no input handler
        // is set. It is retried on its own limit of 3 attempts, which is 2
        // resends each after a wait. The event never goes back to the source as
        // a `Retry`.
        let (action, waits) = handle_counting_waits(&mut handler, &error, true);
        assert_eq!(action, ErrorAction::Drop);
        assert_eq!(waits, 2);
        assert_eq!(handler.consecutive_error_count(), 0);
    }

    #[test]
    fn test_dlq_fallback_retry_aborts_when_cancelled() {
        let mut handler = retry_handler_with_dlq(1, DlqFallbackStrategy::Retry);
        let error = EventFluxError::Configuration {
            message: "Bad config".to_string(),
            config_key: None,
        };

        // max_attempts = 1 leaves no resends, so it drops without waiting.
        let (action, waits) = handle_counting_waits(&mut handler, &error, false);
        assert_eq!(action, ErrorAction::Drop);
        assert_eq!(waits, 0);

        // With resends left, a refused wait (stop requested) cancels.
        let mut handler = retry_handler_with_dlq(3, DlqFallbackStrategy::Retry);
        let (action, waits) = handle_counting_waits(&mut handler, &error, false);
        assert_eq!(action, ErrorAction::Cancelled);
        assert_eq!(waits, 1);
    }

    #[test]
    fn test_retry_strategy_non_retriable_error_goes_to_dlq() {
        let mut handler = retry_handler_with_dlq(3, DlqFallbackStrategy::Log);
        let error = EventFluxError::Configuration {
            message: "Bad config".to_string(),
            config_key: None,
        };

        // An error that cannot be retried goes straight to the DLQ. Then
        // fallback-strategy=log drops it and resets the counter.
        let action = handler.handle_error(Some(&create_test_event()), &error);
        assert_eq!(action, ErrorAction::Drop);
        assert_eq!(handler.consecutive_error_count(), 0);
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

        handler.handle_error(Some(&create_test_event()), &error);
        handler.handle_error(Some(&create_test_event()), &error);
        assert_eq!(handler.consecutive_error_count(), 2);

        handler.reset_consecutive_errors();
        assert_eq!(handler.consecutive_error_count(), 0);
    }

    #[test]
    fn test_is_retriable() {
        // Can be retried.
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

        // Cannot be retried.
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
