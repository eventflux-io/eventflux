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

//! # Error handling helpers for sources
//!
//! Lets a source add error handling with little extra code.
//!
//! ## Overview
//!
//! [`SourceErrorContext`] holds the error handling state and gives sources simple
//! methods to call from their event loops. [`deliver_with_error_handling`] runs
//! the whole retry loop for one payload.
//!
//! ## Usage example
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

/// Optional property keys read by [`SourceErrorContext::from_properties`]
/// through `ErrorConfig`.
///
/// Source factories add these to their `optional_parameters()` list, so the
/// error handling keys are listed in exactly one place.
///
/// Delays are duration strings such as `100ms` or `30s`. `error.retry.backoff` is
/// `exponential`, `linear` or `fixed`. The DLQ fallback retry keys
/// (`error.dlq.fallback-retry.*`) share a prefix and are not listed one by one.
/// `error.dlq.fallback-strategy` stands in for them.
pub const SOURCE_ERROR_PARAMETERS: &[&str] = &[
    "error.strategy",
    "error.log-level",
    "error.retry.max-attempts",
    "error.retry.backoff",
    "error.retry.initial-delay",
    "error.retry.max-delay",
    super::dlq::DLQ_STREAM_KEY,
    "error.dlq.fallback-strategy",
];
use crate::core::stream::input::input_handler::InputHandler;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Error handling state for one source.
pub struct SourceErrorContext {
    handler: ErrorHandler,
    /// The owning worker's running flag, bound at `start()`. Retry waits are
    /// split into short sleeps that check it, so `stop()` cuts them short instead
    /// of waiting out a user configured delay (#141). An unbound context sleeps
    /// without checking, which is fine at factory time and in tests.
    running: Option<Arc<AtomicBool>>,
}

impl SourceErrorContext {
    /// Create a source error context.
    ///
    /// # Arguments
    /// * `error_config` - Error handling settings.
    /// * `dlq_junction` - Input handler of the DLQ stream, if any.
    /// * `stream_name` - Name of the source stream.
    pub fn new(
        error_config: ErrorConfig,
        dlq_junction: Option<Arc<Mutex<InputHandler>>>,
        stream_name: String,
    ) -> Self {
        Self {
            handler: ErrorHandler::new(error_config, dlq_junction, stream_name),
            running: None,
        }
    }

    /// Returns a new context with the same settings, DLQ input handler and
    /// stream name, but with the error count at zero and no running flag bound.
    ///
    /// Sources call this on each `start()` instead of taking the stored context
    /// with `take()`. That way the configured strategy survives a stop and start
    /// (#142) and clones.
    pub fn fresh(&self) -> Self {
        Self::new(
            self.handler.config().clone(),
            self.handler.dlq_junction(),
            self.handler.stream_name().to_string(),
        )
    }

    /// Bind the owning worker's running flag. Call at the top of the worker
    /// closure.
    ///
    /// Once bound, retry waits end quickly when the flag turns false, and the
    /// delivery loop stops calling the callback again.
    pub fn bind_cancellation(&mut self, running: Arc<AtomicBool>) {
        self.running = Some(running);
    }

    /// Returns true if the bound worker was told to stop. Always false when no
    /// flag is bound.
    pub fn is_cancelled(&self) -> bool {
        is_stopping(&self.running)
    }

    /// Build a context from `error.*` properties in a `FlatConfig`.
    ///
    /// # Arguments
    /// * `config` - Settings holding the `error.*` properties.
    /// * `dlq_junction` - Input handler of the DLQ stream, if any.
    /// * `stream_name` - Name of the source stream.
    ///
    /// # Errors
    ///
    /// Returns an error message if the settings fail to parse.
    pub fn from_config(
        config: &FlatConfig,
        dlq_junction: Option<Arc<Mutex<InputHandler>>>,
        stream_name: String,
    ) -> Result<Self, String> {
        let error_config = ErrorConfig::from_flat_config(config)?;
        Ok(Self::new(error_config, dlq_junction, stream_name))
    }

    /// Build a context from the raw `WITH` clause properties.
    ///
    /// Returns `Ok(None)` when `error.strategy` is not set, even if other
    /// `error.*` keys are. That matches the optional context every source
    /// stores. Only keys starting with `error.` are read. Saves each connector
    /// from repeating the ErrorConfigBuilder and FlatConfig setup in its own
    /// `from_properties`.
    ///
    /// # Errors
    ///
    /// Returns an error message if the settings fail to parse.
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

    /// Handle an error and return whether to keep processing.
    ///
    /// Applies the strategy and does the work it needs, such as waiting before a
    /// retry or sending to the DLQ.
    ///
    /// # Arguments
    /// * `event` - The event that failed, if there is one.
    /// * `error` - The error that happened.
    ///
    /// # Returns
    /// * `true` - Keep processing. The event was retried, dropped or sent to the
    ///   DLQ.
    /// * `false` - Stop processing. The action was `Fail` or `Cancelled`.
    pub fn handle_error(&mut self, event: Option<&Event>, error: &EventFluxError) -> bool {
        let action = self.handle_error_with_action(event, error);
        !matches!(action, ErrorAction::Fail | ErrorAction::Cancelled)
    }

    /// Handle an error and return the action taken.
    ///
    /// Like `handle_error`, but returns the `ErrorAction` so callers can act on
    /// it, for example to decide whether a broker should requeue a message.
    ///
    /// # Arguments
    /// * `event` - The event that failed, if there is one.
    /// * `error` - The error that happened.
    ///
    /// # Returns
    /// - `Retry { delay }` - Retry now. The delay has already been waited.
    /// - `Drop` - The event was dropped.
    /// - `SendToDlq` - The event was sent to the DLQ.
    /// - `Fail` - The source should stop.
    /// - `Cancelled` - A stop was requested during a wait. The source should stop
    ///   and leave the record for delivery again.
    pub fn handle_error_with_action(
        &mut self,
        event: Option<&Event>,
        error: &EventFluxError,
    ) -> ErrorAction {
        // DLQ fallback retries wait inside the handler. Source retries come back
        // as `Retry` and wait here. Both waits respect `stop()`.
        let running = &self.running;
        let action = self
            .handler
            .handle_error_with_backoff(event, error, &mut |delay| {
                sleep_unless_cancelled(running, delay)
            });

        if let ErrorAction::Retry { delay } = action {
            if !sleep_unless_cancelled(running, delay) {
                log::info!(
                    "[{}] Stop requested during retry backoff — aborting retries",
                    self.handler.stream_name()
                );
                // Not Fail. Fail marks the record as bad (RabbitMQ rejects it
                // without requeue). A cancel must leave the record to be
                // delivered again.
                return ErrorAction::Cancelled;
            }
        }

        action
    }

    /// Reset the error count. Call after an event succeeds.
    #[inline]
    pub fn reset_errors(&mut self) {
        self.handler.reset_consecutive_errors();
    }

    /// Returns the failures in a row for the current event.
    #[inline]
    pub fn error_count(&self) -> usize {
        self.handler.consecutive_error_count()
    }

    #[inline]
    pub fn handler(&self) -> &ErrorHandler {
        &self.handler
    }

    #[inline]
    pub fn handler_mut(&mut self) -> &mut ErrorHandler {
        &mut self.handler
    }

    /// Set the DLQ input handler after the context is built.
    ///
    /// Needed because a factory builds the source before the DLQ stream is
    /// available. The runtime wires it in when the source is attached. Also sets
    /// the stream name written into DLQ events.
    ///
    /// # Arguments
    /// * `stream_name` - Name of the stream whose failed events go to the DLQ.
    /// * `junction` - The input handler of the DLQ stream.
    #[inline]
    pub fn set_dlq_junction(&mut self, stream_name: &str, junction: Arc<Mutex<InputHandler>>) {
        self.handler.set_dlq_junction(stream_name, junction);
    }
}

/// Returns true if the worker that owns `running` was told to stop. An unbound
/// context never is.
fn is_stopping(running: &Option<Arc<AtomicBool>>) -> bool {
    running.as_ref().is_some_and(|r| !r.load(Ordering::SeqCst))
}

/// Sleep for `delay`. Returns `false` as soon as the worker is told to stop, and
/// `true` if the full delay passed.
///
/// The sleep blocks on purpose, since sources are synchronous. It is split into
/// slices of at most 50 ms that check the running flag, so a user configured
/// delay never holds `stop()` past the worker grace period.
///
/// The flag is checked before every slice, also when `delay` is zero and after
/// the last slice. So a stopping worker never moves on to another attempt, and a
/// disposed record is never acknowledged, just because no sleep happened.
fn sleep_unless_cancelled(running: &Option<Arc<AtomicBool>>, delay: Duration) -> bool {
    const SLICE: Duration = Duration::from_millis(50);
    let mut remaining = delay;
    loop {
        if is_stopping(running) {
            return false;
        }
        if remaining.is_zero() {
            return true;
        }
        let slice = remaining.min(SLICE);
        thread::sleep(slice);
        remaining -= slice;
    }
}

impl std::fmt::Debug for SourceErrorContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceErrorContext")
            .field("handler", &self.handler)
            .finish()
    }
}

/// Builds an `ErrorConfig` from raw properties.
///
/// Lets factories read error settings while building a source or sink.
pub struct ErrorConfigBuilder {
    /// Only the `error.*` properties.
    config: FlatConfig,
}

impl ErrorConfigBuilder {
    /// Create a builder that keeps only the keys starting with `error.`.
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

    /// Returns true if `error.strategy` is set. Other `error.*` keys alone do not
    /// count.
    pub fn is_configured(&self) -> bool {
        self.config.contains("error.strategy")
    }

    /// Build the settings, or return `Ok(None)` if `error.strategy` is not set.
    ///
    /// # Errors
    ///
    /// Returns an error message if the settings fail to parse.
    pub fn build(&self) -> Result<Option<ErrorConfig>, String> {
        if !self.is_configured() {
            return Ok(None);
        }

        let error_config = ErrorConfig::from_flat_config(&self.config)?;
        Ok(Some(error_config))
    }

    /// Build the settings, or the default settings if `error.strategy` is not set.
    ///
    /// # Errors
    ///
    /// Returns an error message if the settings fail to parse.
    pub fn build_or_default(&self) -> Result<ErrorConfig, String> {
        Ok(self.build()?.unwrap_or_default())
    }
}

/// Result of delivering one payload through the error strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryVerdict {
    /// The pipeline accepted the payload.
    Delivered,
    /// The error strategy dropped it or sent it to the DLQ. The record counts as
    /// consumed and can be acknowledged or committed.
    Disposed,
    /// The fail strategy fired, or a stop was requested. Stop the source. Do not
    /// acknowledge or commit, so the record is delivered again after a restart.
    Fail,
}

/// Deliver one payload to the pipeline, applying the source's error strategy.
///
/// Runs the retry loop every source needs. It retries until success or an action
/// other than retry, builds the DLQ event from the raw bytes, and resets the
/// error count on success. With no error context, a failure is logged and the
/// payload is dropped.
///
/// Acknowledging the record stays with the caller and depends on the verdict.
/// `Delivered` or `Disposed` means acknowledge or commit. `Fail` means do not
/// acknowledge and stop the source.
pub fn deliver_with_error_handling(
    callback: &dyn crate::core::stream::input::source::SourceCallback,
    payload: &[u8],
    error_ctx: &mut Option<SourceErrorContext>,
    source_tag: &str,
) -> DeliveryVerdict {
    loop {
        // A stopping worker must not deliver again. The record stays
        // unacknowledged and is delivered again after a restart.
        if error_ctx.as_ref().is_some_and(|ctx| ctx.is_cancelled()) {
            return DeliveryVerdict::Fail;
        }
        match callback.on_data(payload) {
            Ok(()) => {
                if let Some(ctx) = error_ctx {
                    ctx.reset_errors();
                }
                return DeliveryVerdict::Delivered;
            }
            Err(e) => {
                let action = if let Some(ctx) = error_ctx {
                    // Wrap the raw bytes in an event so the DLQ can carry it.
                    // Built only when there is a strategy to use it.
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
                    // handle_error_with_action has already waited the delay.
                    ErrorAction::Retry { .. } => continue,
                    ErrorAction::Drop | ErrorAction::SendToDlq => return DeliveryVerdict::Disposed,
                    // Both stop the source without acknowledging. Connectors
                    // with a separate path for bad records (RabbitMQ) check the
                    // action themselves before it becomes a verdict.
                    ErrorAction::Fail | ErrorAction::Cancelled => return DeliveryVerdict::Fail,
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
        let error_config = ErrorConfig::default(); // Drop
        let mut ctx = SourceErrorContext::new(error_config, None, "TestStream".to_string());

        let error = EventFluxError::Other("Test error".to_string());

        // Drop keeps processing.
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

        // Fail stops processing.
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

        ctx.handle_error(Some(&create_test_event()), &error);
        assert_eq!(ctx.error_count(), 1);

        ctx.reset_errors();
        assert_eq!(ctx.error_count(), 0);
    }

    fn retry_ctx(initial_delay: std::time::Duration) -> SourceErrorContext {
        use crate::core::error::{ErrorStrategy, LogLevel};
        let retry_config = RetryConfig {
            max_attempts: 5,
            backoff: BackoffStrategy::Fixed,
            initial_delay,
            max_delay: initial_delay,
        };
        let error_config = ErrorConfig::new(
            ErrorStrategy::Retry,
            LogLevel::Warn,
            Some(retry_config),
            None,
            None,
        )
        .unwrap();
        SourceErrorContext::new(error_config, None, "TestStream".to_string())
    }

    #[test]
    fn test_fresh_keeps_config_and_zeroes_counters() {
        let mut ctx = retry_ctx(std::time::Duration::from_millis(1));
        let error = EventFluxError::ConnectionUnavailable {
            message: "x".to_string(),
            source: None,
        };
        ctx.handle_error(None, &error);
        assert_eq!(ctx.error_count(), 1);

        let fresh = ctx.fresh();
        assert_eq!(fresh.error_count(), 0, "runtime state is zeroed");
        assert_eq!(fresh.handler().stream_name(), "TestStream");
        assert!(
            !fresh.is_cancelled(),
            "cancellation binding is not inherited"
        );
    }

    #[test]
    fn test_retry_backoff_aborts_promptly_on_stop() {
        // Regression test for #141. A user configured delay must not hold stop()
        // past the worker grace period.
        let mut ctx = retry_ctx(std::time::Duration::from_secs(30));
        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        ctx.bind_cancellation(Arc::clone(&running));

        let handle = std::thread::spawn(move || {
            let started = std::time::Instant::now();
            let action = ctx.handle_error_with_action(
                None,
                &EventFluxError::ConnectionUnavailable {
                    message: "down".to_string(),
                    source: None,
                },
            );
            (action, started.elapsed())
        });

        std::thread::sleep(std::time::Duration::from_millis(150));
        running.store(false, std::sync::atomic::Ordering::SeqCst);
        let (action, elapsed) = handle.join().unwrap();

        assert!(
            matches!(action, ErrorAction::Cancelled),
            "stop maps to Cancelled, never poison-message Fail"
        );
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "a 30s backoff must abort within the stop window, took {elapsed:?}"
        );
    }

    #[test]
    fn test_sleep_unless_cancelled_checks_stop_even_without_sleeping() {
        let zero = std::time::Duration::ZERO;
        let stopped = Some(Arc::new(std::sync::atomic::AtomicBool::new(false)));
        let running = Some(Arc::new(std::sync::atomic::AtomicBool::new(true)));

        assert!(
            !sleep_unless_cancelled(&stopped, zero),
            "zero delay still sees stop"
        );
        assert!(sleep_unless_cancelled(&running, zero));
        assert!(sleep_unless_cancelled(&None, zero), "unbound never cancels");
    }

    #[test]
    fn test_zero_delay_dlq_retry_cancels_on_stop() {
        // A stopping worker whose DLQ send keeps failing must get Cancelled, so
        // the record is delivered again, not Drop, which acknowledges it. This
        // holds even when the DLQ fallback retry delay is 0ms.
        let zero = std::time::Duration::ZERO;
        let dlq = crate::core::error::DlqConfig {
            stream: "ErrorStream".to_string(),
            fallback_strategy: crate::core::error::DlqFallbackStrategy::Retry,
            fallback_retry: Some(RetryConfig {
                max_attempts: 3,
                backoff: BackoffStrategy::Fixed,
                initial_delay: zero,
                max_delay: zero,
            }),
        };
        let config = ErrorConfig::new(
            crate::core::error::ErrorStrategy::Dlq,
            crate::core::error::LogLevel::Warn,
            None,
            Some(dlq),
            None,
        )
        .unwrap();
        // No DLQ input handler is set, so every DLQ send fails.
        let mut ctx = SourceErrorContext::new(config, None, "TestStream".to_string());
        ctx.bind_cancellation(Arc::new(std::sync::atomic::AtomicBool::new(false)));

        let action = ctx.handle_error_with_action(
            Some(&create_test_event()),
            &EventFluxError::Other("mapping failed".to_string()),
        );
        assert_eq!(action, ErrorAction::Cancelled);
    }

    #[test]
    fn test_delivery_refuses_after_cancellation() {
        #[derive(Debug)]
        struct FailingCallback;
        impl crate::core::stream::input::source::SourceCallback for FailingCallback {
            fn on_data(&self, _data: &[u8]) -> Result<(), EventFluxError> {
                Err(EventFluxError::Other("pipeline down".to_string()))
            }
        }

        let mut ctx = retry_ctx(std::time::Duration::from_millis(10));
        let running = Arc::new(std::sync::atomic::AtomicBool::new(false)); // already stopped
        ctx.bind_cancellation(Arc::clone(&running));
        let mut error_ctx = Some(ctx);

        let verdict =
            deliver_with_error_handling(&FailingCallback, b"payload", &mut error_ctx, "TestSource");
        assert_eq!(
            verdict,
            DeliveryVerdict::Fail,
            "a stopping worker must not keep invoking the callback; Fail \
             leaves the record unacknowledged for redelivery"
        );
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
