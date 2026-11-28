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

//! # Timer Source with Error Handling
//!
//! A simple source that also shows how a source plugs in error handling.
//!
//! ## Error Handling Pattern
//!
//! Sources should
//! 1. hold an optional `SourceErrorContext` field
//! 2. offer a constructor that takes a properties HashMap
//! 3. call `error_ctx.handle_error()` in the event loop
//! 4. call `error_ctx.reset_errors()` after an event is processed
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use std::collections::HashMap;
//!
//! // Create with error handling
//! let mut properties = HashMap::new();
//! properties.insert("timer.interval".to_string(), "1000".to_string());
//! properties.insert("error.strategy".to_string(), "retry".to_string());
//! properties.insert("error.retry.max-attempts".to_string(), "3".to_string());
//!
//! let source = TimerSource::from_properties(&properties, None, "TimerStream")?;
//! ```

use super::{sleep_while_running, Source, SourceCallback, SourceWorker};
use crate::core::error::source_support::SourceErrorContext;
use crate::core::event::event::Event;
use crate::core::event::value::AttributeValue;
use crate::core::exception::EventFluxError;
use crate::core::stream::input::input_handler::InputHandler;
use crate::core::stream::input::mapper::PassthroughMapper;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{atomic::Ordering, Arc, Mutex};
use std::time::Duration;

/// Property that sets the time between ticks, in ms.
pub const INTERVAL_KEY: &str = "timer.interval";

/// Time between ticks when `timer.interval` is not set, in ms.
const DEFAULT_INTERVAL_MS: u64 = 1000;

/// Timer source that sends a tick event on a fixed interval.
///
/// Each tick is one event with timestamp 0 and the single string value "tick".
/// It shows the error handling pattern
/// - optional error handling context
/// - constructor that takes properties
/// - error handling in the event loop
/// - error counter reset after success
#[derive(Debug)]
pub struct TimerSource {
    /// Time between ticks in ms
    interval_ms: u64,
    /// Runs the worker thread. Starts it, signals it, and joins it on stop.
    worker: SourceWorker,
    /// Optional error handling context
    error_ctx: Option<SourceErrorContext>,
    /// Chance that a tick fails, for tests. 0.0 never fails, 1.0 always fails.
    #[cfg(test)]
    simulate_failure_rate: f64,
}

impl TimerSource {
    /// Creates a timer source with no error handling.
    ///
    /// For real use, prefer `from_properties()`, which supports error handling.
    pub fn new(interval_ms: u64) -> Self {
        Self {
            interval_ms,
            worker: SourceWorker::new("TimerSource"),
            error_ctx: None,
            #[cfg(test)]
            simulate_failure_rate: 0.0,
        }
    }

    /// Creates a timer source from its stream properties. `TimerSourceFactory`
    /// builds sources this way.
    ///
    /// # Properties
    /// - `timer.interval`: time between ticks in ms. Defaults to 1000.
    /// - `error.*`: error handling, read by [`SourceErrorContext::from_properties`].
    ///   There is none unless `error.strategy` is set.
    ///
    /// # Arguments
    /// * `properties` - Stream properties
    /// * `dlq_junction` - Optional DLQ stream junction, for the DLQ strategy
    /// * `stream_name` - Name of the source stream
    ///
    /// # Errors
    ///
    /// Returns an error message if `timer.interval` is not a whole number, or the
    /// error handling settings are invalid.
    pub fn from_properties(
        properties: &HashMap<String, String>,
        dlq_junction: Option<Arc<Mutex<InputHandler>>>,
        stream_name: &str,
    ) -> Result<Self, String> {
        let interval_ms = match properties.get(INTERVAL_KEY) {
            Some(text) => text.parse::<u64>().map_err(|_| {
                format!(
                    "{} must be a whole number of milliseconds, got '{}'",
                    INTERVAL_KEY, text
                )
            })?,
            None => DEFAULT_INTERVAL_MS,
        };
        let error_ctx = SourceErrorContext::from_properties(properties, dlq_junction, stream_name)?;

        Ok(Self {
            interval_ms,
            worker: SourceWorker::new("TimerSource"),
            error_ctx,
            #[cfg(test)]
            simulate_failure_rate: 0.0,
        })
    }

    /// Sets the fake failure rate for tests, clamped to 0.0 to 1.0. Test builds only.
    #[cfg(test)]
    pub fn with_failure_rate(mut self, rate: f64) -> Self {
        self.simulate_failure_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Returns a fake error at the set failure rate, for tests. Not called by
    /// `start`, which has its own copy of this check.
    #[cfg(test)]
    #[allow(dead_code)]
    fn simulate_error(&self) -> Result<(), EventFluxError> {
        use rand::Rng;
        if self.simulate_failure_rate > 0.0 {
            let mut rng = rand::thread_rng();
            if rng.gen::<f64>() < self.simulate_failure_rate {
                return Err(EventFluxError::ConnectionUnavailable {
                    message: "Simulated timer error for testing".to_string(),
                    source: None,
                });
            }
        }
        Ok(())
    }

    /// Always returns Ok outside test builds
    #[cfg(not(test))]
    #[allow(dead_code)]
    fn simulate_error(&self) -> Result<(), EventFluxError> {
        Ok(())
    }
}

// Written by hand because SourceErrorContext does not implement Clone
impl Clone for TimerSource {
    fn clone(&self) -> Self {
        Self {
            interval_ms: self.interval_ms,
            worker: self.worker.clone(), // the clone is a new worker that has not started
            // New context with the same strategy and DLQ, and runtime counters reset
            error_ctx: self.error_ctx.as_ref().map(SourceErrorContext::fresh),
            #[cfg(test)]
            simulate_failure_rate: self.simulate_failure_rate,
        }
    }
}

impl Source for TimerSource {
    fn start(&mut self, callback: Arc<dyn SourceCallback>) {
        let interval = self.interval_ms;

        // The thread gets a new copy, so self keeps its config for a later restart
        let mut error_ctx = self.error_ctx.as_ref().map(SourceErrorContext::fresh);

        #[cfg(test)]
        let failure_rate = self.simulate_failure_rate;

        self.worker.start(move |running| {
            if let Some(ctx) = &mut error_ctx {
                ctx.bind_cancellation(Arc::clone(&running));
            }
            while running.load(Ordering::SeqCst) {
                let event =
                    Event::new_with_data(0, vec![AttributeValue::String("tick".to_string())]);

                // Fake error for tests. Always Ok in normal builds.
                #[cfg(test)]
                let error_result = if failure_rate > 0.0 {
                    use rand::Rng;
                    if rand::thread_rng().gen::<f64>() < failure_rate {
                        Err(EventFluxError::ConnectionUnavailable {
                            message: "Simulated timer error for testing".to_string(),
                            source: None,
                        })
                    } else {
                        Ok(())
                    }
                } else {
                    Ok(())
                };

                #[cfg(not(test))]
                let error_result: Result<(), EventFluxError> = Ok(());

                match error_result {
                    Ok(_) => {
                        // Encode the event to bytes and pass it to the callback
                        match PassthroughMapper::serialize(std::slice::from_ref(&event)) {
                            Ok(bytes) => {
                                if let Err(e) = callback.on_data(&bytes) {
                                    if let Some(ctx) = &mut error_ctx {
                                        if !ctx.handle_error(Some(&event), &e) {
                                            // The strategy says stop
                                            break;
                                        }
                                    } else {
                                        // No error handling set, so print and go on
                                        eprintln!("Timer source callback error: {}", e);
                                    }
                                } else {
                                    // Sent, so reset the error counter
                                    if let Some(ctx) = &mut error_ctx {
                                        ctx.reset_errors();
                                    }
                                }
                            }
                            Err(e) => {
                                let serialize_error = EventFluxError::app_runtime(format!(
                                    "Failed to serialize timer event: {}",
                                    e
                                ));
                                if let Some(ctx) = &mut error_ctx {
                                    if !ctx.handle_error(Some(&event), &serialize_error) {
                                        break;
                                    }
                                } else {
                                    eprintln!("Timer source serialization error: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        if let Some(ctx) = &mut error_ctx {
                            if !ctx.handle_error(Some(&event), &e) {
                                // The strategy says stop, for example the fail strategy
                                break;
                            }
                            // Handled by drop, retry, or DLQ, so go on
                        } else {
                            // No error handling set, so print and go on
                            eprintln!("Timer source error: {}", e);
                        }
                    }
                }

                // Wakes early on stop, so a long interval never delays stop()
                sleep_while_running(&running, Duration::from_millis(interval));
            }
        });
    }

    fn stop(&mut self) {
        self.worker.stop();
    }

    fn clone_box(&self) -> Box<dyn Source> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    // These tests cover the error handling config. Tests that deliver real
    // events through a timer source are in tests/source_sink.rs.

    #[test]
    fn test_timer_source_from_properties_no_error_handling() {
        let mut properties = HashMap::new();
        properties.insert(INTERVAL_KEY.to_string(), "100".to_string());

        let source = TimerSource::from_properties(&properties, None, "TestStream");
        assert!(source.is_ok());

        let source = source.unwrap();
        assert_eq!(source.interval_ms, 100);
        assert!(source.error_ctx.is_none());
    }

    #[test]
    fn test_timer_source_from_properties_with_error_handling() {
        let mut properties = HashMap::new();
        properties.insert(INTERVAL_KEY.to_string(), "100".to_string());
        properties.insert("error.strategy".to_string(), "drop".to_string());
        properties.insert("error.log-level".to_string(), "warn".to_string());

        let source = TimerSource::from_properties(&properties, None, "TestStream");
        assert!(source.is_ok());

        let source = source.unwrap();
        assert_eq!(source.interval_ms, 100);
        assert!(source.error_ctx.is_some());
    }

    /// The factory used to build a bare `TimerSource::new` and drop every
    /// `error.*` key, so YAML or SQL error handling on a timer did nothing.
    #[test]
    fn test_timer_factory_reads_error_properties() {
        use crate::core::extension::{SourceFactory, TimerSourceFactory};

        let mut properties = HashMap::new();
        properties.insert("error.strategy".to_string(), "bogus".to_string());
        let err = TimerSourceFactory
            .create_initialized(&properties)
            .expect_err("an invalid error.strategy must be rejected");
        assert!(err.to_string().contains("Invalid error strategy"), "{err}");

        properties.insert("error.strategy".to_string(), "fail".to_string());
        properties.insert(INTERVAL_KEY.to_string(), "250".to_string());
        TimerSourceFactory.create_initialized(&properties).unwrap();
    }

    #[test]
    fn test_timer_source_missing_interval_uses_default() {
        let source = TimerSource::from_properties(&HashMap::new(), None, "TestStream").unwrap();
        assert_eq!(source.interval_ms, DEFAULT_INTERVAL_MS);
        assert!(source.error_ctx.is_none());
    }

    #[test]
    fn test_timer_source_invalid_interval() {
        let mut properties = HashMap::new();
        properties.insert(INTERVAL_KEY.to_string(), "not_a_number".to_string());

        let result = TimerSource::from_properties(&properties, None, "TestStream");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "timer.interval must be a whole number of milliseconds, got 'not_a_number'"
        );
    }

    #[test]
    fn test_timer_source_with_simulated_errors() {
        let mut properties = HashMap::new();
        properties.insert(INTERVAL_KEY.to_string(), "50".to_string());
        properties.insert("error.strategy".to_string(), "drop".to_string());

        let source = TimerSource::from_properties(&properties, None, "TestStream")
            .unwrap()
            .with_failure_rate(0.3); // 30% failure rate

        assert!(source.error_ctx.is_some());
        assert_eq!(source.simulate_failure_rate, 0.3);
    }

    // =========================================================================
    // Shutdown Lifecycle Tests
    // =========================================================================

    use std::sync::atomic::AtomicU64;

    #[derive(Debug)]
    struct CountingCallback {
        count: Arc<AtomicU64>,
    }

    impl SourceCallback for CountingCallback {
        fn on_data(&self, _data: &[u8]) -> Result<(), EventFluxError> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    // Repeat stops and stop before start are covered once by the SourceWorker
    // tests in source/mod.rs. This end to end test checks that the Source trait
    // really hands off to the worker.
    #[test]
    fn test_no_callbacks_after_stop_returns() {
        let count = Arc::new(AtomicU64::new(0));
        let mut source = TimerSource::new(10);
        source.start(Arc::new(CountingCallback {
            count: Arc::clone(&count),
        }));

        // Let the worker deliver a few ticks
        thread::sleep(Duration::from_millis(50));
        source.stop();

        // stop() joined the worker, so the count must not change from here on.
        // Before the join this check could fail at random, since the worker
        // could still be in the middle of a round when stop() returned.
        let count_at_stop = count.load(Ordering::SeqCst);
        assert!(count_at_stop > 0, "worker should have delivered ticks");
        thread::sleep(Duration::from_millis(50));
        assert_eq!(
            count.load(Ordering::SeqCst),
            count_at_stop,
            "no callback may fire after stop() returns"
        );
    }
}
