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

//! # Timer Source with Error Handling Integration
//!
//! This module demonstrates how to integrate M5 error handling into Source implementations.
//!
//! ## Error Handling Integration Pattern
//!
//! Sources should:
//! 1. Include an optional `SourceErrorContext` field
//! 2. Provide a factory method that accepts properties HashMap
//! 3. Use `error_ctx.handle_error()` in the event loop
//! 4. Call `error_ctx.reset_errors()` after successful event processing
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use std::collections::HashMap;
//!
//! // Create with error handling
//! let mut properties = HashMap::new();
//! properties.insert("interval".to_string(), "1000".to_string());
//! properties.insert("error.strategy".to_string(), "retry".to_string());
//! properties.insert("error.retry.max-attempts".to_string(), "3".to_string());
//!
//! let source = TimerSource::from_properties(&properties, None, "TimerStream")?;
//! ```

use super::{Source, SourceCallback};
use crate::core::error::source_support::{ErrorConfigBuilder, SourceErrorContext};
use crate::core::event::event::Event;
use crate::core::event::value::AttributeValue;
use crate::core::exception::EventFluxError;
use crate::core::stream::input::input_handler::InputHandler;
use crate::core::stream::input::mapper::PassthroughMapper;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::Duration;

/// Timer source that generates tick events at regular intervals
///
/// This implementation demonstrates M5 error handling integration:
/// - Optional error handling context
/// - Factory method with properties
/// - Error handling in event loop
/// - Error counter reset after success
#[derive(Debug)]
pub struct TimerSource {
    /// Interval between ticks in milliseconds
    interval_ms: u64,
    /// Running flag for graceful shutdown
    running: Arc<AtomicBool>,
    /// Optional error handling context (M5 integration)
    error_ctx: Option<SourceErrorContext>,
    /// Optional simulated failure rate for testing (0.0 = never, 1.0 = always)
    #[cfg(test)]
    simulate_failure_rate: f64,
}

impl TimerSource {
    /// Create a new timer source without error handling
    ///
    /// For production use, prefer `from_properties()` which supports error handling.
    pub fn new(interval_ms: u64) -> Self {
        Self {
            interval_ms,
            running: Arc::new(AtomicBool::new(false)),
            error_ctx: None,
            #[cfg(test)]
            simulate_failure_rate: 0.0,
        }
    }

    /// Create a timer source with error handling from properties
    ///
    /// This is the recommended factory method for M7 stream initialization.
    ///
    /// # Properties
    /// - `interval` (required): Interval in milliseconds
    /// - `error.strategy`: Error handling strategy (drop/retry/dlq/fail)
    /// - `error.retry.*`: Retry configuration if strategy is "retry"
    /// - `error.dlq.*`: DLQ configuration if strategy is "dlq"
    ///
    /// # Arguments
    /// * `properties` - Configuration properties
    /// * `dlq_junction` - Optional DLQ stream junction (for DLQ strategy)
    /// * `stream_name` - Name of the source stream
    ///
    /// # Returns
    /// * `Ok(TimerSource)` - Successfully created source
    /// * `Err(String)` - Configuration error
    pub fn from_properties(
        properties: &HashMap<String, String>,
        dlq_junction: Option<Arc<Mutex<InputHandler>>>,
        stream_name: &str,
    ) -> Result<Self, String> {
        // Parse interval (required)
        let interval_ms = properties
            .get("interval")
            .ok_or_else(|| "Missing required property: interval".to_string())?
            .parse::<u64>()
            .map_err(|e| format!("Invalid interval value: {}", e))?;

        // Parse error handling configuration (optional)
        let error_config_builder = ErrorConfigBuilder::from_properties(properties);
        let error_ctx = if error_config_builder.is_configured() {
            // Build FlatConfig for SourceErrorContext
            use crate::core::config::{FlatConfig, PropertySource};
            let mut config = FlatConfig::new();
            for (key, value) in properties {
                if key.starts_with("error.") {
                    config.set(key.clone(), value.clone(), PropertySource::SqlWith);
                }
            }

            Some(SourceErrorContext::from_config(
                &config,
                dlq_junction,
                stream_name.to_string(),
            )?)
        } else {
            None
        };

        Ok(Self {
            interval_ms,
            running: Arc::new(AtomicBool::new(false)),
            error_ctx,
            #[cfg(test)]
            simulate_failure_rate: 0.0,
        })
    }

    /// Set simulated failure rate for testing (only available in test builds)
    #[cfg(test)]
    pub fn with_failure_rate(mut self, rate: f64) -> Self {
        self.simulate_failure_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Simulate a failure for testing purposes
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

    /// Check for simulated errors (no-op in non-test builds)
    #[cfg(not(test))]
    #[allow(dead_code)]
    fn simulate_error(&self) -> Result<(), EventFluxError> {
        Ok(())
    }
}

// Manual Clone implementation since SourceErrorContext doesn't implement Clone
impl Clone for TimerSource {
    fn clone(&self) -> Self {
        Self {
            interval_ms: self.interval_ms,
            running: Arc::new(AtomicBool::new(false)),
            error_ctx: None, // Error context is not cloneable (contains runtime state)
            #[cfg(test)]
            simulate_failure_rate: self.simulate_failure_rate,
        }
    }
}

impl Source for TimerSource {
    fn start(&mut self, callback: Arc<dyn SourceCallback>) {
        let running = self.running.clone();
        running.store(true, Ordering::SeqCst);
        let interval = self.interval_ms;

        // Move error_ctx into the thread (take ownership)
        let mut error_ctx = self.error_ctx.take();

        #[cfg(test)]
        let failure_rate = self.simulate_failure_rate;

        thread::spawn(move || {
            while running.load(Ordering::SeqCst) {
                // Create event
                let event =
                    Event::new_with_data(0, vec![AttributeValue::String("tick".to_string())]);

                // Simulate error for testing (no-op in production builds)
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

                // Process event or handle error
                match error_result {
                    Ok(_) => {
                        // Success case - serialize event to bytes and deliver via callback
                        match PassthroughMapper::serialize(std::slice::from_ref(&event)) {
                            Ok(bytes) => {
                                if let Err(e) = callback.on_data(&bytes) {
                                    // Handle send error with error context
                                    if let Some(ctx) = &mut error_ctx {
                                        if !ctx.handle_error(Some(&event), &e) {
                                            // Stop source on unrecoverable error
                                            break;
                                        }
                                    } else {
                                        // No error handling configured - log and continue
                                        eprintln!("Timer source callback error: {}", e);
                                    }
                                } else {
                                    // Event sent successfully - reset error counter
                                    if let Some(ctx) = &mut error_ctx {
                                        ctx.reset_errors();
                                    }
                                }
                            }
                            Err(e) => {
                                // Serialization error
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
                        // Error case - use error context to handle
                        if let Some(ctx) = &mut error_ctx {
                            if !ctx.handle_error(Some(&event), &e) {
                                // Stop source on unrecoverable error (e.g., fail strategy)
                                break;
                            }
                            // Continue processing (error was handled via drop/retry/dlq)
                        } else {
                            // No error handling configured - log and continue
                            eprintln!("Timer source error: {}", e);
                        }
                    }
                }

                thread::sleep(Duration::from_millis(interval));
            }
        });
    }

    fn stop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
    }

    fn clone_box(&self) -> Box<dyn Source> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Full timer source tests with actual event delivery are in tests/source_sink.rs
    // These tests focus on error handling configuration

    #[test]
    fn test_timer_source_from_properties_no_error_handling() {
        let mut properties = HashMap::new();
        properties.insert("interval".to_string(), "100".to_string());

        let source = TimerSource::from_properties(&properties, None, "TestStream");
        assert!(source.is_ok());

        let source = source.unwrap();
        assert_eq!(source.interval_ms, 100);
        assert!(source.error_ctx.is_none());
    }

    #[test]
    fn test_timer_source_from_properties_with_error_handling() {
        let mut properties = HashMap::new();
        properties.insert("interval".to_string(), "100".to_string());
        properties.insert("error.strategy".to_string(), "drop".to_string());
        properties.insert("error.log-level".to_string(), "warn".to_string());

        let source = TimerSource::from_properties(&properties, None, "TestStream");
        assert!(source.is_ok());

        let source = source.unwrap();
        assert_eq!(source.interval_ms, 100);
        assert!(source.error_ctx.is_some());
    }

    #[test]
    fn test_timer_source_missing_interval() {
        let properties = HashMap::new();
        let result = TimerSource::from_properties(&properties, None, "TestStream");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Missing required property: interval"));
    }

    #[test]
    fn test_timer_source_invalid_interval() {
        let mut properties = HashMap::new();
        properties.insert("interval".to_string(), "not_a_number".to_string());

        let result = TimerSource::from_properties(&properties, None, "TestStream");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid interval value"));
    }

    #[test]
    fn test_timer_source_with_simulated_errors() {
        let mut properties = HashMap::new();
        properties.insert("interval".to_string(), "50".to_string());
        properties.insert("error.strategy".to_string(), "drop".to_string());

        let source = TimerSource::from_properties(&properties, None, "TestStream")
            .unwrap()
            .with_failure_rate(0.3); // 30% failure rate

        // Verify error handling is configured
        assert!(source.error_ctx.is_some());
        assert_eq!(source.simulate_failure_rate, 0.3);
    }
}
