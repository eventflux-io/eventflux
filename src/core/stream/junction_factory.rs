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

//! StreamJunction Factory for Event Routing
//!
//! Config and builders for StreamJunctions, which route events from a stream
//! to its subscribers over a crossbeam based pipeline.

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::stream::StreamJunction;
use crate::query_api::definition::StreamDefinition;
use std::sync::{Arc, Mutex};

/// Config for building a StreamJunction
#[derive(Debug, Clone)]
pub struct JunctionConfig {
    pub stream_id: String,
    /// Buffer size used when no throughput hint is given. Default 4096.
    pub buffer_size: usize,
    /// Async mode. Default false, which keeps event order.
    pub is_async: bool,
    pub expected_throughput: Option<u64>, // events per second
    pub subscriber_count: Option<usize>,
}

impl JunctionConfig {
    /// Creates a junction config. Sync mode by default, with a buffer size of
    /// 4096 and no hints.
    ///
    /// **Default mode: sync (is_async: false)**
    /// - Keeps strict event order
    /// - Events are processed one at a time in the order they arrive
    /// - Use when event order matters
    ///
    /// **Async mode: with_async(true), for high throughput**
    /// - Gives up the order promise for more speed
    /// - Events may be processed out of order, since they run in parallel
    /// - Use when throughput matters more than order
    pub fn new(stream_id: String) -> Self {
        Self {
            stream_id,
            buffer_size: 4096,
            is_async: false, // sync by default to keep event order
            expected_throughput: None,
            subscriber_count: None,
        }
    }

    /// Sets the buffer size. Used only when no throughput hint is set.
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// Turns async mode on or off. This trades event order for speed.
    ///
    /// **IMPORTANT: async mode may break the event order promise.**
    ///
    /// **Sync mode (false, the default)**
    /// - **Strict event order is kept**
    /// - Events are processed one at a time in arrival order
    /// - Behaviour is predictable and repeatable
    /// - Lower throughput, around thousands of events per second
    /// - **Use when** event order matters for correct results
    ///
    /// **Async mode (true)**
    /// - **High throughput**, more than 100K events per second possible
    /// - Uses resources better by processing in parallel
    /// - Senders do not wait for processing
    /// - **Events may be processed out of order**
    /// - Timing is less predictable
    /// - **Use when** throughput matters more than strict order
    ///
    /// # Example
    /// ```
    /// use eventflux::core::stream::JunctionConfig;
    ///
    /// // Default: Synchronous processing (guaranteed ordering)
    /// let sync_config = JunctionConfig::new("stream".to_string());
    ///
    /// // High-throughput async processing (potential reordering)
    /// let async_config = JunctionConfig::new("stream".to_string())
    ///     .with_async(true)
    ///     .with_expected_throughput(100_000);
    /// ```
    pub fn with_async(mut self, async_mode: bool) -> Self {
        self.is_async = async_mode;
        self
    }

    /// Sets the expected events per second. A value above 0 makes the buffer
    /// size come from the hints instead of `buffer_size`.
    pub fn with_expected_throughput(mut self, throughput: u64) -> Self {
        self.expected_throughput = Some(throughput);
        self
    }

    /// Sets the expected number of subscribers. Used to size the buffer and to
    /// reserve room in the subscriber list.
    pub fn with_subscriber_count(mut self, count: usize) -> Self {
        self.subscriber_count = Some(count);
        self
    }
}

/// Builds StreamJunctions
pub struct StreamJunctionFactory;

impl StreamJunctionFactory {
    /// Builds a StreamJunction from the config.
    ///
    /// # Errors
    ///
    /// Returns the error from `StreamJunction::new`.
    pub fn create(
        config: JunctionConfig,
        stream_definition: Arc<StreamDefinition>,
        eventflux_app_context: Arc<EventFluxAppContext>,
        fault_stream_junction: Option<Arc<Mutex<StreamJunction>>>,
    ) -> Result<Arc<Mutex<StreamJunction>>, String> {
        Self::create_junction(
            config,
            stream_definition,
            eventflux_app_context,
            fault_stream_junction,
        )
    }

    /// Builds a StreamJunction. Does the work for [`Self::create`].
    ///
    /// # Errors
    ///
    /// Returns the error from `StreamJunction::new`.
    pub fn create_junction(
        config: JunctionConfig,
        stream_definition: Arc<StreamDefinition>,
        eventflux_app_context: Arc<EventFluxAppContext>,
        fault_stream_junction: Option<Arc<Mutex<StreamJunction>>>,
    ) -> Result<Arc<Mutex<StreamJunction>>, String> {
        let buffer_size = Self::calculate_buffer_size(&config);

        let mut junction = StreamJunction::new(
            config.stream_id,
            stream_definition,
            eventflux_app_context,
            buffer_size,
            config.is_async,
            fault_stream_junction,
        )?;

        // Reserve room for the expected subscribers up front
        if let Some(subscriber_count) = config.subscriber_count {
            junction.reserve_subscriber_capacity(subscriber_count);
        }

        Ok(Arc::new(Mutex::new(junction)))
    }

    /// Works out the buffer size from the hints.
    ///
    /// The raw size is max(1, expected_throughput * subscriber_count * 0.1), which
    /// is about 100 ms of events at the expected rate. It is rounded up to a
    /// power of 2 and kept between 64 and 1_048_576.
    ///
    /// # Heuristic
    /// - 100K events per second with 4 subscribers gives 100000 * 4 * 0.1 = 40K,
    ///   rounded up to 65536
    /// - Enough room for bursts without using too much memory
    /// - With only a throughput hint, one subscriber is assumed
    /// - With no throughput hint, or a hint of 0, it uses `buffer_size` as is
    ///   (no rounding or clamping)
    ///
    /// # Safety
    /// - The raw size is at least 1. Below 10 total, the 0.1 factor would round
    ///   it down to 0.
    fn calculate_buffer_size(config: &JunctionConfig) -> usize {
        match (config.expected_throughput, config.subscriber_count) {
            (Some(throughput), Some(subscribers)) if throughput > 0 && subscribers > 0 => {
                // About 100 ms of events at the expected rate. Keep it at least
                // 1, since under 10 total (for example 1 to 9 events per
                // second) the 0.1 factor rounds down to 0.
                let calculated = ((throughput as f64 * subscribers as f64 * 0.1) as usize).max(1);

                // Round up to a power of 2
                let mut buffer_size = calculated.next_power_of_two();

                // Keep it between 64 and 1M
                buffer_size = buffer_size.clamp(64, 1_048_576);

                log::debug!(
                    "[{}] Calculated buffer size {} from hints (throughput: {}/sec, subscribers: {}, raw: {})",
                    config.stream_id,
                    buffer_size,
                    throughput,
                    subscribers,
                    calculated
                );

                buffer_size
            }
            (Some(throughput), None) if throughput > 0 => {
                // Only a throughput hint, so assume 1 subscriber. Keep it at least 1.
                let calculated = ((throughput as f64 * 0.1) as usize).max(1);
                let mut buffer_size = calculated.next_power_of_two();
                buffer_size = buffer_size.clamp(64, 1_048_576);

                log::debug!(
                    "[{}] Calculated buffer size {} from throughput hint: {}/sec (raw: {})",
                    config.stream_id,
                    buffer_size,
                    throughput,
                    calculated
                );

                buffer_size
            }
            _ => {
                // No usable hint, so use buffer_size as given
                config.buffer_size
            }
        }
    }

    /// Builds a sync junction from hints. A missing throughput counts as 0,
    /// which falls back to the default buffer size of 4096. A missing
    /// subscriber count counts as 1.
    ///
    /// # Errors
    ///
    /// Returns the error from `StreamJunction::new`.
    pub fn create_with_hints(
        stream_id: String,
        stream_definition: Arc<StreamDefinition>,
        eventflux_app_context: Arc<EventFluxAppContext>,
        expected_throughput: Option<u64>,
        subscriber_count: Option<usize>,
    ) -> Result<Arc<Mutex<StreamJunction>>, String> {
        let config = JunctionConfig::new(stream_id)
            .with_expected_throughput(expected_throughput.unwrap_or(0))
            .with_subscriber_count(subscriber_count.unwrap_or(1));

        Self::create(config, stream_definition, eventflux_app_context, None)
    }

    /// Builds an async junction with the given buffer size, for known high
    /// throughput cases. Async mode may process events out of order.
    ///
    /// # Errors
    ///
    /// Returns the error from `StreamJunction::new`.
    pub fn create_high_performance(
        stream_id: String,
        stream_definition: Arc<StreamDefinition>,
        eventflux_app_context: Arc<EventFluxAppContext>,
        buffer_size: usize,
    ) -> Result<Arc<Mutex<StreamJunction>>, String> {
        let config = JunctionConfig::new(stream_id)
            .with_buffer_size(buffer_size)
            .with_async(true);

        Self::create(config, stream_definition, eventflux_app_context, None)
    }
}

/// Simple throughput benchmark for a junction
pub struct JunctionBenchmark;

impl JunctionBenchmark {
    /// Sends `num_events` events from `num_threads` threads and measures the
    /// rate. Each thread sends `num_events / num_threads` events, so any
    /// remainder is not sent, but `events_sent` still reports `num_events`.
    /// Send errors are ignored.
    ///
    /// # Errors
    ///
    /// Returns an error when a sender thread panics.
    pub fn benchmark_throughput(
        junction: &Arc<Mutex<StreamJunction>>,
        num_events: usize,
        num_threads: usize,
    ) -> Result<BenchmarkResult, String> {
        use crate::core::event::{value::AttributeValue, Event};
        use std::thread;
        use std::time::Instant;

        let start = Instant::now();
        let mut handles = Vec::new();

        for thread_id in 0..num_threads {
            let events_per_thread = num_events / num_threads;
            let junction_clone = Arc::clone(junction);

            handles.push(thread::spawn(move || {
                for i in 0..events_per_thread {
                    let event = Event::new_with_data(
                        i as i64,
                        vec![AttributeValue::Int(thread_id as i32 * 1000 + i as i32)],
                    );
                    let _ = junction_clone.lock().unwrap().send_event(event);
                }
            }));
        }

        for handle in handles {
            handle.join().map_err(|_| "Thread join failed")?;
        }

        let duration = start.elapsed();
        let throughput = num_events as f64 / duration.as_secs_f64();

        Ok(BenchmarkResult {
            events_sent: num_events,
            duration,
            throughput,
            implementation: "StreamJunction".to_string(),
        })
    }
}

/// Result of a junction benchmark run
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub events_sent: usize,
    pub duration: std::time::Duration,
    /// Events per second
    pub throughput: f64,
    pub implementation: String,
}

impl BenchmarkResult {
    /// Prints the results to stdout
    pub fn print(&self) {
        println!("Junction Benchmark Results:");
        println!("  Implementation: {}", self.implementation);
        println!("  Events sent: {}", self.events_sent);
        println!("  Duration: {:.2?}", self.duration);
        println!("  Throughput: {:.0} events/sec", self.throughput);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::eventflux_context::EventFluxContext;
    use crate::query_api::definition::attribute::Type as AttrType;

    fn create_test_context() -> Arc<EventFluxAppContext> {
        let eventflux_context = Arc::new(EventFluxContext::new());
        let app = Arc::new(crate::query_api::eventflux_app::EventFluxApp::new(
            "TestApp".to_string(),
        ));
        Arc::new(EventFluxAppContext::new(
            eventflux_context,
            "TestApp".to_string(),
            app,
            String::new(),
        ))
    }

    fn create_test_stream_definition() -> Arc<StreamDefinition> {
        Arc::new(
            StreamDefinition::new("TestStream".to_string())
                .attribute("id".to_string(), AttrType::INT),
        )
    }

    #[test]
    fn test_create_with_config() {
        let config = JunctionConfig::new("TestStream".to_string())
            .with_expected_throughput(100)
            .with_buffer_size(4096);

        let context = create_test_context();
        let stream_def = create_test_stream_definition();

        let junction = StreamJunctionFactory::create(config, stream_def, context, None).unwrap();
        assert_eq!(junction.lock().unwrap().stream_id, "TestStream");
    }

    #[test]
    fn test_high_performance_factory_method() {
        let context = create_test_context();
        let stream_def = create_test_stream_definition();

        let junction = StreamJunctionFactory::create_high_performance(
            "HighPerfStream".to_string(),
            stream_def,
            context,
            32768,
        )
        .unwrap();

        assert_eq!(junction.lock().unwrap().stream_id, "HighPerfStream");
    }

    #[test]
    fn test_junction_with_hints() {
        let context = create_test_context();
        let stream_def = create_test_stream_definition();

        // A high throughput hint must still build the junction
        let junction = StreamJunctionFactory::create_with_hints(
            "HintedStream".to_string(),
            stream_def,
            context,
            Some(150000), // High throughput
            Some(4),      // Multiple subscribers
        )
        .unwrap();

        assert_eq!(junction.lock().unwrap().stream_id, "HintedStream");
    }

    #[test]
    fn test_buffer_size_calculation_with_hints() {
        // Heuristic: buffer_size = throughput * subscribers * 0.1

        // Case 1: 100K events per second, 4 subscribers
        // Expected: 100000 * 4 * 0.1 = 40000 → next_power_of_two = 65536
        let config1 = JunctionConfig::new("test1".to_string())
            .with_expected_throughput(100_000)
            .with_subscriber_count(4);
        let buffer1 = StreamJunctionFactory::calculate_buffer_size(&config1);
        assert_eq!(
            buffer1, 65536,
            "100K throughput with 4 subscribers should use 64K buffer"
        );

        // Case 2: 10K events per second, 2 subscribers
        // Expected: 10000 * 2 * 0.1 = 2000 → next_power_of_two = 2048
        let config2 = JunctionConfig::new("test2".to_string())
            .with_expected_throughput(10_000)
            .with_subscriber_count(2);
        let buffer2 = StreamJunctionFactory::calculate_buffer_size(&config2);
        assert_eq!(
            buffer2, 2048,
            "10K throughput with 2 subscribers should use 2K buffer"
        );

        // Case 3: only a throughput hint, no subscriber count
        // Expected: 50000 * 0.1 = 5000 → next_power_of_two = 8192
        let config3 = JunctionConfig::new("test3".to_string()).with_expected_throughput(50_000);
        let buffer3 = StreamJunctionFactory::calculate_buffer_size(&config3);
        assert_eq!(
            buffer3, 8192,
            "50K throughput with no subscriber hint should use 8K buffer"
        );

        // Case 4: no hints, so buffer_size is used as given
        let config4 = JunctionConfig::new("test4".to_string()).with_buffer_size(4096);
        let buffer4 = StreamJunctionFactory::calculate_buffer_size(&config4);
        assert_eq!(buffer4, 4096, "No hints should use explicit buffer_size");

        // Case 5: very low throughput is raised to the minimum of 64
        let config5 = JunctionConfig::new("test5".to_string())
            .with_expected_throughput(10)
            .with_subscriber_count(1);
        let buffer5 = StreamJunctionFactory::calculate_buffer_size(&config5);
        assert_eq!(
            buffer5, 64,
            "Very low throughput should clamp to minimum 64"
        );

        // Case 6: very high throughput is capped at the maximum of 1M
        let config6 = JunctionConfig::new("test6".to_string())
            .with_expected_throughput(100_000_000)
            .with_subscriber_count(100);
        let buffer6 = StreamJunctionFactory::calculate_buffer_size(&config6);
        assert_eq!(
            buffer6, 1_048_576,
            "Very high throughput should clamp to maximum 1M"
        );
    }

    #[test]
    fn test_subscriber_capacity_reservation() {
        let context = create_test_context();
        let stream_def = create_test_stream_definition();

        let config = JunctionConfig::new("ReservedStream".to_string())
            .with_subscriber_count(10)
            .with_buffer_size(4096);

        let junction = StreamJunctionFactory::create(config, stream_def, context, None).unwrap();

        // The reserved room has no public getter, so only check the junction exists
        assert_eq!(junction.lock().unwrap().stream_id, "ReservedStream");

        // The room lives inside a Vec. Reserving it avoids growing the Vec while
        // up to 10 subscribers are added.
    }

    #[test]
    fn test_buffer_size_calculation_edge_cases() {
        // Edge cases where throughput * subscribers * 0.1 rounds down to 0.
        // The .max(1) floor keeps the raw size at 1, and the result is the
        // minimum of 64.

        // Case 1: throughput=1, subscribers=1
        // Calculation: 1 * 1 * 0.1 = 0.1 → 0 (usize) → max(1) = 1 → next_power_of_two() = 1 → clamp(64) = 64
        let config1 = JunctionConfig::new("test1".to_string())
            .with_expected_throughput(1)
            .with_subscriber_count(1);
        let buffer1 = StreamJunctionFactory::calculate_buffer_size(&config1);
        assert_eq!(
            buffer1, 64,
            "throughput=1, subscribers=1 should not panic and should use min buffer 64"
        );

        // Case 2: throughput=5, subscribers=1
        // Calculation: 5 * 1 * 0.1 = 0.5 → 0 (usize) → max(1) = 1 → next_power_of_two() = 1 → clamp(64) = 64
        let config2 = JunctionConfig::new("test2".to_string())
            .with_expected_throughput(5)
            .with_subscriber_count(1);
        let buffer2 = StreamJunctionFactory::calculate_buffer_size(&config2);
        assert_eq!(
            buffer2, 64,
            "throughput=5, subscribers=1 should not panic and should use min buffer 64"
        );

        // Case 3: throughput=1, subscribers=5
        // Calculation: 1 * 5 * 0.1 = 0.5 → 0 (usize) → max(1) = 1 → next_power_of_two() = 1 → clamp(64) = 64
        let config3 = JunctionConfig::new("test3".to_string())
            .with_expected_throughput(1)
            .with_subscriber_count(5);
        let buffer3 = StreamJunctionFactory::calculate_buffer_size(&config3);
        assert_eq!(
            buffer3, 64,
            "throughput=1, subscribers=5 should not panic and should use min buffer 64"
        );

        // Case 4: throughput=9, subscribers=1 (just below 10)
        // Calculation: 9 * 1 * 0.1 = 0.9 → 0 (usize) → max(1) = 1 → next_power_of_two() = 1 → clamp(64) = 64
        let config4 = JunctionConfig::new("test4".to_string())
            .with_expected_throughput(9)
            .with_subscriber_count(1);
        let buffer4 = StreamJunctionFactory::calculate_buffer_size(&config4);
        assert_eq!(
            buffer4, 64,
            "throughput=9, subscribers=1 should not panic and should use min buffer 64"
        );

        // Case 5: throughput=10, subscribers=1 (first value that does not round to 0)
        // Calculation: 10 * 1 * 0.1 = 1.0 → 1 (usize) → max(1) = 1 → next_power_of_two() = 1 → clamp(64) = 64
        let config5 = JunctionConfig::new("test5".to_string())
            .with_expected_throughput(10)
            .with_subscriber_count(1);
        let buffer5 = StreamJunctionFactory::calculate_buffer_size(&config5);
        assert_eq!(
            buffer5, 64,
            "throughput=10, subscribers=1 should use min buffer 64"
        );

        // Case 6: throughput=100, subscribers=1, still under the minimum
        // Calculation: 100 * 1 * 0.1 = 10.0 → 10 (usize) → max(1) = 10 → next_power_of_two() = 16 → clamp(64) = 64
        let config6 = JunctionConfig::new("test6".to_string())
            .with_expected_throughput(100)
            .with_subscriber_count(1);
        let buffer6 = StreamJunctionFactory::calculate_buffer_size(&config6);
        assert_eq!(
            buffer6, 64,
            "throughput=100, subscribers=1 should use min buffer 64"
        );

        // Case 7: throughput=1000, subscribers=1, first case above the minimum
        // Calculation: 1000 * 1 * 0.1 = 100.0 → 100 (usize) → max(1) = 100 → next_power_of_two() = 128
        let config7 = JunctionConfig::new("test7".to_string())
            .with_expected_throughput(1000)
            .with_subscriber_count(1);
        let buffer7 = StreamJunctionFactory::calculate_buffer_size(&config7);
        assert_eq!(
            buffer7, 128,
            "throughput=1000, subscribers=1 should use 128 buffer"
        );
    }

    #[test]
    fn test_buffer_size_calculation_throughput_only_edge_cases() {
        // Edge cases for the branch with only a throughput hint

        // Case 1: throughput=1, no subscribers
        // Calculation: 1 * 0.1 = 0.1 → 0 (usize) → max(1) = 1 → next_power_of_two() = 1 → clamp(64) = 64
        let config1 = JunctionConfig::new("test1".to_string()).with_expected_throughput(1);
        let buffer1 = StreamJunctionFactory::calculate_buffer_size(&config1);
        assert_eq!(
            buffer1, 64,
            "throughput=1 (no subscribers) should not panic and should use min buffer 64"
        );

        // Case 2: throughput=5, no subscribers
        // Calculation: 5 * 0.1 = 0.5 → 0 (usize) → max(1) = 1 → next_power_of_two() = 1 → clamp(64) = 64
        let config2 = JunctionConfig::new("test2".to_string()).with_expected_throughput(5);
        let buffer2 = StreamJunctionFactory::calculate_buffer_size(&config2);
        assert_eq!(
            buffer2, 64,
            "throughput=5 (no subscribers) should not panic and should use min buffer 64"
        );

        // Case 3: throughput=9, no subscribers (just below 10)
        // Calculation: 9 * 0.1 = 0.9 → 0 (usize) → max(1) = 1 → next_power_of_two() = 1 → clamp(64) = 64
        let config3 = JunctionConfig::new("test3".to_string()).with_expected_throughput(9);
        let buffer3 = StreamJunctionFactory::calculate_buffer_size(&config3);
        assert_eq!(
            buffer3, 64,
            "throughput=9 (no subscribers) should not panic and should use min buffer 64"
        );

        // Case 4: throughput=10, no subscribers (first value that does not round to 0)
        // Calculation: 10 * 0.1 = 1.0 → 1 (usize) → max(1) = 1 → next_power_of_two() = 1 → clamp(64) = 64
        let config4 = JunctionConfig::new("test4".to_string()).with_expected_throughput(10);
        let buffer4 = StreamJunctionFactory::calculate_buffer_size(&config4);
        assert_eq!(
            buffer4, 64,
            "throughput=10 (no subscribers) should use min buffer 64"
        );

        // Case 5: throughput=1000, no subscribers
        // Calculation: 1000 * 0.1 = 100.0 → 100 (usize) → max(1) = 100 → next_power_of_two() = 128
        let config5 = JunctionConfig::new("test5".to_string()).with_expected_throughput(1000);
        let buffer5 = StreamJunctionFactory::calculate_buffer_size(&config5);
        assert_eq!(
            buffer5, 128,
            "throughput=1000 (no subscribers) should use 128 buffer"
        );
    }

    #[test]
    fn test_junction_creation_with_very_low_throughput() {
        // Building a junction with very low throughput hints must succeed
        let context = create_test_context();
        let stream_def = create_test_stream_definition();

        let config = JunctionConfig::new("LowThroughputStream".to_string())
            .with_expected_throughput(1)
            .with_subscriber_count(1);

        let result = StreamJunctionFactory::create(config, stream_def, context, None);
        assert!(
            result.is_ok(),
            "Junction creation with throughput=1, subscribers=1 should succeed"
        );

        let junction = result.unwrap();
        assert_eq!(junction.lock().unwrap().stream_id, "LowThroughputStream");
    }
}
