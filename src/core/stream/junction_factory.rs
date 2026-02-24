// SPDX-License-Identifier: MIT OR Apache-2.0

//! StreamJunction Factory for Event Routing
//!
//! Provides configuration and creation of high-performance StreamJunctions
//! using crossbeam pipeline-based implementation.

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::stream::StreamJunction;
use crate::query_api::definition::StreamDefinition;
use std::sync::{Arc, Mutex};

/// Configuration for StreamJunction creation
#[derive(Debug, Clone)]
pub struct JunctionConfig {
    pub stream_id: String,
    pub buffer_size: usize,
    pub is_async: bool,
    pub expected_throughput: Option<u64>, // events/second
    pub subscriber_count: Option<usize>,
}

impl JunctionConfig {
    /// Create a new junction configuration with synchronous processing by default
    ///
    /// **Default Mode: Synchronous (is_async: false)**
    /// - Guarantees strict event ordering
    /// - Events are processed sequentially in the order they arrive
    /// - Suitable for scenarios where event order is critical
    ///
    /// **Async Mode Option: Use with_async(true) for high-throughput scenarios**
    /// - Trades event ordering guarantees for higher performance
    /// - Events may be processed out of order due to concurrent processing
    /// - Suitable for scenarios where throughput > ordering
    pub fn new(stream_id: String) -> Self {
        Self {
            stream_id,
            buffer_size: 4096,
            is_async: false, // DEFAULT: Synchronous to guarantee event ordering
            expected_throughput: None,
            subscriber_count: None,
        }
    }

    /// Set buffer size
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// Enable or disable async processing mode [CRITICAL ORDERING TRADE-OFF]
    ///
    /// **⚠️  IMPORTANT: Enabling async mode may break event ordering guarantees!**
    ///
    /// **Synchronous Mode (false - DEFAULT):**
    /// - ✅ **Strict event ordering preserved**
    /// - ✅ Events processed sequentially in arrival order
    /// - ✅ Predictable, deterministic behavior
    /// - ❌ Lower throughput (~thousands events/sec)
    /// - **Use when**: Event order is critical for correctness
    ///
    /// **Async Mode (true):**
    /// - ✅ **High throughput** (>100K events/sec capability)
    /// - ✅ Better resource utilization with concurrent processing
    /// - ✅ Non-blocking, scalable performance
    /// - ❌ **Events may be processed out of order**
    /// - ❌ Less predictable timing behavior
    /// - **Use when**: Throughput > strict ordering requirements
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

    /// Set expected throughput hint
    pub fn with_expected_throughput(mut self, throughput: u64) -> Self {
        self.expected_throughput = Some(throughput);
        self
    }

    /// Set expected subscriber count hint
    pub fn with_subscriber_count(mut self, count: usize) -> Self {
        self.subscriber_count = Some(count);
        self
    }
}

/// Factory for creating StreamJunctions
pub struct StreamJunctionFactory;

impl StreamJunctionFactory {
    /// Create a StreamJunction with the given configuration
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

    /// Create a StreamJunction (internal implementation)
    pub fn create_junction(
        config: JunctionConfig,
        stream_definition: Arc<StreamDefinition>,
        eventflux_app_context: Arc<EventFluxAppContext>,
        fault_stream_junction: Option<Arc<Mutex<StreamJunction>>>,
    ) -> Result<Arc<Mutex<StreamJunction>>, String> {
        // Apply performance hints to calculate optimal buffer size
        let buffer_size = Self::calculate_buffer_size(&config);

        let mut junction = StreamJunction::new(
            config.stream_id,
            stream_definition,
            eventflux_app_context,
            buffer_size,
            config.is_async,
            fault_stream_junction,
        )?;

        // Pre-allocate subscriber capacity based on hint
        if let Some(subscriber_count) = config.subscriber_count {
            junction.reserve_subscriber_capacity(subscriber_count);
        }

        Ok(Arc::new(Mutex::new(junction)))
    }

    /// Calculate optimal buffer size based on performance hints
    ///
    /// Uses the formula: buffer_size = max(1, expected_throughput * subscriber_count * 0.1)
    /// This provides approximately 100ms of buffering at the expected throughput rate.
    ///
    /// # Heuristic
    /// - If throughput is 100K events/sec and 4 subscribers: 100000 * 4 * 0.1 = 40K buffer
    /// - Ensures sufficient buffering for burst traffic while avoiding excessive memory use
    /// - Falls back to explicit buffer_size if hints not provided
    ///
    /// # Safety
    /// - Ensures calculated value is at least 1 to prevent panic in next_power_of_two()
    /// - Handles edge cases where throughput * subscribers < 10 (which would round to 0)
    fn calculate_buffer_size(config: &JunctionConfig) -> usize {
        match (config.expected_throughput, config.subscriber_count) {
            (Some(throughput), Some(subscribers)) if throughput > 0 && subscribers > 0 => {
                // Calculate based on heuristic: 100ms buffer at expected rate
                // SAFETY: Ensure at least 1 to prevent panic in next_power_of_two()
                // For very low throughput (e.g., 1-9 events/sec), 0.1 multiplication
                // would round down to 0 when cast to usize, causing a panic.
                let calculated = ((throughput as f64 * subscribers as f64 * 0.1) as usize).max(1);

                // Ensure power of 2 for optimal performance (crossbeam ArrayQueue requirement)
                // next_power_of_two() panics on 0, so we ensure calculated >= 1 above
                let mut buffer_size = calculated.next_power_of_two();

                // Clamp to reasonable limits: min 64, max 1M
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
                // Only throughput hint - assume 1 subscriber
                // SAFETY: Ensure at least 1 to prevent panic in next_power_of_two()
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
                // No hints or invalid - use explicit buffer_size
                config.buffer_size
            }
        }
    }

    /// Create a junction with performance hints
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

    /// Create a high-performance junction for known high-throughput scenarios
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

/// Performance benchmark for comparing junction implementations
pub struct JunctionBenchmark;

impl JunctionBenchmark {
    /// Run a simple throughput benchmark
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

/// Benchmark result for junction performance testing
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub events_sent: usize,
    pub duration: std::time::Duration,
    pub throughput: f64,
    pub implementation: String,
}

impl BenchmarkResult {
    /// Print benchmark results
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

        // High throughput hint should create junction successfully
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
        // Test heuristic: buffer_size = throughput * subscribers * 0.1

        // Test case 1: 100K events/sec, 4 subscribers
        // Expected: 100000 * 4 * 0.1 = 40000 → next_power_of_two = 65536
        let config1 = JunctionConfig::new("test1".to_string())
            .with_expected_throughput(100_000)
            .with_subscriber_count(4);
        let buffer1 = StreamJunctionFactory::calculate_buffer_size(&config1);
        assert_eq!(
            buffer1, 65536,
            "100K throughput with 4 subscribers should use 64K buffer"
        );

        // Test case 2: 10K events/sec, 2 subscribers
        // Expected: 10000 * 2 * 0.1 = 2000 → next_power_of_two = 2048
        let config2 = JunctionConfig::new("test2".to_string())
            .with_expected_throughput(10_000)
            .with_subscriber_count(2);
        let buffer2 = StreamJunctionFactory::calculate_buffer_size(&config2);
        assert_eq!(
            buffer2, 2048,
            "10K throughput with 2 subscribers should use 2K buffer"
        );

        // Test case 3: Only throughput hint (no subscriber count)
        // Expected: 50000 * 0.1 = 5000 → next_power_of_two = 8192
        let config3 = JunctionConfig::new("test3".to_string()).with_expected_throughput(50_000);
        let buffer3 = StreamJunctionFactory::calculate_buffer_size(&config3);
        assert_eq!(
            buffer3, 8192,
            "50K throughput with no subscriber hint should use 8K buffer"
        );

        // Test case 4: No hints - should use explicit buffer_size
        let config4 = JunctionConfig::new("test4".to_string()).with_buffer_size(4096);
        let buffer4 = StreamJunctionFactory::calculate_buffer_size(&config4);
        assert_eq!(buffer4, 4096, "No hints should use explicit buffer_size");

        // Test case 5: Very low throughput - should clamp to minimum 64
        let config5 = JunctionConfig::new("test5".to_string())
            .with_expected_throughput(10)
            .with_subscriber_count(1);
        let buffer5 = StreamJunctionFactory::calculate_buffer_size(&config5);
        assert_eq!(
            buffer5, 64,
            "Very low throughput should clamp to minimum 64"
        );

        // Test case 6: Very high throughput - should clamp to maximum 1M
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

        // Create junction with subscriber count hint
        let config = JunctionConfig::new("ReservedStream".to_string())
            .with_subscriber_count(10)
            .with_buffer_size(4096);

        let junction = StreamJunctionFactory::create(config, stream_def, context, None).unwrap();

        // Verify the junction was created (capacity reservation doesn't expose public API)
        assert_eq!(junction.lock().unwrap().stream_id, "ReservedStream");

        // Note: The actual capacity is internal to Vec, but the reservation prevents
        // reallocation when adding up to 10 subscribers
    }

    #[test]
    fn test_buffer_size_calculation_edge_cases() {
        // CRITICAL: Test edge cases that would cause panic before the fix
        // These tests verify the .max(1) fix prevents next_power_of_two() panic on 0

        // Test case 1: throughput=1, subscribers=1
        // Calculation: 1 * 1 * 0.1 = 0.1 → 0 (usize) → max(1) = 1 → next_power_of_two() = 1 → clamp(64) = 64
        let config1 = JunctionConfig::new("test1".to_string())
            .with_expected_throughput(1)
            .with_subscriber_count(1);
        let buffer1 = StreamJunctionFactory::calculate_buffer_size(&config1);
        assert_eq!(
            buffer1, 64,
            "throughput=1, subscribers=1 should not panic and should use min buffer 64"
        );

        // Test case 2: throughput=5, subscribers=1
        // Calculation: 5 * 1 * 0.1 = 0.5 → 0 (usize) → max(1) = 1 → next_power_of_two() = 1 → clamp(64) = 64
        let config2 = JunctionConfig::new("test2".to_string())
            .with_expected_throughput(5)
            .with_subscriber_count(1);
        let buffer2 = StreamJunctionFactory::calculate_buffer_size(&config2);
        assert_eq!(
            buffer2, 64,
            "throughput=5, subscribers=1 should not panic and should use min buffer 64"
        );

        // Test case 3: throughput=1, subscribers=5
        // Calculation: 1 * 5 * 0.1 = 0.5 → 0 (usize) → max(1) = 1 → next_power_of_two() = 1 → clamp(64) = 64
        let config3 = JunctionConfig::new("test3".to_string())
            .with_expected_throughput(1)
            .with_subscriber_count(5);
        let buffer3 = StreamJunctionFactory::calculate_buffer_size(&config3);
        assert_eq!(
            buffer3, 64,
            "throughput=1, subscribers=5 should not panic and should use min buffer 64"
        );

        // Test case 4: throughput=9, subscribers=1 (edge case just below 10)
        // Calculation: 9 * 1 * 0.1 = 0.9 → 0 (usize) → max(1) = 1 → next_power_of_two() = 1 → clamp(64) = 64
        let config4 = JunctionConfig::new("test4".to_string())
            .with_expected_throughput(9)
            .with_subscriber_count(1);
        let buffer4 = StreamJunctionFactory::calculate_buffer_size(&config4);
        assert_eq!(
            buffer4, 64,
            "throughput=9, subscribers=1 should not panic and should use min buffer 64"
        );

        // Test case 5: throughput=10, subscribers=1 (first value that doesn't round to 0)
        // Calculation: 10 * 1 * 0.1 = 1.0 → 1 (usize) → max(1) = 1 → next_power_of_two() = 1 → clamp(64) = 64
        let config5 = JunctionConfig::new("test5".to_string())
            .with_expected_throughput(10)
            .with_subscriber_count(1);
        let buffer5 = StreamJunctionFactory::calculate_buffer_size(&config5);
        assert_eq!(
            buffer5, 64,
            "throughput=10, subscribers=1 should use min buffer 64"
        );

        // Test case 6: throughput=100, subscribers=1 (should calculate meaningful buffer)
        // Calculation: 100 * 1 * 0.1 = 10.0 → 10 (usize) → max(1) = 10 → next_power_of_two() = 16 → clamp(64) = 64
        let config6 = JunctionConfig::new("test6".to_string())
            .with_expected_throughput(100)
            .with_subscriber_count(1);
        let buffer6 = StreamJunctionFactory::calculate_buffer_size(&config6);
        assert_eq!(
            buffer6, 64,
            "throughput=100, subscribers=1 should use min buffer 64"
        );

        // Test case 7: throughput=1000, subscribers=1 (should calculate meaningful buffer)
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
        // CRITICAL: Test throughput-only branch edge cases

        // Test case 1: throughput=1, no subscribers
        // Calculation: 1 * 0.1 = 0.1 → 0 (usize) → max(1) = 1 → next_power_of_two() = 1 → clamp(64) = 64
        let config1 = JunctionConfig::new("test1".to_string()).with_expected_throughput(1);
        let buffer1 = StreamJunctionFactory::calculate_buffer_size(&config1);
        assert_eq!(
            buffer1, 64,
            "throughput=1 (no subscribers) should not panic and should use min buffer 64"
        );

        // Test case 2: throughput=5, no subscribers
        // Calculation: 5 * 0.1 = 0.5 → 0 (usize) → max(1) = 1 → next_power_of_two() = 1 → clamp(64) = 64
        let config2 = JunctionConfig::new("test2".to_string()).with_expected_throughput(5);
        let buffer2 = StreamJunctionFactory::calculate_buffer_size(&config2);
        assert_eq!(
            buffer2, 64,
            "throughput=5 (no subscribers) should not panic and should use min buffer 64"
        );

        // Test case 3: throughput=9, no subscribers (edge case just below 10)
        // Calculation: 9 * 0.1 = 0.9 → 0 (usize) → max(1) = 1 → next_power_of_two() = 1 → clamp(64) = 64
        let config3 = JunctionConfig::new("test3".to_string()).with_expected_throughput(9);
        let buffer3 = StreamJunctionFactory::calculate_buffer_size(&config3);
        assert_eq!(
            buffer3, 64,
            "throughput=9 (no subscribers) should not panic and should use min buffer 64"
        );

        // Test case 4: throughput=10, no subscribers (first value that doesn't round to 0)
        // Calculation: 10 * 0.1 = 1.0 → 1 (usize) → max(1) = 1 → next_power_of_two() = 1 → clamp(64) = 64
        let config4 = JunctionConfig::new("test4".to_string()).with_expected_throughput(10);
        let buffer4 = StreamJunctionFactory::calculate_buffer_size(&config4);
        assert_eq!(
            buffer4, 64,
            "throughput=10 (no subscribers) should use min buffer 64"
        );

        // Test case 5: throughput=1000, no subscribers
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
        // Integration test: Verify junction creation doesn't panic with low throughput
        let context = create_test_context();
        let stream_def = create_test_stream_definition();

        // This would have panicked before the fix
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
