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

//! Crossbeam-based High-Performance Event Pipeline
//!
//! This provides a lock-free, high-throughput event processing pipeline using
//! battle-tested crossbeam primitives optimized for Rust's performance characteristics.

use crossbeam_queue::ArrayQueue;
use crossbeam_utils::CachePadded;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use super::{BackpressureHandler, BackpressureStrategy, EventPool, PipelineMetrics, PooledEvent};
use crate::core::event::stream::StreamEvent;

/// Configuration for the event pipeline
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Ring buffer capacity (must be power of 2 for optimal performance)
    pub capacity: usize,
    /// Backpressure handling strategy
    pub backpressure: BackpressureStrategy,
    /// Enable object pooling for zero-allocation processing
    pub use_object_pool: bool,
    /// Number of consumer threads for parallel event consumption
    ///
    /// Controls how many concurrent consumers will poll events from the pipeline.
    /// Multiple consumers enable work-stealing for higher throughput.
    ///
    /// - `1` (default for sync mode): Single-threaded, sequential processing
    /// - `N` (default for async mode): N parallel consumers competing for events
    ///
    /// **Important**: This setting is read by StreamJunction to spawn N consumer threads.
    /// All consumers share the same lock-free queue, naturally load-balancing via competition.
    pub consumer_threads: usize,
    /// Batch size for consumer processing
    pub batch_size: usize,
    /// Enable detailed metrics collection
    pub enable_metrics: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            capacity: 65536, // 64K slots - good balance of memory vs performance
            backpressure: BackpressureStrategy::Drop,
            use_object_pool: true,
            consumer_threads: num_cpus::get(),
            batch_size: 64,
            enable_metrics: true,
        }
    }
}

/// Result of pipeline operations
#[derive(Debug, Clone, PartialEq)]
pub enum PipelineResult {
    /// Event processed successfully
    Success { sequence: u64 },
    /// Pipeline is full - backpressure applied
    Full,
    /// Pipeline is shutting down
    Shutdown,
    /// Operation timed out
    Timeout,
    /// Processing error occurred
    Error(String),
}

/// High-performance event processing pipeline
pub struct EventPipeline {
    /// Core event queue - lock-free bounded queue
    event_queue: Arc<ArrayQueue<PooledEvent>>,
    /// Object pool for zero-allocation event processing
    event_pool: Arc<EventPool>,
    /// Pipeline configuration
    config: PipelineConfig,
    /// Backpressure handler
    backpressure: Arc<BackpressureHandler>,
    /// Pipeline metrics
    metrics: Arc<PipelineMetrics>,
    /// Shutdown coordination
    shutdown: Arc<AtomicBool>,
    /// Sequence generator for ordering
    sequence: CachePadded<AtomicU64>,
}

impl EventPipeline {
    /// Create a new event pipeline with the given configuration
    pub fn new(config: PipelineConfig) -> Result<Self, String> {
        // Validate configuration
        if !config.capacity.is_power_of_two() {
            return Err("Capacity must be power of 2 for optimal performance".to_string());
        }
        if config.capacity < 64 {
            return Err("Minimum capacity is 64".to_string());
        }

        let event_queue = Arc::new(ArrayQueue::new(config.capacity));
        let event_pool = Arc::new(EventPool::new(config.capacity * 2)); // 2x for buffering
        let backpressure = Arc::new(BackpressureHandler::new(config.backpressure));
        let metrics = Arc::new(PipelineMetrics::new());
        let shutdown = Arc::new(AtomicBool::new(false));

        Ok(Self {
            event_queue,
            event_pool,
            config,
            backpressure,
            metrics,
            shutdown,
            sequence: CachePadded::new(AtomicU64::new(0)),
        })
    }

    /// Publish an event to the pipeline (producer side)
    pub fn publish(&self, event: StreamEvent) -> PipelineResult {
        if self.shutdown.load(Ordering::Acquire) {
            return PipelineResult::Shutdown;
        }

        let start_time = Instant::now();
        let sequence = self.sequence.fetch_add(1, Ordering::AcqRel);

        // Get pooled event holder for zero-allocation processing
        let pooled_event = match self.event_pool.acquire() {
            Some(mut pooled) => {
                pooled.set_event(event);
                pooled.set_sequence(sequence);
                pooled
            }
            None => {
                // Pool exhausted - handle based on backpressure strategy
                self.metrics.increment_pool_exhausted();
                return self.backpressure.handle_pool_exhausted(event, sequence);
            }
        };

        // Attempt to enqueue - this is the main bottleneck
        match self.event_queue.push(pooled_event) {
            Ok(()) => {
                // Success - update metrics
                let latency = start_time.elapsed();
                self.metrics.record_publish_success(latency);
                PipelineResult::Success { sequence }
            }
            Err(pooled_event) => {
                // Queue full - handle backpressure
                self.event_pool.release(pooled_event); // Return to pool
                self.metrics.increment_queue_full();
                self.backpressure.handle_queue_full(sequence)
            }
        }
    }

    /// Try to publish without blocking (non-blocking producer)
    pub fn try_publish(&self, event: StreamEvent) -> PipelineResult {
        // For crossbeam ArrayQueue, publish() is already non-blocking
        self.publish(event)
    }

    /// Publish a batch of events for higher throughput
    pub fn publish_batch(&self, events: Vec<StreamEvent>) -> Vec<PipelineResult> {
        let mut results = Vec::with_capacity(events.len());

        for event in events {
            results.push(self.publish(event));

            // Early exit on shutdown
            if self.shutdown.load(Ordering::Acquire) {
                break;
            }
        }

        results
    }

    /// Consume events from the pipeline (consumer side)
    pub fn consume<F>(&self, mut handler: F) -> Result<u64, String>
    where
        F: FnMut(StreamEvent, u64) -> Result<(), String>,
    {
        let mut processed = 0u64;

        loop {
            match self.event_queue.pop() {
                Some(mut pooled_event) => {
                    let start_time = Instant::now();
                    let sequence = pooled_event.sequence();
                    let event = pooled_event.take_event();

                    // Process the event
                    match handler(event, sequence) {
                        Ok(()) => {
                            processed += 1;
                            let latency = start_time.elapsed();
                            self.metrics.record_consume_success(latency);
                        }
                        Err(e) => {
                            self.metrics.increment_processing_errors();
                            return Err(format!("Processing error at sequence {sequence}: {e}"));
                        }
                    }

                    // Return pooled event for reuse
                    self.event_pool.release(pooled_event);
                }
                None => {
                    // No events available - check if we should shutdown
                    if self.shutdown.load(Ordering::Acquire) {
                        break; // Exit only when no events and shutdown requested
                    }
                    // Otherwise yield to prevent busy spinning
                    std::thread::yield_now();
                }
            }
        }

        Ok(processed)
    }

    /// Consume events in batches for higher throughput
    pub fn consume_batch<F>(&self, mut handler: F, batch_size: usize) -> Result<u64, String>
    where
        F: FnMut(Vec<(StreamEvent, u64)>) -> Result<(), String>,
    {
        let mut processed = 0u64;
        let mut batch = Vec::with_capacity(batch_size);

        loop {
            // Collect a batch
            for _ in 0..batch_size {
                match self.event_queue.pop() {
                    Some(mut pooled_event) => {
                        let sequence = pooled_event.sequence();
                        let event = pooled_event.take_event();
                        batch.push((event, sequence));
                        self.event_pool.release(pooled_event);
                    }
                    None => break, // No more events available
                }
            }

            if !batch.is_empty() {
                let start_time = Instant::now();
                let batch_len = batch.len();

                // Process the batch
                match handler(batch.clone()) {
                    Ok(()) => {
                        processed += batch_len as u64;
                        let latency = start_time.elapsed();
                        self.metrics.record_batch_success(batch_len, latency);
                    }
                    Err(e) => {
                        self.metrics.increment_processing_errors();
                        return Err(format!("Batch processing error: {e}"));
                    }
                }

                batch.clear();
            } else {
                // No events in batch - check if we should shutdown
                if self.shutdown.load(Ordering::Acquire) {
                    break; // Exit only when no events and shutdown requested
                }
                // Otherwise yield to prevent busy spinning
                std::thread::yield_now();
            }
        }

        Ok(processed)
    }

    /// Get current pipeline capacity utilization (0.0 to 1.0)
    pub fn utilization(&self) -> f64 {
        let used = self.config.capacity - self.event_queue.len();
        used as f64 / self.config.capacity as f64
    }

    /// Get remaining capacity in the pipeline
    pub fn remaining_capacity(&self) -> usize {
        self.config.capacity - self.event_queue.len()
    }

    /// Check if pipeline is empty
    pub fn is_empty(&self) -> bool {
        self.event_queue.is_empty()
    }

    /// Check if pipeline is full
    pub fn is_full(&self) -> bool {
        self.event_queue.is_full()
    }

    /// Get pipeline metrics
    pub fn metrics(&self) -> &PipelineMetrics {
        &self.metrics
    }

    /// Get pipeline configuration
    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }

    /// Initiate graceful shutdown
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    /// Check if pipeline is shutting down
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Reset pipeline metrics
    pub fn reset_metrics(&self) {
        self.metrics.reset();
    }
}

/// Builder for creating event pipelines with fluent API
pub struct PipelineBuilder {
    config: PipelineConfig,
}

impl PipelineBuilder {
    /// Create a new pipeline builder with default configuration
    pub fn new() -> Self {
        Self {
            config: PipelineConfig::default(),
        }
    }

    /// Set pipeline capacity
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.config.capacity = capacity;
        self
    }

    /// Set backpressure strategy
    pub fn with_backpressure(mut self, strategy: BackpressureStrategy) -> Self {
        self.config.backpressure = strategy;
        self
    }

    /// Enable or disable object pooling
    pub fn with_object_pool(mut self, enable: bool) -> Self {
        self.config.use_object_pool = enable;
        self
    }

    /// Set number of consumer threads
    pub fn with_consumer_threads(mut self, threads: usize) -> Self {
        self.config.consumer_threads = threads;
        self
    }

    /// Set batch size for batch processing
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }

    /// Enable or disable metrics collection
    pub fn with_metrics(mut self, enable: bool) -> Self {
        self.config.enable_metrics = enable;
        self
    }

    /// Build the event pipeline
    pub fn build(self) -> Result<EventPipeline, String> {
        EventPipeline::new(self.config)
    }
}

impl Default for PipelineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

unsafe impl Send for EventPipeline {}
unsafe impl Sync for EventPipeline {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::value::AttributeValue;

    fn create_test_event(id: i32) -> StreamEvent {
        StreamEvent::new_with_data(0, vec![AttributeValue::Int(id)])
    }

    #[test]
    fn test_pipeline_creation() {
        let pipeline = PipelineBuilder::new().with_capacity(1024).build();
        assert!(pipeline.is_ok());
    }

    #[test]
    fn test_single_event_processing() {
        let pipeline = PipelineBuilder::new().with_capacity(64).build().unwrap();

        let event = create_test_event(42);
        let result = pipeline.publish(event);

        assert!(matches!(result, PipelineResult::Success { .. }));
        assert_eq!(pipeline.event_queue.len(), 1);
    }

    #[test]
    fn test_batch_processing() {
        let pipeline = PipelineBuilder::new().with_capacity(128).build().unwrap();

        let events: Vec<_> = (0..10).map(|i| create_test_event(i)).collect();

        let results = pipeline.publish_batch(events);
        assert_eq!(results.len(), 10);

        for result in results {
            assert!(matches!(result, PipelineResult::Success { .. }));
        }
    }

    #[test]
    fn test_capacity_limits() {
        let pipeline = PipelineBuilder::new()
            .with_capacity(64) // Use minimum capacity
            .with_object_pool(false) // Disable object pool to test queue limits directly
            .build()
            .unwrap();

        // Fill pipeline beyond capacity to trigger backpressure
        let mut full_result_count = 0;
        for i in 0..100 {
            // Try more events than capacity
            let result = pipeline.publish(create_test_event(i));
            match result {
                PipelineResult::Success { .. } => {} // Allow some success
                PipelineResult::Full => full_result_count += 1,
                _ => {} // Other results possible
            }
        }

        // At least some events should trigger backpressure
        assert!(
            full_result_count > 0,
            "Expected some events to trigger backpressure"
        );
    }

    #[test]
    fn test_consumer_processing() {
        let pipeline = Arc::new(PipelineBuilder::new().with_capacity(64).build().unwrap());

        // Publish events
        for i in 0..10 {
            let _ = pipeline.publish(create_test_event(i));
        }

        // Consume events in a separate thread with shutdown after processing expected events
        let processed_events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let events_clone = processed_events.clone();
        let pipeline_for_shutdown = Arc::clone(&pipeline);
        let pipeline_for_consume = Arc::clone(&pipeline);

        let handle = std::thread::spawn(move || {
            let mut count = 0;
            let result = pipeline_for_consume.consume(move |event, _sequence| {
                if let Some(AttributeValue::Int(value)) = event.before_window_data.get(0) {
                    events_clone.lock().unwrap().push(*value);
                    count += 1;

                    // Signal shutdown after processing all events
                    if count >= 10 {
                        pipeline_for_shutdown.shutdown();
                    }
                }
                Ok(())
            });
            result
        });

        // Wait for completion with timeout
        let result = handle.join().expect("Consumer thread panicked");
        assert!(result.is_ok(), "Consumer should complete successfully");

        let processed = processed_events.lock().unwrap();
        assert_eq!(processed.len(), 10);
    }
}
