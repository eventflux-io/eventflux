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

//! Event Pipeline built on crossbeam
//!
//! A bounded, lock free event queue between producers and consumers, built on the
//! crossbeam `ArrayQueue`, with pooled event holders, backpressure handling and metrics.

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
    /// Queue capacity. Must be a power of 2 and at least 64.
    pub capacity: usize,
    /// What to do when the queue or the pool is full
    pub backpressure: BackpressureStrategy,
    /// Meant to turn event holder pooling on or off. Not read by EventPipeline today,
    /// which always uses the pool.
    pub use_object_pool: bool,
    /// Number of consumer threads that take events from the pipeline
    ///
    /// Several consumers share the work for higher throughput.
    ///
    /// - `1` (default for sync mode): one thread, events processed in order
    /// - `N` (default for async mode): N consumers competing for events
    ///
    /// **Important**: StreamJunction reads this to start N consumer threads. They all
    /// share the same lock free queue, so the load spreads by competition.
    pub consumer_threads: usize,
    /// Batch size for consumers. Not used today, since consume() handles one event at
    /// a time.
    pub batch_size: usize,
    /// Meant to turn on detailed metrics.
    ///
    /// NOTE: not read today, so counters always record. If it is ever honored,
    /// events_published and events_consumed MUST keep recording all the time. The
    /// shutdown drain decides the pipeline is idle from `published == consumed`.
    pub enable_metrics: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            capacity: 65536, // 64K slots, a good balance of memory and speed
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
    /// Event queued, with its sequence number
    Success { sequence: u64 },
    /// Pipeline is full and backpressure was applied
    Full,
    /// Pipeline is shutting down
    Shutdown,
    /// Operation timed out
    Timeout,
    /// Processing error occurred
    Error(String),
}

/// Bounded event queue between producers and consumers. Safe to share between threads.
pub struct EventPipeline {
    /// Lock free bounded queue of events
    event_queue: Arc<ArrayQueue<PooledEvent>>,
    /// Pool of reusable event holders, twice the queue capacity
    event_pool: Arc<EventPool>,
    config: PipelineConfig,
    backpressure: Arc<BackpressureHandler>,
    metrics: Arc<PipelineMetrics>,
    /// Graceful stop. Consumers drain the queue, then exit.
    shutdown: AtomicBool,
    /// Hard stop. Consumers exit at the next iteration WITHOUT draining.
    /// Used when a shutdown drain times out and the leftover queue has
    /// already been counted as abandoned.
    halted: AtomicBool,
    /// Next sequence number. One is taken per publish attempt, so a rejected
    /// publish leaves a gap.
    sequence: CachePadded<AtomicU64>,
}

impl EventPipeline {
    /// Creates a pipeline with the given configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if `capacity` is not a power of 2 or is below 64.
    pub fn new(config: PipelineConfig) -> Result<Self, String> {
        if !config.capacity.is_power_of_two() {
            return Err("Capacity must be power of 2 for optimal performance".to_string());
        }
        if config.capacity < 64 {
            return Err("Minimum capacity is 64".to_string());
        }

        let event_queue = Arc::new(ArrayQueue::new(config.capacity));
        let event_pool = Arc::new(EventPool::new(config.capacity * 2)); // 2x for headroom
        let backpressure = Arc::new(BackpressureHandler::new(config.backpressure));
        let metrics = Arc::new(PipelineMetrics::new());

        Ok(Self {
            event_queue,
            event_pool,
            config,
            backpressure,
            metrics,
            shutdown: AtomicBool::new(false),
            halted: AtomicBool::new(false),
            sequence: CachePadded::new(AtomicU64::new(0)),
        })
    }

    /// Publishes an event to the pipeline (producer side). Never blocks.
    ///
    /// Returns `Shutdown` after shutdown, otherwise `Success` or what the backpressure
    /// handler decides when the pool or the queue is full.
    pub fn publish(&self, event: StreamEvent) -> PipelineResult {
        if self.shutdown.load(Ordering::Acquire) {
            return PipelineResult::Shutdown;
        }

        let start_time = Instant::now();
        let sequence = self.sequence.fetch_add(1, Ordering::AcqRel);

        // Take a pooled holder so the hot path does not allocate
        let pooled_event = match self.event_pool.acquire() {
            Some(mut pooled) => {
                pooled.set_event(event);
                pooled.set_sequence(sequence);
                pooled
            }
            None => {
                // Pool is empty, so the backpressure strategy decides
                self.metrics.increment_pool_exhausted();
                return self.backpressure.handle_pool_exhausted(event, sequence);
            }
        };

        // Enqueue. This is the main bottleneck.
        match self.event_queue.push(pooled_event) {
            Ok(()) => {
                // NOTE: the push lands BEFORE events_published increments, so
                // "queue empty && published == consumed" can be seen while an
                // event is still in the queue. Idle detection stays correct only
                // because every publisher inside the system is covered by another
                // counter the drain reads first (a dispatch's in_flight guard, or
                // a consumer's held back consumed increment). A publisher thread
                // that nothing covers and that pauses between these two lines is
                // invisible to the drain.
                let latency = start_time.elapsed();
                self.metrics.record_publish_success(latency);
                PipelineResult::Success { sequence }
            }
            Err(pooled_event) => {
                // Queue is full. Return the holder and let backpressure decide. release()
                // does not clear the holder, so the event stays in the pool until the
                // holder is reused.
                self.event_pool.release(pooled_event);
                self.metrics.increment_queue_full();
                self.backpressure.handle_queue_full(sequence)
            }
        }
    }

    /// Publishes without blocking. Same as [`publish`](Self::publish), which never
    /// blocks since the crossbeam ArrayQueue push does not block.
    pub fn try_publish(&self, event: StreamEvent) -> PipelineResult {
        self.publish(event)
    }

    /// Publishes a batch of events, one at a time, and returns one result per event.
    ///
    /// Every input event gets a result. Once shutdown lands mid batch, publish()
    /// returns `Shutdown` for each remaining event (a cheap flag check), and callers
    /// rely on that to count rejected events. An early `break` here would leave the
    /// tail with no result at all, so callers counting failures from the results
    /// would report the batch as successful while silently losing the rest.
    pub fn publish_batch(&self, events: Vec<StreamEvent>) -> Vec<PipelineResult> {
        let mut results = Vec::with_capacity(events.len());

        for event in events {
            results.push(self.publish(event));
        }

        results
    }

    /// Consumes events until shutdown (consumer side). Runs on the calling thread.
    ///
    /// Calls `handler` with each event and its sequence number. After shutdown it keeps
    /// going until the queue is empty. After halt it stops at once. While the queue is
    /// empty it yields briefly, then sleeps. Returns the number of events handled.
    ///
    /// # Errors
    ///
    /// Stops and returns an error on the first handler error. That event still counts as
    /// consumed.
    pub fn consume<F>(&self, mut handler: F) -> Result<u64, String>
    where
        F: FnMut(StreamEvent, u64) -> Result<(), String>,
    {
        let mut processed = 0u64;
        let mut idle_spins = 0u32;

        loop {
            if self.halted.load(Ordering::Acquire) {
                break; // Hard stop. Leave remaining events in the queue.
            }
            match self.event_queue.pop() {
                Some(mut pooled_event) => {
                    idle_spins = 0;
                    let start_time = Instant::now();
                    let sequence = pooled_event.sequence();
                    let event = pooled_event.take_event();

                    match handler(event, sequence) {
                        Ok(()) => {
                            processed += 1;
                            let latency = start_time.elapsed();
                            self.metrics.record_consume_success(latency);
                        }
                        Err(e) => {
                            // The event left the queue, so count it as consumed (as an
                            // error) to keep published == consumed reachable for the
                            // shutdown drain, and return its pool slot.
                            self.metrics.record_consume_error();
                            self.event_pool.release(pooled_event);
                            return Err(format!("Processing error at sequence {sequence}: {e}"));
                        }
                    }

                    self.event_pool.release(pooled_event);
                }
                None => {
                    // Queue is empty. Exit only if shutdown was requested.
                    if self.shutdown.load(Ordering::Acquire) {
                        break;
                    }
                    Self::idle_backoff(&mut idle_spins);
                }
            }
        }

        Ok(processed)
    }

    /// Backs off while the queue is empty. Yields for the first 64 tries so bursts
    /// are picked up fast, then sleeps 100 µs, then 1 ms from the 128th try on. That
    /// way idle consumer threads neither burn a core nor wake the scheduler 10k times
    /// a second each (there are more consumer threads as junctions are added).
    /// Once fully idle, a new event waits at most about 1 ms.
    fn idle_backoff(idle_spins: &mut u32) {
        const SPIN_LIMIT: u32 = 64;
        const SHORT_SLEEP_LIMIT: u32 = 128;
        if *idle_spins < SHORT_SLEEP_LIMIT {
            *idle_spins += 1;
        }
        if *idle_spins <= SPIN_LIMIT {
            std::thread::yield_now();
        } else if *idle_spins < SHORT_SLEEP_LIMIT {
            std::thread::sleep(std::time::Duration::from_micros(100));
        } else {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// Meant to return the share of capacity in use (0.0 to 1.0). As written it computes
    /// `(capacity - len) / capacity`, which is the free share.
    pub fn utilization(&self) -> f64 {
        let used = self.config.capacity - self.event_queue.len();
        used as f64 / self.config.capacity as f64
    }

    /// Free slots left in the queue
    pub fn remaining_capacity(&self) -> usize {
        self.config.capacity - self.event_queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.event_queue.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.event_queue.is_full()
    }

    pub fn metrics(&self) -> &PipelineMetrics {
        &self.metrics
    }

    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }

    /// Starts a graceful shutdown. New publishes return `Shutdown`, and consumers
    /// exit once the queue is empty.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    /// Hard stop. Consumers exit at their next iteration without draining the
    /// remaining queue. Callers must account for the leftover events (see
    /// `StreamJunction::abandon_pending`). Also does `shutdown()`.
    pub fn halt(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.halted.store(true, Ordering::Release);
    }

    /// Whether `halt()` was called (and not yet cleared by `clear_shutdown`).
    pub fn is_halted(&self) -> bool {
        self.halted.load(Ordering::Acquire)
    }

    /// Discards every queued event, returns the slots to the pool, and counts the
    /// discards as consumed so `published == consumed` holds again. Without that,
    /// every drain after a restart would see a permanent shortfall.
    /// Call only while halted, with no live consumers competing for the queue. A
    /// restart after a timed out drain must not replay events that were already
    /// reported as abandoned or dropped. Returns the number discarded.
    pub fn purge(&self) -> u64 {
        let mut purged = 0u64;
        while let Some(mut pooled_event) = self.event_queue.pop() {
            // Drop the payload before returning the slot. A released slot sits in
            // the pool until reused, so a purged backlog of large events would
            // otherwise stay in memory indefinitely.
            drop(pooled_event.take_event());
            self.event_pool.release(pooled_event);
            purged += 1;
        }
        if purged > 0 {
            self.metrics.record_purged(purged);
        }
        purged
    }

    /// True after `shutdown()` or `halt()`, until `clear_shutdown()`
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Clears the shutdown and halt flags so the pipeline can be reused after
    /// `shutdown()` or `halt()`. The caller must start new consumers, since any
    /// consumer that already saw the flags has exited.
    pub fn clear_shutdown(&self) {
        self.halted.store(false, Ordering::Release);
        self.shutdown.store(false, Ordering::Release);
    }

    pub fn reset_metrics(&self) {
        self.metrics.reset();
    }
}

/// Builder for event pipelines. Starts from [`PipelineConfig::default`].
pub struct PipelineBuilder {
    config: PipelineConfig,
}

impl PipelineBuilder {
    pub fn new() -> Self {
        Self {
            config: PipelineConfig::default(),
        }
    }

    /// Queue capacity. Must be a power of 2 and at least 64, checked in `build`.
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.config.capacity = capacity;
        self
    }

    pub fn with_backpressure(mut self, strategy: BackpressureStrategy) -> Self {
        self.config.backpressure = strategy;
        self
    }

    /// Sets `use_object_pool`, which EventPipeline does not read today
    pub fn with_object_pool(mut self, enable: bool) -> Self {
        self.config.use_object_pool = enable;
        self
    }

    pub fn with_consumer_threads(mut self, threads: usize) -> Self {
        self.config.consumer_threads = threads;
        self
    }

    /// Sets `batch_size`, which is not used today
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }

    /// Sets `enable_metrics`, which is not read today
    pub fn with_metrics(mut self, enable: bool) -> Self {
        self.config.enable_metrics = enable;
        self
    }

    /// Builds the pipeline.
    ///
    /// # Errors
    ///
    /// Returns an error if the capacity is not a power of 2 or is below 64.
    pub fn build(self) -> Result<EventPipeline, String> {
        EventPipeline::new(self.config)
    }
}

impl Default for PipelineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// Send and Sync come from the fields automatically. A hand written `unsafe impl` here
// would hide the compiler error if a later field (for example a pool optimization
// with raw pointers) stopped being thread safe. This check fails the build instead.
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<EventPipeline>()
};

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

        let events: Vec<_> = (0..10).map(create_test_event).collect();

        let results = pipeline.publish_batch(events);
        assert_eq!(results.len(), 10);

        for result in results {
            assert!(matches!(result, PipelineResult::Success { .. }));
        }
    }

    #[test]
    fn test_capacity_limits() {
        let pipeline = PipelineBuilder::new()
            .with_capacity(64) // The minimum capacity
            .with_object_pool(false) // Not read by EventPipeline, the pool stays on
            .build()
            .unwrap();

        // Publish more events than the capacity to trigger backpressure
        let mut full_result_count = 0;
        for i in 0..100 {
            let result = pipeline.publish(create_test_event(i));
            match result {
                PipelineResult::Success { .. } => {}
                PipelineResult::Full => full_result_count += 1,
                _ => {}
            }
        }

        // At least some events should hit backpressure
        assert!(
            full_result_count > 0,
            "Expected some events to trigger backpressure"
        );
    }

    #[test]
    fn test_consumer_processing() {
        let pipeline = Arc::new(PipelineBuilder::new().with_capacity(64).build().unwrap());

        for i in 0..10 {
            let _ = pipeline.publish(create_test_event(i));
        }

        // Consume on another thread, which shuts down after the expected events
        let processed_events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let events_clone = processed_events.clone();
        let pipeline_for_shutdown = Arc::clone(&pipeline);
        let pipeline_for_consume = Arc::clone(&pipeline);

        let handle = std::thread::spawn(move || {
            let mut count = 0;
            let result = pipeline_for_consume.consume(move |event, _sequence| {
                if let Some(AttributeValue::Int(value)) = event.before_window_data.first() {
                    events_clone.lock().unwrap().push(*value);
                    count += 1;

                    // Shut down once all events are processed
                    if count >= 10 {
                        pipeline_for_shutdown.shutdown();
                    }
                }
                Ok(())
            });
            result
        });

        // Wait for the consumer. There is no timeout.
        let result = handle.join().expect("Consumer thread panicked");
        assert!(result.is_ok(), "Consumer should complete successfully");

        let processed = processed_events.lock().unwrap();
        assert_eq!(processed.len(), 10);
    }

    /// A handler error must still count the popped event as consumed (the
    /// shutdown drain's `published == consumed` check depends on it) and
    /// return its slot to the pool. Going back to not counting errors would
    /// stall every later drain until its full timeout and leak a pool slot
    /// per error.
    #[test]
    fn test_consume_error_counts_event_and_releases_slot() {
        let pipeline = PipelineBuilder::new().with_capacity(64).build().unwrap();

        assert!(matches!(
            pipeline.publish(create_test_event(1)),
            PipelineResult::Success { .. }
        ));
        let result = pipeline.consume(|_event, _seq| Err("boom".to_string()));
        assert!(result.is_err(), "handler error must propagate");

        let metrics = pipeline.metrics();
        assert_eq!(
            metrics.events_consumed_count(),
            1,
            "errored event left the queue and must count as consumed"
        );
        assert_eq!(metrics.snapshot().processing_errors, 1);
        assert_eq!(
            metrics.events_published_count(),
            metrics.events_consumed_count(),
            "drain predicate must be balanced after a handler error"
        );
        assert!(pipeline.is_empty());

        // The slot must be reusable, so publishing and consuming again succeeds.
        assert!(matches!(
            pipeline.publish(create_test_event(2)),
            PipelineResult::Success { .. }
        ));
        pipeline.shutdown();
        let processed = pipeline.consume(|_event, _seq| Ok(())).unwrap();
        assert_eq!(processed, 1);
    }

    /// Once shutdown lands mid batch, every remaining event must still get a
    /// `Shutdown` result. An early exit would leave the tail unreported and
    /// callers would count the batch as fully successful.
    #[test]
    fn test_publish_batch_reports_shutdown_for_full_tail() {
        let pipeline = PipelineBuilder::new().with_capacity(64).build().unwrap();
        pipeline.shutdown();

        let events: Vec<_> = (0..5).map(create_test_event).collect();
        let results = pipeline.publish_batch(events);
        assert_eq!(results.len(), 5, "every event needs a result");
        assert!(results
            .iter()
            .all(|r| matches!(r, PipelineResult::Shutdown)));
    }
}
