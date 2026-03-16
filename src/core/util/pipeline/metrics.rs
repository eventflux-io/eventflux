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

//! Pipeline Performance Metrics
//!
//! Comprehensive metrics collection for monitoring pipeline performance,
//! latency distribution, and throughput characteristics.

use crossbeam_utils::CachePadded;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Core pipeline metrics for performance monitoring
pub struct PipelineMetrics {
    // Throughput metrics
    pub(crate) events_published: CachePadded<AtomicU64>,
    pub(crate) events_consumed: CachePadded<AtomicU64>,
    pub(crate) events_dropped: CachePadded<AtomicU64>,
    pub(crate) batches_processed: CachePadded<AtomicU64>,

    // Backpressure metrics
    pub(crate) queue_full_events: CachePadded<AtomicU64>,
    pub(crate) pool_exhausted_events: CachePadded<AtomicU64>,
    pub(crate) processing_errors: CachePadded<AtomicU64>,

    // Latency metrics (in nanoseconds)
    pub(crate) total_publish_latency_ns: CachePadded<AtomicU64>,
    pub(crate) total_consume_latency_ns: CachePadded<AtomicU64>,
    pub(crate) max_publish_latency_ns: CachePadded<AtomicU64>,
    pub(crate) max_consume_latency_ns: CachePadded<AtomicU64>,

    // Timing
    start_time: Instant,
}

impl PipelineMetrics {
    /// Create new metrics instance
    pub fn new() -> Self {
        Self {
            events_published: CachePadded::new(AtomicU64::new(0)),
            events_consumed: CachePadded::new(AtomicU64::new(0)),
            events_dropped: CachePadded::new(AtomicU64::new(0)),
            batches_processed: CachePadded::new(AtomicU64::new(0)),
            queue_full_events: CachePadded::new(AtomicU64::new(0)),
            pool_exhausted_events: CachePadded::new(AtomicU64::new(0)),
            processing_errors: CachePadded::new(AtomicU64::new(0)),
            total_publish_latency_ns: CachePadded::new(AtomicU64::new(0)),
            total_consume_latency_ns: CachePadded::new(AtomicU64::new(0)),
            max_publish_latency_ns: CachePadded::new(AtomicU64::new(0)),
            max_consume_latency_ns: CachePadded::new(AtomicU64::new(0)),
            start_time: Instant::now(),
        }
    }

    /// Record a successful publish operation
    pub fn record_publish_success(&self, latency: Duration) {
        self.events_published.fetch_add(1, Ordering::Relaxed);

        let latency_ns = latency.as_nanos() as u64;
        self.total_publish_latency_ns
            .fetch_add(latency_ns, Ordering::Relaxed);

        // Update max latency using compare-and-swap loop
        let mut current_max = self.max_publish_latency_ns.load(Ordering::Relaxed);
        while latency_ns > current_max {
            match self.max_publish_latency_ns.compare_exchange_weak(
                current_max,
                latency_ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }
    }

    /// Record a successful consume operation
    pub fn record_consume_success(&self, latency: Duration) {
        self.events_consumed.fetch_add(1, Ordering::Relaxed);

        let latency_ns = latency.as_nanos() as u64;
        self.total_consume_latency_ns
            .fetch_add(latency_ns, Ordering::Relaxed);

        // Update max latency
        let mut current_max = self.max_consume_latency_ns.load(Ordering::Relaxed);
        while latency_ns > current_max {
            match self.max_consume_latency_ns.compare_exchange_weak(
                current_max,
                latency_ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }
    }

    /// Record a successful batch operation
    pub fn record_batch_success(&self, batch_size: usize, latency: Duration) {
        self.batches_processed.fetch_add(1, Ordering::Relaxed);
        self.events_consumed
            .fetch_add(batch_size as u64, Ordering::Relaxed);

        let latency_ns = latency.as_nanos() as u64;
        self.total_consume_latency_ns
            .fetch_add(latency_ns, Ordering::Relaxed);

        // Update max latency
        let mut current_max = self.max_consume_latency_ns.load(Ordering::Relaxed);
        while latency_ns > current_max {
            match self.max_consume_latency_ns.compare_exchange_weak(
                current_max,
                latency_ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }
    }

    /// Increment queue full counter
    pub fn increment_queue_full(&self) {
        self.queue_full_events.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment pool exhausted counter  
    pub fn increment_pool_exhausted(&self) {
        self.pool_exhausted_events.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment dropped events counter
    pub fn increment_dropped(&self) {
        self.events_dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment processing errors counter
    pub fn increment_processing_errors(&self) {
        self.processing_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Get current throughput (events/second)
    pub fn throughput(&self) -> f64 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed == 0.0 {
            return 0.0;
        }

        let total_events = self.events_published.load(Ordering::Relaxed) as f64;
        total_events / elapsed
    }

    /// Get average publish latency in microseconds
    pub fn avg_publish_latency_us(&self) -> f64 {
        let total_events = self.events_published.load(Ordering::Relaxed);
        if total_events == 0 {
            return 0.0;
        }

        let total_latency = self.total_publish_latency_ns.load(Ordering::Relaxed);
        (total_latency as f64 / total_events as f64) / 1000.0
    }

    /// Get average consume latency in microseconds
    pub fn avg_consume_latency_us(&self) -> f64 {
        let total_events = self.events_consumed.load(Ordering::Relaxed);
        if total_events == 0 {
            return 0.0;
        }

        let total_latency = self.total_consume_latency_ns.load(Ordering::Relaxed);
        (total_latency as f64 / total_events as f64) / 1000.0
    }

    /// Get maximum publish latency in microseconds
    pub fn max_publish_latency_us(&self) -> f64 {
        self.max_publish_latency_ns.load(Ordering::Relaxed) as f64 / 1000.0
    }

    /// Get maximum consume latency in microseconds
    pub fn max_consume_latency_us(&self) -> f64 {
        self.max_consume_latency_ns.load(Ordering::Relaxed) as f64 / 1000.0
    }

    /// Get error rate (0.0 to 1.0)
    pub fn error_rate(&self) -> f64 {
        let total_attempts = self.events_published.load(Ordering::Relaxed);
        if total_attempts == 0 {
            return 0.0;
        }

        let errors = self.processing_errors.load(Ordering::Relaxed);
        errors as f64 / total_attempts as f64
    }

    /// Get drop rate (0.0 to 1.0)
    pub fn drop_rate(&self) -> f64 {
        let total_attempts = self.events_published.load(Ordering::Relaxed)
            + self.events_dropped.load(Ordering::Relaxed);
        if total_attempts == 0 {
            return 0.0;
        }

        let dropped = self.events_dropped.load(Ordering::Relaxed);
        dropped as f64 / total_attempts as f64
    }

    /// Get comprehensive metrics snapshot
    pub fn snapshot(&self) -> MetricsSnapshot {
        let elapsed = self.start_time.elapsed();

        MetricsSnapshot {
            // Counts
            events_published: self.events_published.load(Ordering::Relaxed),
            events_consumed: self.events_consumed.load(Ordering::Relaxed),
            events_dropped: self.events_dropped.load(Ordering::Relaxed),
            batches_processed: self.batches_processed.load(Ordering::Relaxed),
            queue_full_events: self.queue_full_events.load(Ordering::Relaxed),
            pool_exhausted_events: self.pool_exhausted_events.load(Ordering::Relaxed),
            processing_errors: self.processing_errors.load(Ordering::Relaxed),

            // Computed metrics
            throughput_events_per_sec: self.throughput(),
            avg_publish_latency_us: self.avg_publish_latency_us(),
            avg_consume_latency_us: self.avg_consume_latency_us(),
            max_publish_latency_us: self.max_publish_latency_us(),
            max_consume_latency_us: self.max_consume_latency_us(),
            error_rate: self.error_rate(),
            drop_rate: self.drop_rate(),

            // Timing
            uptime_seconds: elapsed.as_secs_f64(),
        }
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.events_published.store(0, Ordering::Relaxed);
        self.events_consumed.store(0, Ordering::Relaxed);
        self.events_dropped.store(0, Ordering::Relaxed);
        self.batches_processed.store(0, Ordering::Relaxed);
        self.queue_full_events.store(0, Ordering::Relaxed);
        self.pool_exhausted_events.store(0, Ordering::Relaxed);
        self.processing_errors.store(0, Ordering::Relaxed);
        self.total_publish_latency_ns.store(0, Ordering::Relaxed);
        self.total_consume_latency_ns.store(0, Ordering::Relaxed);
        self.max_publish_latency_ns.store(0, Ordering::Relaxed);
        self.max_consume_latency_ns.store(0, Ordering::Relaxed);
    }
}

impl Default for PipelineMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of pipeline metrics at a point in time
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    // Raw counts
    pub events_published: u64,
    pub events_consumed: u64,
    pub events_dropped: u64,
    pub batches_processed: u64,
    pub queue_full_events: u64,
    pub pool_exhausted_events: u64,
    pub processing_errors: u64,

    // Computed metrics
    pub throughput_events_per_sec: f64,
    pub avg_publish_latency_us: f64,
    pub avg_consume_latency_us: f64,
    pub max_publish_latency_us: f64,
    pub max_consume_latency_us: f64,
    pub error_rate: f64,
    pub drop_rate: f64,

    // Timing
    pub uptime_seconds: f64,
}

impl MetricsSnapshot {
    /// Check if pipeline is performing within acceptable bounds
    pub fn is_healthy(&self) -> bool {
        self.error_rate < 0.01 &&     // <1% error rate
        self.drop_rate < 0.05 &&      // <5% drop rate
        self.throughput_events_per_sec > 1000.0 && // >1K events/sec minimum
        self.max_publish_latency_us < 10000.0 // <10ms max latency
    }

    /// Get health score (0.0 to 1.0, higher is better)
    pub fn health_score(&self) -> f64 {
        let mut score = 1.0;

        // Penalize high error rates
        score -= self.error_rate * 2.0; // 2x penalty for errors

        // Penalize high drop rates
        score -= self.drop_rate;

        // Penalize high latency (normalize to 0-1 range assuming 10ms is bad)
        let latency_penalty = (self.max_publish_latency_us / 10000.0).min(1.0);
        score -= latency_penalty * 0.5;

        // Ensure score is in valid range
        score.clamp(0.0, 1.0)
    }
}

/// Metrics collector for aggregating metrics across multiple pipelines
pub struct MetricsCollector {
    pipelines: Vec<String>,
    snapshots: std::collections::HashMap<String, MetricsSnapshot>,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            pipelines: Vec::new(),
            snapshots: std::collections::HashMap::new(),
        }
    }

    /// Add a pipeline to track
    pub fn add_pipeline(&mut self, name: String) {
        if !self.pipelines.contains(&name) {
            self.pipelines.push(name);
        }
    }

    /// Update metrics for a pipeline
    pub fn update_pipeline(&mut self, name: String, snapshot: MetricsSnapshot) {
        self.snapshots.insert(name, snapshot);
    }

    /// Get aggregated metrics across all pipelines
    pub fn aggregate(&self) -> Option<MetricsSnapshot> {
        if self.snapshots.is_empty() {
            return None;
        }

        let mut total_published = 0u64;
        let mut total_consumed = 0u64;
        let mut total_dropped = 0u64;
        let mut total_batches = 0u64;
        let mut total_queue_full = 0u64;
        let mut total_pool_exhausted = 0u64;
        let mut total_errors = 0u64;
        let mut total_throughput = 0.0;
        let mut max_publish_latency = 0.0f64;
        let mut max_consume_latency = 0.0f64;
        let mut max_uptime = 0.0f64;
        let mut weighted_avg_publish = 0.0;
        let mut weighted_avg_consume = 0.0;

        for snapshot in self.snapshots.values() {
            total_published += snapshot.events_published;
            total_consumed += snapshot.events_consumed;
            total_dropped += snapshot.events_dropped;
            total_batches += snapshot.batches_processed;
            total_queue_full += snapshot.queue_full_events;
            total_pool_exhausted += snapshot.pool_exhausted_events;
            total_errors += snapshot.processing_errors;
            total_throughput += snapshot.throughput_events_per_sec;
            max_publish_latency = max_publish_latency.max(snapshot.max_publish_latency_us);
            max_consume_latency = max_consume_latency.max(snapshot.max_consume_latency_us);
            max_uptime = max_uptime.max(snapshot.uptime_seconds);

            // Weight average latencies by event count
            weighted_avg_publish +=
                snapshot.avg_publish_latency_us * snapshot.events_published as f64;
            weighted_avg_consume +=
                snapshot.avg_consume_latency_us * snapshot.events_consumed as f64;
        }

        let avg_publish_latency_us = if total_published > 0 {
            weighted_avg_publish / total_published as f64
        } else {
            0.0
        };

        let avg_consume_latency_us = if total_consumed > 0 {
            weighted_avg_consume / total_consumed as f64
        } else {
            0.0
        };

        let error_rate = if total_published > 0 {
            total_errors as f64 / total_published as f64
        } else {
            0.0
        };

        let drop_rate = if total_published + total_dropped > 0 {
            total_dropped as f64 / (total_published + total_dropped) as f64
        } else {
            0.0
        };

        Some(MetricsSnapshot {
            events_published: total_published,
            events_consumed: total_consumed,
            events_dropped: total_dropped,
            batches_processed: total_batches,
            queue_full_events: total_queue_full,
            pool_exhausted_events: total_pool_exhausted,
            processing_errors: total_errors,
            throughput_events_per_sec: total_throughput,
            avg_publish_latency_us,
            avg_consume_latency_us,
            max_publish_latency_us: max_publish_latency,
            max_consume_latency_us: max_consume_latency,
            error_rate,
            drop_rate,
            uptime_seconds: max_uptime,
        })
    }

    /// Get individual pipeline snapshots
    pub fn get_snapshots(&self) -> &std::collections::HashMap<String, MetricsSnapshot> {
        &self.snapshots
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_metrics_creation() {
        let metrics = PipelineMetrics::new();
        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.events_published, 0);
        assert_eq!(snapshot.events_consumed, 0);
        assert_eq!(snapshot.throughput_events_per_sec, 0.0);
    }

    #[test]
    fn test_publish_metrics() {
        let metrics = PipelineMetrics::new();

        // Record some publishes
        for _ in 0..10 {
            metrics.record_publish_success(Duration::from_micros(100));
        }

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.events_published, 10);
        assert!(snapshot.avg_publish_latency_us > 90.0); // Should be around 100us
        assert!(snapshot.avg_publish_latency_us < 110.0);
    }

    #[test]
    fn test_consume_metrics() {
        let metrics = PipelineMetrics::new();

        // Record some consumes
        for _ in 0..5 {
            metrics.record_consume_success(Duration::from_micros(200));
        }

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.events_consumed, 5);
        assert!(snapshot.avg_consume_latency_us > 190.0);
        assert!(snapshot.avg_consume_latency_us < 210.0);
    }

    #[test]
    fn test_batch_metrics() {
        let metrics = PipelineMetrics::new();

        // Record a batch
        metrics.record_batch_success(20, Duration::from_micros(500));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.batches_processed, 1);
        assert_eq!(snapshot.events_consumed, 20);
    }

    #[test]
    fn test_error_metrics() {
        let metrics = PipelineMetrics::new();

        // Record some successful and failed operations
        for _ in 0..8 {
            metrics.record_publish_success(Duration::from_micros(100));
        }
        for _ in 0..2 {
            metrics.increment_processing_errors();
        }

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.processing_errors, 2);
        assert_eq!(snapshot.error_rate, 0.25); // 2/8 = 0.25
    }

    #[test]
    fn test_throughput_calculation() {
        let metrics = PipelineMetrics::new();

        // Record events
        for _ in 0..100 {
            metrics.record_publish_success(Duration::from_nanos(1000));
        }

        // Wait a bit to get measurable throughput
        thread::sleep(Duration::from_millis(10));

        let snapshot = metrics.snapshot();
        assert!(snapshot.throughput_events_per_sec > 0.0);
    }

    #[test]
    fn test_health_check() {
        let metrics = PipelineMetrics::new();

        // Simulate healthy pipeline
        for _ in 0..10000 {
            metrics.record_publish_success(Duration::from_micros(50));
        }

        thread::sleep(Duration::from_millis(1));

        let snapshot = metrics.snapshot();
        assert!(snapshot.is_healthy());
        assert!(snapshot.health_score() > 0.8);
    }

    #[test]
    fn test_metrics_collector() {
        let mut collector = MetricsCollector::new();

        // Add pipelines
        collector.add_pipeline("pipeline1".to_string());
        collector.add_pipeline("pipeline2".to_string());

        // Create test snapshots
        let snapshot1 = MetricsSnapshot {
            events_published: 100,
            events_consumed: 95,
            events_dropped: 5,
            batches_processed: 10,
            queue_full_events: 2,
            pool_exhausted_events: 1,
            processing_errors: 3,
            throughput_events_per_sec: 1000.0,
            avg_publish_latency_us: 100.0,
            avg_consume_latency_us: 150.0,
            max_publish_latency_us: 500.0,
            max_consume_latency_us: 600.0,
            error_rate: 0.03,
            drop_rate: 0.05,
            uptime_seconds: 1.0,
        };

        let snapshot2 = MetricsSnapshot {
            events_published: 200,
            events_consumed: 190,
            events_dropped: 10,
            batches_processed: 20,
            queue_full_events: 4,
            pool_exhausted_events: 2,
            processing_errors: 6,
            throughput_events_per_sec: 2000.0,
            avg_publish_latency_us: 80.0,
            avg_consume_latency_us: 120.0,
            max_publish_latency_us: 400.0,
            max_consume_latency_us: 500.0,
            error_rate: 0.03,
            drop_rate: 0.05,
            uptime_seconds: 1.0,
        };

        collector.update_pipeline("pipeline1".to_string(), snapshot1);
        collector.update_pipeline("pipeline2".to_string(), snapshot2);

        let aggregated = collector.aggregate().unwrap();
        assert_eq!(aggregated.events_published, 300);
        assert_eq!(aggregated.events_consumed, 285);
        assert_eq!(aggregated.throughput_events_per_sec, 3000.0);
    }
}
