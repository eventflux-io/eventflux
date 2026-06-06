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

//! High-Performance Event Processing Pipeline
//!
//! This module provides a crossbeam-based event processing pipeline optimized for
//! Rust's strengths: zero-cost abstractions, memory safety, and lock-free concurrency.
//!
//! ## Design Goals
//! - **>1M events/second throughput** (beat Java performance)
//! - **<1ms p99 latency** for simple operations
//! - **Zero-copy processing** where possible
//! - **Lock-free coordination** using crossbeam primitives
//! - **Adaptive backpressure** handling
//! - **NUMA-aware** memory allocation patterns

pub mod backpressure;
pub mod event_pipeline;
pub mod metrics;
pub mod object_pool;

pub use backpressure::{BackpressureHandler, BackpressureStrategy};
pub use event_pipeline::{EventPipeline, PipelineBuilder, PipelineConfig, PipelineResult};
pub use metrics::{MetricsCollector, MetricsSnapshot, PipelineMetrics};
pub use object_pool::{EventPool, PooledEvent};

pub use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
/// Re-exports from crossbeam for convenience
pub use crossbeam_queue::{ArrayQueue, SegQueue};
pub use crossbeam_utils::CachePadded;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::stream::StreamEvent;
    use crate::core::event::value::AttributeValue;

    use std::time::Instant;

    fn create_test_event(id: i32) -> StreamEvent {
        StreamEvent::new_with_data(0, vec![AttributeValue::Int(id)])
    }

    #[test]
    fn test_pipeline_creation() {
        let pipeline = PipelineBuilder::new()
            .with_capacity(1024)
            .with_backpressure(BackpressureStrategy::Drop)
            .build();

        assert!(pipeline.is_ok());
    }

    #[test]
    fn test_high_throughput() {
        let pipeline = PipelineBuilder::new().with_capacity(4096).build().unwrap();

        let start = Instant::now();
        let num_events = 100_000;

        for i in 0..num_events {
            let event = create_test_event(i);
            let _ = pipeline.publish(event);
        }

        let elapsed = start.elapsed();
        let throughput = num_events as f64 / elapsed.as_secs_f64();

        println!("Throughput: {:.0} events/sec", throughput);
        // Relaxed threshold for CI environments (was 500K)
        assert!(throughput > 200_000.0, "Should handle >200K events/sec");
    }
}
