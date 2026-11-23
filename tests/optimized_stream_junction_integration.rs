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

//! Integration tests for StreamJunction
//!
//! Tests the StreamJunction built on the crossbeam pipeline for correctness,
//! performance, and compatibility with existing code.
//!
//! Functional checks (event counts, ordering, error and drop accounting) always
//! run. Wall clock performance limits (minimum throughput, maximum latency) run
//! only under `--features perf-tests`. On busy shared CI runners a single
//! scheduling stall can miss them, which says nothing about correctness (#139).

use eventflux::core::config::{
    eventflux_app_context::EventFluxAppContext, eventflux_context::EventFluxContext,
    eventflux_query_context::EventFluxQueryContext,
};
use eventflux::core::event::complex_event::ComplexEvent;
use eventflux::core::event::event::Event;
use eventflux::core::event::stream::StreamEvent;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::query::processor::{ProcessingMode, Processor};
use eventflux::core::stream::{
    BackpressureStrategy, JunctionConfig, StreamJunction, StreamJunctionFactory,
};
use eventflux::core::util::pipeline::{
    BackpressureStrategy as PipelineBackpressure, EventPipeline, PipelineConfig, PipelineResult,
};
use eventflux::query_api::definition::attribute::Type as AttrType;
use eventflux::query_api::definition::StreamDefinition;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::{Duration, Instant};

/// Processor that counts events. Latency is a fixed fake value, not measured.
#[derive(Debug)]
struct PerformanceTestProcessor {
    events_received: Arc<AtomicUsize>,
    total_latency_ns: Arc<AtomicUsize>,
    start_time: Instant,
    name: String,
}

impl PerformanceTestProcessor {
    fn new(name: String) -> Self {
        Self {
            events_received: Arc::new(AtomicUsize::new(0)),
            total_latency_ns: Arc::new(AtomicUsize::new(0)),
            start_time: Instant::now(),
            name,
        }
    }

    fn get_stats(&self) -> (usize, f64, f64) {
        let events = self.events_received.load(Ordering::Relaxed);
        let total_latency = self.total_latency_ns.load(Ordering::Relaxed);
        let elapsed = self.start_time.elapsed().as_secs_f64();

        let throughput = events as f64 / elapsed;
        let avg_latency_us = if events > 0 {
            (total_latency as f64 / events as f64) / 1000.0
        } else {
            0.0
        };

        (events, throughput, avg_latency_us)
    }
}

impl Processor for PerformanceTestProcessor {
    fn process(&self, mut chunk: Option<Box<dyn ComplexEvent>>) {
        let _received_time = Instant::now();

        while let Some(mut ce) = chunk {
            chunk = ce.set_next(None);

            if let Some(_se) = ce.as_any().downcast_ref::<StreamEvent>() {
                // Real latency cannot be worked out because the test timestamps are
                // sequence numbers, not clock times. Add a fake 100 ns per event instead.
                self.total_latency_ns.fetch_add(100, Ordering::Relaxed);

                self.events_received.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        None
    }

    fn set_next_processor(&mut self, _n: Option<Arc<Mutex<dyn Processor>>>) {}

    fn clone_processor(&self, _c: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        Box::new(PerformanceTestProcessor::new(self.name.clone()))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::new(EventFluxAppContext::new(
            Arc::new(EventFluxContext::new()),
            "TestApp".to_string(),
            Arc::new(eventflux::query_api::eventflux_app::EventFluxApp::new(
                "TestApp".to_string(),
            )),
            String::new(),
        ))
    }

    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        Arc::new(EventFluxQueryContext::new(
            self.get_eventflux_app_context(),
            "TestQuery".to_string(),
            None,
        ))
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::DEFAULT
    }

    fn is_stateful(&self) -> bool {
        false
    }
}

fn setup_test_context() -> (Arc<EventFluxAppContext>, Arc<StreamDefinition>) {
    let eventflux_context = Arc::new(EventFluxContext::new());
    let app = Arc::new(eventflux::query_api::eventflux_app::EventFluxApp::new(
        "TestApp".to_string(),
    ));
    let mut app_ctx = EventFluxAppContext::new(
        Arc::clone(&eventflux_context),
        "TestApp".to_string(),
        Arc::clone(&app),
        String::new(),
    );
    app_ctx.root_metrics_level =
        eventflux::core::config::eventflux_app_context::MetricsLevelPlaceholder::BASIC;

    let stream_def = Arc::new(
        StreamDefinition::new("PerfTestStream".to_string())
            .attribute("id".to_string(), AttrType::INT)
            .attribute("timestamp".to_string(), AttrType::LONG)
            .attribute("value".to_string(), AttrType::STRING),
    );

    (Arc::new(app_ctx), stream_def)
}

#[test]
fn test_optimized_junction_basic_functionality() {
    let (app_ctx, stream_def) = setup_test_context();

    let junction = StreamJunction::new(
        "TestStream".to_string(),
        stream_def,
        app_ctx,
        4096,
        true, // async
        None,
    )
    .unwrap();

    let processor = Arc::new(Mutex::new(PerformanceTestProcessor::new(
        "TestProcessor".to_string(),
    )));
    junction.subscribe(processor.clone() as Arc<Mutex<dyn Processor>>);

    junction.start_processing().unwrap();

    // Send test events
    let num_events = 1000;
    for i in 0..num_events {
        let event = Event::new_with_data(
            i,
            vec![
                AttributeValue::Int(i as i32),
                AttributeValue::Long(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as i64,
                ),
                AttributeValue::String(format!("event_{}", i)),
            ],
        );
        junction.send_event(event).unwrap();
    }

    thread::sleep(Duration::from_millis(500));
    junction.stop_processing();

    // Give async processing another 200 ms to finish
    thread::sleep(Duration::from_millis(200));

    let (events_received, throughput, avg_latency) = processor.lock().unwrap().get_stats();
    println!(
        "Optimized Junction - Events: {}, Throughput: {:.0}/sec, Avg Latency: {:.2}µs",
        events_received, throughput, avg_latency
    );

    println!(
        "DEBUG: events_received = {}, num_events = {}",
        events_received, num_events
    );
    assert_eq!(events_received, num_events as usize);
    #[cfg(feature = "perf-tests")]
    assert!(throughput > 1000.0, "Should handle >1K events/sec");

    let metrics = junction.get_performance_metrics();
    // is_healthy() includes wall clock limits (at least 1K events/sec and at most
    // 10 ms publish latency), so it runs under perf-tests only. The functional
    // parts, error and drop rates, are asserted below on every run.
    #[cfg(feature = "perf-tests")]
    assert!(metrics.is_healthy());
    assert!(metrics.pipeline_metrics.error_rate < 0.01);
    assert!(metrics.pipeline_metrics.drop_rate < 0.05);
    assert_eq!(metrics.events_processed, num_events as u64);
}

#[test]
fn test_default_synchronous_mode() {
    let (app_ctx, stream_def) = setup_test_context();

    // The default configuration is synchronous
    let config = JunctionConfig::new("TestStream".to_string());
    assert!(
        !config.is_async,
        "Default configuration should be synchronous (is_async: false)"
    );

    let junction =
        StreamJunctionFactory::create(config, stream_def.clone(), app_ctx.clone(), None).unwrap();

    // The junction must NOT use async mode internally
    let metrics = junction.lock().unwrap().get_performance_metrics();
    assert!(
        !metrics.is_async,
        "Default junction should not be in async mode"
    );

    // Async mode can still be turned on explicitly
    let async_config = JunctionConfig::new("AsyncTestStream".to_string())
        .with_async(true)
        .with_expected_throughput(100_000);
    assert!(
        async_config.is_async,
        "Explicitly enabled async mode should be true"
    );
}

#[test]
fn test_synchronous_mode_ordering_guarantee() {
    let (app_ctx, stream_def) = setup_test_context();

    // Create junction in explicit synchronous mode
    let junction = StreamJunction::new(
        "SyncOrderTest".to_string(),
        stream_def,
        app_ctx,
        4096,
        false, // explicit synchronous mode
        None,
    )
    .unwrap();

    // Create processor that records event order
    let received_order = Arc::new(Mutex::new(Vec::new()));
    let received_order_clone = Arc::clone(&received_order);

    #[derive(Debug)]
    struct OrderTrackingProcessor {
        received_order: Arc<Mutex<Vec<i32>>>,
    }

    impl Processor for OrderTrackingProcessor {
        fn process(&self, mut chunk: Option<Box<dyn ComplexEvent>>) {
            while let Some(mut ce) = chunk {
                chunk = ce.set_next(None);
                if let Some(se) = ce.as_any().downcast_ref::<StreamEvent>() {
                    if let Some(AttributeValue::Int(value)) = se.before_window_data.first() {
                        self.received_order.lock().unwrap().push(*value);
                    }
                }
            }
        }

        fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
            None
        }
        fn set_next_processor(&mut self, _n: Option<Arc<Mutex<dyn Processor>>>) {}
        fn clone_processor(&self, _c: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
            Box::new(OrderTrackingProcessor {
                received_order: Arc::clone(&self.received_order),
            })
        }
        fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
            Arc::new(EventFluxAppContext::new(
                Arc::new(EventFluxContext::new()),
                "TestApp".to_string(),
                Arc::new(eventflux::query_api::eventflux_app::EventFluxApp::new(
                    "TestApp".to_string(),
                )),
                String::new(),
            ))
        }
        fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
            Arc::new(EventFluxQueryContext::new(
                self.get_eventflux_app_context(),
                "TestQuery".to_string(),
                None,
            ))
        }
        fn get_processing_mode(&self) -> ProcessingMode {
            ProcessingMode::DEFAULT
        }
        fn is_stateful(&self) -> bool {
            false
        }
    }

    let processor = Arc::new(Mutex::new(OrderTrackingProcessor {
        received_order: received_order_clone,
    }));

    junction.subscribe(processor.clone() as Arc<Mutex<dyn Processor>>);
    junction.start_processing().unwrap();

    // Send events in specific order
    let expected_order: Vec<i32> = (1..=50).collect();
    for i in expected_order.iter() {
        let event = Event::new_with_data(*i as i64, vec![AttributeValue::Int(*i)]);
        junction.send_event(event).unwrap();
    }

    // In synchronous mode, processing happens during send. The 10 ms sleep only
    // covers system delays.
    std::thread::sleep(Duration::from_millis(10));
    junction.stop_processing();

    // Verify events were processed in exact order
    let actual_order = received_order.lock().unwrap().clone();
    assert_eq!(
        actual_order, expected_order,
        "Synchronous mode MUST preserve exact event ordering! Expected: {:?}, Got: {:?}",
        expected_order, actual_order
    );

    println!(
        "✅ Synchronous mode correctly preserved ordering of {} events",
        actual_order.len()
    );
}

#[test]
fn test_junction_factory_auto_selection() {
    let (app_ctx, stream_def) = setup_test_context();

    // Low throughput - still creates junction successfully
    let low_config = JunctionConfig::new("LowThroughput".to_string())
        .with_expected_throughput(500)
        .with_subscriber_count(1);

    let low_junction =
        StreamJunctionFactory::create(low_config, stream_def.clone(), app_ctx.clone(), None)
            .unwrap();

    assert_eq!(
        low_junction.lock().unwrap().stream_id,
        "LowThroughput",
        "Junction created successfully"
    );

    // High throughput - should also create junction successfully
    let high_config = JunctionConfig::new("HighThroughput".to_string())
        .with_expected_throughput(200000)
        .with_subscriber_count(5)
        .with_buffer_size(16384);

    let high_junction =
        StreamJunctionFactory::create(high_config, stream_def, app_ctx, None).unwrap();

    assert_eq!(
        high_junction.lock().unwrap().stream_id,
        "HighThroughput",
        "High throughput junction created successfully"
    );
}

// test_optimized_vs_standard_performance was removed. The standard implementation
// no longer exists, and every junction now uses the crossbeam pipeline.

#[test]
fn test_concurrent_publishers_optimized_junction() {
    let (app_ctx, stream_def) = setup_test_context();

    let junction = Arc::new(Mutex::new(
        StreamJunction::new(
            "ConcurrentTest".to_string(),
            stream_def,
            app_ctx,
            16384, // Large buffer for concurrent access
            true,  // async
            None,
        )
        .unwrap(),
    ));

    let processor = Arc::new(Mutex::new(PerformanceTestProcessor::new(
        "ConcurrentProcessor".to_string(),
    )));
    junction
        .lock()
        .unwrap()
        .subscribe(processor.clone() as Arc<Mutex<dyn Processor>>);
    junction.lock().unwrap().start_processing().unwrap();

    // Spawn multiple publisher threads
    let num_threads = 8;
    let events_per_thread = 1000;
    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let junction_clone = Arc::clone(&junction);
        handles.push(thread::spawn(move || {
            for i in 0..events_per_thread {
                let event = Event::new_with_data(
                    (thread_id * events_per_thread + i) as i64,
                    vec![
                        AttributeValue::Int(thread_id * 1000 + i),
                        AttributeValue::Long(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_nanos() as i64,
                        ),
                        AttributeValue::String(format!("thread_{}_event_{}", thread_id, i)),
                    ],
                );

                // A send can fail under backpressure. That event is dropped, which is
                // expected, and the thread backs off for 1 microsecond.
                match junction_clone.lock().unwrap().send_event(event) {
                    Ok(_) => {}
                    Err(_) => {
                        thread::sleep(Duration::from_micros(1));
                    }
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Give processing 1 second to finish
    thread::sleep(Duration::from_millis(1000));
    junction.lock().unwrap().stop_processing();

    let (events_received, throughput, avg_latency) = processor.lock().unwrap().get_stats();
    let total_sent = num_threads * events_per_thread;

    println!(
        "Concurrent test - Sent: {}, Received: {}, Throughput: {:.0}/sec, Avg Latency: {:.2}µs",
        total_sent, events_received, throughput, avg_latency
    );

    // Backpressure may drop some events, but more than 80% must arrive
    let success_rate = events_received as f64 / total_sent as f64;
    assert!(
        success_rate > 0.8,
        "Should successfully process >80% of events, got {:.1}%",
        success_rate * 100.0
    );
    #[cfg(feature = "perf-tests")]
    assert!(
        throughput > 5000.0,
        "Should handle >5K events/sec in concurrent scenario"
    );

    let metrics = junction.lock().unwrap().get_performance_metrics();
    println!("Junction metrics: {:?}", metrics);
    #[cfg(feature = "perf-tests")]
    assert!(metrics.pipeline_metrics.throughput_events_per_sec > 1000.0);
}

#[test]
fn test_pipeline_drop_backpressure_is_exact() {
    // The reliable check for dropping when full (#139). A bare EventPipeline has
    // NO consumers, because the junction spawns those when it is built. So
    // publishing past capacity cannot race anything. Exactly `capacity` publishes
    // succeed and the rest are rejected.
    let capacity = 64;
    let pipeline = EventPipeline::new(PipelineConfig {
        capacity,
        backpressure: PipelineBackpressure::Drop,
        ..PipelineConfig::default()
    })
    .unwrap();

    let num_events = 500;
    let mut success = 0;
    let mut full = 0;
    for i in 0..num_events {
        let event = StreamEvent::new_with_data(i as i64, vec![AttributeValue::Int(i as i32)]);
        match pipeline.publish(event) {
            PipelineResult::Success { .. } => success += 1,
            PipelineResult::Full => full += 1,
            other => panic!("unexpected publish outcome: {other:?}"),
        }
    }

    assert_eq!(success, capacity, "exactly the buffer capacity is accepted");
    assert_eq!(
        full,
        num_events - capacity,
        "every overflow event is rejected"
    );
}

#[test]
fn test_junction_backpressure_invariants() {
    // At the junction level the result of a flood always depends on timing. Async
    // consumers start when the junction is BUILT and move events from the bounded
    // queue to the executor as fast as they can, so how many sends see a full queue
    // depends on thread scheduling. Two earlier versions of this test asserted
    // `dropped > 0` and both were flaky (#139). What IS guaranteed, and checked
    // here, is that every send either succeeds or reports a drop, the metrics match
    // what the senders saw, and every accepted event is delivered exactly once.
    let (app_ctx, stream_def) = setup_test_context();

    let junction = StreamJunction::new_with_backpressure(
        "BackpressureTest".to_string(),
        stream_def,
        app_ctx,
        64, // buffer size
        true,
        None,
        BackpressureStrategy::Drop, // drop right away when the buffer is full
    )
    .unwrap();

    let processor = Arc::new(Mutex::new(PerformanceTestProcessor::new(
        "BackpressureProcessor".to_string(),
    )));
    let events_received = processor.lock().unwrap().events_received.clone();
    junction.subscribe(processor);
    junction.start_processing().unwrap();

    let mut success_count: usize = 0;
    let mut dropped_count: usize = 0;
    let num_events: usize = 500;

    for i in 0..num_events {
        let event = Event::new_with_data(
            i as i64,
            vec![
                AttributeValue::Int(i as i32),
                AttributeValue::String(format!("flood_event_{}", i)),
            ],
        );

        match junction.send_event(event) {
            Ok(_) => success_count += 1,
            Err(_) => dropped_count += 1,
        }
    }

    println!(
        "Backpressure invariants - Success: {}, Dropped: {}",
        success_count, dropped_count
    );

    // Every send is accounted for, one way or the other
    assert_eq!(success_count + dropped_count, num_events);
    assert!(
        success_count > 0,
        "a 64-slot buffer accepts at least one event"
    );

    // Every accepted event must be delivered
    let deadline = Instant::now() + Duration::from_secs(10);
    while events_received.load(Ordering::Relaxed) < success_count && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    junction.stop_processing();

    assert_eq!(
        events_received.load(Ordering::Relaxed),
        success_count,
        "every accepted event must be delivered exactly once"
    );

    // The junction's drop metric agrees with the send-side outcomes
    let metrics = junction.get_performance_metrics();
    assert_eq!(metrics.events_dropped, dropped_count as u64);
}

#[test]
fn test_junction_metrics_accuracy() {
    let (app_ctx, stream_def) = setup_test_context();

    let junction = StreamJunction::new(
        "MetricsTest".to_string(),
        stream_def,
        app_ctx,
        4096,
        false, // Use synchronous mode for reliable metrics testing
        None,
    )
    .unwrap();

    let processor = Arc::new(Mutex::new(PerformanceTestProcessor::new(
        "MetricsProcessor".to_string(),
    )));
    junction.subscribe(processor);
    junction.start_processing().unwrap();

    let num_events = 5000;
    let start_time = Instant::now();

    for i in 0..num_events {
        let event = Event::new_with_data(
            i,
            vec![
                AttributeValue::Int(i as i32),
                AttributeValue::Long(start_time.elapsed().as_nanos() as i64),
            ],
        );
        junction.send_event(event).unwrap();
    }

    thread::sleep(Duration::from_millis(200));
    junction.stop_processing();

    let metrics = junction.get_performance_metrics();

    // Metrics as expected for synchronous mode
    assert_eq!(metrics.events_processed, num_events as u64);
    assert_eq!(metrics.events_dropped, 0); // sync mode never drops
    assert_eq!(metrics.processing_errors, 0);
    assert!(!metrics.is_async);

    // Pipeline metrics may be filled differently in synchronous mode, so only
    // check that there is some health score
    assert!(metrics.health_score() > 0.0);

    // Older compatibility method
    assert_eq!(junction.total_events().unwrap(), num_events as u64);

    println!(
        "Metrics test - Processed: {}, Throughput: {:.0}/sec, Health: {:.2}",
        metrics.events_processed,
        metrics.pipeline_metrics.throughput_events_per_sec,
        metrics.health_score()
    );
}
