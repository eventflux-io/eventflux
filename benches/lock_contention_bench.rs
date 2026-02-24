// SPDX-License-Identifier: MIT OR Apache-2.0

//! Lock Contention Benchmark
//!
//! Measures lock contention in hot paths under concurrent load.
//! This benchmark helps identify mutex contention bottlenecks that
//! could be optimized with RwLock or lock-free structures.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use eventflux::core::config::eventflux_app_context::EventFluxAppContext;
use eventflux::core::config::eventflux_context::EventFluxContext;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::event::Event;
use eventflux::core::query::processor::Processor;
use eventflux::core::stream::{JunctionConfig, StreamJunction, StreamJunctionFactory};
use eventflux::query_api::definition::attribute::Type as AttrType;
use eventflux::query_api::definition::StreamDefinition;
use eventflux::query_api::eventflux_app::EventFluxApp;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Dummy processor for benchmarking
#[derive(Debug)]
struct BenchProcessor {
    name: String,
}

impl BenchProcessor {
    fn new(name: String) -> Self {
        Self { name }
    }
}

impl Processor for BenchProcessor {
    fn process(
        &self,
        _chunk: Option<Box<dyn eventflux::core::event::complex_event::ComplexEvent>>,
    ) {
        // Minimal processing - just consume the event
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        None
    }

    fn set_next_processor(&mut self, _n: Option<Arc<Mutex<dyn Processor>>>) {}

    fn clone_processor(
        &self,
        _c: &Arc<eventflux::core::config::eventflux_query_context::EventFluxQueryContext>,
    ) -> Box<dyn Processor> {
        Box::new(BenchProcessor::new(self.name.clone()))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        let ctx = Arc::new(EventFluxContext::new());
        let app = Arc::new(EventFluxApp::new("BenchApp".to_string()));
        Arc::new(EventFluxAppContext::new(
            ctx,
            "BenchApp".to_string(),
            app,
            String::new(),
        ))
    }

    fn get_eventflux_query_context(
        &self,
    ) -> Arc<eventflux::core::config::eventflux_query_context::EventFluxQueryContext> {
        Arc::new(
            eventflux::core::config::eventflux_query_context::EventFluxQueryContext::new(
                self.get_eventflux_app_context(),
                "BenchQuery".to_string(),
                None,
            ),
        )
    }

    fn get_processing_mode(&self) -> eventflux::core::query::processor::ProcessingMode {
        eventflux::core::query::processor::ProcessingMode::DEFAULT
    }

    fn is_stateful(&self) -> bool {
        false
    }
}

fn create_junction(subscriber_count: usize, is_async: bool) -> Arc<Mutex<StreamJunction>> {
    let ctx = Arc::new(EventFluxContext::new());
    let app = Arc::new(EventFluxApp::new("BenchApp".to_string()));
    let app_ctx = Arc::new(EventFluxAppContext::new(
        ctx,
        "BenchApp".to_string(),
        app,
        String::new(),
    ));

    let stream_def = Arc::new(
        StreamDefinition::new("BenchStream".to_string()).attribute("id".to_string(), AttrType::INT),
    );

    let config = JunctionConfig::new("BenchStream".to_string())
        .with_buffer_size(65536)
        .with_async(is_async);

    let junction = StreamJunctionFactory::create(config, stream_def, app_ctx, None).unwrap();

    // Add subscribers
    for i in 0..subscriber_count {
        let processor = Arc::new(Mutex::new(BenchProcessor::new(format!("Processor{}", i))));
        junction.lock().unwrap().subscribe(processor);
    }

    // Set to DROP mode for benchmarking to reduce noise
    junction
        .lock()
        .unwrap()
        .set_on_error_action(eventflux::core::stream::stream_junction::OnErrorAction::DROP);

    junction
}

/// Benchmark subscriber lock contention with varying subscriber counts
fn bench_subscriber_lock_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscriber_lock_contention");

    for subscriber_count in [1, 4, 8, 16].iter() {
        group.throughput(Throughput::Elements(10000));
        group.bench_with_input(
            BenchmarkId::new("async_junction", subscriber_count),
            subscriber_count,
            |b, &subscriber_count| {
                let junction = create_junction(subscriber_count, true);

                b.iter(|| {
                    // Send 10K events concurrently with pacing to avoid overwhelming pipeline
                    let mut handles = vec![];
                    for thread_id in 0..4 {
                        let junction_clone = Arc::clone(&junction);
                        handles.push(thread::spawn(move || {
                            for i in 0..2500 {
                                let event = Event::new_with_data(
                                    i as i64,
                                    vec![AttributeValue::Int((thread_id * 1000 + i) as i32)],
                                );
                                // Use send_event and handle backpressure gracefully
                                let _ = junction_clone.lock().unwrap().send_event(event);

                                // Micro-sleep every 100 events to allow consumers to keep up
                                if i % 100 == 0 {
                                    thread::sleep(Duration::from_micros(100));
                                }
                            }
                        }));
                    }

                    for handle in handles {
                        handle.join().unwrap();
                    }
                });

                // Give more time for async processing to complete
                junction.lock().unwrap().stop_processing();
                thread::sleep(Duration::from_millis(500));
            },
        );
    }

    group.finish();
}

/// Benchmark single-threaded throughput (baseline with no contention)
fn bench_single_threaded_baseline(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_threaded_baseline");
    group.throughput(Throughput::Elements(10000));

    for subscriber_count in [1, 4, 8, 16].iter() {
        group.bench_with_input(
            BenchmarkId::new("async_junction", subscriber_count),
            subscriber_count,
            |b, &subscriber_count| {
                let junction = create_junction(subscriber_count, true);

                b.iter(|| {
                    // Send 10K events from single thread
                    for i in 0..10000 {
                        let event =
                            Event::new_with_data(i as i64, vec![AttributeValue::Int(i as i32)]);
                        let _ = black_box(junction.lock().unwrap().send_event(event));
                    }
                });

                // Cleanup - longer wait for async processing
                junction.lock().unwrap().stop_processing();
                thread::sleep(Duration::from_millis(500));
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_subscriber_lock_contention,
    bench_single_threaded_baseline
);
criterion_main!(benches);
