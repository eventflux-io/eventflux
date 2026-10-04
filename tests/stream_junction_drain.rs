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

//! Shutdown-drain behavior of async StreamJunctions (#138):
//! `wait_quiescent` must observe queued events and in-flight subscriber
//! dispatches, and `abandon_pending` must count what a timed-out drain leaves
//! behind.

use eventflux::core::config::eventflux_app_context::EventFluxAppContext;
use eventflux::core::config::eventflux_context::EventFluxContext;
use eventflux::core::event::complex_event::ComplexEvent;
use eventflux::core::event::event::Event;
use eventflux::core::event::stream::StreamEvent;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::query::processor::Processor;
use eventflux::core::stream::stream_junction::StreamJunction;
use eventflux::core::util::executor_service::ExecutorService;
use eventflux::query_api::definition::attribute::Type as AttrType;
use eventflux::query_api::definition::StreamDefinition;
use eventflux::query_api::eventflux_app::EventFluxApp;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Processor that sleeps per event to simulate a slow subscriber.
#[derive(Debug)]
struct SlowProcessor {
    events: Arc<Mutex<Vec<Vec<AttributeValue>>>>,
    delay: Duration,
}

impl SlowProcessor {
    fn new(delay: Duration) -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            delay,
        }
    }
}

impl Processor for SlowProcessor {
    fn process(&self, mut chunk: Option<Box<dyn ComplexEvent>>) {
        while let Some(mut ce) = chunk {
            chunk = ce.set_next(None);
            if let Some(se) = ce.as_any().downcast_ref::<StreamEvent>() {
                thread::sleep(self.delay);
                self.events
                    .lock()
                    .unwrap()
                    .push(se.before_window_data.clone());
            }
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        None
    }

    fn set_next_processor(&mut self, _n: Option<Arc<Mutex<dyn Processor>>>) {}

    fn clone_processor(
        &self,
        _c: &Arc<eventflux::core::config::eventflux_query_context::EventFluxQueryContext>,
    ) -> Box<dyn Processor> {
        Box::new(SlowProcessor::new(self.delay))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::new(EventFluxAppContext::new(
            Arc::new(EventFluxContext::new()),
            "TestApp".to_string(),
            Arc::new(EventFluxApp::new("TestApp".to_string())),
            String::new(),
        ))
    }

    fn get_eventflux_query_context(
        &self,
    ) -> Arc<eventflux::core::config::eventflux_query_context::EventFluxQueryContext> {
        Arc::new(
            eventflux::core::config::eventflux_query_context::EventFluxQueryContext::new(
                self.get_eventflux_app_context(),
                "TestQuery".to_string(),
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

fn make_junction(
    executor: Option<Arc<ExecutorService>>,
    subscriber_delay: Duration,
) -> (StreamJunction, Arc<Mutex<SlowProcessor>>) {
    let eventflux_context = Arc::new(EventFluxContext::new());
    let app = Arc::new(EventFluxApp::new("TestApp".to_string()));
    let mut app_ctx = EventFluxAppContext::new(
        Arc::clone(&eventflux_context),
        "TestApp".to_string(),
        Arc::clone(&app),
        String::new(),
    );
    if let Some(exec) = executor {
        app_ctx.set_executor_service(exec);
    }

    let stream_def = Arc::new(
        StreamDefinition::new("DrainStream".to_string()).attribute("id".to_string(), AttrType::INT),
    );

    let junction = StreamJunction::new(
        "DrainStream".to_string(),
        stream_def,
        Arc::new(app_ctx),
        1024,
        true, // async
        None,
    )
    .unwrap();

    let processor = Arc::new(Mutex::new(SlowProcessor::new(subscriber_delay)));
    junction.subscribe(processor.clone() as Arc<Mutex<dyn Processor>>);

    (junction, processor)
}

/// A slow-but-finite subscriber drains fully within the timeout: quiescence is
/// reached, every event is processed, nothing is dropped.
#[test]
fn test_wait_quiescent_drains_slow_subscriber() {
    let (junction, processor) = make_junction(None, Duration::from_millis(20));

    let events: Vec<_> = (0..100)
        .map(|i| Event::new_with_data(1000 + i, vec![AttributeValue::Int(i as i32)]))
        .collect();
    junction.send_events(events).unwrap();

    assert!(
        junction.wait_quiescent(Duration::from_secs(5)),
        "junction should reach quiescence within the drain timeout"
    );

    // Quiescent means queue empty AND all subscriber dispatches finished
    let received = processor.lock().unwrap().events.lock().unwrap().len();
    assert_eq!(received, 100, "all events must reach the subscriber");

    let metrics = junction.get_performance_metrics();
    assert_eq!(metrics.events_processed, 100);
    assert_eq!(metrics.events_dropped, 0);
    let (queued, in_flight) = junction.pending_drain_estimate();
    assert_eq!((queued, in_flight), (0, 0));
}

/// A subscriber too slow for the timeout: quiescence fails, and after
/// stop_processing the leftover queue is counted via abandon_pending.
#[test]
fn test_drain_timeout_abandons_pending() {
    // Single-threaded executor forces inline subscriber processing, so the
    // 200ms/event subscriber backs the queue up deterministically.
    let executor = Arc::new(ExecutorService::new("drain-timeout-test", 1));
    let (junction, _processor) = make_junction(Some(executor), Duration::from_millis(200));

    let events: Vec<_> = (0..50)
        .map(|i| Event::new_with_data(1000 + i, vec![AttributeValue::Int(i as i32)]))
        .collect();
    junction.send_events(events).unwrap();

    assert!(
        !junction.wait_quiescent(Duration::from_millis(10)),
        "10ms cannot drain 50 events at 200ms each"
    );

    junction.stop_processing();
    let abandoned = junction.abandon_pending();
    assert!(
        abandoned > 0,
        "abandon_pending must report the still-queued events"
    );

    let metrics = junction.get_performance_metrics();
    assert!(
        metrics.events_dropped >= abandoned,
        "abandoned events must be counted in events_dropped"
    );
}
