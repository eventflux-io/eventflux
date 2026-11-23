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

//! Performance tests for the EventFlux engine
//!
//! These tests run for a long time and put heavy load on several components.
//! They are compiled and run only when the `perf-tests` feature is enabled.
//!
//! Run with `cargo test --features perf-tests`.

#![cfg(feature = "perf-tests")]

use eventflux::core::config::eventflux_app_context::EventFluxAppContext;
use eventflux::core::config::eventflux_context::EventFluxContext;
use eventflux::core::event::complex_event::ComplexEvent;
use eventflux::core::event::event::Event;
use eventflux::core::event::stream::StreamEvent;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::query::processor::Processor;
use eventflux::core::stream::stream_junction::StreamJunction;
use eventflux::query_api::definition::attribute::Type as AttrType;
use eventflux::query_api::definition::StreamDefinition;
use eventflux::query_api::eventflux_app::EventFluxApp;
use std::sync::{Arc, Mutex};

// Processor that records the data of every event it receives
#[derive(Debug)]
struct TestProcessor {
    events: Arc<Mutex<Vec<Vec<AttributeValue>>>>,
    name: String,
}

impl TestProcessor {
    fn new(name: String) -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            name,
        }
    }

    #[allow(dead_code)]
    fn get_events(&self) -> Vec<Vec<AttributeValue>> {
        self.events.lock().unwrap().clone()
    }
}

impl Processor for TestProcessor {
    fn process(&self, mut chunk: Option<Box<dyn ComplexEvent>>) {
        while let Some(mut ce) = chunk {
            chunk = ce.set_next(None);
            if let Some(se) = ce.as_any().downcast_ref::<StreamEvent>() {
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
        Box::new(TestProcessor::new(self.name.clone()))
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

#[test]
fn test_junction_backpressure() {
    let eventflux_context = Arc::new(EventFluxContext::new());
    let app = Arc::new(EventFluxApp::new("TestApp".to_string()));
    let app_ctx = Arc::new(EventFluxAppContext::new(
        Arc::clone(&eventflux_context),
        "TestApp".to_string(),
        Arc::clone(&app),
        String::new(),
    ));

    let stream_def = Arc::new(
        StreamDefinition::new("TestStream".to_string()).attribute("id".to_string(), AttrType::INT),
    );

    // A 64 slot async buffer is far too small for 50,000 events, so it must overflow
    let junction = StreamJunction::new(
        "TestStream".to_string(),
        stream_def,
        app_ctx,
        64, // buffer size
        true,
        None,
    )
    .unwrap();

    let processor = Arc::new(Mutex::new(TestProcessor::new("TestProcessor".to_string())));
    junction.subscribe(processor.clone() as Arc<Mutex<dyn Processor>>);

    junction.start_processing().unwrap();

    let mut dropped = 0;
    for i in 0..50000 {
        let event = Event::new_with_data(i, vec![AttributeValue::Int(i as i32)]);
        if junction.send_event(event).is_err() {
            dropped += 1;
        }
    }

    println!("Dropped events due to backpressure: {}", dropped);

    let metrics = junction.get_performance_metrics();
    println!("Junction metrics: {:?}", metrics);

    // Some events must be dropped, either seen as send errors or in the junction metrics
    assert!(metrics.events_dropped > 0 || dropped > 0);
}
