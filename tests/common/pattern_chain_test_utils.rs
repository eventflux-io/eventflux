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

//! Shared helpers for pattern chain tests.
//!
//! Builds test contexts, stream definitions and pattern chains, and wraps the last
//! post processor so tests can read the pattern matches it emits.

use eventflux::core::config::eventflux_app_context::EventFluxAppContext;
use eventflux::core::config::eventflux_context::EventFluxContext;
use eventflux::core::config::eventflux_query_context::EventFluxQueryContext;
use eventflux::core::event::complex_event::ComplexEvent;
use eventflux::core::event::state::state_event::StateEvent;
use eventflux::core::event::stream::stream_event::StreamEvent;
use eventflux::core::query::input::stream::state::pattern_chain_builder::{
    PatternChainBuilder, PatternStepConfig, ProcessorChain,
};
use eventflux::core::query::input::stream::state::post_state_processor::PostStateProcessor;
use eventflux::core::query::input::stream::state::stream_pre_state_processor::StateType;
use eventflux::query_api::definition::attribute::Type as AttributeType;
use eventflux::query_api::definition::stream_definition::StreamDefinition;
use eventflux::query_api::eventflux_app::EventFluxApp;
use std::sync::{Arc, Mutex};

#[allow(dead_code)]
/// Create an app context and a query context for an empty app named "TestApp".
pub fn create_test_contexts() -> (Arc<EventFluxAppContext>, Arc<EventFluxQueryContext>) {
    let eventflux_context = Arc::new(EventFluxContext::new());
    let app = Arc::new(EventFluxApp::new("TestApp".to_string()));
    let app_ctx = Arc::new(EventFluxAppContext::new(
        eventflux_context,
        "TestApp".to_string(),
        app,
        String::new(),
    ));
    let query_ctx = Arc::new(EventFluxQueryContext::new(
        app_ctx.clone(),
        "test_query".to_string(),
        None,
    ));
    (app_ctx, query_ctx)
}

#[allow(dead_code)]
/// Create a stream definition with one LONG attribute named `value`.
pub fn create_stream_definition(name: &str) -> Arc<StreamDefinition> {
    Arc::new(
        StreamDefinition::new(name.to_string()).attribute("value".to_string(), AttributeType::LONG),
    )
}

#[allow(dead_code)]
/// Create a StreamEvent with the given timestamp and no attribute data.
pub fn create_stream_event(timestamp: i64) -> StreamEvent {
    StreamEvent::new(timestamp, 0, 0, 0)
}

#[allow(dead_code)]
/// Build a ready to use pattern chain with one step per entry in `steps`.
///
/// Each step is `(alias, stream_name, min, max)`. Returns the chain and a collector
/// that captures every match the last step emits. Panics if the chain fails to build.
pub fn build_pattern_chain(
    steps: Vec<(String, String, usize, usize)>, // (alias, stream_name, min, max)
    state_type: StateType,
) -> (ProcessorChain, OutputCollector) {
    let (app_ctx, query_ctx) = create_test_contexts();

    let mut builder = PatternChainBuilder::new(state_type);

    // Build the stream definitions first because the loop below consumes `steps`
    let stream_defs: Vec<Arc<StreamDefinition>> = steps
        .iter()
        .map(|(_, stream_name, _, _)| create_stream_definition(stream_name))
        .collect();

    for (alias, stream_name, min, max) in steps {
        builder.add_step(PatternStepConfig::new(alias, stream_name, min, max));
    }

    let mut chain = builder
        .build(app_ctx, query_ctx)
        .expect("Failed to build chain");

    chain.init();

    chain.setup_cloners(stream_defs);

    // CRITICAL: move the initial state from new_list to pending_list so the first
    // step is ready to match events
    chain.update_state();

    // Wrap the last post processor so the collector sees every match
    let collector = OutputCollector::new();
    let last_idx = chain.post_processors.len() - 1;
    let original_last = chain.post_processors[last_idx].clone();
    let wrapped = Arc::new(Mutex::new(collector.create_wrapper(original_last)));
    chain.post_processors[last_idx] = wrapped.clone() as Arc<Mutex<dyn PostStateProcessor>>;

    // Point the last pre processor at the wrapper, or matches skip the collector
    chain.pre_processors_concrete[last_idx]
        .lock()
        .unwrap()
        .stream_processor
        .set_this_state_post_processor(wrapped as Arc<Mutex<dyn PostStateProcessor>>);

    (chain, collector)
}

#[allow(dead_code)]
/// Same as [`build_pattern_chain`] but also sets a WITHIN limit.
///
/// `within_time_ms` is the most time in milliseconds allowed between the first
/// event of a match and the later ones.
pub fn build_pattern_chain_with_within(
    steps: Vec<(String, String, usize, usize)>,
    state_type: StateType,
    within_time_ms: i64,
) -> (ProcessorChain, OutputCollector) {
    let (app_ctx, query_ctx) = create_test_contexts();

    let mut builder = PatternChainBuilder::new(state_type);

    // Build the stream definitions first because the loop below consumes `steps`
    let stream_defs: Vec<Arc<StreamDefinition>> = steps
        .iter()
        .map(|(_, stream_name, _, _)| create_stream_definition(stream_name))
        .collect();

    for (alias, stream_name, min, max) in steps {
        builder.add_step(PatternStepConfig::new(alias, stream_name, min, max));
    }

    let mut chain = builder
        .build(app_ctx, query_ctx)
        .expect("Failed to build chain");

    chain.init();

    chain.setup_cloners(stream_defs);

    // Set the WITHIN time on the first processor, where the pattern starts
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .set_within_time(within_time_ms);

    // Later processors check the WITHIN limit against the start state ids
    for i in 1..chain.pre_processors.len() {
        chain.pre_processors_concrete[i]
            .lock()
            .unwrap()
            .stream_processor
            .set_start_state_ids(vec![0]); // measure from position 0, the first event
    }

    // CRITICAL: move the initial state from new_list to pending_list so the first
    // step is ready to match events
    chain.update_state();

    // Wrap the last post processor so the collector sees every match
    let collector = OutputCollector::new();
    let last_idx = chain.post_processors.len() - 1;
    let original_last = chain.post_processors[last_idx].clone();
    let wrapped = Arc::new(Mutex::new(collector.create_wrapper(original_last)));
    chain.post_processors[last_idx] = wrapped.clone() as Arc<Mutex<dyn PostStateProcessor>>;

    // Point the last pre processor at the wrapper, or matches skip the collector
    chain.pre_processors_concrete[last_idx]
        .lock()
        .unwrap()
        .stream_processor
        .set_this_state_post_processor(wrapped as Arc<Mutex<dyn PostStateProcessor>>);

    (chain, collector)
}

#[allow(dead_code)]
/// Collects the pattern matches that reach the wrapped last post processor.
pub struct OutputCollector {
    outputs: Arc<Mutex<Vec<StateEvent>>>,
}

#[allow(dead_code)]
impl OutputCollector {
    pub fn new() -> Self {
        Self {
            outputs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get_outputs(&self) -> Vec<StateEvent> {
        self.outputs.lock().unwrap().clone()
    }

    /// Wrap `original` in a post processor that records each StateEvent into this
    /// collector, then passes it on to `original`.
    pub fn create_wrapper(
        &self,
        original: Arc<Mutex<dyn PostStateProcessor>>,
    ) -> CollectorPostProcessor {
        CollectorPostProcessor {
            original,
            outputs: self.outputs.clone(),
        }
    }
}

#[allow(dead_code)]
/// PostStateProcessor wrapper that records each StateEvent it processes. Every
/// trait call is passed through to the wrapped processor.
#[derive(Debug)]
pub struct CollectorPostProcessor {
    original: Arc<Mutex<dyn PostStateProcessor>>,
    outputs: Arc<Mutex<Vec<StateEvent>>>,
}

impl PostStateProcessor for CollectorPostProcessor {
    fn process(&mut self, chunk: Option<Box<dyn ComplexEvent>>) -> Option<Box<dyn ComplexEvent>> {
        if let Some(ref event) = chunk {
            if let Some(state_event) = event.as_any().downcast_ref::<StateEvent>() {
                println!("  -> Captured StateEvent!");
                self.outputs.lock().unwrap().push(state_event.clone());
            }
        }

        self.original.lock().unwrap().process(chunk)
    }

    fn set_next_processor(&mut self, processor: Arc<Mutex<dyn PostStateProcessor>>) {
        self.original.lock().unwrap().set_next_processor(processor);
    }

    fn get_next_processor(&self) -> Option<Arc<Mutex<dyn PostStateProcessor>>> {
        self.original.lock().unwrap().get_next_processor()
    }

    fn state_id(&self) -> usize {
        self.original.lock().unwrap().state_id()
    }

    fn set_next_state_pre_processor(
        &mut self,
        next_state_pre_processor: Arc<Mutex<dyn eventflux::core::query::input::stream::state::pre_state_processor::PreStateProcessor>>,
    ) {
        self.original
            .lock()
            .unwrap()
            .set_next_state_pre_processor(next_state_pre_processor);
    }

    fn set_next_every_state_pre_processor(
        &mut self,
        next_every_state_pre_processor: Arc<Mutex<dyn eventflux::core::query::input::stream::state::pre_state_processor::PreStateProcessor>>,
    ) {
        self.original
            .lock()
            .unwrap()
            .set_next_every_state_pre_processor(next_every_state_pre_processor);
    }

    fn set_callback_pre_state_processor(
        &mut self,
        callback_pre_state_processor: Arc<Mutex<dyn eventflux::core::query::input::stream::state::pre_state_processor::PreStateProcessor>>,
    ) {
        self.original
            .lock()
            .unwrap()
            .set_callback_pre_state_processor(callback_pre_state_processor);
    }

    fn get_next_every_state_pre_processor(
        &self,
    ) -> Option<Arc<Mutex<dyn eventflux::core::query::input::stream::state::pre_state_processor::PreStateProcessor>>>{
        self.original
            .lock()
            .unwrap()
            .get_next_every_state_pre_processor()
    }

    fn is_event_returned(&self) -> bool {
        self.original.lock().unwrap().is_event_returned()
    }

    fn clear_processed_event(&mut self) {
        self.original.lock().unwrap().clear_processed_event();
    }

    fn this_state_pre_processor(&self) -> Option<Arc<Mutex<dyn eventflux::core::query::input::stream::state::pre_state_processor::PreStateProcessor>>>{
        self.original.lock().unwrap().this_state_pre_processor()
    }
}
