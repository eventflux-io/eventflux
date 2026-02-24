// SPDX-License-Identifier: MIT OR Apache-2.0

//! Common test utilities for pattern chain tests
//!
//! Provides shared helpers, mock objects, and utilities to eliminate code duplication
//! across Phase 2b test files.

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

/// Create test contexts for pattern chain tests
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

/// Create a stream definition for testing
pub fn create_stream_definition(name: &str) -> Arc<StreamDefinition> {
    Arc::new(
        StreamDefinition::new(name.to_string()).attribute("value".to_string(), AttributeType::LONG),
    )
}

/// Create a StreamEvent with specified timestamp
pub fn create_stream_event(timestamp: i64) -> StreamEvent {
    StreamEvent::new(timestamp, 0, 0, 0)
}

/// Build a pattern chain with specified number of steps
pub fn build_pattern_chain(
    steps: Vec<(String, String, usize, usize)>, // (alias, stream_name, min, max)
    state_type: StateType,
) -> (ProcessorChain, OutputCollector) {
    let (app_ctx, query_ctx) = create_test_contexts();

    let mut builder = PatternChainBuilder::new(state_type);

    // Collect stream definitions before consuming steps
    let stream_defs: Vec<Arc<StreamDefinition>> = steps
        .iter()
        .map(|(_, stream_name, _, _)| create_stream_definition(stream_name))
        .collect();

    // Add steps to builder
    for (alias, stream_name, min, max) in steps {
        builder.add_step(PatternStepConfig::new(alias, stream_name, min, max));
    }

    let mut chain = builder
        .build(app_ctx, query_ctx)
        .expect("Failed to build chain");

    // Initialize chain
    chain.init();

    // Set up event cloners
    chain.setup_cloners(stream_defs);

    // CRITICAL: Move initial state from new_list to pending_list
    chain.update_state();

    // Attach output collector to last post processor
    let collector = OutputCollector::new();
    let last_idx = chain.post_processors.len() - 1;
    let original_last = chain.post_processors[last_idx].clone();
    let wrapped = Arc::new(Mutex::new(collector.create_wrapper(original_last)));
    chain.post_processors[last_idx] = wrapped.clone() as Arc<Mutex<dyn PostStateProcessor>>;

    // Re-wire the last pre-processor to use the wrapped post processor
    chain.pre_processors_concrete[last_idx]
        .lock()
        .unwrap()
        .stream_processor
        .set_this_state_post_processor(wrapped as Arc<Mutex<dyn PostStateProcessor>>);

    (chain, collector)
}

/// Build a pattern chain with WITHIN time constraint
pub fn build_pattern_chain_with_within(
    steps: Vec<(String, String, usize, usize)>,
    state_type: StateType,
    within_time_ms: i64,
) -> (ProcessorChain, OutputCollector) {
    let (app_ctx, query_ctx) = create_test_contexts();

    let mut builder = PatternChainBuilder::new(state_type);

    // Collect stream definitions before consuming steps
    let stream_defs: Vec<Arc<StreamDefinition>> = steps
        .iter()
        .map(|(_, stream_name, _, _)| create_stream_definition(stream_name))
        .collect();

    // Add steps to builder
    for (alias, stream_name, min, max) in steps {
        builder.add_step(PatternStepConfig::new(alias, stream_name, min, max));
    }

    let mut chain = builder
        .build(app_ctx, query_ctx)
        .expect("Failed to build chain");

    // Initialize chain
    chain.init();

    // Set up event cloners
    chain.setup_cloners(stream_defs);

    // Set WITHIN time on the first processor (where pattern starts)
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .set_within_time(within_time_ms);

    // Set start_state_ids on subsequent processors to check WITHIN constraint
    for i in 1..chain.pre_processors.len() {
        chain.pre_processors_concrete[i]
            .lock()
            .unwrap()
            .stream_processor
            .set_start_state_ids(vec![0]); // Check against position 0 (first event)
    }

    // CRITICAL: Move initial state from new_list to pending_list
    chain.update_state();

    // Attach output collector to last post processor
    let collector = OutputCollector::new();
    let last_idx = chain.post_processors.len() - 1;
    let original_last = chain.post_processors[last_idx].clone();
    let wrapped = Arc::new(Mutex::new(collector.create_wrapper(original_last)));
    chain.post_processors[last_idx] = wrapped.clone() as Arc<Mutex<dyn PostStateProcessor>>;

    // Re-wire the last pre-processor to use the wrapped post processor
    chain.pre_processors_concrete[last_idx]
        .lock()
        .unwrap()
        .stream_processor
        .set_this_state_post_processor(wrapped as Arc<Mutex<dyn PostStateProcessor>>);

    (chain, collector)
}

/// Output collector for capturing pattern match results
pub struct OutputCollector {
    outputs: Arc<Mutex<Vec<StateEvent>>>,
}

impl OutputCollector {
    pub fn new() -> Self {
        Self {
            outputs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get_outputs(&self) -> Vec<StateEvent> {
        self.outputs.lock().unwrap().clone()
    }

    /// Create a wrapper post processor that captures outputs
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

/// PostStateProcessor wrapper that captures outputs
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

        // Pass through to original processor
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
