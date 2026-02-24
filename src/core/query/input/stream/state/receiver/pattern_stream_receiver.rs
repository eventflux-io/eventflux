// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/input/stream/state/receiver/pattern_stream_receiver.rs
// PatternStreamReceiver for stabilizing pattern processor state

use crate::core::query::input::stream::state::pre_state_processor::PreStateProcessor;
use std::sync::{Arc, Mutex};

/// PatternStreamReceiver stabilizes pattern processor state after event processing.
///
/// **Purpose**: After receiving a batch of events, stabilize the state by expiring old events
/// and updating pending state for pattern processing.
///
/// **Stabilization Flow (Pattern)**:
/// 1. `expire_events(timestamp)` on all processors - Remove events outside time window
/// 2. `update_state()` on first processor - Process pending events and emit matches
///
/// **Pattern Semantics**:
/// - Pattern allows multiple matches from the same state
/// - State is retained after match (not cleared)
/// - WITHIN time constraints honored via expire_events()
#[derive(Debug)]
pub struct PatternStreamReceiver {
    /// All PreStateProcessors in the chain (for expiring events)
    all_state_processors: Vec<Arc<Mutex<dyn PreStateProcessor>>>,

    /// The first processor for the stream (for updating state)
    first_state_processor: Option<Arc<Mutex<dyn PreStateProcessor>>>,
}

impl PatternStreamReceiver {
    /// Create a new PatternStreamReceiver
    pub fn new() -> Self {
        Self {
            all_state_processors: Vec::new(),
            first_state_processor: None,
        }
    }

    /// Add a state processor to the chain
    pub fn add_state_processor(&mut self, processor: Arc<Mutex<dyn PreStateProcessor>>) {
        self.all_state_processors.push(processor);
    }

    /// Set the first state processor (for updating state)
    pub fn set_first_state_processor(&mut self, processor: Arc<Mutex<dyn PreStateProcessor>>) {
        self.first_state_processor = Some(processor);
    }

    /// Get all state processors
    pub fn all_state_processors(&self) -> &Vec<Arc<Mutex<dyn PreStateProcessor>>> {
        &self.all_state_processors
    }

    /// Get the first state processor
    pub fn first_state_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>> {
        self.first_state_processor.clone()
    }

    /// Stabilize states after event processing (Pattern semantics).
    ///
    /// **Flow**:
    /// 1. Call `expire_events(timestamp)` on ALL processors in chain
    /// 2. Call `update_state()` on FIRST processor
    ///
    /// **Pattern Behavior**:
    /// - Expires old events outside WITHIN time window
    /// - Updates state to process pending events
    /// - Retains state after match (allows multiple matches)
    ///
    /// # Arguments
    /// * `timestamp` - Current timestamp for expiring old events
    pub fn stabilize_states(&mut self, timestamp: i64) {
        // Step 1: Expire old events on all processors
        for processor in &self.all_state_processors {
            if let Ok(mut guard) = processor.lock() {
                guard.expire_events(timestamp);
            }
        }

        // Step 2: Update state on first processor (if exists)
        if let Some(ref first) = self.first_state_processor {
            if let Ok(mut guard) = first.lock() {
                guard.update_state();
            }
        }
    }
}

impl Default for PatternStreamReceiver {
    fn default() -> Self {
        Self::new()
    }
}

// ===== Tests =====

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::eventflux_app_context::EventFluxAppContext;
    use crate::core::config::eventflux_query_context::EventFluxQueryContext;
    use crate::core::query::input::stream::state::stream_pre_state_processor::{
        StateType, StreamPreStateProcessor,
    };

    fn create_test_context() -> (Arc<EventFluxAppContext>, Arc<EventFluxQueryContext>) {
        use crate::core::config::eventflux_context::EventFluxContext;
        use crate::query_api::eventflux_app::EventFluxApp;

        let eventflux_context = Arc::new(EventFluxContext::new());
        let app = Arc::new(EventFluxApp::new("TestApp".to_string()));
        let app_ctx = Arc::new(EventFluxAppContext::new(
            eventflux_context,
            "test_app".to_string(),
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

    // ===== Constructor Tests =====

    #[test]
    fn test_new_pattern_receiver() {
        let receiver = PatternStreamReceiver::new();
        assert_eq!(receiver.all_state_processors().len(), 0);
        assert!(receiver.first_state_processor().is_none());
    }

    #[test]
    fn test_default_pattern_receiver() {
        let receiver = PatternStreamReceiver::default();
        assert_eq!(receiver.all_state_processors().len(), 0);
        assert!(receiver.first_state_processor().is_none());
    }

    // ===== Processor Management Tests =====

    #[test]
    fn test_add_state_processor() {
        let mut receiver = PatternStreamReceiver::new();
        let (app_ctx, query_ctx) = create_test_context();
        let processor = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Pattern,
            app_ctx,
            query_ctx,
        )));

        receiver.add_state_processor(processor);
        assert_eq!(receiver.all_state_processors().len(), 1);
    }

    #[test]
    fn test_add_multiple_processors() {
        let mut receiver = PatternStreamReceiver::new();
        let (app_ctx, query_ctx) = create_test_context();

        let processor1 = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Pattern,
            app_ctx.clone(),
            query_ctx.clone(),
        )));
        let processor2 = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            1,
            false,
            StateType::Pattern,
            app_ctx,
            query_ctx,
        )));

        receiver.add_state_processor(processor1);
        receiver.add_state_processor(processor2);
        assert_eq!(receiver.all_state_processors().len(), 2);
    }

    #[test]
    fn test_set_first_state_processor() {
        let mut receiver = PatternStreamReceiver::new();
        let (app_ctx, query_ctx) = create_test_context();
        let processor = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Pattern,
            app_ctx,
            query_ctx,
        )));

        receiver.set_first_state_processor(processor);
        assert!(receiver.first_state_processor().is_some());
    }

    // ===== Stabilization Tests =====

    #[test]
    fn test_stabilize_states_no_processors() {
        let mut receiver = PatternStreamReceiver::new();
        receiver.stabilize_states(1000);
        // Should not panic with no processors
    }

    #[test]
    fn test_stabilize_states_with_first_processor() {
        let mut receiver = PatternStreamReceiver::new();
        let (app_ctx, query_ctx) = create_test_context();
        let processor = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Pattern,
            app_ctx,
            query_ctx,
        )));

        receiver.add_state_processor(processor.clone());
        receiver.set_first_state_processor(processor);

        // Initialize processor before stabilizing
        {
            let first_processor = receiver.first_state_processor().unwrap();
            let mut guard = first_processor.lock().unwrap();
            guard.init();
        }

        receiver.stabilize_states(1000);
        // Should successfully stabilize states
    }

    #[test]
    fn test_stabilize_states_expires_and_updates() {
        let mut receiver = PatternStreamReceiver::new();
        let (app_ctx, query_ctx) = create_test_context();
        let processor = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Pattern,
            app_ctx,
            query_ctx,
        )));

        receiver.add_state_processor(processor.clone());
        receiver.set_first_state_processor(processor.clone());

        // Initialize processor
        {
            let mut guard = processor.lock().unwrap();
            guard.init();
        }

        // Stabilize at timestamp 1000
        receiver.stabilize_states(1000);

        // Stabilize again at timestamp 2000 (should expire events from 1000)
        receiver.stabilize_states(2000);

        // Verify processor is still accessible
        assert!(receiver.first_state_processor().is_some());
    }

    #[test]
    fn test_stabilize_states_multiple_processors() {
        let mut receiver = PatternStreamReceiver::new();
        let (app_ctx, query_ctx) = create_test_context();

        let processor1 = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Pattern,
            app_ctx.clone(),
            query_ctx.clone(),
        )));
        let processor2 = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            1,
            false,
            StateType::Pattern,
            app_ctx,
            query_ctx,
        )));

        receiver.add_state_processor(processor1.clone());
        receiver.add_state_processor(processor2.clone());
        receiver.set_first_state_processor(processor1);

        // Initialize processors
        {
            let first_processor = receiver.first_state_processor().unwrap();
            let mut guard1 = first_processor.lock().unwrap();
            guard1.init();
        }

        receiver.stabilize_states(1000);

        // Verify all processors are still accessible
        assert_eq!(receiver.all_state_processors().len(), 2);
    }

    // ===== Debug Implementation Test =====

    #[test]
    fn test_debug_impl() {
        let receiver = PatternStreamReceiver::new();
        let debug_str = format!("{:?}", receiver);
        assert!(debug_str.contains("PatternStreamReceiver"));
    }
}
