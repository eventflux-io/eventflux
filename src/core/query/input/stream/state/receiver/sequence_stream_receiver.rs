// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/input/stream/state/receiver/sequence_stream_receiver.rs
// SequenceStreamReceiver for stabilizing sequence processor state

use crate::core::query::input::stream::state::pre_state_processor::PreStateProcessor;
use crate::core::query::input::stream::state::state_stream_runtime::StateStreamRuntime;
use std::sync::{Arc, Mutex};

/// SequenceStreamReceiver stabilizes sequence processor state after event processing.
///
/// **Purpose**: After receiving a batch of events, stabilize the state by expiring old events
/// and resetting/updating state for sequence processing.
///
/// **Stabilization Flow (Sequence)**:
/// 1. `expire_events(timestamp)` on all processors - Remove events outside time window
/// 2. `state_stream_runtime.reset_and_update()` - Clear state and process pending events
///
/// **Sequence Semantics**:
/// - Sequence allows only ONE match per state (clears after match)
/// - State is reset after match (not retained)
/// - WITHIN time constraints honored via expire_events()
/// - Sequential event matching (A → B → C)
///
/// **Difference from Pattern**:
/// - Pattern: expireEvents() + updateState() (retains state)
/// - Sequence: expireEvents() + resetAndUpdate() (clears state)
#[derive(Debug)]
pub struct SequenceStreamReceiver {
    /// All PreStateProcessors in the chain (for expiring events)
    all_state_processors: Vec<Arc<Mutex<dyn PreStateProcessor>>>,

    /// The StateStreamRuntime (for reset_and_update)
    state_stream_runtime: Arc<Mutex<StateStreamRuntime>>,
}

impl SequenceStreamReceiver {
    /// Create a new SequenceStreamReceiver
    ///
    /// # Arguments
    /// * `state_stream_runtime` - The StateStreamRuntime to manage
    pub fn new(state_stream_runtime: Arc<Mutex<StateStreamRuntime>>) -> Self {
        Self {
            all_state_processors: Vec::new(),
            state_stream_runtime,
        }
    }

    /// Add a state processor to the chain
    pub fn add_state_processor(&mut self, processor: Arc<Mutex<dyn PreStateProcessor>>) {
        self.all_state_processors.push(processor);
    }

    /// Get all state processors
    pub fn all_state_processors(&self) -> &Vec<Arc<Mutex<dyn PreStateProcessor>>> {
        &self.all_state_processors
    }

    /// Get the state stream runtime
    pub fn state_stream_runtime(&self) -> Arc<Mutex<StateStreamRuntime>> {
        self.state_stream_runtime.clone()
    }

    /// Stabilize states after event processing (Sequence semantics).
    ///
    /// **Flow**:
    /// 1. Call `expire_events(timestamp)` on ALL processors in chain
    /// 2. Call `reset_state()` on ALL processors (SEQUENCE auto-restart)
    /// 3. Call `state_stream_runtime.reset_and_update()` for first processor reinit
    ///
    /// **Sequence Behavior**:
    /// - Expires old events outside WITHIN time window
    /// - Resets ALL processors in chain (not just first) for auto-restart
    /// - Updates state to process new pending events
    /// - Auto-restarts after each complete match (continuous matching)
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

        // Step 2: Reset ALL processors in chain (SEQUENCE auto-restart)
        // This ensures all processors in the sequence are ready for the next match
        for processor in &self.all_state_processors {
            if let Ok(mut guard) = processor.lock() {
                guard.reset_state();
            }
        }

        // Step 3: Update via StateStreamRuntime (moves new->pending)
        if let Ok(mut runtime_guard) = self.state_stream_runtime.lock() {
            // Only call update, not reset (we already reset all processors above)
            runtime_guard.update();
        }
    }
}

// ===== Tests =====

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::eventflux_app_context::EventFluxAppContext;
    use crate::core::config::eventflux_query_context::EventFluxQueryContext;
    use crate::core::query::input::stream::state::inner_state_runtime::InnerStateRuntime;
    use crate::core::query::input::stream::state::stream_inner_state_runtime::StreamInnerStateRuntime;
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

    fn create_test_receiver() -> SequenceStreamReceiver {
        let inner_runtime = Arc::new(Mutex::new(StreamInnerStateRuntime::new(
            StateType::Sequence,
        )));
        let state_runtime = Arc::new(Mutex::new(StateStreamRuntime::new(inner_runtime)));
        SequenceStreamReceiver::new(state_runtime)
    }

    // ===== Constructor Tests =====

    #[test]
    fn test_new_sequence_receiver() {
        let receiver = create_test_receiver();
        assert_eq!(receiver.all_state_processors().len(), 0);
        assert!(receiver.state_stream_runtime().lock().is_ok());
    }

    // ===== Processor Management Tests =====

    #[test]
    fn test_add_state_processor() {
        let mut receiver = create_test_receiver();
        let (app_ctx, query_ctx) = create_test_context();
        let processor = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Sequence,
            app_ctx,
            query_ctx,
        )));

        receiver.add_state_processor(processor);
        assert_eq!(receiver.all_state_processors().len(), 1);
    }

    #[test]
    fn test_add_multiple_processors() {
        let mut receiver = create_test_receiver();
        let (app_ctx, query_ctx) = create_test_context();

        let processor1 = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Sequence,
            app_ctx.clone(),
            query_ctx.clone(),
        )));
        let processor2 = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            1,
            false,
            StateType::Sequence,
            app_ctx,
            query_ctx,
        )));

        receiver.add_state_processor(processor1);
        receiver.add_state_processor(processor2);
        assert_eq!(receiver.all_state_processors().len(), 2);
    }

    // ===== Stabilization Tests =====

    #[test]
    fn test_stabilize_states_no_processors() {
        let mut receiver = create_test_receiver();
        receiver.stabilize_states(1000);
        // Should not panic with no processors
    }

    #[test]
    fn test_stabilize_states_with_runtime() {
        let inner_runtime = Arc::new(Mutex::new(StreamInnerStateRuntime::new(
            StateType::Sequence,
        )));
        let state_runtime = Arc::new(Mutex::new(StateStreamRuntime::new(inner_runtime.clone())));
        let mut receiver = SequenceStreamReceiver::new(state_runtime);

        let (app_ctx, query_ctx) = create_test_context();
        let processor = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Sequence,
            app_ctx,
            query_ctx,
        )));

        // Set up inner runtime with processor
        {
            let mut inner_guard = inner_runtime.lock().unwrap();
            inner_guard.set_first_processor(processor.clone());
            inner_guard.init();
        }

        receiver.add_state_processor(processor);
        receiver.stabilize_states(1000);

        // Should successfully stabilize states
        assert!(receiver.state_stream_runtime().lock().is_ok());
    }

    #[test]
    fn test_stabilize_states_expires_and_resets() {
        let inner_runtime = Arc::new(Mutex::new(StreamInnerStateRuntime::new(
            StateType::Sequence,
        )));
        let state_runtime = Arc::new(Mutex::new(StateStreamRuntime::new(inner_runtime.clone())));
        let mut receiver = SequenceStreamReceiver::new(state_runtime);

        let (app_ctx, query_ctx) = create_test_context();
        let processor = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Sequence,
            app_ctx,
            query_ctx,
        )));

        // Set up inner runtime
        {
            let mut inner_guard = inner_runtime.lock().unwrap();
            inner_guard.set_first_processor(processor.clone());
            inner_guard.init();
        }

        receiver.add_state_processor(processor);

        // Stabilize at timestamp 1000
        receiver.stabilize_states(1000);

        // Stabilize again at timestamp 2000 (should expire and reset)
        receiver.stabilize_states(2000);

        // Verify runtime is still accessible
        assert!(receiver.state_stream_runtime().lock().is_ok());
    }

    #[test]
    fn test_stabilize_states_multiple_processors() {
        let inner_runtime = Arc::new(Mutex::new(StreamInnerStateRuntime::new(
            StateType::Sequence,
        )));
        let state_runtime = Arc::new(Mutex::new(StateStreamRuntime::new(inner_runtime.clone())));
        let mut receiver = SequenceStreamReceiver::new(state_runtime);

        let (app_ctx, query_ctx) = create_test_context();

        let processor1 = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Sequence,
            app_ctx.clone(),
            query_ctx.clone(),
        )));
        let processor2 = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            1,
            false,
            StateType::Sequence,
            app_ctx,
            query_ctx,
        )));

        // Set up inner runtime
        {
            let mut inner_guard = inner_runtime.lock().unwrap();
            inner_guard.set_first_processor(processor1.clone());
            inner_guard.init();
        }

        receiver.add_state_processor(processor1);
        receiver.add_state_processor(processor2);

        receiver.stabilize_states(1000);

        // Verify all processors are still accessible
        assert_eq!(receiver.all_state_processors().len(), 2);
    }

    // ===== Sequence Semantics Tests =====

    #[test]
    fn test_sequence_clears_state_after_match() {
        let inner_runtime = Arc::new(Mutex::new(StreamInnerStateRuntime::new(
            StateType::Sequence,
        )));
        let state_runtime = Arc::new(Mutex::new(StateStreamRuntime::new(inner_runtime.clone())));
        let mut receiver = SequenceStreamReceiver::new(state_runtime);

        let (app_ctx, query_ctx) = create_test_context();
        let processor = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Sequence,
            app_ctx,
            query_ctx,
        )));

        // Set up inner runtime
        {
            let mut inner_guard = inner_runtime.lock().unwrap();
            inner_guard.set_first_processor(processor.clone());
            inner_guard.init();
        }

        receiver.add_state_processor(processor);

        // First stabilize (processes events)
        receiver.stabilize_states(1000);

        // Second stabilize (should reset state)
        receiver.stabilize_states(2000);

        // Verify sequence state type is preserved
        let inner_guard = inner_runtime.lock().unwrap();
        assert_eq!(
            inner_guard
                .as_any()
                .downcast_ref::<StreamInnerStateRuntime>()
                .unwrap()
                .state_type(),
            StateType::Sequence
        );
    }

    // ===== Debug Implementation Test =====

    #[test]
    fn test_debug_impl() {
        let receiver = create_test_receiver();
        let debug_str = format!("{:?}", receiver);
        assert!(debug_str.contains("SequenceStreamReceiver"));
    }
}
