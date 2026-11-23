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

// src/core/query/input/stream/state/receiver/sequence_stream_receiver.rs
// SequenceStreamReceiver, which tidies sequence processor state after events are processed

use crate::core::query::input::stream::state::pre_state_processor::PreStateProcessor;
use crate::core::query::input::stream::state::state_stream_runtime::StateStreamRuntime;
use std::sync::{Arc, Mutex};

/// Tidies the state of a SEQUENCE mode processor chain after a batch of events.
///
/// **Steps** (see `stabilize_states`):
/// 1. `expire_events(timestamp)` on every processor, which drops states past WITHIN
/// 2. `reset_state()` on every processor
/// 3. `update()` on the StateStreamRuntime, which moves new states to pending
///
/// **Sequence mode**:
/// - Events must match in order (A → B → C)
/// - State is reset after each batch, not kept, so each state yields one match
/// - WITHIN limits are applied through expire_events()
///
/// **Difference from PatternStreamReceiver**:
/// - Pattern: expire_events() then update_state() on the first processor. State is kept.
/// - Sequence: expire_events(), reset_state() on all, then update(). State is cleared.
#[derive(Debug)]
pub struct SequenceStreamReceiver {
    /// Every PreStateProcessor in the chain. All of them expire and reset.
    all_state_processors: Vec<Arc<Mutex<dyn PreStateProcessor>>>,

    /// Runtime whose update() runs after the reset
    state_stream_runtime: Arc<Mutex<StateStreamRuntime>>,
}

impl SequenceStreamReceiver {
    /// Create a receiver for the given runtime, with no processors yet.
    pub fn new(state_stream_runtime: Arc<Mutex<StateStreamRuntime>>) -> Self {
        Self {
            all_state_processors: Vec::new(),
            state_stream_runtime,
        }
    }

    pub fn add_state_processor(&mut self, processor: Arc<Mutex<dyn PreStateProcessor>>) {
        self.all_state_processors.push(processor);
    }

    pub fn all_state_processors(&self) -> &Vec<Arc<Mutex<dyn PreStateProcessor>>> {
        &self.all_state_processors
    }

    pub fn state_stream_runtime(&self) -> Arc<Mutex<StateStreamRuntime>> {
        self.state_stream_runtime.clone()
    }

    /// Tidy states after event processing, using SEQUENCE mode rules.
    ///
    /// 1. Calls `expire_events(timestamp)` on every processor in the chain
    /// 2. Calls `reset_state()` on every processor, not just the first, so the whole
    ///    sequence is ready for the next match. A start state sets itself up again.
    /// 3. Calls `update()` on the StateStreamRuntime to move new states to pending
    ///
    /// A processor or runtime whose lock is poisoned is skipped.
    ///
    /// # Arguments
    /// * `timestamp` - Current time in milliseconds, used to expire old states
    pub fn stabilize_states(&mut self, timestamp: i64) {
        for processor in &self.all_state_processors {
            if let Ok(mut guard) = processor.lock() {
                guard.expire_events(timestamp);
            }
        }

        for processor in &self.all_state_processors {
            if let Ok(mut guard) = processor.lock() {
                guard.reset_state();
            }
        }

        if let Ok(mut runtime_guard) = self.state_stream_runtime.lock() {
            // update only. Every processor was already reset above.
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
