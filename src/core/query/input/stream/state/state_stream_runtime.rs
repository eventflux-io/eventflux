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

// src/core/query/input/stream/state/state_stream_runtime.rs
// StateStreamRuntime, which wraps an InnerStateRuntime with higher level steps

use crate::core::query::input::stream::state::inner_state_runtime::InnerStateRuntime;
use crate::core::query::input::stream::state::pre_state_processor::PreStateProcessor;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// Wraps an InnerStateRuntime and runs its setup, reset and update steps for a pattern
/// or sequence.
///
/// **Main steps**:
/// - `reset_and_update()` resets the chain, then moves new states to pending
/// - `update()` only moves new states to pending
/// - `init_partition()` sets the chain up for a new partition instance
///
/// **How the receivers use it**:
/// - **PatternStreamReceiver** does not use it. It calls expire_events() and
///   update_state() on the processors directly.
/// - **SequenceStreamReceiver** calls expire_events() and reset_state() on the
///   processors, then `update()` here.
#[derive(Debug)]
pub struct StateStreamRuntime {
    /// Runtime that holds the processor chain
    inner_state_runtime: Arc<Mutex<dyn InnerStateRuntime>>,

    /// Start processors to notify when a partition is created
    startup_pre_state_processors: Vec<Arc<Mutex<dyn PreStateProcessor>>>,
}

impl StateStreamRuntime {
    /// Create a runtime wrapping `inner_state_runtime`, with no startup processors.
    pub fn new(inner_state_runtime: Arc<Mutex<dyn InnerStateRuntime>>) -> Self {
        Self {
            inner_state_runtime,
            startup_pre_state_processors: Vec::new(),
        }
    }

    pub fn inner_state_runtime(&self) -> Arc<Mutex<dyn InnerStateRuntime>> {
        self.inner_state_runtime.clone()
    }

    /// Add a start processor to notify when a partition is created
    pub fn add_startup_pre_state_processor(
        &mut self,
        processor: Arc<Mutex<dyn PreStateProcessor>>,
    ) {
        self.startup_pre_state_processors.push(processor);
    }

    /// Reset the chain, then move new states to pending.
    ///
    /// Does nothing when the inner runtime's lock is poisoned.
    pub fn reset_and_update(&mut self) {
        if let Ok(mut guard) = self.inner_state_runtime.lock() {
            guard.reset();
            guard.update();
        }
    }

    /// Move new states to pending, without a reset.
    ///
    /// Used when every processor has already been reset. Does nothing when the inner
    /// runtime's lock is poisoned.
    pub fn update(&mut self) {
        if let Ok(mut guard) = self.inner_state_runtime.lock() {
            guard.update();
        }
    }

    /// Set the chain up for a new partition instance.
    ///
    /// Calls init() on the InnerStateRuntime. The startup processors are not notified
    /// yet, because there is no PartitionCreationListener in Rust.
    pub fn init_partition(&mut self) {
        if let Ok(mut guard) = self.inner_state_runtime.lock() {
            guard.init();
        }

        // Placeholder for notifying startup processors. Needs PartitionCreationListener,
        // which does not exist yet.
        for _processor in &self.startup_pre_state_processors {
            // Future: if processor implements PartitionCreationListener {
            //     processor.partition_created();
            // }
        }
    }
}

// ===== Tests =====

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::eventflux_app_context::EventFluxAppContext;
    use crate::core::config::eventflux_query_context::EventFluxQueryContext;
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

    fn create_test_runtime() -> StateStreamRuntime {
        let inner_runtime = Arc::new(Mutex::new(StreamInnerStateRuntime::new(StateType::Pattern)));
        StateStreamRuntime::new(inner_runtime)
    }

    // ===== Constructor Tests =====

    #[test]
    fn test_new_state_stream_runtime() {
        let runtime = create_test_runtime();
        assert!(runtime.inner_state_runtime().lock().is_ok());
    }

    // ===== Processor Management Tests =====

    #[test]
    fn test_add_startup_pre_state_processor() {
        let mut runtime = create_test_runtime();
        let (app_ctx, query_ctx) = create_test_context();
        let processor = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Pattern,
            app_ctx,
            query_ctx,
        )));

        runtime.add_startup_pre_state_processor(processor);
        assert_eq!(runtime.startup_pre_state_processors.len(), 1);
    }

    #[test]
    fn test_add_multiple_startup_processors() {
        let mut runtime = create_test_runtime();
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

        runtime.add_startup_pre_state_processor(processor1);
        runtime.add_startup_pre_state_processor(processor2);
        assert_eq!(runtime.startup_pre_state_processors.len(), 2);
    }

    // ===== Lifecycle Tests =====

    #[test]
    fn test_reset_and_update() {
        let mut runtime = create_test_runtime();
        let (app_ctx, query_ctx) = create_test_context();
        let pre_processor = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Sequence,
            app_ctx,
            query_ctx,
        )));

        // Set up inner runtime with processor
        {
            let inner_runtime = runtime.inner_state_runtime();
            let mut inner_guard = inner_runtime.lock().unwrap();
            inner_guard.set_first_processor(pre_processor);
            inner_guard.init();
        }

        // Test reset_and_update
        runtime.reset_and_update();

        // Verify runtime is still accessible
        assert!(runtime.inner_state_runtime().lock().is_ok());
    }

    #[test]
    fn test_init_partition() {
        let mut runtime = create_test_runtime();
        let (app_ctx, query_ctx) = create_test_context();
        let pre_processor = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Pattern,
            app_ctx,
            query_ctx,
        )));

        // Set up inner runtime with processor
        {
            let inner_runtime = runtime.inner_state_runtime();
            let mut inner_guard = inner_runtime.lock().unwrap();
            inner_guard.set_first_processor(pre_processor.clone());
        }

        // Add startup processor
        runtime.add_startup_pre_state_processor(pre_processor);

        // Test init_partition
        runtime.init_partition();

        // Verify runtime is initialized
        assert!(runtime.inner_state_runtime().lock().is_ok());
    }

    // ===== Sequence vs Pattern Tests =====

    #[test]
    fn test_sequence_reset_and_update() {
        let inner_runtime = Arc::new(Mutex::new(StreamInnerStateRuntime::new(
            StateType::Sequence,
        )));
        let mut runtime = StateStreamRuntime::new(inner_runtime.clone());
        let (app_ctx, query_ctx) = create_test_context();
        let pre_processor = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Sequence,
            app_ctx,
            query_ctx,
        )));

        // Set up inner runtime
        {
            let mut inner_guard = inner_runtime.lock().unwrap();
            inner_guard.set_first_processor(pre_processor);
            inner_guard.init();
        }

        // Sequence should reset and update
        runtime.reset_and_update();

        // Verify state type is Sequence
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

    #[test]
    fn test_pattern_init_partition() {
        let inner_runtime = Arc::new(Mutex::new(StreamInnerStateRuntime::new(StateType::Pattern)));
        let mut runtime = StateStreamRuntime::new(inner_runtime.clone());
        let (app_ctx, query_ctx) = create_test_context();
        let pre_processor = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Pattern,
            app_ctx,
            query_ctx,
        )));

        // Set up inner runtime
        {
            let mut inner_guard = inner_runtime.lock().unwrap();
            inner_guard.set_first_processor(pre_processor.clone());
        }

        runtime.add_startup_pre_state_processor(pre_processor);

        // Initialize partition
        runtime.init_partition();

        // Verify state type is Pattern
        let inner_guard = inner_runtime.lock().unwrap();
        assert_eq!(
            inner_guard
                .as_any()
                .downcast_ref::<StreamInnerStateRuntime>()
                .unwrap()
                .state_type(),
            StateType::Pattern
        );
    }

    // ===== Debug Implementation Test =====

    #[test]
    fn test_debug_impl() {
        let runtime = create_test_runtime();
        let debug_str = format!("{:?}", runtime);
        assert!(debug_str.contains("StateStreamRuntime"));
    }
}
