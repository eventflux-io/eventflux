// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/input/stream/state/state_stream_runtime.rs
// StateStreamRuntime - manages InnerStateRuntime lifecycle

use crate::core::query::input::stream::state::inner_state_runtime::InnerStateRuntime;
use crate::core::query::input::stream::state::pre_state_processor::PreStateProcessor;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// StateStreamRuntime manages the lifecycle of InnerStateRuntime.
///
/// **Purpose**: Provides high-level operations for pattern/sequence processing runtime.
///
/// **Key Operations**:
/// - `reset_and_update()` - Reset state then update (used by SequenceReceiver)
/// - `init_partition()` - Initialize runtime for partitioned processing
///
/// **Usage in Receivers**:
/// - **PatternReceiver**: Calls expireEvents() then updateState() directly on processors
/// - **SequenceReceiver**: Calls expireEvents() on processors, then stateStreamRuntime.resetAndUpdate()
#[derive(Debug)]
pub struct StateStreamRuntime {
    /// The inner state runtime managing processor chains
    inner_state_runtime: Arc<Mutex<dyn InnerStateRuntime>>,

    /// List of startup PreStateProcessors (for partition initialization)
    startup_pre_state_processors: Vec<Arc<Mutex<dyn PreStateProcessor>>>,
}

impl StateStreamRuntime {
    /// Create a new StateStreamRuntime
    ///
    /// # Arguments
    /// * `inner_state_runtime` - The InnerStateRuntime to manage
    pub fn new(inner_state_runtime: Arc<Mutex<dyn InnerStateRuntime>>) -> Self {
        Self {
            inner_state_runtime,
            startup_pre_state_processors: Vec::new(),
        }
    }

    /// Get the inner state runtime
    pub fn inner_state_runtime(&self) -> Arc<Mutex<dyn InnerStateRuntime>> {
        self.inner_state_runtime.clone()
    }

    /// Add a startup PreStateProcessor (for partition initialization)
    pub fn add_startup_pre_state_processor(
        &mut self,
        processor: Arc<Mutex<dyn PreStateProcessor>>,
    ) {
        self.startup_pre_state_processors.push(processor);
    }

    /// Reset and update state.
    ///
    /// **Purpose**: Used by SequenceReceiver to clear state and process pending events.
    ///
    /// **Flow**:
    /// 1. reset() - Clears all pending events and state
    /// 2. update() - Processes any new pending events
    pub fn reset_and_update(&mut self) {
        if let Ok(mut guard) = self.inner_state_runtime.lock() {
            guard.reset();
            guard.update();
        }
    }

    /// Update state only (without reset).
    ///
    /// **Purpose**: Used when reset has already been done on all processors.
    /// Moves new events to pending for processing.
    pub fn update(&mut self) {
        if let Ok(mut guard) = self.inner_state_runtime.lock() {
            guard.update();
        }
    }

    /// Initialize runtime for partitioned processing.
    ///
    /// **Purpose**: Called when creating a new partition instance.
    ///
    /// **Flow**:
    /// 1. init() - Initialize the InnerStateRuntime
    /// 2. Notify all startup processors (if they support PartitionCreationListener)
    pub fn init_partition(&mut self) {
        // Initialize the inner state runtime
        if let Ok(mut guard) = self.inner_state_runtime.lock() {
            guard.init();
        }

        // Notify startup processors of partition creation
        // Note: PartitionCreationListener is not yet implemented in Rust
        // This will be added when partition support is implemented
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
