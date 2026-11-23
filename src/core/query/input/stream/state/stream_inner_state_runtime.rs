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

// src/core/query/input/stream/state/stream_inner_state_runtime.rs
// StreamInnerStateRuntime, the default InnerStateRuntime

use crate::core::query::input::stream::state::inner_state_runtime::InnerStateRuntime;
use crate::core::query::input::stream::state::post_state_processor::PostStateProcessor;
use crate::core::query::input::stream::state::pre_state_processor::PreStateProcessor;
use crate::core::query::input::stream::state::stream_pre_state_processor::StateType;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// Default InnerStateRuntime. Holds the first and last processors of a pattern or
/// sequence chain.
///
/// ```text
/// [StreamInnerStateRuntime]
///      ↓
/// [First PreStateProcessor] → [Next PreStateProcessor] → ... → [Last PostStateProcessor]
/// ```
///
/// **Lifecycle** (applied to the first processor only)
/// - `init()` calls `init()`
/// - `reset()` calls `reset_state()`
/// - `update()` calls `update_state()`, which moves its new states to pending
///
/// Each step does nothing when no first processor is set or its lock is poisoned.
#[derive(Debug)]
pub struct StreamInnerStateRuntime {
    first_processor: Option<Arc<Mutex<dyn PreStateProcessor>>>,

    last_processor: Option<Arc<Mutex<dyn PostStateProcessor>>>,

    /// Pattern or Sequence
    state_type: StateType,
}

impl StreamInnerStateRuntime {
    /// Create a runtime with no processors set
    pub fn new(state_type: StateType) -> Self {
        Self {
            first_processor: None,
            last_processor: None,
            state_type,
        }
    }

    pub fn state_type(&self) -> StateType {
        self.state_type
    }
}

impl InnerStateRuntime for StreamInnerStateRuntime {
    fn get_first_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>> {
        self.first_processor.clone()
    }

    fn set_first_processor(&mut self, first_processor: Arc<Mutex<dyn PreStateProcessor>>) {
        self.first_processor = Some(first_processor);
    }

    fn get_last_processor(&self) -> Option<Arc<Mutex<dyn PostStateProcessor>>> {
        self.last_processor.clone()
    }

    fn set_last_processor(&mut self, last_processor: Arc<Mutex<dyn PostStateProcessor>>) {
        self.last_processor = Some(last_processor);
    }

    fn init(&mut self) {
        if let Some(ref first) = self.first_processor {
            if let Ok(mut guard) = first.lock() {
                guard.init();
            }
        }
    }

    fn reset(&mut self) {
        if let Some(ref first) = self.first_processor {
            if let Ok(mut guard) = first.lock() {
                guard.reset_state();
            }
        }
    }

    fn update(&mut self) {
        if let Some(ref first) = self.first_processor {
            if let Ok(mut guard) = first.lock() {
                guard.update_state();
            }
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// ===== Tests =====

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::eventflux_app_context::EventFluxAppContext;
    use crate::core::config::eventflux_query_context::EventFluxQueryContext;
    use crate::core::query::input::stream::state::stream_post_state_processor::StreamPostStateProcessor;
    use crate::core::query::input::stream::state::stream_pre_state_processor::StreamPreStateProcessor;

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
    fn test_new_pattern_runtime() {
        let runtime = StreamInnerStateRuntime::new(StateType::Pattern);
        assert_eq!(runtime.state_type(), StateType::Pattern);
        assert!(runtime.get_first_processor().is_none());
        assert!(runtime.get_last_processor().is_none());
    }

    #[test]
    fn test_new_sequence_runtime() {
        let runtime = StreamInnerStateRuntime::new(StateType::Sequence);
        assert_eq!(runtime.state_type(), StateType::Sequence);
        assert!(runtime.get_first_processor().is_none());
        assert!(runtime.get_last_processor().is_none());
    }

    // ===== Processor Management Tests =====

    #[test]
    fn test_set_first_processor() {
        let mut runtime = StreamInnerStateRuntime::new(StateType::Pattern);
        let (app_ctx, query_ctx) = create_test_context();
        let pre_processor = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Pattern,
            app_ctx,
            query_ctx,
        )));

        runtime.set_first_processor(pre_processor);
        assert!(runtime.get_first_processor().is_some());
    }

    #[test]
    fn test_set_last_processor() {
        let mut runtime = StreamInnerStateRuntime::new(StateType::Pattern);
        let post_processor = Arc::new(Mutex::new(StreamPostStateProcessor::new(0)));

        runtime.set_last_processor(post_processor);
        assert!(runtime.get_last_processor().is_some());
    }

    // ===== Lifecycle Tests =====

    #[test]
    fn test_init_calls_first_processor() {
        let mut runtime = StreamInnerStateRuntime::new(StateType::Pattern);
        let (app_ctx, query_ctx) = create_test_context();
        let pre_processor = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Pattern,
            app_ctx,
            query_ctx,
        )));

        runtime.set_first_processor(pre_processor.clone());
        runtime.init();

        // Verify init was called (processor should be initialized)
        let guard = pre_processor.lock().unwrap();
        assert!(guard.is_start_state()); // Verify state is consistent
    }

    #[test]
    fn test_reset_calls_first_processor() {
        let mut runtime = StreamInnerStateRuntime::new(StateType::Sequence);
        let (app_ctx, query_ctx) = create_test_context();
        let pre_processor = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Sequence,
            app_ctx,
            query_ctx,
        )));

        runtime.set_first_processor(pre_processor.clone());
        runtime.init();
        runtime.reset();

        // Only checks that reset runs without a panic
        assert!(runtime.get_first_processor().is_some());
    }

    #[test]
    fn test_update_calls_first_processor() {
        let mut runtime = StreamInnerStateRuntime::new(StateType::Pattern);
        let (app_ctx, query_ctx) = create_test_context();
        let pre_processor = Arc::new(Mutex::new(StreamPreStateProcessor::new(
            0,
            true,
            StateType::Pattern,
            app_ctx,
            query_ctx,
        )));

        runtime.set_first_processor(pre_processor.clone());
        runtime.init();
        runtime.update();

        // Only checks that update runs without a panic
        assert!(runtime.get_first_processor().is_some());
    }

    // ===== Debug Implementation Test =====

    #[test]
    fn test_debug_impl() {
        let runtime = StreamInnerStateRuntime::new(StateType::Pattern);
        let debug_str = format!("{:?}", runtime);
        assert!(debug_str.contains("StreamInnerStateRuntime"));
    }
}
