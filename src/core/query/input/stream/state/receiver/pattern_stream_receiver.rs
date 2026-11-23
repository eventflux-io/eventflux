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

// src/core/query/input/stream/state/receiver/pattern_stream_receiver.rs
// PatternStreamReceiver, which tidies pattern processor state after events are processed

use crate::core::query::input::stream::state::pre_state_processor::PreStateProcessor;
use std::sync::{Arc, Mutex};

/// Tidies the state of a PATTERN mode processor chain after a batch of events.
///
/// **Steps** (see `stabilize_states`):
/// 1. `expire_events(timestamp)` on every processor, which drops states past WITHIN
/// 2. `update_state()` on the first processor, which moves its new states to pending
///
/// **Pattern mode**:
/// - One state can produce several matches
/// - A state is kept after a match, not cleared. Clearing on reset is left to the
///   processors.
/// - WITHIN limits are applied through expire_events()
#[derive(Debug)]
pub struct PatternStreamReceiver {
    /// Every PreStateProcessor in the chain. All of them expire events.
    all_state_processors: Vec<Arc<Mutex<dyn PreStateProcessor>>>,

    /// First processor for the stream. Only this one has update_state() called.
    first_state_processor: Option<Arc<Mutex<dyn PreStateProcessor>>>,
}

impl PatternStreamReceiver {
    pub fn new() -> Self {
        Self {
            all_state_processors: Vec::new(),
            first_state_processor: None,
        }
    }

    pub fn add_state_processor(&mut self, processor: Arc<Mutex<dyn PreStateProcessor>>) {
        self.all_state_processors.push(processor);
    }

    pub fn set_first_state_processor(&mut self, processor: Arc<Mutex<dyn PreStateProcessor>>) {
        self.first_state_processor = Some(processor);
    }

    pub fn all_state_processors(&self) -> &Vec<Arc<Mutex<dyn PreStateProcessor>>> {
        &self.all_state_processors
    }

    pub fn first_state_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>> {
        self.first_state_processor.clone()
    }

    /// Tidy states after event processing, using PATTERN mode rules.
    ///
    /// Calls `expire_events(timestamp)` on every processor in the chain, then
    /// `update_state()` on the first processor if one is set. A processor whose lock
    /// is poisoned is skipped.
    ///
    /// # Arguments
    /// * `timestamp` - Current time in milliseconds, used to expire old states
    pub fn stabilize_states(&mut self, timestamp: i64) {
        for processor in &self.all_state_processors {
            if let Ok(mut guard) = processor.lock() {
                guard.expire_events(timestamp);
            }
        }

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
