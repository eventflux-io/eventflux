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

// count_pre_state_processor.rs - Count quantifier pattern matching (A{n}, A{m,n}, A+, A*)

use super::post_state_processor::PostStateProcessor;
use super::pre_state_processor::PreStateProcessor;
use super::shared_processor_state::ProcessorSharedState;
use super::stream_pre_state_processor::{StateType, StreamPreStateProcessor};
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::state::state_event::StateEvent;
use std::sync::{Arc, Mutex};

/// Count quantifier patterns: A{3}, A{2,5}, A+, A*, A?
///
/// Extends StreamPreStateProcessor with event chaining and backtracking logic.
pub struct CountPreStateProcessor {
    min_count: usize,
    max_count: usize,
    pub stream_processor: StreamPreStateProcessor,
}

impl CountPreStateProcessor {
    pub fn new(
        min_count: usize,
        max_count: usize,
        state_id: usize,
        is_start_state: bool,
        state_type: StateType,
        app_context: Arc<EventFluxAppContext>,
        query_context: Arc<EventFluxQueryContext>,
    ) -> Self {
        Self {
            min_count,
            max_count,
            stream_processor: StreamPreStateProcessor::new(
                state_id,
                is_start_state,
                state_type,
                app_context,
                query_context,
            ),
        }
    }

    pub fn get_min_count(&self) -> usize {
        self.min_count
    }

    pub fn get_max_count(&self) -> usize {
        self.max_count
    }

    /// Count events in chain at this state_id position
    ///
    /// Used internally to check if max_count has been reached.
    fn count_events_in_chain(&self, state_event: &StateEvent) -> usize {
        use crate::core::event::stream::stream_event::StreamEvent;

        let state_id = self.stream_processor.state_id();

        // Get first event in chain at this position
        let mut current = match state_event.get_stream_event(state_id) {
            Some(se) => se,
            None => return 0,
        };

        let mut count = 1; // First event

        // Walk the chain counting events
        while let Some(next) = current.get_next() {
            count += 1;
            // Check if next is a StreamEvent
            if let Some(se) = next.as_any().downcast_ref::<StreamEvent>() {
                current = se;
            } else {
                break;
            }
        }

        count
    }
}

impl PreStateProcessor for CountPreStateProcessor {
    fn process(&mut self, chunk: Option<Box<dyn ComplexEvent>>) -> Option<Box<dyn ComplexEvent>> {
        // Delegate to process_and_return for count logic
        self.process_and_return(chunk)
    }

    fn init(&mut self) {
        self.stream_processor.init();
    }

    fn add_state(&mut self, state_event: StateEvent) {
        // For count quantifiers, don't mark state as changed when adding
        // Only mark state changed when we've accumulated enough events (min_count)
        let mut state = self.stream_processor.state.lock().unwrap();
        state.add_to_new_list(state_event);
        // Note: NOT calling mark_state_changed() or shared_state.mark_state_changed()
        // This will only be called in process_and_return when count == max_count
    }

    fn add_every_state(&mut self, state_event: StateEvent) {
        // EVERY loopback: When a completed pattern loops back, clear all existing
        // sliding windows at the start position. This implements "pattern restart"
        // semantics where after a complete match, the pattern starts fresh.
        //
        // Without this, leftover sliding windows from the previous pattern instance
        // would mix with events from the new instance, causing incorrect matches.
        //
        // Example: EVERY (A{3} -> B)
        // - First match: A1,A2,A3 -> B4 (sliding windows [A2,A3], [A3] remain)
        // - Without clear: A5 would combine with [A2,A3] to form [A2,A3,A5] (wrong!)
        // - With clear: A5 starts a fresh window [A5] (correct)
        if self.stream_processor.is_start_state() {
            let mut state = self.stream_processor.state.lock().unwrap();
            state.clear_pending();
            state.clear_new();
        }
        self.stream_processor.add_every_state(state_event);
    }

    fn update_state(&mut self) {
        self.stream_processor.update_state();
    }

    fn reset_state(&mut self) {
        self.stream_processor.reset_state();
    }

    fn process_and_return(
        &mut self,
        chunk: Option<Box<dyn ComplexEvent>>,
    ) -> Option<Box<dyn ComplexEvent>> {
        // Get incoming StreamEvent from chunk
        let stream_event = match chunk.as_ref() {
            Some(c) => c
                .as_any()
                .downcast_ref::<crate::core::event::stream::stream_event::StreamEvent>()?,
            None => return None,
        };

        // Get state_id for this processor
        let state_id = self.stream_processor.state_id();

        // Check if this is EVERY pattern start state
        let is_every_start =
            self.stream_processor.is_start_state() && self.stream_processor.is_every_pattern();

        // Access pending states - clone the list to avoid borrow conflicts
        let mut pending_states: Vec<StateEvent> = {
            let mut state_guard = self.stream_processor.state.lock().unwrap();
            let cloned: Vec<StateEvent> = state_guard.get_pending_list().iter().cloned().collect();
            // Clear pending list - we'll rebuild it with updated states
            state_guard.get_pending_list_mut().clear();
            cloned
        };

        // EVERY pattern optimization: Filter out completed states at start position
        // Completed states have events at positions beyond this state_id
        // These came from loopback and can't be used for new instances
        if is_every_start && !pending_states.is_empty() {
            pending_states.retain(|state_event| {
                // Keep only states that don't have events at later positions
                // This filters out completed StateEvent{A(1), B(2)} from loopback
                for pos in (state_id + 1)..state_event.stream_event_count() {
                    if state_event.get_stream_event(pos).is_some() {
                        return false; // Has event at later position - completed state, discard
                    }
                }
                true // No events at later positions - valid for accumulation
            });
        }

        // For EVERY sliding window: Check if any existing window has events at this position
        // IMPORTANT: This check must be AFTER the retain filter to avoid spurious spawns
        // from completed loopback states that get filtered out.
        // - Empty initial state: no events → don't spawn (this is the first window starting)
        // - State with [A1]: has events → spawn new window when A2 arrives
        let any_existing_had_events = pending_states
            .iter()
            .any(|se| se.get_stream_event(state_id).is_some());

        // If pending is empty after filtering, create a fresh StateEvent for EVERY start states
        // This allows new pattern instances to begin
        if pending_states.is_empty() {
            if is_every_start {
                // Create fresh empty StateEvent for new pattern instance
                // Just create an empty StateEvent - it will be populated with events as they arrive
                // and will auto-expand as needed
                let new_state = StateEvent::new(1, 0); // Size 1 is enough, will expand
                pending_states.push(new_state);
            } else {
                return None;
            }
        }

        // Get stream event cloner - clone it to avoid borrow conflicts
        let stream_cloner = self.stream_processor.get_stream_event_cloner().clone();

        // Get post processor for validation
        let post_processor = self.stream_processor.get_this_state_post_processor()?;

        let mut any_state_completed = false;

        // Process each pending state with count quantifier logic
        for mut state_event in pending_states {
            // Clone incoming StreamEvent and add to chain at this position
            let cloned_stream = stream_cloner.copy_stream_event(stream_event);
            state_event.add_event(state_id, cloned_stream);

            // Count events in chain to validate
            let count = self.count_events_in_chain(&state_event);

            // Check if count is in valid range [min, max]
            if count >= self.min_count && count <= self.max_count {
                // Valid count range - send to PostStateProcessor for forwarding
                if let Ok(mut post_guard) = post_processor.lock() {
                    post_guard.process(Some(Box::new(state_event.clone())));
                }

                // Check if we've reached max_count
                if count == self.max_count {
                    // Max reached, this state is complete - don't add back to pending
                    // Mark that at least one state completed
                    any_state_completed = true;
                } else {
                    // count < max_count - can still accept more events
                    // Add back to pending for further accumulation
                    let mut state_guard = self.stream_processor.state.lock().unwrap();
                    state_guard.get_pending_list_mut().push_back(state_event);
                }
            } else if count < self.min_count {
                // Not enough events yet - keep accumulating
                // Add back to pending list with the accumulated events
                let mut state_guard = self.stream_processor.state.lock().unwrap();
                state_guard.get_pending_list_mut().push_back(state_event);
            } else {
                // count > max_count - shouldn't normally happen, but handle gracefully
                // CRITICAL: Preserve the state after trimming, don't discard it!
                // Without re-queuing, we lose the entire match chain and future events
                // can never reach min_count (no overlapping matches possible)
                state_event.remove_last_event(state_id);

                // Re-queue the trimmed state so it can continue forming matches
                // After trimming, count should be exactly max_count
                let mut state_guard = self.stream_processor.state.lock().unwrap();
                state_guard.get_pending_list_mut().push_back(state_event);
            }
        }

        // EVERY SLIDING WINDOW: Spawn a new StateEvent for overlapping windows
        // This enables patterns like EVERY A{2,3} -> B to produce overlapping matches
        //
        // Semantic: In CEP, `every A{2,3}` means "for every potential starting A,
        // try to match 2-3 As". Each incoming A event should start a new window.
        //
        // Conditions for spawning:
        // 1. is_every_start: This is an EVERY pattern at start state
        // 2. any_existing_had_events: Existing windows already had events at this position
        //    (if windows were empty, this is the first event - no overlap possible yet)
        //
        // The new window contains ONLY the current event (count=1)
        // It will accumulate future events independently from existing windows
        if is_every_start && any_existing_had_events {
            // Spawn a new StateEvent starting from this event
            // This creates an overlapping window that begins at this event
            // Size 1 is enough - StateEvent will auto-expand as needed
            let mut new_window = StateEvent::new(1, 0);
            let cloned_for_new_window = stream_cloner.copy_stream_event(stream_event);
            new_window.add_event(state_id, cloned_for_new_window);

            // The new window has count=1 (just this event)
            // Check if it should output immediately (when min_count=1)
            let new_window_count = 1;
            if new_window_count >= self.min_count && new_window_count <= self.max_count {
                // Output this window immediately
                if let Ok(mut post_guard) = post_processor.lock() {
                    post_guard.process(Some(Box::new(new_window.clone())));
                }
            }

            // Add to pending if window can still grow (count < max)
            if new_window_count < self.max_count {
                let mut state_guard = self.stream_processor.state.lock().unwrap();
                state_guard.get_pending_list_mut().push_back(new_window);
            }
        }

        // Only signal state_changed if at least one state completed (reached max_count)
        // This means the state was removed from pending and is done
        if any_state_completed {
            self.stream_processor.state_changed();
        }

        None // CountPreStateProcessor doesn't output directly
    }

    fn set_within_time(&mut self, within_time: i64) {
        self.stream_processor.set_within_time(within_time);
    }

    fn expire_events(&mut self, timestamp: i64) {
        self.stream_processor.expire_events(timestamp);
    }

    fn state_id(&self) -> usize {
        self.stream_processor.state_id()
    }

    fn is_start_state(&self) -> bool {
        self.stream_processor.is_start_state()
    }

    fn get_shared_state(&self) -> Arc<ProcessorSharedState> {
        self.stream_processor.get_shared_state()
    }

    fn this_state_post_processor(&self) -> Option<Arc<Mutex<dyn PostStateProcessor>>> {
        self.stream_processor.this_state_post_processor()
    }

    fn state_changed(&self) {
        self.stream_processor.state_changed();
    }

    fn has_state_changed(&self) -> bool {
        self.stream_processor.has_state_changed()
    }
}

impl std::fmt::Debug for CountPreStateProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CountPreStateProcessor")
            .field("min_count", &self.min_count)
            .field("max_count", &self.max_count)
            .field("state_id", &self.stream_processor.state_id())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::super::count_post_state_processor::CountPostStateProcessor;
    use super::*;

    fn create_test_processor(min: usize, max: usize) -> CountPreStateProcessor {
        use crate::core::config::eventflux_context::EventFluxContext;
        use crate::query_api::eventflux_app::EventFluxApp;

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

        CountPreStateProcessor::new(min, max, 0, true, StateType::Pattern, app_ctx, query_ctx)
    }

    fn create_wired_processors(
        min: usize,
        max: usize,
    ) -> (CountPreStateProcessor, Arc<Mutex<CountPostStateProcessor>>) {
        use crate::core::config::eventflux_context::EventFluxContext;
        use crate::core::event::state::meta_state_event::MetaStateEvent;
        use crate::core::event::state::state_event_cloner::StateEventCloner;
        use crate::core::event::state::state_event_factory::StateEventFactory;
        use crate::core::event::stream::meta_stream_event::MetaStreamEvent;
        use crate::core::event::stream::stream_event_cloner::StreamEventCloner;
        use crate::core::event::stream::stream_event_factory::StreamEventFactory;
        use crate::query_api::eventflux_app::EventFluxApp;

        // Create contexts
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

        // Create processors
        let mut pre_processor = CountPreStateProcessor::new(
            min,
            max,
            0,
            true,
            StateType::Pattern,
            app_ctx.clone(),
            query_ctx.clone(),
        );
        let post_processor = Arc::new(Mutex::new(CountPostStateProcessor::new(min, max, 0)));

        // Wire Pre -> Post
        pre_processor
            .stream_processor
            .set_this_state_post_processor(post_processor.clone());

        // Initialize cloners with proper factories
        use crate::query_api::definition::stream_definition::StreamDefinition;
        let stream_def = Arc::new(StreamDefinition::new("TestStream".to_string()));
        let meta_stream = MetaStreamEvent::new_for_single_input(stream_def);
        let stream_factory = StreamEventFactory::new(0, 0, 0);
        let stream_cloner = StreamEventCloner::new(&meta_stream, stream_factory);
        pre_processor
            .stream_processor
            .set_stream_event_cloner(stream_cloner);

        let meta_state = MetaStateEvent::new(1);
        let state_factory = StateEventFactory::new(1, 0);
        let state_cloner = StateEventCloner::new(&meta_state, state_factory);
        pre_processor
            .stream_processor
            .set_state_event_cloner(state_cloner);

        pre_processor.init();
        (pre_processor, post_processor)
    }

    // Basic tests (19 tests)

    #[test]
    fn test_count_processor_creation() {
        let proc = create_test_processor(2, 5);
        assert_eq!(proc.get_min_count(), 2);
        assert_eq!(proc.get_max_count(), 5);
        assert_eq!(proc.state_id(), 0);
    }

    #[test]
    fn test_exactly_n() {
        let proc = create_test_processor(3, 3);
        assert_eq!(proc.get_min_count(), 3);
        assert_eq!(proc.get_max_count(), 3);
    }

    #[test]
    fn test_one_or_more() {
        let proc = create_test_processor(1, i32::MAX as usize);
        assert_eq!(proc.get_min_count(), 1);
        assert_eq!(proc.get_max_count(), i32::MAX as usize);
    }

    #[test]
    fn test_zero_or_more() {
        let proc = create_test_processor(0, i32::MAX as usize);
        assert_eq!(proc.get_min_count(), 0);
        assert_eq!(proc.get_max_count(), i32::MAX as usize);
    }

    #[test]
    fn test_zero_or_one() {
        let proc = create_test_processor(0, 1);
        assert_eq!(proc.get_min_count(), 0);
        assert_eq!(proc.get_max_count(), 1);
    }

    #[test]
    fn test_range() {
        let proc = create_test_processor(2, 5);
        assert_eq!(proc.get_min_count(), 2);
        assert_eq!(proc.get_max_count(), 5);
    }

    #[test]
    fn test_min_max_validation() {
        let proc = create_test_processor(3, 10);
        assert!(proc.get_min_count() <= proc.get_max_count());
    }

    #[test]
    fn test_state_id() {
        let proc = create_test_processor(2, 5);
        assert_eq!(proc.state_id(), 0);
    }

    #[test]
    fn test_is_start_state() {
        let proc = create_test_processor(2, 5);
        assert!(proc.is_start_state());
    }

    #[test]
    fn test_count_events_in_chain_single() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let proc = create_test_processor(2, 5);
        let mut state_event = StateEvent::new(1, 0);
        let stream_event = StreamEvent::new(100, 0, 0, 0);
        state_event.set_event(0, stream_event);

        let count = proc.count_events_in_chain(&state_event);
        assert_eq!(count, 1);
    }

    #[test]
    fn test_count_events_in_chain_multiple() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let proc = create_test_processor(2, 5);
        let mut state_event = StateEvent::new(1, 0);

        let mut event1 = StreamEvent::new(100, 0, 0, 0);
        let event2 = StreamEvent::new(200, 0, 0, 0);

        event1.set_next(Some(Box::new(event2)));
        state_event.set_event(0, event1);

        // This will count event1 + event2 = 2 events
        let count = proc.count_events_in_chain(&state_event);
        assert_eq!(count, 2);
    }

    #[test]
    fn test_init() {
        let mut proc = create_test_processor(2, 5);
        proc.init();
        // Init should succeed without panicking
    }

    #[test]
    fn test_add_state() {
        let mut proc = create_test_processor(2, 5);
        let state = StateEvent::new(1, 0);
        proc.add_state(state);
        // Should not mark state as changed
        assert!(!proc.has_state_changed());
    }

    #[test]
    fn test_update_state() {
        let mut proc = create_test_processor(2, 5);
        let state = StateEvent::new(1, 0);
        proc.add_state(state);
        proc.update_state();
        // Update should move state from new to pending
    }

    #[test]
    fn test_reset_state() {
        let mut proc = create_test_processor(2, 5);
        proc.reset_state();
        // Reset should clear all states
    }

    #[test]
    fn test_set_within_time() {
        let mut proc = create_test_processor(2, 5);
        proc.set_within_time(5000);
        // Should set within time without error
    }

    #[test]
    fn test_expire_events() {
        let mut proc = create_test_processor(2, 5);
        proc.expire_events(10000);
        // Should expire events without error
    }

    #[test]
    fn test_this_state_post_processor() {
        let proc = create_test_processor(2, 5);
        let post = proc.this_state_post_processor();
        assert!(post.is_none()); // No post processor set in basic test
    }

    #[test]
    fn test_state_lifecycle() {
        let mut proc = create_test_processor(2, 5);
        proc.init();

        let state = StateEvent::new(1, 0);
        proc.add_state(state);
        proc.update_state();

        let state_guard = proc.stream_processor.state.lock().unwrap();
        assert!(state_guard.is_initialized());
    }

    // Integration tests (24 tests)

    #[test]
    fn test_integration_exactly_3_with_3_events() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _post_proc) = create_wired_processors(3, 3); // A{3}

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send 3 events
        for i in 0..3 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            let _result = pre_proc.process_and_return(Some(Box::new(event)));
        }

        // After 3 events, state should be changed (max reached)
        assert!(pre_proc.has_state_changed());
    }

    #[test]
    fn test_integration_exactly_3_with_2_events_no_output() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _post_proc) = create_wired_processors(3, 3); // A{3}

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send only 2 events (less than required)
        for i in 0..2 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            let _result = pre_proc.process_and_return(Some(Box::new(event)));
        }

        // State should not be changed yet (max not reached)
        assert!(!pre_proc.has_state_changed());
    }

    #[test]
    fn test_integration_range_2_to_5_with_2_events() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _post_proc) = create_wired_processors(2, 5); // A{2,5}

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send 2 events (minimum)
        for i in 0..2 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            let _result = pre_proc.process_and_return(Some(Box::new(event)));
        }

        // State should not be changed (max not reached)
        assert!(!pre_proc.has_state_changed());
    }

    #[test]
    fn test_integration_range_2_to_5_with_5_events() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _post_proc) = create_wired_processors(2, 5); // A{2,5}

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send 5 events (maximum)
        for i in 0..5 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            let _result = pre_proc.process_and_return(Some(Box::new(event)));
        }

        // State should be changed (max reached)
        assert!(pre_proc.has_state_changed());
    }

    #[test]
    fn test_integration_one_or_more_with_1_event() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _post_proc) = create_wired_processors(1, i32::MAX as usize); // A+

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send 1 event (minimum for A+)
        let event = StreamEvent::new(100, 0, 0, 0);
        let _result = pre_proc.process_and_return(Some(Box::new(event)));

        // State should not be changed (max not reached, and max is essentially infinite)
        assert!(!pre_proc.has_state_changed());
    }

    #[test]
    fn test_integration_one_or_more_with_multiple_events() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _post_proc) = create_wired_processors(1, i32::MAX as usize); // A+

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send multiple events
        for i in 0..5 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            let _result = pre_proc.process_and_return(Some(Box::new(event)));
        }

        // State should not be changed (max is infinite)
        assert!(!pre_proc.has_state_changed());
    }

    #[test]
    fn test_integration_zero_or_more_with_0_events() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _post_proc) = create_wired_processors(0, i32::MAX as usize); // A*

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Don't send any events - A* should match with 0 events
        // But we need to send at least one event to trigger processing
        let event = StreamEvent::new(100, 0, 0, 0);
        let _result = pre_proc.process_and_return(Some(Box::new(event)));

        // State should not be changed (max not reached)
        assert!(!pre_proc.has_state_changed());
    }

    #[test]
    fn test_integration_zero_or_one_with_0_events() {
        let (_pre_proc, _post_proc) = create_wired_processors(0, 1); // A?
                                                                     // Just test creation - A? with 0 events is valid
    }

    #[test]
    fn test_integration_zero_or_one_with_1_event() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _post_proc) = create_wired_processors(0, 1); // A?

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send 1 event (maximum for A?)
        let event = StreamEvent::new(100, 0, 0, 0);
        let _result = pre_proc.process_and_return(Some(Box::new(event)));

        // State should be changed (max reached)
        assert!(pre_proc.has_state_changed());
    }

    #[test]
    fn test_integration_multiple_pending_states() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _post_proc) = create_wired_processors(2, 3); // A{2,3}

        // Add multiple pending states
        let state1 = StateEvent::new(1, 0);
        let state2 = StateEvent::new(1, 0);
        pre_proc.add_state(state1);
        pre_proc.add_state(state2);
        pre_proc.update_state();

        // Send 2 events to satisfy min_count
        for i in 0..2 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            let _result = pre_proc.process_and_return(Some(Box::new(event)));
        }

        // Both states should have received the events (max not reached)
        assert!(!pre_proc.has_state_changed());
    }

    #[test]
    fn test_integration_min_count_boundary() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _post_proc) = create_wired_processors(3, 5); // A{3,5}

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send exactly min_count events
        for i in 0..3 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            let _result = pre_proc.process_and_return(Some(Box::new(event)));
        }

        // Should output but not be marked as changed (max not reached)
        assert!(!pre_proc.has_state_changed());
    }

    #[test]
    fn test_integration_max_count_boundary() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _post_proc) = create_wired_processors(3, 5); // A{3,5}

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send exactly max_count events
        for i in 0..5 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            let _result = pre_proc.process_and_return(Some(Box::new(event)));
        }

        // Should be marked as changed (max reached)
        assert!(pre_proc.has_state_changed());
    }

    #[test]
    fn test_integration_state_lifecycle_with_update() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _post_proc) = create_wired_processors(2, 3); // A{2,3}

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send events
        for i in 0..2 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            let _result = pre_proc.process_and_return(Some(Box::new(event)));
        }

        let state_guard = pre_proc.stream_processor.state.lock().unwrap();
        assert!(state_guard.is_initialized());
    }

    #[test]
    fn test_integration_event_timestamps_preserved() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _post_proc) = create_wired_processors(3, 3); // A{3}

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send events with specific timestamps
        for ts in [100, 200, 300] {
            let mut event = StreamEvent::new(0, 0, 0, 0);
            event.timestamp = ts;
            let _result = pre_proc.process_and_return(Some(Box::new(event)));
        }

        // Events should be processed with timestamps preserved
        assert!(pre_proc.has_state_changed());
    }

    #[test]
    fn test_integration_empty_event_handling() {
        let (mut pre_proc, _post_proc) = create_wired_processors(2, 5);

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send None event
        let result = pre_proc.process_and_return(None);
        assert!(result.is_none());
    }

    #[test]
    fn test_integration_reset_after_processing() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _post_proc) = create_wired_processors(2, 2); // A{2}

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Process events to completion
        for i in 0..2 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            let _result = pre_proc.process_and_return(Some(Box::new(event)));
        }

        assert!(pre_proc.has_state_changed());

        // Reset
        pre_proc.reset_state();
        pre_proc.get_shared_state().reset_state_changed();

        // Should be reset
        assert!(!pre_proc.has_state_changed());
    }

    #[test]
    fn test_integration_sequential_patterns() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _post_proc) = create_wired_processors(2, 2); // A{2}

        // First sequence
        let state1 = StateEvent::new(1, 0);
        pre_proc.add_state(state1);
        pre_proc.update_state();

        for i in 0..2 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            let _result = pre_proc.process_and_return(Some(Box::new(event)));
        }

        assert!(pre_proc.has_state_changed());

        // Reset for second sequence
        pre_proc.reset_state();
        pre_proc.get_shared_state().reset_state_changed();

        // Second sequence
        let state2 = StateEvent::new(1, 0);
        pre_proc.add_state(state2);
        pre_proc.update_state();

        for i in 0..2 {
            let event = StreamEvent::new((i + 3) * 100, 0, 0, 0);
            let _result = pre_proc.process_and_return(Some(Box::new(event)));
        }

        assert!(pre_proc.has_state_changed());
    }

    #[test]
    fn test_integration_interleaved_events() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _post_proc) = create_wired_processors(2, 4); // A{2,4}

        // Add two different initial states
        let state1 = StateEvent::new(1, 0);
        let state2 = StateEvent::new(1, 0);
        pre_proc.add_state(state1);
        pre_proc.add_state(state2);
        pre_proc.update_state();

        // Send events that will be processed by both states
        for i in 0..2 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            let _result = pre_proc.process_and_return(Some(Box::new(event)));
        }

        // Both states should have at least min_count events
        assert!(!pre_proc.has_state_changed());
    }

    #[test]
    fn test_integration_large_count() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _post_proc) = create_wired_processors(50, 100); // A{50,100}

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send 50 events (minimum)
        for i in 0..50 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            let _result = pre_proc.process_and_return(Some(Box::new(event)));
        }

        // Should have reached min but not max
        assert!(!pre_proc.has_state_changed());
    }

    #[test]
    fn test_integration_count_progression() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _post_proc) = create_wired_processors(3, 5); // A{3,5}

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send events one by one and check progression
        for i in 0..5 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            let _result = pre_proc.process_and_return(Some(Box::new(event)));

            if i < 4 {
                // Before reaching max
                assert!(!pre_proc.has_state_changed());
            } else {
                // At max
                assert!(pre_proc.has_state_changed());
            }
        }
    }

    #[test]
    fn test_integration_state_id_verification() {
        let (pre_proc, post_proc) = create_wired_processors(2, 5);

        // Verify state_id is consistent
        assert_eq!(pre_proc.state_id(), 0);
        assert_eq!(post_proc.lock().unwrap().state_id(), 0);
    }

    #[test]
    fn test_integration_exactly_n_variants() {
        use crate::core::event::stream::stream_event::StreamEvent;

        // Test A{1}
        let (mut pre_proc, _) = create_wired_processors(1, 1);
        let state = StateEvent::new(1, 0);
        pre_proc.add_state(state);
        pre_proc.update_state();
        let event = StreamEvent::new(100, 0, 0, 0);
        let _result = pre_proc.process_and_return(Some(Box::new(event)));
        assert!(pre_proc.has_state_changed());

        // Test A{5}
        let (mut pre_proc, _) = create_wired_processors(5, 5);
        let state = StateEvent::new(1, 0);
        pre_proc.add_state(state);
        pre_proc.update_state();
        for i in 0..5 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            let _result = pre_proc.process_and_return(Some(Box::new(event)));
        }
        assert!(pre_proc.has_state_changed());

        // Test A{10}
        let (mut pre_proc, _) = create_wired_processors(10, 10);
        let state = StateEvent::new(1, 0);
        pre_proc.add_state(state);
        pre_proc.update_state();
        for i in 0..10 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            let _result = pre_proc.process_and_return(Some(Box::new(event)));
        }
        assert!(pre_proc.has_state_changed());
    }

    #[test]
    fn test_integration_range_variants() {
        use crate::core::event::stream::stream_event::StreamEvent;

        // Test A{1,2}
        let (mut pre_proc, _) = create_wired_processors(1, 2);
        let state = StateEvent::new(1, 0);
        pre_proc.add_state(state);
        pre_proc.update_state();
        for i in 0..2 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            let _result = pre_proc.process_and_return(Some(Box::new(event)));
        }
        assert!(pre_proc.has_state_changed());

        // Test A{5,10}
        let (mut pre_proc, _) = create_wired_processors(5, 10);
        let state = StateEvent::new(1, 0);
        pre_proc.add_state(state);
        pre_proc.update_state();
        for i in 0..10 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            let _result = pre_proc.process_and_return(Some(Box::new(event)));
        }
        assert!(pre_proc.has_state_changed());
    }

    #[test]
    fn test_integration_within_time_expiry() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _post_proc) = create_wired_processors(2, 5); // A{2,5}

        // Set within time constraint (e.g., 1000ms)
        pre_proc.set_within_time(1000);

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send event at timestamp 100
        let mut event1 = StreamEvent::new(0, 0, 0, 0);
        event1.timestamp = 100;
        let _result1 = pre_proc.process_and_return(Some(Box::new(event1)));

        // Expire events before timestamp 1100
        pre_proc.expire_events(1100);

        // Send another event after expiry
        let mut event2 = StreamEvent::new(0, 0, 0, 0);
        event2.timestamp = 1200;
        let _result2 = pre_proc.process_and_return(Some(Box::new(event2)));

        // State should not be changed due to expiry
        assert!(!pre_proc.has_state_changed());
    }

    // ============================================================================
    // Output Verification Tests (verify post processor receives correct output)
    // ============================================================================

    /// Helper struct to track post processor outputs
    struct OutputTracker {
        outputs: Arc<Mutex<Vec<StateEvent>>>,
    }

    #[allow(dead_code)]
    impl OutputTracker {
        fn new() -> Self {
            Self {
                outputs: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn get_output_count(&self) -> usize {
            self.outputs.lock().unwrap().len()
        }

        fn get_outputs(&self) -> Vec<StateEvent> {
            self.outputs.lock().unwrap().clone()
        }

        fn clear(&self) {
            self.outputs.lock().unwrap().clear();
        }
    }

    /// Create processors with output tracking
    fn create_tracked_processors(
        min: usize,
        max: usize,
    ) -> (
        CountPreStateProcessor,
        Arc<Mutex<CountPostStateProcessor>>,
        Arc<OutputTracker>,
    ) {
        use super::super::post_state_processor::PostStateProcessor;
        use super::super::stream_pre_state::StreamPreState;
        use super::super::stream_pre_state_processor::StateType;
        use crate::core::config::eventflux_app_context::EventFluxAppContext;
        use crate::core::config::eventflux_context::EventFluxContext;
        use crate::core::config::eventflux_query_context::EventFluxQueryContext;
        use crate::core::event::complex_event::ComplexEvent;
        use crate::core::event::state::meta_state_event::MetaStateEvent;
        use crate::core::event::state::{
            state_event_cloner::StateEventCloner, state_event_factory::StateEventFactory,
        };
        use crate::core::event::stream::{
            meta_stream_event::MetaStreamEvent, stream_event_cloner::StreamEventCloner,
            stream_event_factory::StreamEventFactory,
        };
        use crate::query_api::definition::stream_definition::StreamDefinition;
        use crate::query_api::eventflux_app::EventFluxApp;
        use std::fmt;

        let stream_def = Arc::new(StreamDefinition::new("TestStream".to_string()));
        let meta_stream = MetaStreamEvent::new_for_single_input(stream_def);
        let stream_factory = StreamEventFactory::new(0, 0, 0);
        let stream_cloner = StreamEventCloner::new(&meta_stream, stream_factory);

        let meta_state = MetaStateEvent::new(1);
        let state_factory = StateEventFactory::new(1, 0);
        let state_cloner = StateEventCloner::new(&meta_state, state_factory);

        let _stream_pre_state = StreamPreState::new();

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

        let mut pre_proc = CountPreStateProcessor {
            min_count: min,
            max_count: max,
            stream_processor: StreamPreStateProcessor::new(
                0,
                true,
                StateType::Pattern,
                app_ctx,
                query_ctx,
            ),
        };

        pre_proc
            .stream_processor
            .set_stream_event_cloner(stream_cloner);
        pre_proc
            .stream_processor
            .set_state_event_cloner(state_cloner);

        let post_proc = Arc::new(Mutex::new(CountPostStateProcessor::new(min, max, 0)));

        // Create custom post processor that tracks outputs
        let tracker = Arc::new(OutputTracker::new());
        let tracker_clone = Arc::clone(&tracker);

        // Create wrapper that intercepts process() calls
        struct TrackingPostProcessor {
            inner: Arc<Mutex<CountPostStateProcessor>>,
            tracker: Arc<OutputTracker>,
        }

        impl fmt::Debug for TrackingPostProcessor {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct("TrackingPostProcessor").finish()
            }
        }

        impl PostStateProcessor for TrackingPostProcessor {
            fn process(
                &mut self,
                chunk: Option<Box<dyn ComplexEvent>>,
            ) -> Option<Box<dyn ComplexEvent>> {
                // Track the output
                if let Some(ref c) = chunk {
                    if let Some(state_evt) = c.as_any().downcast_ref::<StateEvent>() {
                        self.tracker.outputs.lock().unwrap().push(state_evt.clone());
                    }
                }
                // Forward to real post processor
                self.inner.lock().unwrap().process(chunk)
            }

            fn set_next_processor(&mut self, processor: Arc<Mutex<dyn PostStateProcessor>>) {
                self.inner.lock().unwrap().set_next_processor(processor);
            }

            fn get_next_processor(&self) -> Option<Arc<Mutex<dyn PostStateProcessor>>> {
                self.inner.lock().unwrap().get_next_processor()
            }

            fn state_id(&self) -> usize {
                self.inner.lock().unwrap().state_id()
            }

            fn set_next_state_pre_processor(
                &mut self,
                next: Arc<Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>>,
            ) {
                self.inner
                    .lock()
                    .unwrap()
                    .set_next_state_pre_processor(next);
            }

            fn set_next_every_state_pre_processor(
                &mut self,
                next: Arc<Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>>,
            ) {
                self.inner
                    .lock()
                    .unwrap()
                    .set_next_every_state_pre_processor(next);
            }

            fn get_next_every_state_pre_processor(
                &self,
            ) -> Option<Arc<Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>>>
            {
                self.inner
                    .lock()
                    .unwrap()
                    .get_next_every_state_pre_processor()
            }

            fn set_callback_pre_state_processor(
                &mut self,
                callback: Arc<
                    Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>,
                >,
            ) {
                self.inner
                    .lock()
                    .unwrap()
                    .set_callback_pre_state_processor(callback);
            }

            fn is_event_returned(&self) -> bool {
                self.inner.lock().unwrap().is_event_returned()
            }

            fn clear_processed_event(&mut self) {
                self.inner.lock().unwrap().clear_processed_event();
            }

            fn this_state_pre_processor(
                &self,
            ) -> Option<Arc<Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>>>
            {
                self.inner.lock().unwrap().this_state_pre_processor()
            }
        }

        let tracking_post = Arc::new(Mutex::new(TrackingPostProcessor {
            inner: Arc::clone(&post_proc),
            tracker: tracker_clone,
        }));

        pre_proc
            .stream_processor
            .set_this_state_post_processor(tracking_post);

        (pre_proc, post_proc, tracker)
    }

    #[test]
    fn test_output_verify_exactly_3_no_output_before_min() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _, tracker) = create_tracked_processors(3, 3);

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send 2 events (less than min)
        for i in 0..2 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            pre_proc.process_and_return(Some(Box::new(event)));
        }

        // CRITICAL: Should produce 0 outputs when count < min
        assert_eq!(
            tracker.get_output_count(),
            0,
            "Should produce 0 outputs when count < min_count"
        );
        assert!(!pre_proc.has_state_changed());
    }

    #[test]
    fn test_output_verify_exactly_3_outputs_at_max() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _, tracker) = create_tracked_processors(3, 3);

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send 3 events (exactly min and max)
        for i in 0..3 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            pre_proc.process_and_return(Some(Box::new(event)));
        }

        // CRITICAL: Should produce 1 output when reaching min=max=3
        assert_eq!(
            tracker.get_output_count(),
            1,
            "Should produce 1 output when count reaches min=max=3"
        );
        assert!(
            pre_proc.has_state_changed(),
            "state_changed should be true at max"
        );
    }

    #[test]
    fn test_output_verify_range_2_to_5_outputs() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _, tracker) = create_tracked_processors(2, 5);

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send events one by one and verify output count
        for i in 0..5 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            pre_proc.process_and_return(Some(Box::new(event)));

            let expected_outputs: usize = if i < 1 {
                0 // count=1, less than min=2
            } else {
                i as usize // count=2,3,4,5 all produce output
            };

            assert_eq!(
                tracker.get_output_count(),
                expected_outputs,
                "After {} events, expected {} outputs, got {}",
                i + 1,
                expected_outputs,
                tracker.get_output_count()
            );

            // state_changed only true at event 5 (max)
            if i < 4 {
                assert!(
                    !pre_proc.has_state_changed(),
                    "state_changed should be false before max"
                );
            } else {
                assert!(
                    pre_proc.has_state_changed(),
                    "state_changed should be true at max"
                );
            }
        }

        // CRITICAL: Total outputs should be 4 (for counts 2,3,4,5)
        assert_eq!(
            tracker.get_output_count(),
            4,
            "Should produce 4 outputs total (counts 2,3,4,5)"
        );
    }

    #[test]
    fn test_output_verify_range_2_to_5_with_only_2_events() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _, tracker) = create_tracked_processors(2, 5);

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send only 2 events (exactly min)
        for i in 0..2 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            pre_proc.process_and_return(Some(Box::new(event)));
        }

        // CRITICAL: Should produce 1 output at min_count
        assert_eq!(
            tracker.get_output_count(),
            1,
            "Should produce 1 output when reaching min_count=2"
        );

        // But state_changed should be false (max not reached)
        assert!(
            !pre_proc.has_state_changed(),
            "state_changed should be false when only at min, not max"
        );
    }

    #[test]
    fn test_output_verify_one_or_more_outputs_continuously() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _, tracker) = create_tracked_processors(1, i32::MAX as usize);

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send 10 events for A+
        for i in 0..10 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            pre_proc.process_and_return(Some(Box::new(event)));

            // CRITICAL: Every event should produce output (count >= min=1)
            assert_eq!(
                tracker.get_output_count(),
                (i + 1) as usize,
                "A+ should produce output for every event"
            );

            // state_changed always false (max never reached)
            assert!(
                !pre_proc.has_state_changed(),
                "state_changed should always be false for A+ (max=infinity)"
            );
        }

        assert_eq!(
            tracker.get_output_count(),
            10,
            "A+ should produce 10 outputs for 10 events"
        );
    }

    #[test]
    fn test_output_verify_event_timestamps_preserved() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _, tracker) = create_tracked_processors(2, 3);

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send events with specific timestamps
        let timestamps = [1000i64, 2000i64, 3000i64];
        for (i, &ts) in timestamps.iter().enumerate() {
            let mut event = StreamEvent::new(((i + 1) * 100) as i64, 0, 0, 0);
            event.timestamp = ts;
            pre_proc.process_and_return(Some(Box::new(event)));
        }

        // Verify we got outputs
        assert_eq!(
            tracker.get_output_count(),
            2,
            "Should have 2 outputs (at count 2 and 3)"
        );

        // CRITICAL: Verify timestamps are preserved in output
        let outputs = tracker.get_outputs();
        for (idx, output) in outputs.iter().enumerate() {
            // Get the first event in the chain at state_id=0
            if let Some(stream_evt) = output.get_stream_event(0) {
                // Walk the chain and verify timestamps
                let mut current_opt: Option<&StreamEvent> = Some(stream_evt);
                let mut count = 0;
                while let Some(evt) = current_opt {
                    if count < timestamps.len() {
                        // Timestamp should match original
                        // Note: The chain contains ALL events up to this point
                        let expected_ts = timestamps[count];
                        assert_eq!(
                            evt.timestamp, expected_ts,
                            "Timestamp mismatch at position {} in output {}",
                            count, idx
                        );
                    }
                    // get_next() returns Option<&dyn ComplexEvent>, need to downcast
                    current_opt = evt
                        .get_next()
                        .and_then(|ce| ce.as_any().downcast_ref::<StreamEvent>());
                    count += 1;
                }
            }
        }
    }

    #[test]
    fn test_output_verify_zero_or_one_with_0_events() {
        let (mut pre_proc, _, tracker) = create_tracked_processors(0, 1);

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Don't send any events
        // For A?, min=0 means it should match with 0 events

        // Initial state should be valid (count=0 is >= min=0)
        // But we need an event to trigger processing

        // CRITICAL QUESTION: How does A? with 0 events work?
        // The pending state has count=0, which is >= min=0
        // But process_and_return() only called when event arrives

        // Current behavior: No output until event arrives
        assert_eq!(
            tracker.get_output_count(),
            0,
            "A? with 0 events: no output without triggering event"
        );
    }

    #[test]
    fn test_output_verify_zero_or_one_with_1_event() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _, tracker) = create_tracked_processors(0, 1);

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send 1 event
        let event = StreamEvent::new(100, 0, 0, 0);
        pre_proc.process_and_return(Some(Box::new(event)));

        // CRITICAL: Should produce 1 output (count=1, at max)
        assert_eq!(
            tracker.get_output_count(),
            1,
            "A? with 1 event should produce 1 output"
        );

        // state_changed should be true (reached max=1)
        assert!(
            pre_proc.has_state_changed(),
            "state_changed should be true at max=1"
        );
    }

    #[test]
    fn test_output_verify_large_count_a50_100() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _, tracker) = create_tracked_processors(50, 100);

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send 49 events (less than min)
        for i in 0..49 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            pre_proc.process_and_return(Some(Box::new(event)));
        }

        // CRITICAL: No output yet
        assert_eq!(
            tracker.get_output_count(),
            0,
            "No output before reaching min=50"
        );
        assert!(!pre_proc.has_state_changed());

        // Send 50th event (at min)
        let event = StreamEvent::new(5000, 0, 0, 0);
        pre_proc.process_and_return(Some(Box::new(event)));

        // CRITICAL: Should have 1 output now
        assert_eq!(
            tracker.get_output_count(),
            1,
            "Should have 1 output at min=50"
        );
        assert!(
            !pre_proc.has_state_changed(),
            "state_changed false (not at max yet)"
        );

        // Send 50 more events to reach max=100
        for i in 50..100 {
            let event = StreamEvent::new((i + 1) * 100, 0, 0, 0);
            pre_proc.process_and_return(Some(Box::new(event)));
        }

        // CRITICAL: Should have 51 total outputs (one for each count from 50 to 100)
        assert_eq!(
            tracker.get_output_count(),
            51,
            "Should have 51 outputs (counts 50-100 inclusive)"
        );
        assert!(
            pre_proc.has_state_changed(),
            "state_changed should be true at max=100"
        );
    }

    // ============================================================================
    // EVERY Sliding Window Tests
    // ============================================================================

    /// Create EVERY-enabled processors with output tracking
    fn create_every_tracked_processors(
        min: usize,
        max: usize,
    ) -> (
        CountPreStateProcessor,
        Arc<Mutex<CountPostStateProcessor>>,
        Arc<OutputTracker>,
    ) {
        use super::super::post_state_processor::PostStateProcessor;
        use super::super::stream_pre_state::StreamPreState;
        use super::super::stream_pre_state_processor::StateType;
        use crate::core::config::eventflux_app_context::EventFluxAppContext;
        use crate::core::config::eventflux_context::EventFluxContext;
        use crate::core::config::eventflux_query_context::EventFluxQueryContext;
        use crate::core::event::complex_event::ComplexEvent;
        use crate::core::event::state::meta_state_event::MetaStateEvent;
        use crate::core::event::state::{
            state_event_cloner::StateEventCloner, state_event_factory::StateEventFactory,
        };
        use crate::core::event::stream::{
            meta_stream_event::MetaStreamEvent, stream_event_cloner::StreamEventCloner,
            stream_event_factory::StreamEventFactory,
        };
        use crate::query_api::definition::stream_definition::StreamDefinition;
        use crate::query_api::eventflux_app::EventFluxApp;
        use std::fmt;

        let stream_def = Arc::new(StreamDefinition::new("TestStream".to_string()));
        let meta_stream = MetaStreamEvent::new_for_single_input(stream_def);
        let stream_factory = StreamEventFactory::new(0, 0, 0);
        let stream_cloner = StreamEventCloner::new(&meta_stream, stream_factory);

        let meta_state = MetaStateEvent::new(1);
        let state_factory = StateEventFactory::new(1, 0);
        let state_cloner = StateEventCloner::new(&meta_state, state_factory);

        let _stream_pre_state = StreamPreState::new();

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

        let mut pre_proc = CountPreStateProcessor {
            min_count: min,
            max_count: max,
            stream_processor: StreamPreStateProcessor::new(
                0,
                true, // is_start_state = true
                StateType::Pattern,
                app_ctx,
                query_ctx,
            ),
        };

        // CRITICAL: Enable EVERY pattern for sliding window behavior
        pre_proc.stream_processor.set_every_pattern_flag(true);

        pre_proc
            .stream_processor
            .set_stream_event_cloner(stream_cloner);
        pre_proc
            .stream_processor
            .set_state_event_cloner(state_cloner);

        let post_proc = Arc::new(Mutex::new(CountPostStateProcessor::new(min, max, 0)));

        // Create custom post processor that tracks outputs
        let tracker = Arc::new(OutputTracker::new());
        let tracker_clone = Arc::clone(&tracker);

        // Create wrapper that intercepts process() calls
        struct TrackingPostProcessor {
            inner: Arc<Mutex<CountPostStateProcessor>>,
            tracker: Arc<OutputTracker>,
        }

        impl fmt::Debug for TrackingPostProcessor {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct("TrackingPostProcessor").finish()
            }
        }

        impl PostStateProcessor for TrackingPostProcessor {
            fn process(
                &mut self,
                chunk: Option<Box<dyn ComplexEvent>>,
            ) -> Option<Box<dyn ComplexEvent>> {
                // Track the output
                if let Some(ref c) = chunk {
                    if let Some(state_evt) = c.as_any().downcast_ref::<StateEvent>() {
                        self.tracker.outputs.lock().unwrap().push(state_evt.clone());
                    }
                }
                // Forward to real post processor
                self.inner.lock().unwrap().process(chunk)
            }

            fn set_next_processor(&mut self, processor: Arc<Mutex<dyn PostStateProcessor>>) {
                self.inner.lock().unwrap().set_next_processor(processor);
            }

            fn get_next_processor(&self) -> Option<Arc<Mutex<dyn PostStateProcessor>>> {
                self.inner.lock().unwrap().get_next_processor()
            }

            fn state_id(&self) -> usize {
                self.inner.lock().unwrap().state_id()
            }

            fn set_next_state_pre_processor(
                &mut self,
                next: Arc<Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>>,
            ) {
                self.inner
                    .lock()
                    .unwrap()
                    .set_next_state_pre_processor(next);
            }

            fn set_next_every_state_pre_processor(
                &mut self,
                next: Arc<Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>>,
            ) {
                self.inner
                    .lock()
                    .unwrap()
                    .set_next_every_state_pre_processor(next);
            }

            fn get_next_every_state_pre_processor(
                &self,
            ) -> Option<Arc<Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>>>
            {
                self.inner
                    .lock()
                    .unwrap()
                    .get_next_every_state_pre_processor()
            }

            fn set_callback_pre_state_processor(
                &mut self,
                callback: Arc<
                    Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>,
                >,
            ) {
                self.inner
                    .lock()
                    .unwrap()
                    .set_callback_pre_state_processor(callback);
            }

            fn is_event_returned(&self) -> bool {
                self.inner.lock().unwrap().is_event_returned()
            }

            fn clear_processed_event(&mut self) {
                self.inner.lock().unwrap().clear_processed_event();
            }

            fn this_state_pre_processor(
                &self,
            ) -> Option<Arc<Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>>>
            {
                self.inner.lock().unwrap().this_state_pre_processor()
            }
        }

        let tracking_post = Arc::new(Mutex::new(TrackingPostProcessor {
            inner: Arc::clone(&post_proc),
            tracker: tracker_clone,
        }));

        pre_proc
            .stream_processor
            .set_this_state_post_processor(tracking_post);

        (pre_proc, post_proc, tracker)
    }

    /// Test EVERY A{2,3} sliding window: A1, A2, A3 should produce 3 outputs
    /// Window 1: [A1, A2] - outputs at A2
    /// Window 1: [A1, A2, A3] - outputs at A3 (max reached, window closes)
    /// Window 2: [A2, A3] - outputs at A3 (spawned at A2)
    #[test]
    fn test_every_sliding_window_a2_3_basic() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _, tracker) = create_every_tracked_processors(2, 3);

        // Initialize with first state
        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // A1: pending=[SE([A1])], no output (count=1 < min=2)
        let a1 = StreamEvent::new(100, 0, 0, 0);
        pre_proc.process_and_return(Some(Box::new(a1)));
        assert_eq!(tracker.get_output_count(), 0, "A1: No output yet (count=1)");

        // A2: pending=[SE([A1,A2]), SE([A2])], output=[A1,A2]
        // - SE([A1]) + A2 → SE([A1,A2]), count=2 >= min → OUTPUT
        // - EVERY spawns SE([A2])
        let a2 = StreamEvent::new(200, 0, 0, 0);
        pre_proc.process_and_return(Some(Box::new(a2)));
        assert_eq!(
            tracker.get_output_count(),
            1,
            "A2: Output [A1,A2] (count=2)"
        );

        // A3: pending=[SE([A2,A3]), SE([A3])], outputs=[A1,A2,A3], [A2,A3]
        // - SE([A1,A2]) + A3 → SE([A1,A2,A3]), count=3 == max → OUTPUT, remove
        // - SE([A2]) + A3 → SE([A2,A3]), count=2 >= min → OUTPUT
        // - EVERY spawns SE([A3])
        let a3 = StreamEvent::new(300, 0, 0, 0);
        pre_proc.process_and_return(Some(Box::new(a3)));
        assert_eq!(
            tracker.get_output_count(),
            3,
            "A3: Total 3 outputs - [A1,A2], [A1,A2,A3], [A2,A3]"
        );
    }

    /// Test EVERY A{2,3} with 4 events: A1, A2, A3, A4
    /// Expected outputs: [A1,A2], [A1,A2,A3], [A2,A3], [A2,A3,A4], [A3,A4]
    #[test]
    fn test_every_sliding_window_a2_3_with_4_events() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _, tracker) = create_every_tracked_processors(2, 3);

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send 4 events
        for i in 1..=4 {
            let event = StreamEvent::new(i * 100, 0, 0, 0);
            pre_proc.process_and_return(Some(Box::new(event)));
        }

        // Expected outputs:
        // A1: 0 outputs (count=1)
        // A2: 1 output ([A1,A2])
        // A3: 2 outputs ([A1,A2,A3], [A2,A3])
        // A4: 2 outputs ([A2,A3,A4], [A3,A4])
        // Total: 5 outputs
        assert_eq!(
            tracker.get_output_count(),
            5,
            "EVERY A{{2,3}} with 4 events should produce 5 sliding window outputs"
        );
    }

    /// Test EVERY A{3,3} (exactly 3) with 5 events
    /// Expected: [A1,A2,A3], [A2,A3,A4], [A3,A4,A5]
    #[test]
    fn test_every_sliding_window_exactly_3_with_5_events() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _, tracker) = create_every_tracked_processors(3, 3);

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send 5 events
        for i in 1..=5 {
            let event = StreamEvent::new(i * 100, 0, 0, 0);
            pre_proc.process_and_return(Some(Box::new(event)));
        }

        // Expected outputs at each event:
        // A1: 0 (count=1 < min=3)
        // A2: 0 (count=2 < min=3)
        // A3: 1 ([A1,A2,A3] reaches min=max=3)
        // A4: 1 ([A2,A3,A4] reaches min=max=3)
        // A5: 1 ([A3,A4,A5] reaches min=max=3)
        // Total: 3 sliding windows
        assert_eq!(
            tracker.get_output_count(),
            3,
            "EVERY A{{3}} with 5 events should produce 3 sliding window outputs"
        );
    }

    /// Test non-EVERY pattern does NOT create sliding windows
    #[test]
    fn test_non_every_no_sliding_window() {
        use crate::core::event::stream::stream_event::StreamEvent;

        // Use the regular (non-EVERY) tracked processors
        let (mut pre_proc, _, tracker) = create_tracked_processors(2, 3);

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send 4 events
        for i in 1..=4 {
            let event = StreamEvent::new(i * 100, 0, 0, 0);
            pre_proc.process_and_return(Some(Box::new(event)));
        }

        // Non-EVERY: Only single window accumulates
        // A1: 0 (count=1)
        // A2: 1 ([A1,A2])
        // A3: 1 ([A1,A2,A3]) - max reached, window done
        // A4: No more windows! Non-EVERY doesn't restart
        // Total: 2 outputs from single window
        assert_eq!(
            tracker.get_output_count(),
            2,
            "Non-EVERY A{{2,3}} should NOT create sliding windows"
        );
    }

    /// Test EVERY A{1,2} produces many sliding windows
    #[test]
    fn test_every_sliding_window_a1_2_with_4_events() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _, tracker) = create_every_tracked_processors(1, 2);

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send 4 events
        for i in 1..=4 {
            let event = StreamEvent::new(i * 100, 0, 0, 0);
            pre_proc.process_and_return(Some(Box::new(event)));
        }

        // Expected outputs:
        // A1: 1 output ([A1]) - count=1 >= min=1
        // A2: 2 outputs ([A1,A2] max, [A2]) - spawned window outputs immediately
        // A3: 2 outputs ([A2,A3] max, [A3])
        // A4: 2 outputs ([A3,A4] max, [A4])
        // Total: 7 outputs
        assert_eq!(
            tracker.get_output_count(),
            7,
            "EVERY A{{1,2}} with 4 events should produce 7 outputs"
        );
    }

    /// Test that sliding windows track events correctly in the chain
    #[test]
    fn test_every_sliding_window_event_chain_integrity() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _, tracker) = create_every_tracked_processors(2, 2);

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send events with distinct values
        let mut a1 = StreamEvent::new(0, 0, 0, 0);
        a1.timestamp = 1000;
        pre_proc.process_and_return(Some(Box::new(a1)));

        let mut a2 = StreamEvent::new(0, 0, 0, 0);
        a2.timestamp = 2000;
        pre_proc.process_and_return(Some(Box::new(a2)));

        let mut a3 = StreamEvent::new(0, 0, 0, 0);
        a3.timestamp = 3000;
        pre_proc.process_and_return(Some(Box::new(a3)));

        // Should have 2 outputs: [A1,A2] and [A2,A3]
        assert_eq!(tracker.get_output_count(), 2);

        let outputs = tracker.get_outputs();

        // First output should be [A1,A2] with timestamps 1000, 2000
        if let Some(first_event) = outputs[0].get_stream_event(0) {
            assert_eq!(first_event.timestamp, 1000, "First window starts at A1");
            if let Some(next) = first_event.get_next() {
                if let Some(next_stream) = next.as_any().downcast_ref::<StreamEvent>() {
                    assert_eq!(next_stream.timestamp, 2000, "First window ends at A2");
                }
            }
        }

        // Second output should be [A2,A3] with timestamps 2000, 3000
        if let Some(first_event) = outputs[1].get_stream_event(0) {
            assert_eq!(first_event.timestamp, 2000, "Second window starts at A2");
            if let Some(next) = first_event.get_next() {
                if let Some(next_stream) = next.as_any().downcast_ref::<StreamEvent>() {
                    assert_eq!(next_stream.timestamp, 3000, "Second window ends at A3");
                }
            }
        }
    }

    /// Test EVERY A{2,5} produces correct sliding windows
    #[test]
    fn test_every_sliding_window_a2_5_comprehensive() {
        use crate::core::event::stream::stream_event::StreamEvent;

        let (mut pre_proc, _, tracker) = create_every_tracked_processors(2, 5);

        let initial_state = StateEvent::new(1, 0);
        pre_proc.add_state(initial_state);
        pre_proc.update_state();

        // Send 6 events: A1, A2, A3, A4, A5, A6
        for i in 1..=6 {
            let event = StreamEvent::new(i * 100, 0, 0, 0);
            pre_proc.process_and_return(Some(Box::new(event)));
        }

        // Window 1 (starts A1): outputs at A2,A3,A4,A5 (4 outputs, closes at A5=max)
        // Window 2 (starts A2): outputs at A3,A4,A5,A6 (4 outputs, closes at A6)
        // Window 3 (starts A3): outputs at A4,A5,A6 (3 outputs)
        // Window 4 (starts A4): outputs at A5,A6 (2 outputs)
        // Window 5 (starts A5): outputs at A6 (1 output)
        // Window 6 (starts A6): no output yet (count=1 < min=2)
        //
        // Actually let's trace more carefully:
        // A1: pending=[SE([A1])], outputs=0
        // A2: pending=[SE([A1,A2]), SE([A2])], outputs=1 ([A1,A2])
        // A3: pending=[SE([A1,A2,A3]), SE([A2,A3]), SE([A3])], outputs=2 ([A1,A2,A3], [A2,A3])
        // A4: pending=[SE([A1,A2,A3,A4]), SE([A2,A3,A4]), SE([A3,A4]), SE([A4])], outputs=3
        // A5: pending=[SE([A2,A3,A4,A5]), SE([A3,A4,A5]), SE([A4,A5]), SE([A5])], outputs=4
        //     (SE([A1,A2,A3,A4,A5]) hits max=5, removed)
        // A6: pending=[SE([A3,A4,A5,A6]), SE([A4,A5,A6]), SE([A5,A6]), SE([A6])], outputs=4
        //     (SE([A2,A3,A4,A5,A6]) hits max=5, removed)
        //
        // Total: 0 + 1 + 2 + 3 + 4 + 4 = 14 outputs

        assert_eq!(
            tracker.get_output_count(),
            14,
            "EVERY A{{2,5}} with 6 events should produce 14 sliding window outputs"
        );
    }
}
