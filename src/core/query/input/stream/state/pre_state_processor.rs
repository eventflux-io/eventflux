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

// src/core/query/input/stream/state/pre_state_processor.rs
// PreStateProcessor trait, the step that matches events in a pattern

use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::state::state_event::StateEvent;
use crate::core::query::input::stream::state::post_state_processor::PostStateProcessor;
use crate::core::query::input::stream::state::shared_processor_state::ProcessorSharedState;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// Matches incoming events for one step of a pattern or sequence.
///
/// Pre processors and their post processors form a chain:
/// - `A and B -> C`: PreA → PostA → PreC and PreB → PostB → PreC, side by side
/// - `A -> B -> C`: PreA → PostA → PreB → PostB → PreC → PostC, one after another
///
/// **Lifecycle**:
/// 1. **init()**: set up. A start state creates an empty StateEvent.
/// 2. **add_state()**: add a candidate state to the new list
/// 3. **update_state()**: move new states to pending, sorted by timestamp
/// 4. **process_and_return()**: try the incoming event against every pending state
/// 5. **reset_state()**: clear after a match. A start state sets itself up again.
///
/// **State lists** (held in StreamPreState):
/// - `current`: the chunk being worked on
/// - `pending`: states waiting for a matching event, sorted by timestamp
/// - `new`: new candidates and 'every' states, not yet pending
///
/// **Implementations**:
/// - `StreamPreStateProcessor`: default for a single stream step
/// - `LogicalPreStateProcessor`: one side of AND/OR, linked to a partner
/// - `CountPreStateProcessor`: count quantifiers such as A{n} and A{m,n}
///
/// **Pattern kinds**:
/// - **Sequence**: A -> B -> C, where events must follow one another with nothing between
/// - **Logical**: A and B, A or B, where the two sides check each other
/// - **Count**: A{2,5}, which collects 2 to 5 events at one position
/// - **Absent**: not A for 10s. Planned, no implementation yet.
/// - **'every'**: every A -> B, which starts the pattern again on each match
///
pub trait PreStateProcessor: Debug + Send + Sync {
    // ===== Core Lifecycle Methods =====

    /// Set up the processor. Called once at query start, before any events arrive.
    ///
    /// A start state (first in the chain) creates an empty StateEvent to begin
    /// matching. Other states create nothing.
    fn init(&mut self);

    /// Entry point for events arriving at this processor.
    ///
    /// Returns matched events if any, otherwise None.
    fn process(&mut self, chunk: Option<Box<dyn ComplexEvent>>) -> Option<Box<dyn ComplexEvent>>;

    /// Add a candidate StateEvent to the new list.
    ///
    /// Called when the previous step matched and passes its state on. For example in
    /// A -> B, a match on A calls B.add_state(state_with_a). In A and B, each side gets
    /// its own states.
    ///
    /// **Parameters**:
    /// - `state_event`: StateEvent holding the events matched so far
    fn add_state(&mut self, state_event: StateEvent);

    /// Add a StateEvent sent back by an 'every' loop, so the pattern can start again.
    ///
    /// For `every A -> B`, a match passes the state both to the next step and back
    /// through this method.
    ///
    /// **Difference from add_state()**:
    /// - `add_state()`: normal forward step (A -> B)
    /// - `add_every_state()`: restart from an 'every' loop
    fn add_every_state(&mut self, state_event: StateEvent);

    /// Move states from the new list to pending, sorted by timestamp.
    ///
    /// Called after the add_state() calls and before the next events are processed.
    /// The sort matters for timing. In `A -> B within 5s`, B must be checked in time
    /// order against A, or matches can be wrongly found or missed.
    fn update_state(&mut self);

    /// Clear state after a full match or after events expire.
    ///
    /// Default behavior:
    /// - Clear the pending list
    /// - A start state with an empty new list sets itself up again with an empty
    ///   StateEvent
    /// - Other states wait for new states from the previous step
    fn reset_state(&mut self);

    // ===== Event Processing =====

    /// Try the incoming event against every pending StateEvent.
    ///
    /// For each pending StateEvent:
    /// 1. Copy it and put the incoming event at this processor's position
    /// 2. Check this step's condition
    /// 3. On a match, send it to the PostStateProcessor
    ///
    /// What happens to a pending state after a match or a miss depends on the
    /// implementation and on Pattern or Sequence mode.
    ///
    /// **Parameters**:
    /// - `chunk`: incoming events as `Option<Box<dyn ComplexEvent>>`
    ///
    /// **Returns**:
    /// - Matched StateEvents for the caller to pass on, or None when nothing matched.
    ///   Some implementations return only the first match or always return None.
    fn process_and_return(
        &mut self,
        chunk: Option<Box<dyn ComplexEvent>>,
    ) -> Option<Box<dyn ComplexEvent>>;

    // ===== Time Management =====

    /// Set the WITHIN limit in milliseconds.
    ///
    /// Example: `A -> B within 5 sec` sets within_time = 5000.
    ///
    /// **Parameters**:
    /// - `within_time`: limit in milliseconds. 0 or less means no limit in the default
    ///   implementation.
    fn set_within_time(&mut self, within_time: i64);

    /// Drop pending StateEvents that are older than the WITHIN limit at `timestamp`.
    ///
    /// **Parameters**:
    /// - `timestamp`: current processing time in milliseconds
    fn expire_events(&mut self, timestamp: i64);

    // ===== State Identification =====

    /// StateEvent position this processor writes to, starting at 0.
    ///
    /// In `A -> B -> C`, A is 0, B is 1 and C is 2. It is the index in
    /// StateEvent.stream_events[].
    fn state_id(&self) -> usize;

    /// True when this is the first step in the chain.
    ///
    /// A start state creates an empty StateEvent in init() and sets itself up again
    /// after reset_state(). Other states wait for the previous step to call
    /// add_state().
    fn is_start_state(&self) -> bool;

    // ===== Shared State Management =====

    /// State shared with the PostStateProcessor.
    ///
    /// The post processor marks state changes through it without locking this
    /// processor, which avoids deadlock.
    fn get_shared_state(&self) -> Arc<ProcessorSharedState>;

    // ===== Post Processor Integration =====

    /// The PostStateProcessor paired with this processor, if set.
    ///
    /// Matched StateEvents are sent to it. It passes them to the next step, to the
    /// 'every' loop, or to the output when this is the last step.
    fn this_state_post_processor(&self) -> Option<Arc<Mutex<dyn PostStateProcessor>>>;

    // ===== State Change Notification =====

    /// Mark that the state changed, for example when a state is added or a match moves
    /// forward.
    ///
    /// Takes `&self`, not `&mut self`, so a PostStateProcessor can call it without a
    /// mutable borrow and without deadlocking while the pre processor is locked.
    fn state_changed(&self);

    /// True when the state has been marked changed
    fn has_state_changed(&self) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::eventflux_app_context::EventFluxAppContext;
    use crate::core::config::eventflux_query_context::EventFluxQueryContext;

    use std::sync::{Arc, Mutex};

    // ===== Mock Implementation for Testing =====

    #[allow(dead_code)]
    #[derive(Debug)]
    struct MockPreStateProcessor {
        app_context: Arc<EventFluxAppContext>,
        query_context: Arc<EventFluxQueryContext>,
        state_id: usize,
        is_start: bool,
        within_time: i64,
        states: Vec<StateEvent>,
        state_changed_flag: std::sync::atomic::AtomicBool,
        init_called: bool,
        add_state_calls: usize,
        add_every_state_calls: usize,
        update_state_calls: usize,
        reset_state_calls: usize,
        process_calls: usize,
        expire_calls: usize,
    }

    impl MockPreStateProcessor {
        fn new(state_id: usize, is_start: bool) -> Self {
            // Create minimal contexts for testing
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

            Self {
                app_context: app_ctx,
                query_context: query_ctx,
                state_id,
                is_start,
                within_time: 0,
                states: Vec::new(),
                state_changed_flag: std::sync::atomic::AtomicBool::new(false),
                init_called: false,
                add_state_calls: 0,
                add_every_state_calls: 0,
                update_state_calls: 0,
                reset_state_calls: 0,
                process_calls: 0,
                expire_calls: 0,
            }
        }
    }

    impl PreStateProcessor for MockPreStateProcessor {
        fn process(
            &mut self,
            _chunk: Option<Box<dyn ComplexEvent>>,
        ) -> Option<Box<dyn ComplexEvent>> {
            // Mock implementation
            None
        }
        fn init(&mut self) {
            self.init_called = true;
            if self.is_start {
                self.states.push(StateEvent::new(1, 0));
            }
        }

        fn add_state(&mut self, state_event: StateEvent) {
            self.add_state_calls += 1;
            self.states.push(state_event);
            self.state_changed_flag
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }

        fn add_every_state(&mut self, state_event: StateEvent) {
            self.add_every_state_calls += 1;
            self.states.push(state_event);
            self.state_changed_flag
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }

        fn update_state(&mut self) {
            self.update_state_calls += 1;
        }

        fn reset_state(&mut self) {
            self.reset_state_calls += 1;
            self.states.clear();
            if self.is_start {
                self.states.push(StateEvent::new(1, 0));
            }
        }

        fn process_and_return(
            &mut self,
            _chunk: Option<Box<dyn ComplexEvent>>,
        ) -> Option<Box<dyn ComplexEvent>> {
            self.process_calls += 1;
            None
        }

        fn set_within_time(&mut self, within_time: i64) {
            self.within_time = within_time;
        }

        fn expire_events(&mut self, _timestamp: i64) {
            self.expire_calls += 1;
        }

        fn state_id(&self) -> usize {
            self.state_id
        }

        fn is_start_state(&self) -> bool {
            self.is_start
        }

        fn get_shared_state(&self) -> Arc<ProcessorSharedState> {
            Arc::new(ProcessorSharedState::new())
        }

        fn this_state_post_processor(&self) -> Option<Arc<Mutex<dyn PostStateProcessor>>> {
            None
        }

        fn state_changed(&self) {
            self.state_changed_flag
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }

        fn has_state_changed(&self) -> bool {
            self.state_changed_flag
                .load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    // ===== Tests for PreStateProcessor Trait Contract =====

    #[test]
    fn test_trait_compiles() {
        let mut processor = MockPreStateProcessor::new(0, true);
        processor.init();
        assert!(processor.init_called);
    }

    #[test]
    fn test_init_start_state() {
        let mut processor = MockPreStateProcessor::new(0, true);
        processor.init();
        assert!(processor.init_called);
        assert_eq!(processor.states.len(), 1); // Start state creates initial StateEvent
    }

    #[test]
    fn test_init_non_start_state() {
        let mut processor = MockPreStateProcessor::new(1, false);
        processor.init();
        assert!(processor.init_called);
        assert_eq!(processor.states.len(), 0); // Non-start waits for add_state()
    }

    #[test]
    fn test_add_state() {
        let mut processor = MockPreStateProcessor::new(0, true);
        let state = StateEvent::new(1, 0);
        processor.add_state(state);
        assert_eq!(processor.add_state_calls, 1);
        assert!(processor.has_state_changed());
    }

    #[test]
    fn test_add_every_state() {
        let mut processor = MockPreStateProcessor::new(0, true);
        let state = StateEvent::new(1, 0);
        processor.add_every_state(state);
        assert_eq!(processor.add_every_state_calls, 1);
        assert!(processor.has_state_changed());
    }

    #[test]
    fn test_update_state() {
        let mut processor = MockPreStateProcessor::new(0, true);
        processor.update_state();
        assert_eq!(processor.update_state_calls, 1);
    }

    #[test]
    fn test_reset_state_start() {
        let mut processor = MockPreStateProcessor::new(0, true);
        processor.init();
        processor.add_state(StateEvent::new(1, 0));
        assert_eq!(processor.states.len(), 2);

        processor.reset_state();
        assert_eq!(processor.reset_state_calls, 1);
        assert_eq!(processor.states.len(), 1); // Reinitialized with empty state
    }

    #[test]
    fn test_reset_state_non_start() {
        let mut processor = MockPreStateProcessor::new(1, false);
        processor.add_state(StateEvent::new(1, 0));
        assert_eq!(processor.states.len(), 1);

        processor.reset_state();
        assert_eq!(processor.reset_state_calls, 1);
        assert_eq!(processor.states.len(), 0); // Cleared, waits for new states
    }

    #[test]
    fn test_process_and_return() {
        let mut processor = MockPreStateProcessor::new(0, true);
        let result = processor.process_and_return(None);
        assert_eq!(processor.process_calls, 1);
        assert!(result.is_none());
    }

    #[test]
    fn test_set_within_time() {
        let mut processor = MockPreStateProcessor::new(0, true);
        processor.set_within_time(5000);
        assert_eq!(processor.within_time, 5000);
    }

    #[test]
    fn test_expire_events() {
        let mut processor = MockPreStateProcessor::new(0, true);
        processor.expire_events(10000);
        assert_eq!(processor.expire_calls, 1);
    }

    #[test]
    fn test_state_id() {
        let processor = MockPreStateProcessor::new(2, false);
        assert_eq!(processor.state_id(), 2);
    }

    #[test]
    fn test_is_start_state() {
        let start = MockPreStateProcessor::new(0, true);
        let non_start = MockPreStateProcessor::new(1, false);
        assert!(start.is_start_state());
        assert!(!non_start.is_start_state());
    }

    #[test]
    fn test_state_changed() {
        let processor = MockPreStateProcessor::new(0, true);
        assert!(!processor.has_state_changed());
        processor.state_changed();
        assert!(processor.has_state_changed());
    }

    #[test]
    fn test_multiple_add_state_calls() {
        let mut processor = MockPreStateProcessor::new(0, true);
        processor.add_state(StateEvent::new(1, 0));
        processor.add_state(StateEvent::new(1, 0));
        processor.add_state(StateEvent::new(1, 0));
        assert_eq!(processor.add_state_calls, 3);
        assert_eq!(processor.states.len(), 3);
    }

    #[test]
    fn test_lifecycle_sequence() {
        let mut processor = MockPreStateProcessor::new(0, true);

        // Init
        processor.init();
        assert!(processor.init_called);

        // Add states
        processor.add_state(StateEvent::new(1, 0));
        assert_eq!(processor.add_state_calls, 1);

        // Update
        processor.update_state();
        assert_eq!(processor.update_state_calls, 1);

        // Process
        processor.process_and_return(None);
        assert_eq!(processor.process_calls, 1);

        // Reset
        processor.reset_state();
        assert_eq!(processor.reset_state_calls, 1);
    }
}
