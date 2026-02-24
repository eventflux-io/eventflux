// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/input/stream/state/pre_state_processor.rs
// Core pattern matching interface for state processors

use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::state::state_event::StateEvent;
use crate::core::query::input::stream::state::post_state_processor::PostStateProcessor;
use crate::core::query::input::stream::state::shared_processor_state::ProcessorSharedState;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// PreStateProcessor is the core pattern matching interface.
///
/// **Architecture**: PreStateProcessors form a chain for sequential patterns:
/// - Pattern `A and B -> C`: PreA → PostA → PreC, PreB → PostB → PreC (parallel)
/// - Sequence `A -> B -> C`: PreA → PostA → PreB → PostB → PreC → PostC (sequential)
///
/// **Lifecycle**:
/// 1. **init()**: Initialize processor, start state creates empty StateEvent
/// 2. **add_state()**: Add new candidate state to new/every list
/// 3. **update_state()**: Move new events to pending (sorted by timestamp)
/// 4. **process_and_return()**: Process incoming event against all pending states
/// 5. **reset_state()**: Clear after match, reinitialize if start state
///
/// **State Management**:
/// Each processor maintains three lists (via StreamPreState):
/// - `current`: Current working set being processed
/// - `pending`: Events waiting to be matched (sorted by timestamp)
/// - `new`: New candidates + 'every' pattern events
///
/// **Concrete Implementations**:
/// - `StreamPreStateProcessor`: Base for single stream patterns
/// - `LogicalPreStateProcessor`: Handles AND/OR with partner processor
/// - `CountPreStateProcessor`: Handles A{n}, A{m,n} quantifiers
/// - `AbsentStreamPreStateProcessor`: Handles NOT operator with scheduler
///
/// **Key Pattern Types**:
/// - **Sequence**: A -> B -> C (temporal ordering, each processor waits for previous)
/// - **Pattern**: A and B, A or B (no ordering, check partners)
/// - **Count**: A{2,5} (accumulate 2-5 events at position, backtrack if not satisfied)
/// - **Absent**: not A for 10s (scheduler-based, expire if A arrives within time)
/// - **'every'**: every A -> B (restart pattern on each A match)
///
pub trait PreStateProcessor: Debug + Send + Sync {
    // ===== Core Lifecycle Methods =====

    /// Initialize the processor
    ///
    /// For start states (first in chain), creates an empty StateEvent to begin matching.
    /// For non-start states, prepares internal structures but doesn't create initial state.
    ///
    /// **When called**: During query initialization, before any events arrive
    fn init(&mut self);

    /// Process an incoming event chunk
    ///
    /// This is the entry point for events arriving at this processor.
    /// Returns processed events if any, None otherwise.
    fn process(&mut self, chunk: Option<Box<dyn ComplexEvent>>) -> Option<Box<dyn ComplexEvent>>;

    /// Add a new candidate state event to the new/every list
    ///
    /// Used when:
    /// - Previous processor in chain matched and chains forward
    /// - Fresh event arrives at start state
    ///
    /// **Pattern behavior**:
    /// - Sequence A->B: When A matches, calls B.add_state(stateWithA)
    /// - Pattern A and B: Each side independently receives states
    ///
    /// **Parameters**:
    /// - `state_event`: StateEvent containing matched events so far
    fn add_state(&mut self, state_event: StateEvent);

    /// Add a state event to the new/every list for 'every' pattern matching
    ///
    /// Used for continuous pattern matching: `every A -> B`
    /// - When A matches, adds state to both next processor (B) AND every-next processor
    /// - Allows pattern to restart on each A occurrence
    ///
    /// **Difference from add_state()**:
    /// - `add_state()`: Normal forward chaining (A -> B)
    /// - `add_every_state()`: Restart pattern (every A -> B, triggers on every A)
    fn add_every_state(&mut self, state_event: StateEvent);

    /// Move new/every events to pending list
    ///
    /// Called before processing events to prepare the pending queue.
    /// Events are **sorted by timestamp** before moving (critical for temporal correctness).
    ///
    /// **When called**: At start of each processing iteration, after all add_state() calls
    ///
    /// **Why sorting matters**:
    /// - Pattern `A -> B within 5s`: Must check B arrivals in order relative to A
    /// - Out-of-order events can cause false positives/negatives
    fn update_state(&mut self);

    /// Reset state after a complete match or expiration
    ///
    /// Called when:
    /// - Pattern completes successfully (all processors matched)
    /// - Events expire (WITHIN time exceeded)
    /// - Query needs to reset (e.g., partitioning)
    ///
    /// **Behavior**:
    /// - Clear pending list
    /// - If start state: reinitialize with empty StateEvent
    /// - If not start: wait for new states from previous processor
    fn reset_state(&mut self);

    // ===== Event Processing =====

    /// Process an incoming event against all pending states
    ///
    /// **Core pattern matching logic**:
    /// 1. Iterate through all pending StateEvents
    /// 2. For each StateEvent, check if incoming event matches this processor's condition
    /// 3. If match:
    ///    - Clone StateEvent
    ///    - Add incoming event at this processor's position
    ///    - Forward to PostStateProcessor for chaining/output
    /// 4. Return matched StateEvents (or empty if no matches)
    ///
    /// **Pattern vs Sequence**:
    /// - **Sequence** (A -> B): B waits for A to match, then checks B events
    /// - **Pattern** (A and B): Both run in parallel, check partner match status
    ///
    /// **Parameters**:
    /// - `chunk`: Linked list of incoming events (Option<Box<dyn ComplexEvent>>)
    ///
    /// **Returns**:
    /// - Chain of matched StateEvents to process further or output
    /// - Empty (None) if no matches
    fn process_and_return(
        &mut self,
        chunk: Option<Box<dyn ComplexEvent>>,
    ) -> Option<Box<dyn ComplexEvent>>;

    // ===== Time Management =====

    /// Set the WITHIN time constraint (in milliseconds)
    ///
    /// Example: `A -> B within 5 sec` sets within_time = 5000
    ///
    /// Events older than (latest_timestamp - within_time) are expired.
    ///
    /// **Parameters**:
    /// - `within_time`: Time window in milliseconds (0 = no constraint)
    fn set_within_time(&mut self, within_time: i64);

    /// Expire events older than the given timestamp
    ///
    /// Called periodically or when WITHIN constraint is set.
    /// Removes StateEvents from pending list where timestamp < (current - within_time).
    ///
    /// **Parameters**:
    /// - `timestamp`: Current processing timestamp
    fn expire_events(&mut self, timestamp: i64);

    // ===== State Identification =====

    /// Get the state ID (stream position) this processor manages
    ///
    /// In a sequence `A -> B -> C`:
    /// - A has state_id = 0
    /// - B has state_id = 1
    /// - C has state_id = 2
    ///
    /// The state_id determines which position in StateEvent.stream_events[] this processor writes to.
    ///
    /// **Returns**: Position index (0-based)
    fn state_id(&self) -> usize;

    /// Check if this is the start state (first processor in chain)
    ///
    /// Start states:
    /// - Create initial empty StateEvent in init()
    /// - Receive fresh events directly from stream
    /// - Must reinitialize after reset_state()
    ///
    /// Non-start states:
    /// - Wait for previous processor to chain forward via add_state()
    ///
    /// **Returns**: true if this is the first processor
    fn is_start_state(&self) -> bool;

    // ===== Shared State Management =====

    /// Get the shared state for lock-free communication with PostStateProcessor
    ///
    /// The shared state allows PostStateProcessor to notify PreStateProcessor of
    /// state changes without acquiring locks, preventing deadlock.
    ///
    /// **Returns**: Shared state instance
    fn get_shared_state(&self) -> Arc<ProcessorSharedState>;

    // ===== Post Processor Integration =====

    /// Get the associated PostStateProcessor
    ///
    /// Every PreStateProcessor has a matching PostStateProcessor that handles:
    /// - Chaining to next PreStateProcessor (A -> B)
    /// - Chaining to every-next PreStateProcessor (every A -> B)
    /// - Final output if this is last processor
    ///
    /// Get the PostStateProcessor associated with this PreStateProcessor
    ///
    /// **Returns**: Arc<Mutex> to the PostStateProcessor for this processor
    ///
    /// **Used For**:
    /// - Forwarding matched StateEvents to PostStateProcessor
    /// - Processor chain coordination
    fn this_state_post_processor(&self) -> Option<Arc<Mutex<dyn PostStateProcessor>>>;

    // ===== State Change Notification =====

    /// Mark that state has changed (for optimization)
    ///
    /// Called when:
    /// - New state added
    /// - Match progresses
    /// - Events expired
    ///
    /// Used to skip processing when state is unchanged (optimization).
    ///
    /// **CRITICAL**: Takes `&self` not `&mut self` to avoid deadlock when called
    /// from PostStateProcessor while PreStateProcessor Arc is locked.
    fn state_changed(&self);

    /// Check if state has changed since last check
    ///
    /// **Returns**: true if state changed
    fn has_state_changed(&self) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::eventflux_app_context::EventFluxAppContext;
    use crate::core::config::eventflux_query_context::EventFluxQueryContext;
    use crate::core::query::processor::{CommonProcessorMeta, ProcessingMode};
    use std::sync::{Arc, Mutex};

    // ===== Mock Implementation for Testing =====

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
        let mut processor = MockPreStateProcessor::new(0, true);
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
