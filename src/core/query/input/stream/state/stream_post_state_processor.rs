// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/input/stream/state/stream_post_state_processor.rs
// StreamPostStateProcessor implementation for handling successful pattern matches

use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::state::state_event::StateEvent;
use crate::core::query::input::stream::state::post_state_processor::PostStateProcessor;
use crate::core::query::input::stream::state::pre_state_processor::PreStateProcessor;
use crate::core::query::input::stream::state::shared_processor_state::ProcessorSharedState;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// StreamPostStateProcessor handles successful pattern matches from StreamPreStateProcessor.
///
/// **Pattern Processing Flow**:
/// When a PreStateProcessor matches an event (via condition check), it forwards the
/// matched StateEvent to its associated PostStateProcessor. The PostStateProcessor then:
///
/// 1. **Sets Timestamp**: Updates StateEvent timestamp from the matched StreamEvent
/// 2. **Notifies Pre**: Calls `thisStatePreProcessor.state_changed()` to mark state modification
/// 3. **Chains Forward**: Forwards StateEvent to configured processors:
///    - `nextStatePreProcessor`: Next state in sequence (A -> B -> C)
///    - `nextEveryStatePreProcessor`: Loop back for 'every' patterns
///    - `callbackPreStateProcessor`: Count quantifier callback (A{2,5})
///    - `nextProcessor`: Output to query selector
///
/// **Example Pattern: A -> B -> C**
/// ```text
/// Event arrives at PreA
/// PreA matches condition
/// PreA.processAndReturn() creates StateEvent
/// PreA forwards to PostA
/// PostA.process():
///   - Set timestamp from StreamEvent
///   - Call PreA.stateChanged()
///   - Forward StateEvent to PreB via nextStatePreProcessor
/// PreB receives StateEvent and continues matching...
/// ```
///
/// **Example Pattern: every A**
/// ```text
/// Event arrives at PreA (start state)
/// PreA matches condition
/// PreA forwards to PostA
/// PostA.process():
///   - Forward to nextEveryStatePreProcessor (loops back to PreA)
///   - Forward to nextProcessor (output)
/// PreA receives same StateEvent and continues matching next A...
/// ```
///
/// **Key Design Decisions**:
/// - Uses Arc<Mutex<>> for shared ownership of processors (thread-safe)
/// - Defensive cloning to avoid borrow conflicts
/// - isEventReturned tracks if event was sent to output (optimization)
pub struct StreamPostStateProcessor {
    /// State ID this PostStateProcessor is associated with
    state_id: usize,

    /// Shared state with PreStateProcessor (lock-free communication)
    /// This replaces direct calls to PreStateProcessor.state_changed()
    /// preventing deadlock by avoiding need to lock PreStateProcessor
    shared_state: Option<Arc<ProcessorSharedState>>,

    /// Reference to the PreStateProcessor this PostStateProcessor belongs to
    /// Used to notify PreStateProcessor when state changes
    /// Made public for LogicalPostStateProcessor access
    pub this_state_pre_processor: Option<Arc<Mutex<dyn PreStateProcessor>>>,

    /// Next PreStateProcessor in sequence chain (A -> B -> C)
    next_state_pre_processor: Option<Arc<Mutex<dyn PreStateProcessor>>>,

    /// Next PreStateProcessor for 'every' semantics (loops back)
    next_every_state_pre_processor: Option<Arc<Mutex<dyn PreStateProcessor>>>,

    /// Callback PreStateProcessor for count quantifiers (A{2,5})
    callback_pre_state_processor: Option<Arc<Mutex<dyn PreStateProcessor>>>,

    /// Next processor in the overall processor chain (usually output processor)
    next_processor: Option<Arc<Mutex<dyn PostStateProcessor>>>,

    /// Flag indicating if event was returned to output
    /// Used for optimization to skip unnecessary processing
    /// Made public for LogicalPostStateProcessor access
    pub is_event_returned: bool,
}

impl StreamPostStateProcessor {
    /// Create a new StreamPostStateProcessor
    ///
    /// # Arguments
    /// * `state_id` - The state ID this processor is associated with
    pub fn new(state_id: usize) -> Self {
        Self {
            state_id,
            shared_state: None,
            this_state_pre_processor: None,
            next_state_pre_processor: None,
            next_every_state_pre_processor: None,
            callback_pre_state_processor: None,
            next_processor: None,
            is_event_returned: false,
        }
    }

    /// Set the associated PreStateProcessor
    /// This is the PreStateProcessor that forwards matches to this PostStateProcessor
    ///
    /// **IMPORTANT**: Automatically retrieves and stores shared state for lock-free communication
    pub fn set_this_state_pre_processor(
        &mut self,
        pre_processor: Arc<Mutex<dyn PreStateProcessor>>,
    ) {
        // Automatically get shared state from PreStateProcessor
        // This ensures lock-free state_changed notifications work correctly
        let shared_state = pre_processor.lock().unwrap().get_shared_state();
        self.shared_state = Some(shared_state);

        self.this_state_pre_processor = Some(pre_processor);
    }

    /// Get reference to next state PreStateProcessor
    pub fn get_next_state_pre_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>> {
        self.next_state_pre_processor.clone()
    }

    /// Get reference to next every state PreStateProcessor (public for trait)
    pub fn get_next_every_state_pre_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>> {
        self.next_every_state_pre_processor.clone()
    }

    /// Get reference to callback PreStateProcessor
    pub fn get_callback_pre_state_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>> {
        self.callback_pre_state_processor.clone()
    }

    /// Process a matched StateEvent
    ///
    /// This is the core logic for handling successful pattern matches:
    /// 1. Extract StreamEvent from StateEvent at this state_id
    /// 2. Set StateEvent timestamp from StreamEvent
    /// 3. Notify thisStatePreProcessor that state changed
    /// 4. Forward StateEvent to next/every/callback processors
    /// 5. Mark event as returned if output processor exists
    /// Made public for LogicalPostStateProcessor to call
    pub fn process_state_event(&mut self, state_event: &StateEvent) {
        // Get the StreamEvent at this state position
        if let Some(stream_event) = state_event.get_stream_event(self.state_id) {
            // Notify PreStateProcessor that state changed using shared atomic state
            // This is LOCK-FREE and prevents deadlock!
            if let Some(ref shared) = self.shared_state {
                shared.mark_state_changed();
            }

            // Mark event as returned if output processor exists
            if self.next_processor.is_some() {
                self.is_event_returned = true;
            }

            // Optimize cloning: Only clone if we need to forward
            let needs_next = self.next_state_pre_processor.is_some();
            let needs_every = self.next_every_state_pre_processor.is_some();

            if needs_next || needs_every {
                // Create a mutable copy to set timestamp
                let mut state_event_copy = state_event.clone();
                state_event_copy.set_timestamp(stream_event.timestamp);

                // Forward to next state PreStateProcessor (A -> B)
                // CRITICAL: Expand StateEvent to have enough positions for next processor
                // If next processor has state_id=N, it needs N+1 positions (0..N)
                if needs_next && needs_every {
                    // Both need it - expand and clone for both
                    if let Some(ref next_pre) = self.next_state_pre_processor {
                        let mut expanded = state_event_copy.clone();
                        let next_state_id = next_pre.lock().unwrap().state_id();
                        expanded.expand_to_size(next_state_id + 1);
                        next_pre.lock().unwrap().add_state(expanded);
                    }
                    if let Some(ref next_every) = self.next_every_state_pre_processor {
                        let next_every_state_id = next_every.lock().unwrap().state_id();
                        state_event_copy.expand_to_size(next_every_state_id + 1);
                        next_every.lock().unwrap().add_every_state(state_event_copy);
                    }
                } else if needs_next {
                    // Only next needs it - expand and move directly
                    if let Some(ref next_pre) = self.next_state_pre_processor {
                        let next_state_id = next_pre.lock().unwrap().state_id();
                        state_event_copy.expand_to_size(next_state_id + 1);
                        next_pre.lock().unwrap().add_state(state_event_copy);
                    }
                } else {
                    // Only every needs it - expand and move directly
                    if let Some(ref next_every) = self.next_every_state_pre_processor {
                        let next_every_state_id = next_every.lock().unwrap().state_id();
                        state_event_copy.expand_to_size(next_every_state_id + 1);
                        next_every.lock().unwrap().add_every_state(state_event_copy);
                    }
                }
            }

            // Notify callback PreStateProcessor (count quantifiers)
            if let Some(ref callback) = self.callback_pre_state_processor {
                // For count quantifiers, callback needs to reset start state
                // This is handled by CountPreStateProcessor in Phase 2
                // For now, just mark state changed
                callback.lock().unwrap().state_changed();
            }

            // Forward to next_processor in chain (e.g., TerminalPostStateProcessor)
            // This is used for N-element patterns where the last PostStateProcessor
            // needs to forward to a terminal that bridges to the Processor chain
            if let Some(ref next) = self.next_processor {
                next.lock()
                    .unwrap()
                    .process(Some(Box::new(state_event.clone())));
            }
        }
    }
}

// Manual Debug implementation (needed for trait object compatibility)
impl Debug for StreamPostStateProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamPostStateProcessor")
            .field("state_id", &self.state_id)
            .field(
                "has_this_state_pre_processor",
                &self.this_state_pre_processor.is_some(),
            )
            .field(
                "has_next_state_pre_processor",
                &self.next_state_pre_processor.is_some(),
            )
            .field(
                "has_next_every_state_pre_processor",
                &self.next_every_state_pre_processor.is_some(),
            )
            .field(
                "has_callback_pre_state_processor",
                &self.callback_pre_state_processor.is_some(),
            )
            .field("has_next_processor", &self.next_processor.is_some())
            .field("is_event_returned", &self.is_event_returned)
            .finish()
    }
}

impl PostStateProcessor for StreamPostStateProcessor {
    fn process(&mut self, chunk: Option<Box<dyn ComplexEvent>>) -> Option<Box<dyn ComplexEvent>> {
        if let Some(mut event) = chunk {
            // Try to downcast to StateEvent (immutable first for checking)
            if let Some(state_event) = event.as_any().downcast_ref::<StateEvent>() {
                // CRITICAL: Set timestamp on terminal patterns before forwarding to output
                // Terminal patterns (with only next_processor, no next_state_pre_processor)
                // need timestamp set here, otherwise they'd be emitted with timestamp=-1
                if let Some(stream_event) = state_event.get_stream_event(self.state_id) {
                    let timestamp = stream_event.timestamp;

                    // Now downcast mutably to set the timestamp in-place (zero-cost, we own the Box)
                    if let Some(state_event_mut) = event.as_any_mut().downcast_mut::<StateEvent>() {
                        state_event_mut.set_timestamp(timestamp);
                    }
                }

                // Process the matched StateEvent (still needs to forward to next states)
                // Note: we borrow immutably again since process_state_event doesn't need mut
                if let Some(state_event) = event.as_any().downcast_ref::<StateEvent>() {
                    self.process_state_event(state_event);
                }

                // If we have a next processor in the chain, forward the event
                if self.next_processor.is_some() {
                    return Some(event);
                }
            }
        }
        None
    }

    fn set_next_processor(&mut self, processor: Arc<Mutex<dyn PostStateProcessor>>) {
        self.next_processor = Some(processor);
    }

    fn get_next_processor(&self) -> Option<Arc<Mutex<dyn PostStateProcessor>>> {
        self.next_processor.clone()
    }

    fn state_id(&self) -> usize {
        self.state_id
    }

    fn set_next_state_pre_processor(
        &mut self,
        next_state_pre_processor: Arc<Mutex<dyn PreStateProcessor>>,
    ) {
        self.next_state_pre_processor = Some(next_state_pre_processor);
    }

    fn set_next_every_state_pre_processor(
        &mut self,
        next_every_state_pre_processor: Arc<Mutex<dyn PreStateProcessor>>,
    ) {
        self.next_every_state_pre_processor = Some(next_every_state_pre_processor);
    }

    fn set_callback_pre_state_processor(
        &mut self,
        callback_pre_state_processor: Arc<Mutex<dyn PreStateProcessor>>,
    ) {
        self.callback_pre_state_processor = Some(callback_pre_state_processor);
    }

    fn get_next_every_state_pre_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>> {
        self.next_every_state_pre_processor.clone()
    }

    fn is_event_returned(&self) -> bool {
        self.is_event_returned
    }

    fn clear_processed_event(&mut self) {
        self.is_event_returned = false;
    }

    fn this_state_pre_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>> {
        self.this_state_pre_processor.clone()
    }
}

// ===== Tests =====

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::eventflux_app_context::EventFluxAppContext;
    use crate::core::config::eventflux_query_context::EventFluxQueryContext;
    use crate::core::event::stream::stream_event::StreamEvent;
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

    fn create_test_pre_processor(
        state_id: usize,
        is_start_state: bool,
        state_type: StateType,
    ) -> Arc<Mutex<StreamPreStateProcessor>> {
        let (app_ctx, query_ctx) = create_test_context();
        Arc::new(Mutex::new(StreamPreStateProcessor::new(
            state_id,
            is_start_state,
            state_type,
            app_ctx,
            query_ctx,
        )))
    }

    // ===== Constructor Tests =====

    #[test]
    fn test_new_processor() {
        let processor = StreamPostStateProcessor::new(0);
        assert_eq!(processor.state_id(), 0);
        assert!(!processor.is_event_returned());
        assert!(processor.get_next_state_pre_processor().is_none());
        assert!(processor.get_next_every_state_pre_processor().is_none());
        assert!(processor.get_callback_pre_state_processor().is_none());
    }

    #[test]
    fn test_state_id() {
        let processor = StreamPostStateProcessor::new(42);
        assert_eq!(processor.state_id(), 42);
    }

    // ===== Configuration Tests =====

    #[test]
    fn test_set_this_state_pre_processor() {
        let mut post_processor = StreamPostStateProcessor::new(0);
        let pre_processor = create_test_pre_processor(0, true, StateType::Pattern);

        post_processor.set_this_state_pre_processor(pre_processor);
        assert!(post_processor.this_state_pre_processor.is_some());
    }

    #[test]
    fn test_set_next_state_pre_processor() {
        let mut post_processor = StreamPostStateProcessor::new(0);
        let next_pre_processor = create_test_pre_processor(1, false, StateType::Pattern);

        post_processor.set_next_state_pre_processor(next_pre_processor);
        assert!(post_processor.get_next_state_pre_processor().is_some());
    }

    #[test]
    fn test_set_next_every_state_pre_processor() {
        let mut post_processor = StreamPostStateProcessor::new(0);
        let next_every_pre_processor = create_test_pre_processor(0, true, StateType::Pattern);

        post_processor.set_next_every_state_pre_processor(next_every_pre_processor);
        assert!(post_processor
            .get_next_every_state_pre_processor()
            .is_some());
    }

    #[test]
    fn test_set_callback_pre_state_processor() {
        let mut post_processor = StreamPostStateProcessor::new(0);
        let callback_pre_processor = create_test_pre_processor(0, true, StateType::Pattern);

        post_processor.set_callback_pre_state_processor(callback_pre_processor);
        assert!(post_processor.get_callback_pre_state_processor().is_some());
    }

    // ===== Event Processing Tests =====

    #[test]
    fn test_process_state_event_sets_timestamp() {
        let mut post_processor = StreamPostStateProcessor::new(0);
        let pre_processor = create_test_pre_processor(0, true, StateType::Pattern);
        post_processor.set_this_state_pre_processor(pre_processor);

        // Create StateEvent with StreamEvent at position 0
        let mut state_event = StateEvent::new(1, 0);
        let mut stream_event = StreamEvent::new(1000, 1, 0, 0);
        stream_event.timestamp = 1234;
        state_event.set_event(0, stream_event);

        post_processor.process_state_event(&state_event);

        // Verify state changed was called (indirectly via has_state_changed)
        assert!(post_processor.this_state_pre_processor.is_some());
    }

    #[test]
    fn test_process_marks_event_returned_if_output_exists() {
        let mut post_processor = StreamPostStateProcessor::new(0);
        let pre_processor = create_test_pre_processor(0, true, StateType::Pattern);
        post_processor.set_this_state_pre_processor(pre_processor.clone());

        // Set next processor (output)
        let output_processor = Arc::new(Mutex::new(StreamPostStateProcessor::new(99)));
        post_processor.set_next_processor(output_processor);

        // Create StateEvent
        let mut state_event = StateEvent::new(1, 0);
        let stream_event = StreamEvent::new(1000, 1, 0, 0);
        state_event.set_event(0, stream_event);

        assert!(!post_processor.is_event_returned());
        post_processor.process_state_event(&state_event);
        assert!(post_processor.is_event_returned());
    }

    #[test]
    fn test_clear_processed_event() {
        let mut post_processor = StreamPostStateProcessor::new(0);
        post_processor.is_event_returned = true;

        assert!(post_processor.is_event_returned());
        post_processor.clear_processed_event();
        assert!(!post_processor.is_event_returned());
    }

    #[test]
    fn test_process_with_null_chunk() {
        let mut post_processor = StreamPostStateProcessor::new(0);
        let result = post_processor.process(None);
        assert!(result.is_none());
    }

    #[test]
    fn test_process_with_state_event() {
        let mut post_processor = StreamPostStateProcessor::new(0);
        let pre_processor = create_test_pre_processor(0, true, StateType::Pattern);
        post_processor.set_this_state_pre_processor(pre_processor);

        // Create StateEvent
        let mut state_event = StateEvent::new(1, 0);
        let stream_event = StreamEvent::new(1000, 1, 0, 0);
        state_event.set_event(0, stream_event);

        let result = post_processor.process(Some(Box::new(state_event)));
        assert!(result.is_none()); // No next processor, so returns None
    }

    #[test]
    fn test_process_returns_event_if_next_processor_exists() {
        let mut post_processor = StreamPostStateProcessor::new(0);
        let pre_processor = create_test_pre_processor(0, true, StateType::Pattern);
        post_processor.set_this_state_pre_processor(pre_processor);

        // Set next processor
        let output = Arc::new(Mutex::new(StreamPostStateProcessor::new(99)));
        post_processor.set_next_processor(output);

        // Create StateEvent
        let mut state_event = StateEvent::new(1, 0);
        let stream_event = StreamEvent::new(1000, 1, 0, 0);
        state_event.set_event(0, stream_event);

        let result = post_processor.process(Some(Box::new(state_event)));
        assert!(result.is_some());
        assert!(post_processor.is_event_returned());
    }

    // ===== Processor Chain Tests =====

    #[test]
    fn test_set_next_processor() {
        let mut post_processor = StreamPostStateProcessor::new(0);
        let next = Arc::new(Mutex::new(StreamPostStateProcessor::new(1)));

        post_processor.set_next_processor(next.clone());
        assert!(post_processor.get_next_processor().is_some());

        let retrieved = post_processor.get_next_processor().unwrap();
        let retrieved_id = retrieved.lock().unwrap().state_id();
        assert_eq!(retrieved_id, 1);
    }

    // ===== Debug Implementation Test =====

    #[test]
    fn test_debug_impl() {
        let processor = StreamPostStateProcessor::new(0);
        let debug_str = format!("{:?}", processor);
        assert!(debug_str.contains("StreamPostStateProcessor"));
        assert!(debug_str.contains("state_id"));
    }
}
