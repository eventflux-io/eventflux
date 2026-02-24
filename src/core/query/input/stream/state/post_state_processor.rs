// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/input/stream/state/post_state_processor.rs
// PostStateProcessor trait for pattern match handling

use crate::core::event::complex_event::ComplexEvent;
use crate::core::query::input::stream::state::pre_state_processor::PreStateProcessor;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// PostStateProcessor handles successful pattern matches from PreStateProcessor.
///
/// **Purpose**: When a PreStateProcessor successfully matches an event, it forwards
/// the StateEvent to its PostStateProcessor. The PostStateProcessor then chains the
/// matched StateEvent to:
/// 1. Next PreStateProcessor in sequence (A -> B -> C)
/// 2. 'every' PreStateProcessor for continuous matching
/// 3. Callback PreStateProcessor for count quantifiers (A{2,5})
/// 4. Output processor for query results
///
/// **Pattern Processing Flow**:
/// ```text
/// Event -> PreA -> PostA -> PreB -> PostB -> PreC -> PostC -> Output
///                    |         |         |         |
///                    v         v         v         v
///               (next state) (next state) (final match)
/// ```
///
/// **'every' Pattern**:
/// ```text
/// Event -> PreA (every) -> PostA ─┐
///              ^                   │
///              └───────────────────┘ (loop back for continuous matching)
/// ```
///
/// **Key Responsibilities**:
/// - Forward matched StateEvent to next processors in chain
/// - Set timestamp from matched StreamEvent
/// - Notify PreStateProcessor that state changed
/// - Track if event was returned for optimization
pub trait PostStateProcessor: Debug + Send + Sync {
    /// Process an incoming event chunk (matched StateEvent from PreStateProcessor)
    ///
    /// This is the entry point for matched events from the associated PreStateProcessor.
    /// Returns processed events if any, None otherwise.
    fn process(&mut self, chunk: Option<Box<dyn ComplexEvent>>) -> Option<Box<dyn ComplexEvent>>;

    /// Set next processor in the chain
    fn set_next_processor(&mut self, processor: Arc<Mutex<dyn PostStateProcessor>>);

    /// Get next processor in the chain
    fn get_next_processor(&self) -> Option<Arc<Mutex<dyn PostStateProcessor>>>;

    /// Get the state ID this PostStateProcessor is associated with
    fn state_id(&self) -> usize;

    /// Set the next PreStateProcessor in the sequence chain
    /// This is used for patterns like A -> B -> C
    ///
    /// When PostA matches, it forwards StateEvent to PreB via this link
    fn set_next_state_pre_processor(
        &mut self,
        next_state_pre_processor: Arc<
            Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>,
        >,
    );

    /// Set the next PreStateProcessor for 'every' semantics
    /// This creates a loop for continuous pattern matching
    ///
    /// Example: "every A" - PostA forwards back to PreA to continue matching
    fn set_next_every_state_pre_processor(
        &mut self,
        next_every_state_pre_processor: Arc<
            Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>,
        >,
    );

    /// Set callback PreStateProcessor for count quantifiers
    /// Used for patterns like A{2,5} to notify when count is reached
    ///
    /// Example: A{2,3} - After 2nd A, notify callback to check if pattern is complete
    fn set_callback_pre_state_processor(
        &mut self,
        callback_pre_state_processor: Arc<
            Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>,
        >,
    );

    /// Get the next PreStateProcessor for 'every' semantics
    /// This allows checking if EVERY loopback is configured
    ///
    /// Used by reset_state() to detect EVERY patterns and prevent reset
    fn get_next_every_state_pre_processor(
        &self,
    ) -> Option<Arc<Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>>>;

    /// Check if event was returned to output processor
    /// Used for optimization to skip unnecessary processing
    fn is_event_returned(&self) -> bool;

    /// Clear the event returned flag
    /// Called after processing to reset state
    fn clear_processed_event(&mut self);

    /// Get the PreStateProcessor associated with this PostStateProcessor
    ///
    /// **Returns**: Arc<Mutex> to the PreStateProcessor for this processor
    ///
    /// **Used For**:
    /// - Calling state_changed() to notify PreStateProcessor
    /// - Processor coordination
    fn this_state_pre_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>>;
}

// ===== Tests =====

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::state::state_event::StateEvent;
    use std::sync::{Arc, Mutex};

    /// Mock PostStateProcessor for testing trait contract
    #[derive(Debug)]
    struct MockPostStateProcessor {
        state_id: usize,
        next_state_pre_processor:
            Option<Arc<Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>>>,
        next_every_state_pre_processor:
            Option<Arc<Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>>>,
        callback_pre_state_processor:
            Option<Arc<Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>>>,
        event_returned: bool,
        next_processor: Option<Arc<Mutex<dyn PostStateProcessor>>>,
    }

    impl MockPostStateProcessor {
        fn new(state_id: usize) -> Self {
            Self {
                state_id,
                next_state_pre_processor: None,
                next_every_state_pre_processor: None,
                callback_pre_state_processor: None,
                event_returned: false,
                next_processor: None,
            }
        }
    }

    impl PostStateProcessor for MockPostStateProcessor {
        fn process(
            &mut self,
            chunk: Option<Box<dyn ComplexEvent>>,
        ) -> Option<Box<dyn ComplexEvent>> {
            // Mock: just return the chunk
            chunk
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
            next_state_pre_processor: Arc<
                Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>,
            >,
        ) {
            self.next_state_pre_processor = Some(next_state_pre_processor);
        }

        fn set_next_every_state_pre_processor(
            &mut self,
            next_every_state_pre_processor: Arc<
                Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>,
            >,
        ) {
            self.next_every_state_pre_processor = Some(next_every_state_pre_processor);
        }

        fn set_callback_pre_state_processor(
            &mut self,
            callback_pre_state_processor: Arc<
                Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>,
            >,
        ) {
            self.callback_pre_state_processor = Some(callback_pre_state_processor);
        }

        fn get_next_every_state_pre_processor(
            &self,
        ) -> Option<Arc<Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>>>
        {
            self.next_every_state_pre_processor.clone()
        }

        fn is_event_returned(&self) -> bool {
            self.event_returned
        }

        fn clear_processed_event(&mut self) {
            self.event_returned = false
        }

        fn this_state_pre_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>> {
            None
        }
    }

    // ===== Trait Contract Tests =====

    #[test]
    fn test_trait_compiles() {
        let processor = MockPostStateProcessor::new(0);
        assert_eq!(processor.state_id(), 0);
    }

    #[test]
    fn test_state_id() {
        let processor = MockPostStateProcessor::new(42);
        assert_eq!(processor.state_id(), 42);
    }

    #[test]
    fn test_event_returned_flag() {
        let mut processor = MockPostStateProcessor::new(0);
        assert!(!processor.is_event_returned());

        processor.event_returned = true;
        assert!(processor.is_event_returned());

        processor.clear_processed_event();
        assert!(!processor.is_event_returned());
    }

    #[test]
    fn test_process_passthrough() {
        let mut processor = MockPostStateProcessor::new(0);
        let state_event = StateEvent::new(2, 0);
        let result = processor.process(Some(Box::new(state_event)));
        assert!(result.is_some());
    }

    #[test]
    fn test_set_get_next_processor() {
        let mut processor = MockPostStateProcessor::new(0);

        // Test set_next_processor
        let next = Arc::new(Mutex::new(MockPostStateProcessor::new(1)));
        processor.set_next_processor(next.clone());
        assert!(processor.get_next_processor().is_some());

        let retrieved = processor.get_next_processor().unwrap();
        let retrieved_id = retrieved.lock().unwrap().state_id();
        assert_eq!(retrieved_id, 1);
    }
}
