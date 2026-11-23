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

// src/core/query/input/stream/state/post_state_processor.rs
// PostStateProcessor trait, the step that handles a pattern match

use crate::core::event::complex_event::ComplexEvent;
use crate::core::query::input::stream::state::pre_state_processor::PreStateProcessor;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// Handles a match from its PreStateProcessor and passes it along the chain.
///
/// When a PreStateProcessor matches an event, it sends the StateEvent to its
/// PostStateProcessor. The PostStateProcessor then passes the StateEvent on to:
/// 1. The next PreStateProcessor in order (A -> B -> C)
/// 2. The 'every' PreStateProcessor, so matching starts again
/// 3. The callback PreStateProcessor, which is only told that the state changed
/// 4. The next processor, which leads to the query output
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
/// **Main jobs**:
/// - Pass the matched StateEvent to the next processors in the chain
/// - Set the StateEvent timestamp from the matched StreamEvent
/// - Tell the PreStateProcessor that its state changed
/// - Record whether the event was sent toward the output
pub trait PostStateProcessor: Debug + Send + Sync {
    /// Handle a matched StateEvent sent by the paired PreStateProcessor.
    ///
    /// Returns the event when it should travel further, otherwise None.
    fn process(&mut self, chunk: Option<Box<dyn ComplexEvent>>) -> Option<Box<dyn ComplexEvent>>;

    fn set_next_processor(&mut self, processor: Arc<Mutex<dyn PostStateProcessor>>);

    fn get_next_processor(&self) -> Option<Arc<Mutex<dyn PostStateProcessor>>>;

    /// StateEvent position this processor handles
    fn state_id(&self) -> usize;

    /// Set the next step's PreStateProcessor, for patterns like A -> B -> C.
    ///
    /// When PostA handles a match, it sends the StateEvent to PreB through this link.
    fn set_next_state_pre_processor(
        &mut self,
        next_state_pre_processor: Arc<
            Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>,
        >,
    );

    /// Set the PreStateProcessor that 'every' loops back to, so matching starts again.
    ///
    /// Example: for "every A", PostA sends the StateEvent back to PreA.
    fn set_next_every_state_pre_processor(
        &mut self,
        next_every_state_pre_processor: Arc<
            Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>,
        >,
    );

    /// Set a callback PreStateProcessor, meant for count quantifiers like A{2,5}.
    ///
    /// On each match the callback is told that its state changed. Nothing else is sent
    /// to it.
    fn set_callback_pre_state_processor(
        &mut self,
        callback_pre_state_processor: Arc<
            Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>,
        >,
    );

    /// The PreStateProcessor that 'every' loops back to, if set. Lets callers check
    /// whether an EVERY loopback is wired.
    fn get_next_every_state_pre_processor(
        &self,
    ) -> Option<Arc<Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>>>;

    /// True when a match was sent on while a next processor was set
    fn is_event_returned(&self) -> bool;

    /// Clear the flag read by `is_event_returned`
    fn clear_processed_event(&mut self);

    /// The PreStateProcessor paired with this processor, if set. Used to call
    /// `state_changed()` on it.
    fn this_state_pre_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>>;
}

// ===== Tests =====

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::state::state_event::StateEvent;
    use std::sync::{Arc, Mutex};

    /// Minimal PostStateProcessor used to test the trait
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
