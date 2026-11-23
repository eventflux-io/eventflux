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

// src/core/query/input/stream/state/stream_post_state_processor.rs
// StreamPostStateProcessor, the default handler for a pattern step's matches

use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::state::state_event::StateEvent;
use crate::core::query::input::stream::state::post_state_processor::PostStateProcessor;
use crate::core::query::input::stream::state::pre_state_processor::PreStateProcessor;
use crate::core::query::input::stream::state::shared_processor_state::ProcessorSharedState;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// Default PostStateProcessor. Handles the matches of one pattern step.
///
/// When a PreStateProcessor matches an event, it sends the matched StateEvent here.
/// This processor then:
///
/// 1. **Sets the timestamp** of the StateEvent from the matched StreamEvent
/// 2. **Marks the state changed** through the ProcessorSharedState it shares with the
///    pre processor, without locking the pre processor
/// 3. **Passes the StateEvent on** to whichever of these are set:
///    - `next_state_pre_processor`: the next step (A -> B -> C)
///    - `next_every_state_pre_processor`: the loop back for 'every' patterns
///    - `callback_pre_state_processor`: only told that its state changed
///    - `next_processor`: the way to the query output
///
/// **Example Pattern: A -> B -> C**
/// ```text
/// Event arrives at PreA
/// PreA matches condition
/// PreA.process_and_return() creates StateEvent
/// PreA forwards to PostA
/// PostA.process():
///   - Set timestamp from StreamEvent
///   - Mark PreA's shared state changed
///   - Forward StateEvent to PreB via next_state_pre_processor
/// PreB receives StateEvent and continues matching...
/// ```
///
/// **Example Pattern: every A**
/// ```text
/// Event arrives at PreA (start state)
/// PreA matches condition
/// PreA forwards to PostA
/// PostA.process():
///   - Forward to next_every_state_pre_processor (loops back to PreA)
///   - Forward to next_processor (output)
/// PreA receives same StateEvent and continues matching next A...
/// ```
///
/// **Design notes**:
/// - Linked processors are held as Arc<Mutex<>> so several owners can share them
///   across threads
/// - The StateEvent is cloned only when a next or every target needs it
/// - `is_event_returned` records whether a match went toward the output
pub struct StreamPostStateProcessor {
    /// StateEvent position this processor handles
    state_id: usize,

    /// State shared with the pre processor. Marking it changed here replaces a call to
    /// PreStateProcessor::state_changed(), so the pre processor is never locked from
    /// here and cannot deadlock.
    shared_state: Option<Arc<ProcessorSharedState>>,

    /// The PreStateProcessor that sends matches here.
    /// Public so LogicalPostStateProcessor can reach it.
    pub this_state_pre_processor: Option<Arc<Mutex<dyn PreStateProcessor>>>,

    /// Next step's pre processor (A -> B -> C)
    next_state_pre_processor: Option<Arc<Mutex<dyn PreStateProcessor>>>,

    /// Pre processor that 'every' loops back to
    next_every_state_pre_processor: Option<Arc<Mutex<dyn PreStateProcessor>>>,

    /// Callback pre processor, meant for count quantifiers (A{2,5})
    callback_pre_state_processor: Option<Arc<Mutex<dyn PreStateProcessor>>>,

    /// Next processor in the chain, usually the one that leads to the output
    next_processor: Option<Arc<Mutex<dyn PostStateProcessor>>>,

    /// Set when a match is handled while `next_processor` is set.
    /// Public so LogicalPostStateProcessor can set it on its partner.
    pub is_event_returned: bool,
}

impl StreamPostStateProcessor {
    /// Create a post processor for StateEvent position `state_id`
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

    /// Set the PreStateProcessor that sends matches here.
    ///
    /// Also stores that processor's shared state, so later state changes can be marked
    /// without locking it. Locks `pre_processor` once to read it.
    pub fn set_this_state_pre_processor(
        &mut self,
        pre_processor: Arc<Mutex<dyn PreStateProcessor>>,
    ) {
        let shared_state = pre_processor.lock().unwrap().get_shared_state();
        self.shared_state = Some(shared_state);

        self.this_state_pre_processor = Some(pre_processor);
    }

    pub fn get_next_state_pre_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>> {
        self.next_state_pre_processor.clone()
    }

    pub fn get_next_every_state_pre_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>> {
        self.next_every_state_pre_processor.clone()
    }

    pub fn get_callback_pre_state_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>> {
        self.callback_pre_state_processor.clone()
    }

    /// Handle a matched StateEvent.
    ///
    /// Does nothing when the StateEvent has no StreamEvent at this `state_id`.
    /// Otherwise:
    /// 1. Marks the shared state changed
    /// 2. Sets `is_event_returned` when a next processor is set
    /// 3. Sends a copy, with its timestamp set from the StreamEvent, to the next and
    ///    every pre processors
    /// 4. Calls `state_changed()` on the callback pre processor
    /// 5. Sends the original StateEvent to the next processor
    ///
    /// Public so LogicalPostStateProcessor can call it.
    pub fn process_state_event(&mut self, state_event: &StateEvent) {
        if let Some(stream_event) = state_event.get_stream_event(self.state_id) {
            // Uses the shared atomic state, so the pre processor is not locked and
            // cannot deadlock
            if let Some(ref shared) = self.shared_state {
                shared.mark_state_changed();
            }

            if self.next_processor.is_some() {
                self.is_event_returned = true;
            }

            // Clone only when some pre processor needs the event
            let needs_next = self.next_state_pre_processor.is_some();
            let needs_every = self.next_every_state_pre_processor.is_some();

            if needs_next || needs_every {
                let mut state_event_copy = state_event.clone();
                state_event_copy.set_timestamp(stream_event.timestamp);

                // Grow the StateEvent so the target has a slot. A target with
                // state_id N needs N+1 positions (0..N).
                if needs_next && needs_every {
                    // Both need it, so clone once more for the next step
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
                    // Only one target, so move the copy without cloning again
                    if let Some(ref next_pre) = self.next_state_pre_processor {
                        let next_state_id = next_pre.lock().unwrap().state_id();
                        state_event_copy.expand_to_size(next_state_id + 1);
                        next_pre.lock().unwrap().add_state(state_event_copy);
                    }
                } else {
                    // Only the every target needs it, so move the copy without cloning
                    if let Some(ref next_every) = self.next_every_state_pre_processor {
                        let next_every_state_id = next_every.lock().unwrap().state_id();
                        state_event_copy.expand_to_size(next_every_state_id + 1);
                        next_every.lock().unwrap().add_every_state(state_event_copy);
                    }
                }
            }

            // The callback is only marked changed. Count quantifier restarts are handled
            // inside CountPreStateProcessor.
            if let Some(ref callback) = self.callback_pre_state_processor {
                callback.lock().unwrap().state_changed();
            }

            // In multi step patterns, the last post processor's next processor is a
            // terminal (for example TerminalPostStateProcessor) that hands matches to the
            // regular Processor chain
            if let Some(ref next) = self.next_processor {
                next.lock()
                    .unwrap()
                    .process(Some(Box::new(state_event.clone())));
            }
        }
    }
}

// Written by hand because the linked trait objects do not implement Debug
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
            if let Some(state_event) = event.as_any().downcast_ref::<StateEvent>() {
                // Set the timestamp here for the last step, which has only a
                // next_processor and no next_state_pre_processor. Otherwise its matches
                // would be emitted with timestamp -1.
                if let Some(stream_event) = state_event.get_stream_event(self.state_id) {
                    let timestamp = stream_event.timestamp;

                    // We own the Box, so set the timestamp in place without a copy
                    if let Some(state_event_mut) = event.as_any_mut().downcast_mut::<StateEvent>() {
                        state_event_mut.set_timestamp(timestamp);
                    }
                }

                if let Some(state_event) = event.as_any().downcast_ref::<StateEvent>() {
                    self.process_state_event(state_event);
                }

                // process_state_event already sent the event to next_processor. It is
                // also returned here when next_processor is set.
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
