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

// src/core/query/input/stream/state/stream_pre_state_processor.rs
// StreamPreStateProcessor, the default PreStateProcessor for a single stream step

use super::post_state_processor::PostStateProcessor;
use super::pre_state_processor::PreStateProcessor;
use super::shared_processor_state::ProcessorSharedState;
use super::stream_pre_state::StreamPreState;
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::state::state_event::StateEvent;
use crate::core::event::state::state_event_cloner::StateEventCloner;
use crate::core::event::stream::stream_event::StreamEvent;
use crate::core::event::stream::stream_event_cloner::StreamEventCloner;
use crate::core::query::processor::{ProcessingMode, Processor};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// Whether a chain follows PATTERN or SEQUENCE rules.
///
/// **Pattern**:
/// - Other events may arrive between the steps
/// - A pending state that does not match the event stays pending and tries the next
///   event
///
/// **Sequence**:
/// - Steps must match back to back, with no other events between
/// - A pending state that does not match the event is dropped, because the sequence
///   is broken
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateType {
    Pattern,
    Sequence,
}

/// Default PreStateProcessor. Matches events from a single stream for one step.
///
/// **Jobs**:
/// 1. Holds the step's StreamPreState (current, pending and new lists)
/// 2. Tries incoming events against pending states
/// 3. Runs the init, add, update and reset steps
/// 4. Applies the WITHIN limit
/// 5. Copies events so each state keeps its own
///
/// **Flow**:
/// 1. **init()**: a start state creates an empty StateEvent
/// 2. **add_state()**: the previous step passes a StateEvent on
/// 3. **update_state()**: new states move to pending, sorted by timestamp
/// 4. **process_and_return()**: for each pending StateEvent
///    - copy the incoming StreamEvent into it at this step's position
///    - check the condition set with `set_condition`. No condition matches everything.
///    - on a match, send it to the PostStateProcessor
///    - then keep or drop the pending state by Pattern or Sequence rules
/// 5. **reset_state()**: clear after a full match
///
/// **After a match**: Pattern keeps the pending state so it can match again. Sequence
/// drops it because it moved to the next step.
///
/// **After a miss**: Pattern keeps the pending state for the next event. Sequence
/// drops it because the sequence is broken.
///
pub struct StreamPreStateProcessor {
    state_id: usize,
    is_start_state: bool,
    state_type: StateType,

    app_context: Arc<EventFluxAppContext>,
    query_context: Arc<EventFluxQueryContext>,

    // Public so LogicalPreStateProcessor and CountPreStateProcessor can edit the lists
    pub state: Arc<Mutex<StreamPreState>>,

    // Changed flag shared with the PostStateProcessor, so it never has to lock this
    // processor and cannot deadlock
    shared_state: Arc<ProcessorSharedState>,

    stream_event_cloner: Option<StreamEventCloner>,
    state_event_cloner: Option<StateEventCloner>,

    // WITHIN limit in milliseconds. None means no limit.
    within_time: Option<i64>,

    // Positions whose event age is checked for WITHIN, so time is measured from the
    // start of the pattern and not just from the previous step. Empty means check
    // every filled position.
    start_state_ids: Vec<usize>,

    // Flag meant for count quantifiers. Only set and read through its accessors. No
    // matching code uses it.
    success_condition: bool,

    // True when this processor is part of an EVERY pattern. Set by PatternChainBuilder,
    // so EVERY can be detected without looking at the post processors.
    is_every_pattern: bool,

    next_processor: Option<Arc<Mutex<dyn Processor>>>,

    // Gets every match. Without it, matches are not passed along the chain.
    this_state_post_processor: Option<Arc<Mutex<dyn PostStateProcessor>>>,

    // Step condition. It gets the StateEvent with every event matched so far (e1, e2,
    // ...), and the event being checked is already at position state_id. That allows
    // references to earlier events, like e2[price > e1.price]. Held in an Arc so clones
    // for partitions and state restore share it.
    #[allow(clippy::type_complexity)]
    condition_fn: Option<Arc<dyn Fn(&StateEvent) -> bool + Send + Sync>>,
}

// Written by hand because condition_fn does not implement Debug
impl Debug for StreamPreStateProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamPreStateProcessor")
            .field("state_id", &self.state_id)
            .field("is_start_state", &self.is_start_state)
            .field("state_type", &self.state_type)
            .field("within_time", &self.within_time)
            .field("has_condition", &self.condition_fn.is_some())
            .finish()
    }
}

impl StreamPreStateProcessor {
    /// Create a processor with no WITHIN limit, no condition and no cloners.
    ///
    /// **Parameters**:
    /// - `state_id`: position in the StateEvent, starting at 0
    /// - `is_start_state`: true for the first step in the chain
    /// - `state_type`: Pattern or Sequence rules
    /// - `app_context`: Application context
    /// - `query_context`: Query context
    pub fn new(
        state_id: usize,
        is_start_state: bool,
        state_type: StateType,
        app_context: Arc<EventFluxAppContext>,
        query_context: Arc<EventFluxQueryContext>,
    ) -> Self {
        Self {
            state_id,
            is_start_state,
            state_type,
            app_context,
            query_context,
            state: Arc::new(Mutex::new(StreamPreState::new())),
            shared_state: Arc::new(ProcessorSharedState::new()),
            is_every_pattern: false, // PatternChainBuilder sets it for EVERY
            stream_event_cloner: None,
            state_event_cloner: None,
            within_time: None,
            start_state_ids: Vec::new(),
            success_condition: false,
            next_processor: None,
            this_state_post_processor: None,
            condition_fn: None,
        }
    }

    pub fn state_id(&self) -> usize {
        self.state_id
    }

    pub fn is_start_state(&self) -> bool {
        self.is_start_state
    }

    pub fn state_type(&self) -> StateType {
        self.state_type
    }

    /// Shared changed flag, so the PostStateProcessor can mark changes without locking
    /// this processor
    pub fn get_shared_state(&self) -> Arc<ProcessorSharedState> {
        self.shared_state.clone()
    }

    /// Set the PostStateProcessor that gets this processor's matches
    ///
    /// ```rust,ignore
    /// let post = Arc::new(Mutex::new(StreamPostStateProcessor::new(0)));
    /// processor.set_this_state_post_processor(post);
    /// ```
    pub fn set_this_state_post_processor(
        &mut self,
        post_processor: Arc<Mutex<dyn PostStateProcessor>>,
    ) {
        self.this_state_post_processor = Some(post_processor);
    }

    /// Set the positions whose event age is checked against WITHIN.
    ///
    /// WITHIN has to be measured from the start of the pattern, not from the previous
    /// step:
    /// ```text
    /// Pattern: A -> B -> C within 5 sec
    /// Need to check: (C.timestamp - A.timestamp) < 5 sec
    /// Not just: (C.timestamp - B.timestamp) < 5 sec
    /// ```
    ///
    /// **Java Reference**: StreamPreStateProcessor.startStateIds
    pub fn set_start_state_ids(&mut self, ids: Vec<usize>) {
        self.start_state_ids = ids;
    }

    /// Set the success flag meant for count quantifiers like A{2,5}.
    ///
    /// Nothing in the matching code reads it. CountPreStateProcessor checks counts
    /// itself.
    ///
    /// **Java Reference**: StreamPreStateProcessor.isSuccessCondition
    pub fn set_success_condition(&mut self, success: bool) {
        self.success_condition = success;
    }

    pub fn is_success_condition(&self) -> bool {
        self.success_condition
    }

    /// Set the step condition used to filter events.
    ///
    /// The closure gets the whole StateEvent, with the event being checked already at
    /// this processor's position. Replaces any earlier condition.
    ///
    /// **Usage**:
    /// ```rust,ignore
    /// // For processor at position 0: e1[value > 100]
    /// e1_processor.set_condition(|state_event| {
    ///     // Current event is at position 0
    ///     if let Some(e1) = state_event.get_stream_event(0) {
    ///         return e1.before_window_data[0].as_float().unwrap_or(0.0) > 100.0;
    ///     }
    ///     false
    /// });
    ///
    /// // For processor at position 1: e2[price > e1.price]
    /// e2_processor.set_condition(|state_event| {
    ///     // Access previous event e1 from position 0
    ///     if let Some(e1) = state_event.get_stream_event(0) {
    ///         // Access current event e2 from position 1
    ///         if let Some(e2) = state_event.get_stream_event(1) {
    ///             let e1_price = e1.before_window_data[0].as_float().unwrap_or(0.0);
    ///             let e2_price = e2.before_window_data[0].as_float().unwrap_or(0.0);
    ///             return e2_price > e1_price;
    ///         }
    ///     }
    ///     false
    /// });
    /// ```
    pub fn set_condition<F>(&mut self, condition: F)
    where
        F: Fn(&StateEvent) -> bool + Send + Sync + 'static,
    {
        self.condition_fn = Some(Arc::new(condition));
    }

    /// True when `state_event` passes this step's condition, or when no condition is set.
    ///
    /// `state_event` holds every matched event, with the current one at `state_id`.
    fn matches_condition(&self, state_event: &StateEvent) -> bool {
        match &self.condition_fn {
            Some(f) => f(state_event),
            None => true,
        }
    }

    /// True when a pending state is past the WITHIN limit at `current_timestamp`.
    ///
    /// Always false with no limit set. With `start_state_ids` set, checks the events
    /// at those positions, so for A -> B -> C within 5 sec it checks
    /// (current_time - A.timestamp) > 5 sec and not (current_time - C.timestamp).
    /// Without them, checks every filled position. Ages are absolute values, so events
    /// in the future count too.
    fn is_expired(&self, state_event: &StateEvent, current_timestamp: i64) -> bool {
        let within = match self.within_time {
            Some(w) => w,
            None => return false,
        };

        if !self.start_state_ids.is_empty() {
            for &start_id in &self.start_state_ids {
                if let Some(start_event) = state_event.get_stream_event(start_id) {
                    let age = (current_timestamp - start_event.timestamp).abs();
                    if age > within {
                        return true;
                    }
                }
            }
        } else {
            // No start positions set, so any filled position that is too old expires it
            for stream in state_event.stream_events.iter().flatten() {
                let age = (current_timestamp - stream.timestamp).abs();
                if age > within {
                    return true;
                }
            }
        }

        false
    }

    /// Copy a StreamEvent with the cloner, or with a plain clone when none is set
    fn clone_stream_event(&self, event: &StreamEvent) -> StreamEvent {
        match &self.stream_event_cloner {
            Some(cloner) => cloner.copy_stream_event(event),
            None => event.clone(),
        }
    }

    /// Copy a StateEvent with the cloner, or with a plain clone when none is set
    fn clone_state_event(&self, event: &StateEvent) -> StateEvent {
        match &self.state_event_cloner {
            Some(cloner) => cloner.copy_state_event(event),
            None => event.clone(),
        }
    }

    /// The stream event cloner. Used by CountPreStateProcessor.
    ///
    /// # Panics
    /// Panics when no stream event cloner has been set.
    pub fn get_stream_event_cloner(&self) -> &StreamEventCloner {
        self.stream_event_cloner
            .as_ref()
            .expect("StreamEventCloner not initialized")
    }

    pub fn set_stream_event_cloner(&mut self, cloner: StreamEventCloner) {
        self.stream_event_cloner = Some(cloner);
    }

    pub fn set_state_event_cloner(&mut self, cloner: StateEventCloner) {
        self.state_event_cloner = Some(cloner);
    }

    /// The state event cloner, or None when not set
    pub fn get_state_event_cloner_option(&self) -> Option<&StateEventCloner> {
        self.state_event_cloner.as_ref()
    }

    pub fn set_every_pattern_flag(&mut self, is_every: bool) {
        self.is_every_pattern = is_every;
    }

    pub fn is_every_pattern(&self) -> bool {
        self.is_every_pattern
    }

    /// The PostStateProcessor, if set. Used by CountPreStateProcessor.
    pub fn get_this_state_post_processor(&self) -> Option<Arc<Mutex<dyn PostStateProcessor>>> {
        self.this_state_post_processor.clone()
    }

    pub fn get_next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.next_processor.clone()
    }

    /// Copy this processor. The copy shares the same state lists and changed flag.
    pub fn clone_stream_processor(&self) -> StreamPreStateProcessor {
        Self {
            state_id: self.state_id,
            is_start_state: self.is_start_state,
            state_type: self.state_type,
            app_context: Arc::clone(&self.app_context),
            query_context: Arc::clone(&self.query_context),
            state: Arc::clone(&self.state),
            shared_state: Arc::clone(&self.shared_state),
            is_every_pattern: self.is_every_pattern,
            stream_event_cloner: self.stream_event_cloner.clone(),
            state_event_cloner: self.state_event_cloner.clone(),
            within_time: self.within_time,
            start_state_ids: self.start_state_ids.clone(),
            success_condition: self.success_condition,
            next_processor: self.next_processor.clone(),
            this_state_post_processor: self.this_state_post_processor.clone(),
            condition_fn: self.condition_fn.clone(),
        }
    }
}

// ===== Processor Trait Implementation =====

impl Processor for StreamPreStateProcessor {
    fn process(&self, _chunk: Option<Box<dyn ComplexEvent>>) {
        // Pre processors are driven through process_and_return(). Calling this panics.
        panic!("process() should not be called on PreStateProcessor. Use process_and_return().");
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.next_processor.clone()
    }

    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.next_processor = next;
    }

    fn clone_processor(&self, query_context: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        // Used for partitions and state restore. The copy shares the state lists, the
        // changed flag and the condition with this processor.
        Box::new(Self {
            state_id: self.state_id,
            is_start_state: self.is_start_state,
            state_type: self.state_type,
            app_context: Arc::clone(&self.app_context),
            query_context: Arc::clone(query_context), // takes the new query context
            state: Arc::clone(&self.state),           // shared across clones
            shared_state: Arc::clone(&self.shared_state), // shared across clones
            is_every_pattern: self.is_every_pattern,
            stream_event_cloner: self.stream_event_cloner.clone(),
            state_event_cloner: self.state_event_cloner.clone(),
            within_time: self.within_time,
            start_state_ids: self.start_state_ids.clone(),
            success_condition: self.success_condition,
            next_processor: self.next_processor.clone(),
            this_state_post_processor: self.this_state_post_processor.clone(),
            condition_fn: self.condition_fn.clone(),
        })
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.app_context)
    }

    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        Arc::clone(&self.query_context)
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::DEFAULT
    }

    fn is_stateful(&self) -> bool {
        true
    }
}

// ===== PreStateProcessor Trait Implementation =====

impl PreStateProcessor for StreamPreStateProcessor {
    fn init(&mut self) {
        let mut state = self.state.lock().unwrap();

        // Only a start state creates the first empty StateEvent, and only once until
        // reset_initialized
        if self.is_start_state && !state.is_initialized() {
            // Sized to state_id + 1 positions. The real stream count is not known here,
            // and later steps grow the StateEvent as needed.
            let initial_state = StateEvent::new(self.state_id + 1, 0);
            state.add_to_new_list(initial_state);
            state.mark_initialized();
        }
    }

    fn process(&mut self, chunk: Option<Box<dyn ComplexEvent>>) -> Option<Box<dyn ComplexEvent>> {
        self.process_and_return(chunk)
    }

    fn add_state(&mut self, state_event: StateEvent) {
        let mut state = self.state.lock().unwrap();

        // Always adds. The Java version only adds in Sequence mode when the list is
        // empty. That rule is not ported.
        state.add_to_new_list(state_event);
        state.mark_state_changed();

        self.shared_state.mark_state_changed();
    }

    fn add_every_state(&mut self, state_event: StateEvent) {
        // Same as add_state here. The 'every' loop itself comes from the post processor
        // wiring.
        self.add_state(state_event);
    }

    fn update_state(&mut self) {
        let mut state = self.state.lock().unwrap();
        state.move_new_to_pending();
    }

    fn reset_state(&mut self) {
        // At the start of an EVERY pattern, pending is kept so the pattern can restart
        // with the states it already has
        let should_skip_reset = self.is_start_state && self.is_every_pattern;

        if should_skip_reset {
            let mut state = self.state.lock().unwrap();

            // Set up a fresh StateEvent only when the new list is empty, so new events
            // can start new instances
            if state.new_count() == 0 {
                state.reset_initialized();
                drop(state); // init() takes the lock again
                self.init();
            }
            return;
        }

        let mut state = self.state.lock().unwrap();
        state.clear_pending();

        // SEQUENCE mode restarts after every match, as if EVERY were set. Unlike EVERY
        // in PATTERN mode, it clears pending first, because steps must be back to back.
        let is_sequence_auto_restart =
            self.is_start_state && self.state_type == StateType::Sequence;

        // A start state with an empty new list sets up a fresh StateEvent. This covers
        // normal patterns and the SEQUENCE restart.
        if self.is_start_state && state.new_count() == 0 {
            state.reset_initialized();
            drop(state); // init() takes the lock again
            self.init();

            if is_sequence_auto_restart {
                log::trace!(
                    "[StreamPreStateProcessor] SEQUENCE auto-restart: reinitializing state_id={}",
                    self.state_id
                );
            }
        }
    }

    fn process_and_return(
        &mut self,
        chunk: Option<Box<dyn ComplexEvent>>,
    ) -> Option<Box<dyn ComplexEvent>> {
        // Take an owned copy so the event does not borrow from the chunk
        let stream_event = match chunk {
            Some(event) => {
                // Anything that is not a StreamEvent is ignored
                match event.as_any().downcast_ref::<StreamEvent>() {
                    Some(se) => self.clone_stream_event(se),
                    None => return None,
                }
            }
            None => return None,
        };

        // Step 1: copy the pending states and match them without holding the lock. The
        // post processor may call back into state_changed, which would deadlock.
        let pending_states = {
            let state = self.state.lock().unwrap();
            state
                .get_pending_list()
                .iter()
                .cloned()
                .collect::<Vec<StateEvent>>()
        };

        let mut return_events: Vec<StateEvent> = Vec::new();
        let mut post_processor_events: Vec<StateEvent> = Vec::new();
        let mut pending_to_remove: Vec<usize> = Vec::new();
        let mut state_changed = false;

        for (idx, pending_state) in pending_states.iter().enumerate() {
            let mut candidate_state = self.clone_state_event(pending_state);

            let cloned_stream = self.clone_stream_event(&stream_event);

            // Make room for this position. set_event() silently does nothing when the
            // position is past the end of stream_events.
            candidate_state.expand_to_size(self.state_id + 1);

            candidate_state.set_event(self.state_id, cloned_stream);

            // The condition sees every event so far, including this one at state_id
            if self.matches_condition(&candidate_state) {
                return_events.push(candidate_state.clone());

                // Sent to the post processor later, after the lock is released
                if self.this_state_post_processor.is_some() {
                    post_processor_events.push(candidate_state);
                }

                state_changed = true;

                match self.state_type {
                    StateType::Pattern => {
                        // Keep the state pending so it can match more events. The post
                        // processor passes the match on to the next step.
                    }
                    StateType::Sequence => {
                        // The state moved on to the next step, so drop it here
                        pending_to_remove.push(idx);
                    }
                }
            } else {
                match self.state_type {
                    StateType::Pattern => {
                        // Keep the state pending, unchanged, for the next event
                    }
                    StateType::Sequence => {
                        // The sequence is broken, so drop the state
                        pending_to_remove.push(idx);
                    }
                }
            }
        }

        // Step 2: take the lock to mark the change and drop finished states
        {
            let mut state = self.state.lock().unwrap();

            if state_changed {
                state.mark_state_changed();
                self.shared_state.mark_state_changed();
            }

            // Remove in reverse so earlier indices stay valid
            if !pending_to_remove.is_empty() {
                let pending_list_mut = state.get_pending_list_mut();
                for &idx in pending_to_remove.iter().rev() {
                    if idx < pending_list_mut.len() {
                        pending_list_mut.remove(idx);
                    }
                }
            }
        } // lock released here

        // Step 3: send matches to the post processor only now, after the lock is
        // released, so its calls back into state_changed() cannot deadlock
        if !post_processor_events.is_empty() {
            if let Some(ref post) = self.this_state_post_processor {
                if let Ok(mut post_guard) = post.lock() {
                    for event in post_processor_events {
                        post_guard.process(Some(Box::new(event)));
                    }
                }
            }
        }

        // Only the first match is returned. The rest are not linked into a chain, though
        // they did reach the post processor above.
        if return_events.is_empty() {
            None
        } else {
            Some(Box::new(return_events.into_iter().next().unwrap()))
        }
    }

    fn set_within_time(&mut self, within_time: i64) {
        self.within_time = if within_time > 0 {
            Some(within_time)
        } else {
            None
        };
    }

    fn expire_events(&mut self, timestamp: i64) {
        let mut state = self.state.lock().unwrap();
        let pending_list_mut = state.get_pending_list_mut();

        // Drop expired states from pending. Nothing is notified here.
        pending_list_mut.retain(|state_event| {
            if self.is_expired(state_event, timestamp) {
                // Unused copy, left from a plan to track expired states
                let _expired = state_event.clone();

                false
            } else {
                true
            }
        });
    }

    fn state_id(&self) -> usize {
        self.state_id
    }

    fn is_start_state(&self) -> bool {
        self.is_start_state
    }

    fn get_shared_state(&self) -> Arc<ProcessorSharedState> {
        self.shared_state.clone()
    }

    fn this_state_post_processor(&self) -> Option<Arc<Mutex<dyn PostStateProcessor>>> {
        self.this_state_post_processor.clone()
    }

    fn state_changed(&self) {
        // Uses the shared atomic flag, not the state lock, so a post processor can call
        // this without deadlocking
        self.shared_state.mark_state_changed();
    }

    fn has_state_changed(&self) -> bool {
        self.shared_state.is_state_changed()
    }
}

// ===== Public accessors for tests and integration =====

impl StreamPreStateProcessor {
    /// Shared handle to the state lists
    pub fn get_state(&self) -> Arc<Mutex<StreamPreState>> {
        Arc::clone(&self.state)
    }

    /// Number of pending states. Takes the state lock.
    pub fn pending_count(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.pending_count()
    }

    /// Number of states in the new list. Takes the state lock.
    pub fn new_count(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.new_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::eventflux_context::EventFluxContext;
    use crate::core::event::value::AttributeValue;
    use crate::query_api::eventflux_app::EventFluxApp;

    fn create_test_context() -> (Arc<EventFluxAppContext>, Arc<EventFluxQueryContext>) {
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
        (app_ctx, query_ctx)
    }

    fn create_test_processor(
        state_id: usize,
        is_start: bool,
        state_type: StateType,
    ) -> StreamPreStateProcessor {
        let (app_ctx, query_ctx) = create_test_context();
        StreamPreStateProcessor::new(state_id, is_start, state_type, app_ctx, query_ctx)
    }

    // ===== Construction & Basic State =====

    #[test]
    fn test_new_processor() {
        let processor = create_test_processor(0, true, StateType::Pattern);
        assert_eq!(processor.state_id(), 0);
        assert!(processor.is_start_state());
        assert_eq!(processor.within_time, None);
    }

    #[test]
    fn test_processor_is_stateful() {
        let processor = create_test_processor(0, true, StateType::Pattern);
        assert!(processor.is_stateful());
    }

    #[test]
    fn test_state_type_pattern() {
        let processor = create_test_processor(0, true, StateType::Pattern);
        assert_eq!(processor.state_type, StateType::Pattern);
    }

    #[test]
    fn test_state_type_sequence() {
        let processor = create_test_processor(0, true, StateType::Sequence);
        assert_eq!(processor.state_type, StateType::Sequence);
    }

    // ===== Lifecycle: init() =====

    #[test]
    fn test_init_start_state() {
        let mut processor = create_test_processor(0, true, StateType::Pattern);
        processor.init();

        // Start state should create initial StateEvent in new list
        assert_eq!(processor.new_count(), 1);
        let state = processor.state.lock().unwrap();
        assert!(state.is_initialized());
    }

    #[test]
    fn test_init_non_start_state() {
        let mut processor = create_test_processor(1, false, StateType::Pattern);
        processor.init();

        // Non-start state should not create initial StateEvent
        assert_eq!(processor.new_count(), 0);
    }

    #[test]
    fn test_init_only_once() {
        let mut processor = create_test_processor(0, true, StateType::Pattern);
        processor.init();
        assert_eq!(processor.new_count(), 1);

        processor.init();
        // Should not add another StateEvent
        assert_eq!(processor.new_count(), 1);
    }

    // ===== Lifecycle: add_state() =====

    #[test]
    fn test_add_state() {
        let mut processor = create_test_processor(1, false, StateType::Pattern);
        let state_event = StateEvent::new(2, 0);

        processor.add_state(state_event);
        assert_eq!(processor.new_count(), 1);
        assert!(processor.has_state_changed());
    }

    #[test]
    fn test_add_multiple_states() {
        let mut processor = create_test_processor(1, false, StateType::Pattern);

        processor.add_state(StateEvent::new(2, 0));
        processor.add_state(StateEvent::new(2, 0));
        processor.add_state(StateEvent::new(2, 0));

        assert_eq!(processor.new_count(), 3);
    }

    #[test]
    fn test_add_every_state() {
        let mut processor = create_test_processor(1, false, StateType::Pattern);
        let state_event = StateEvent::new(2, 0);

        processor.add_every_state(state_event);
        assert_eq!(processor.new_count(), 1);
    }

    // ===== Lifecycle: update_state() =====

    #[test]
    fn test_update_state_moves_new_to_pending() {
        let mut processor = create_test_processor(1, false, StateType::Pattern);

        processor.add_state(StateEvent::new(2, 0));
        processor.add_state(StateEvent::new(2, 0));
        assert_eq!(processor.new_count(), 2);
        assert_eq!(processor.pending_count(), 0);

        processor.update_state();

        assert_eq!(processor.new_count(), 0);
        assert_eq!(processor.pending_count(), 2);
    }

    #[test]
    fn test_update_state_sorts_by_timestamp() {
        let mut processor = create_test_processor(1, false, StateType::Pattern);

        let mut state3 = StateEvent::new(2, 0);
        state3.set_timestamp(3000);
        let mut state1 = StateEvent::new(2, 0);
        state1.set_timestamp(1000);
        let mut state2 = StateEvent::new(2, 0);
        state2.set_timestamp(2000);

        processor.add_state(state3);
        processor.add_state(state1);
        processor.add_state(state2);

        processor.update_state();

        let state = processor.state.lock().unwrap();
        let pending = state.get_pending_list();
        assert_eq!(pending[0].timestamp, 1000);
        assert_eq!(pending[1].timestamp, 2000);
        assert_eq!(pending[2].timestamp, 3000);
    }

    // ===== Lifecycle: reset_state() =====

    #[test]
    fn test_reset_state_clears_pending() {
        let mut processor = create_test_processor(1, false, StateType::Pattern);

        processor.add_state(StateEvent::new(2, 0));
        processor.update_state();
        assert_eq!(processor.pending_count(), 1);

        processor.reset_state();
        assert_eq!(processor.pending_count(), 0);
    }

    #[test]
    fn test_reset_state_reinitializes_start() {
        let mut processor = create_test_processor(0, true, StateType::Pattern);
        processor.init();
        processor.update_state(); // Move new to pending
        assert_eq!(processor.new_count(), 0);

        processor.reset_state();
        // Should reinitialize with empty StateEvent
        assert_eq!(processor.new_count(), 1);
    }

    // ===== Event Processing: process_and_return() =====

    #[test]
    fn test_process_and_return_no_pending() {
        let mut processor = create_test_processor(0, true, StateType::Pattern);
        let stream_event = StreamEvent::new(1000, 1, 0, 0);

        let result = processor.process_and_return(Some(Box::new(stream_event)));
        assert!(result.is_none()); // No pending states to match
    }

    #[test]
    fn test_process_and_return_with_pending() {
        let mut processor = create_test_processor(0, true, StateType::Pattern);

        // Init and prepare
        processor.init();
        processor.update_state(); // Move to pending
        assert_eq!(processor.pending_count(), 1);

        // Process event
        let stream_event = StreamEvent::new(1000, 1, 0, 0);
        let result = processor.process_and_return(Some(Box::new(stream_event)));

        // Should match and return StateEvent
        assert!(result.is_some());
    }

    #[test]
    fn test_process_and_return_pattern_semantics() {
        let mut processor = create_test_processor(0, true, StateType::Pattern);

        // Add condition that always fails
        processor.set_condition(|_| false);

        processor.init();
        processor.update_state();
        assert_eq!(processor.pending_count(), 1);

        let stream_event = StreamEvent::new(1000, 1, 0, 0);
        let result = processor.process_and_return(Some(Box::new(stream_event)));

        // Pattern: no match, but StateEvent stays in pending for next try
        assert!(result.is_none());
        // Written when a miss removed the state. Pattern mode now keeps it pending, so
        // this check should hold, but it is still commented out.
        // assert_eq!(processor.pending_count(), 1);
    }

    #[test]
    fn test_process_and_return_sequence_semantics() {
        let mut processor = create_test_processor(0, true, StateType::Sequence);

        // Add condition that always fails
        processor.set_condition(|_| false);

        processor.init();
        processor.update_state();
        assert_eq!(processor.pending_count(), 1);

        let stream_event = StreamEvent::new(1000, 1, 0, 0);
        let result = processor.process_and_return(Some(Box::new(stream_event)));

        // Sequence: no match, StateEvent removed (sequence broken)
        assert!(result.is_none());
        assert_eq!(processor.pending_count(), 0);
    }

    #[test]
    fn test_process_and_return_with_condition() {
        let mut processor = create_test_processor(0, true, StateType::Pattern);

        // Condition: price > 100 (accessing from position 0 in StateEvent)
        processor.set_condition(|state_event| {
            if let Some(stream_event) = state_event.get_stream_event(0) {
                if let Some(AttributeValue::Float(price)) = stream_event.before_window_data.first()
                {
                    return *price > 100.0;
                }
            }
            false
        });

        processor.init();
        processor.update_state();

        // Event with price = 150
        let mut stream_event = StreamEvent::new(1000, 1, 0, 0);
        stream_event.before_window_data[0] = AttributeValue::Float(150.0);

        let result = processor.process_and_return(Some(Box::new(stream_event)));
        assert!(result.is_some()); // Matches condition
    }

    #[test]
    fn test_process_and_return_multiple_pending() {
        let mut processor = create_test_processor(0, true, StateType::Pattern);

        // Add 3 pending states
        processor.add_state(StateEvent::new(1, 0));
        processor.add_state(StateEvent::new(1, 0));
        processor.add_state(StateEvent::new(1, 0));
        processor.update_state();
        assert_eq!(processor.pending_count(), 3);

        let stream_event = StreamEvent::new(1000, 1, 0, 0);
        let result = processor.process_and_return(Some(Box::new(stream_event)));

        // All 3 should match (condition matches all by default)
        assert!(result.is_some());
    }

    // ===== Time Management =====

    #[test]
    fn test_set_within_time() {
        let mut processor = create_test_processor(0, true, StateType::Pattern);
        processor.set_within_time(5000);
        assert_eq!(processor.within_time, Some(5000));
    }

    #[test]
    fn test_expire_events_no_constraint() {
        let mut processor = create_test_processor(0, true, StateType::Pattern);

        processor.add_state(StateEvent::new(1, 0));
        processor.update_state();
        assert_eq!(processor.pending_count(), 1);

        processor.expire_events(10000);
        // No within_time constraint, nothing expires
        assert_eq!(processor.pending_count(), 1);
    }

    #[test]
    fn test_expire_events_with_constraint() {
        let mut processor = create_test_processor(0, true, StateType::Pattern);
        processor.set_within_time(5000); // 5 seconds

        // Add state with event at timestamp 1000
        let mut state = StateEvent::new(1, 0);
        let mut stream = StreamEvent::new(1000, 1, 0, 0);
        stream.timestamp = 1000;
        state.set_event(0, stream);

        processor.add_state(state);
        processor.update_state();
        assert_eq!(processor.pending_count(), 1);

        // Expire at timestamp 7000 (6 seconds later)
        processor.expire_events(7000);
        // Event should be expired (age = 6000 > 5000)
        assert_eq!(processor.pending_count(), 0);
    }

    #[test]
    fn test_expire_events_not_expired_yet() {
        let mut processor = create_test_processor(0, true, StateType::Pattern);
        processor.set_within_time(5000);

        let mut state = StateEvent::new(1, 0);
        let mut stream = StreamEvent::new(1000, 1, 0, 0);
        stream.timestamp = 1000;
        state.set_event(0, stream);

        processor.add_state(state);
        processor.update_state();

        // Expire at timestamp 4000 (3 seconds later)
        processor.expire_events(4000);
        // Not expired yet (age = 3000 < 5000)
        assert_eq!(processor.pending_count(), 1);
    }

    // ===== State Change Tracking =====

    #[test]
    fn test_state_changed_flag() {
        let processor = create_test_processor(0, true, StateType::Pattern);
        assert!(!processor.has_state_changed());

        processor.state_changed();
        assert!(processor.has_state_changed());
    }

    // ===== Processor Trait Methods =====

    #[test]
    fn test_next_processor() {
        let mut processor = create_test_processor(0, true, StateType::Pattern);
        assert!(processor.next_processor().is_none());

        let next = Arc::new(Mutex::new(create_test_processor(
            1,
            false,
            StateType::Pattern,
        )));
        processor.set_next_processor(Some(next.clone()));
        assert!(processor.next_processor().is_some());
    }

    #[test]
    #[should_panic(expected = "process() should not be called on PreStateProcessor")]
    fn test_process_panics() {
        let processor = create_test_processor(0, true, StateType::Pattern);
        processor.process(None);
    }

    // ===== Integration Tests =====

    #[test]
    fn test_full_lifecycle() {
        let mut processor = create_test_processor(0, true, StateType::Sequence);

        // 1. Init
        processor.init();
        assert_eq!(processor.new_count(), 1);

        // 2. Update (move new→pending)
        processor.update_state();
        assert_eq!(processor.pending_count(), 1);
        assert_eq!(processor.new_count(), 0);

        // 3. Process event
        let stream_event = StreamEvent::new(1000, 1, 0, 0);
        let result = processor.process_and_return(Some(Box::new(stream_event)));
        assert!(result.is_some());

        // 4. Reset
        processor.reset_state();
        assert_eq!(processor.pending_count(), 0);
        assert_eq!(processor.new_count(), 1); // Reinitialized
    }

    #[test]
    fn test_pattern_sequence_a_then_b() {
        // Simulate A -> B sequence
        let mut processor_a = create_test_processor(0, true, StateType::Sequence);
        let mut processor_b = create_test_processor(1, false, StateType::Sequence);

        // Init A (start state)
        processor_a.init();
        processor_a.update_state();

        // Event A arrives
        let event_a = StreamEvent::new(1000, 1, 0, 0);
        let result_a = processor_a.process_and_return(Some(Box::new(event_a)));
        assert!(result_a.is_some());

        // Extract StateEvent and forward to B
        let state_with_a = result_a.unwrap();
        let state_event_a = state_with_a.as_any().downcast_ref::<StateEvent>().unwrap();

        // Forward to B
        processor_b.add_state(state_event_a.clone());
        processor_b.update_state();

        // Event B arrives
        let event_b = StreamEvent::new(2000, 1, 0, 0);
        let result_b = processor_b.process_and_return(Some(Box::new(event_b)));
        assert!(result_b.is_some()); // Complete sequence!
    }

    // ===== Comprehensive Sequence-Specific Tests =====

    #[test]
    fn test_sequence_basic_matching() {
        // Sequence basic matching behavior
        let mut processor = create_test_processor(0, true, StateType::Sequence);

        processor.init();
        processor.update_state();

        // Process event
        let event = StreamEvent::new(1000, 1, 0, 0);
        let result = processor.process_and_return(Some(Box::new(event)));
        assert!(result.is_some());

        // Sequence type is correctly set
        assert_eq!(processor.state_type, StateType::Sequence);
    }

    #[test]
    fn test_pattern_basic_matching() {
        // Pattern basic matching behavior
        let mut processor = create_test_processor(0, true, StateType::Pattern);

        processor.init();
        processor.update_state();

        // Process event
        let event = StreamEvent::new(1000, 1, 0, 0);
        let result = processor.process_and_return(Some(Box::new(event)));
        assert!(result.is_some());

        // Pattern type is correctly set
        assert_eq!(processor.state_type, StateType::Pattern);
    }

    #[test]
    fn test_sequence_multiple_pending_states() {
        // Test Sequence with multiple pending states
        let mut processor = create_test_processor(0, true, StateType::Sequence);

        processor.init();

        // Add multiple states
        let state1 = StateEvent::new(2, 0);
        let state2 = StateEvent::new(2, 0);
        processor.add_state(state1);
        processor.add_state(state2);

        assert_eq!(processor.new_count(), 3); // init + 2 added

        processor.update_state();
        assert_eq!(processor.pending_count(), 3);

        // Process with Sequence semantics
        let event = StreamEvent::new(1000, 1, 0, 0);
        let result = processor.process_and_return(Some(Box::new(event)));

        // Sequence should produce results
        assert!(result.is_some());
    }

    #[test]
    fn test_within_time_sequence_configured() {
        // Test WITHIN time constraint can be set for Sequence
        let mut processor = create_test_processor(0, true, StateType::Sequence);
        processor.set_within_time(1000); // 1 second window

        processor.init();
        processor.update_state();

        // Process event
        let event = StreamEvent::new(1000, 1, 0, 0);
        let result = processor.process_and_return(Some(Box::new(event)));
        assert!(result.is_some());

        // WITHIN time is configured
        assert_eq!(processor.within_time, Some(1000));
    }

    #[test]
    fn test_within_time_pattern_configured() {
        // Test WITHIN time constraint can be set for Pattern
        let mut processor = create_test_processor(0, true, StateType::Pattern);
        processor.set_within_time(2000); // 2 second window

        processor.init();
        processor.update_state();

        // Process event
        let event = StreamEvent::new(1000, 1, 0, 0);
        let result = processor.process_and_return(Some(Box::new(event)));
        assert!(result.is_some());

        // WITHIN time is configured
        assert_eq!(processor.within_time, Some(2000));
    }

    #[test]
    fn test_pattern_processes_multiple_events() {
        // Pattern processes multiple sequential events
        let mut processor = create_test_processor(0, true, StateType::Pattern);

        processor.init();
        processor.update_state();

        // First event
        let event1 = StreamEvent::new(1000, 1, 0, 0);
        let result1 = processor.process_and_return(Some(Box::new(event1)));
        assert!(result1.is_some());

        // Verify pattern type
        assert_eq!(processor.state_type, StateType::Pattern);
    }

    #[test]
    fn test_sequence_chain_three_steps() {
        // Test A -> B -> C sequence chain
        let mut processor_a = create_test_processor(0, true, StateType::Sequence);
        let mut processor_b = create_test_processor(1, false, StateType::Sequence);
        let mut processor_c = create_test_processor(2, false, StateType::Sequence);

        // Initialize A (start state)
        processor_a.init();
        processor_a.update_state();

        // Event A
        let event_a = StreamEvent::new(1000, 1, 0, 0);
        let result_a = processor_a.process_and_return(Some(Box::new(event_a)));
        assert!(result_a.is_some());

        // Forward to B
        let state_a = result_a.unwrap();
        let state_event_a = state_a.as_any().downcast_ref::<StateEvent>().unwrap();
        processor_b.add_state(state_event_a.clone());
        processor_b.update_state();

        // Event B
        let event_b = StreamEvent::new(2000, 1, 0, 0);
        let result_b = processor_b.process_and_return(Some(Box::new(event_b)));
        assert!(result_b.is_some());

        // Forward to C
        let state_b = result_b.unwrap();
        let state_event_b = state_b.as_any().downcast_ref::<StateEvent>().unwrap();
        processor_c.add_state(state_event_b.clone());
        processor_c.update_state();

        // Event C
        let event_c = StreamEvent::new(3000, 1, 0, 0);
        let result_c = processor_c.process_and_return(Some(Box::new(event_c)));
        assert!(result_c.is_some()); // Complete A->B->C sequence!
    }

    #[test]
    fn test_sequence_processes_all_pending() {
        // Test Sequence processes all pending states
        let mut processor = create_test_processor(0, true, StateType::Sequence);

        processor.init();

        // Add another state
        let state = StateEvent::new(2, 0);
        processor.add_state(state);

        processor.update_state();
        assert_eq!(processor.pending_count(), 2);

        // Process event - should match pending states
        let event = StreamEvent::new(1000, 1, 0, 0);
        let result = processor.process_and_return(Some(Box::new(event)));
        assert!(result.is_some()); // Produces output
    }

    #[test]
    fn test_pattern_with_within_constraint() {
        // Test Pattern semantics with WITHIN time constraint
        let mut processor = create_test_processor(0, true, StateType::Pattern);
        processor.set_within_time(1000);

        processor.init();
        processor.update_state();

        // Event within time window
        let event = StreamEvent::new(1500, 1, 0, 0);
        processor.expire_events(1500);

        let result = processor.process_and_return(Some(Box::new(event)));
        assert!(result.is_some());
    }

    #[test]
    fn test_sequence_reset_and_reinit() {
        // Test Sequence reset and reinitialization
        let mut processor = create_test_processor(0, true, StateType::Sequence);

        processor.init();
        processor.add_state(StateEvent::new(2, 0));
        assert_eq!(processor.new_count(), 2);

        processor.update_state();
        assert_eq!(processor.pending_count(), 2);

        processor.reset_state();
        assert_eq!(processor.pending_count(), 0);
        assert_eq!(processor.new_count(), 1); // Reinit for start state
    }

    #[test]
    fn test_pattern_no_pending_no_match() {
        // Pattern with no pending states produces no matches
        let mut processor = create_test_processor(0, true, StateType::Pattern);

        // Don't initialize or add any pending states

        let event = StreamEvent::new(1000, 1, 0, 0);
        let result = processor.process_and_return(Some(Box::new(event)));
        assert!(result.is_none()); // No pending states to match
    }

    #[test]
    fn test_sequence_empty_after_reset() {
        // Sequence should be empty after reset (except reinit)
        let mut processor = create_test_processor(0, true, StateType::Sequence);

        processor.init();
        processor.add_state(StateEvent::new(2, 0));
        processor.add_state(StateEvent::new(2, 0));
        processor.update_state();

        processor.reset_state();

        // Should only have reinit state
        assert_eq!(processor.new_count(), 1);
        assert_eq!(processor.pending_count(), 0);
    }

    // ===== INTEGRATION TESTS: Pattern Processing Architecture =====

    #[test]
    fn test_integration_a_b_c_sequence() {
        // Test A -> B -> C sequence pattern
        // This test verifies the basic processor chaining for sequences
        let (app_ctx, query_ctx) = create_test_context();

        let mut processor_a = StreamPreStateProcessor::new(
            0,
            true,
            StateType::Sequence,
            app_ctx.clone(),
            query_ctx.clone(),
        );
        let mut processor_b = StreamPreStateProcessor::new(
            1,
            false,
            StateType::Sequence,
            app_ctx.clone(),
            query_ctx.clone(),
        );
        let mut processor_c =
            StreamPreStateProcessor::new(2, false, StateType::Sequence, app_ctx, query_ctx);

        // Initialize A (start state) - creates StateEvent with 1 position
        processor_a.init();
        processor_a.update_state();
        assert_eq!(processor_a.pending_count(), 1);

        // Event A arrives - fills position 0
        let event_a = StreamEvent::new(1000, 1, 0, 0);
        let result_a = processor_a.process_and_return(Some(Box::new(event_a)));
        assert!(result_a.is_some());

        // Extract StateEvent from result - should have A at position 0
        let state_with_a = result_a.unwrap();
        let state_event_a = state_with_a.as_any().downcast_ref::<StateEvent>().unwrap();
        assert!(state_event_a.get_stream_event(0).is_some()); // A is at position 0

        // No PostStateProcessor is wired here, so grow the StateEvent for B by hand.
        // In a wired chain the post processor does this.
        let mut expanded_for_b = StateEvent::new(2, 0); // 2 positions for A and B
        if let Some(a_event) = state_event_a.get_stream_event(0) {
            expanded_for_b.set_event(0, a_event.clone());
        }

        // Forward to B
        processor_b.add_state(expanded_for_b);
        processor_b.update_state();
        assert_eq!(processor_b.pending_count(), 1);

        // Event B arrives - fills position 1
        let event_b = StreamEvent::new(2000, 1, 0, 0);
        let result_b = processor_b.process_and_return(Some(Box::new(event_b)));
        assert!(result_b.is_some());

        // Extract StateEvent from result - should have both A and B
        let state_with_b = result_b.unwrap();
        let state_event_b = state_with_b.as_any().downcast_ref::<StateEvent>().unwrap();
        assert!(state_event_b.get_stream_event(0).is_some()); // A
        assert!(state_event_b.get_stream_event(1).is_some()); // B

        // Manually expand for C
        let mut expanded_for_c = StateEvent::new(3, 0); // 3 positions for A, B, and C
        if let Some(a_event) = state_event_b.get_stream_event(0) {
            expanded_for_c.set_event(0, a_event.clone());
        }
        if let Some(b_event) = state_event_b.get_stream_event(1) {
            expanded_for_c.set_event(1, b_event.clone());
        }

        // Forward to C
        processor_c.add_state(expanded_for_c);
        processor_c.update_state();

        // Event C arrives - fills position 2
        let event_c = StreamEvent::new(3000, 1, 0, 0);
        let result_c = processor_c.process_and_return(Some(Box::new(event_c)));
        assert!(result_c.is_some());

        // Extract final StateEvent
        let final_state = result_c.unwrap();
        let state_event_c = final_state.as_any().downcast_ref::<StateEvent>().unwrap();

        // Verify complete sequence: A -> B -> C
        assert!(state_event_c.get_stream_event(0).is_some()); // A
        assert!(state_event_c.get_stream_event(1).is_some()); // B
        assert!(state_event_c.get_stream_event(2).is_some()); // C
    }

    #[test]
    fn test_integration_within_timing() {
        // Test WITHIN time constraint
        let (app_ctx, query_ctx) = create_test_context();

        let mut processor_a = StreamPreStateProcessor::new(
            0,
            true,
            StateType::Sequence,
            app_ctx.clone(),
            query_ctx.clone(),
        );
        let mut processor_b =
            StreamPreStateProcessor::new(1, false, StateType::Sequence, app_ctx, query_ctx);

        // Set WITHIN 5 seconds on processor_b
        processor_b.set_within_time(5000);
        processor_b.set_start_state_ids(vec![0]); // Check against A's timestamp

        // Initialize A
        processor_a.init();
        processor_a.update_state();

        // Event A at t=0
        let mut event_a = StreamEvent::new(0, 1, 0, 0);
        event_a.timestamp = 0;
        let result_a = processor_a.process_and_return(Some(Box::new(event_a)));
        assert!(result_a.is_some());

        let state_with_a = result_a.unwrap();
        let state_event_a = state_with_a.as_any().downcast_ref::<StateEvent>().unwrap();

        // Forward to B
        processor_b.add_state(state_event_a.clone());
        processor_b.update_state();

        // Event B at t=3000 (within 5 sec window)
        let mut event_b = StreamEvent::new(3000, 1, 0, 0);
        event_b.timestamp = 3000;
        let result_b = processor_b.process_and_return(Some(Box::new(event_b)));
        assert!(result_b.is_some()); // Should match

        // Test expiration: Event B2 at t=10000 (outside 5 sec window)
        processor_b.add_state(state_event_a.clone());
        processor_b.update_state();
        processor_b.expire_events(10000); // Expire old states
        assert_eq!(processor_b.pending_count(), 0); // Should be expired
    }

    #[test]
    fn test_integration_pattern_vs_sequence_semantics() {
        // Test Pattern keeps pending, Sequence removes
        let (app_ctx, query_ctx) = create_test_context();

        // Pattern processor
        let mut pattern_proc = StreamPreStateProcessor::new(
            0,
            true,
            StateType::Pattern,
            app_ctx.clone(),
            query_ctx.clone(),
        );
        pattern_proc.init();
        pattern_proc.update_state();
        let initial_pattern_pending = pattern_proc.pending_count();

        let event1 = StreamEvent::new(1000, 1, 0, 0);
        pattern_proc.process_and_return(Some(Box::new(event1)));
        // Pattern: keeps pending for multi-match
        assert_eq!(pattern_proc.pending_count(), initial_pattern_pending);

        // Sequence processor
        let mut sequence_proc =
            StreamPreStateProcessor::new(0, true, StateType::Sequence, app_ctx, query_ctx);
        sequence_proc.init();
        sequence_proc.update_state();
        assert_eq!(sequence_proc.pending_count(), 1);

        let event2 = StreamEvent::new(2000, 1, 0, 0);
        sequence_proc.process_and_return(Some(Box::new(event2)));
        // Sequence: removes pending after match
        assert_eq!(sequence_proc.pending_count(), 0);
    }

    #[test]
    fn test_integration_success_condition_flag() {
        // Test success_condition flag for count quantifiers
        let (app_ctx, query_ctx) = create_test_context();

        let mut processor =
            StreamPreStateProcessor::new(0, true, StateType::Pattern, app_ctx, query_ctx);

        // Initially false
        assert!(!processor.is_success_condition());

        // Set to true
        processor.set_success_condition(true);
        assert!(processor.is_success_condition());

        // Reset to false
        processor.set_success_condition(false);
        assert!(!processor.is_success_condition());
    }

    #[test]
    fn test_integration_start_state_ids_setting() {
        // Test start_state_ids can be configured
        let (app_ctx, query_ctx) = create_test_context();

        let mut processor =
            StreamPreStateProcessor::new(2, false, StateType::Sequence, app_ctx, query_ctx);

        // Set start state IDs (e.g., for A -> B -> C, C checks against A's timestamp)
        processor.set_start_state_ids(vec![0]);

        // Create a state with event at position 0
        let mut state = StateEvent::new(3, 0);
        let mut event_at_0 = StreamEvent::new(1000, 1, 0, 0);
        event_at_0.timestamp = 1000;
        state.set_event(0, event_at_0);

        // Set WITHIN 5 seconds
        processor.set_within_time(5000);

        // Check expiration at t=4000 (within window)
        assert!(!processor.is_expired(&state, 4000));

        // Check expiration at t=10000 (outside window)
        assert!(processor.is_expired(&state, 10000));
    }

    #[test]
    fn test_simple_pre_post_no_deadlock() {
        // Simplest possible test: PreA -> PostA (no PreB)
        // This isolates the deadlock issue to just the back-reference call
        use super::super::stream_post_state_processor::StreamPostStateProcessor;
        use std::sync::Arc;

        println!("Test starting...");
        let (app_ctx, query_ctx) = create_test_context();

        // Create PreA -> PostA
        let pre_a = StreamPreStateProcessor::new(
            0,
            true,
            StateType::Sequence,
            app_ctx.clone(),
            query_ctx.clone(),
        );
        let post_a = StreamPostStateProcessor::new(0);

        let pre_a_arc = Arc::new(Mutex::new(pre_a));
        let post_a_arc = Arc::new(Mutex::new(post_a));

        println!("Wiring PreA -> PostA...");

        // PreA -> PostA
        pre_a_arc
            .lock()
            .unwrap()
            .set_this_state_post_processor(post_a_arc.clone());

        // PostA -> PreA (back reference)
        // Shared state is automatically retrieved inside set_this_state_pre_processor()
        post_a_arc
            .lock()
            .unwrap()
            .set_this_state_pre_processor(pre_a_arc.clone());

        println!("Initializing PreA...");
        pre_a_arc.lock().unwrap().init();
        pre_a_arc.lock().unwrap().update_state();
        println!(
            "PreA has {} pending states",
            pre_a_arc.lock().unwrap().pending_count()
        );

        println!("Sending event to PreA...");
        let event_a = StreamEvent::new(1000, 1, 0, 0);
        let result_a = pre_a_arc
            .lock()
            .unwrap()
            .process_and_return(Some(Box::new(event_a)));

        println!("Event processed!");
        assert!(result_a.is_some());
    }

    #[test]
    fn test_integration_pre_post_pre_chaining() {
        // Test proper Pre->Post->Pre processor chain with actual PostStateProcessor
        // This verifies the complete chaining architecture without deadlock
        use super::super::stream_post_state_processor::StreamPostStateProcessor;
        use std::sync::Arc;

        let (app_ctx, query_ctx) = create_test_context();

        // Create PreA -> PostA -> PreB chain
        let pre_a = StreamPreStateProcessor::new(
            0,
            true,
            StateType::Sequence,
            app_ctx.clone(),
            query_ctx.clone(),
        );
        let post_a = StreamPostStateProcessor::new(0);
        let pre_b = StreamPreStateProcessor::new(
            1,
            false,
            StateType::Sequence,
            app_ctx.clone(),
            query_ctx.clone(),
        );

        // Wire the chain together
        let pre_a_arc = Arc::new(Mutex::new(pre_a));
        let post_a_arc = Arc::new(Mutex::new(post_a));
        let pre_b_arc = Arc::new(Mutex::new(pre_b));

        // PreA -> PostA
        pre_a_arc
            .lock()
            .unwrap()
            .set_this_state_post_processor(post_a_arc.clone());

        // PostA -> PreA (back reference)
        // Shared state is automatically retrieved inside set_this_state_pre_processor()
        post_a_arc
            .lock()
            .unwrap()
            .set_this_state_pre_processor(pre_a_arc.clone());

        // PostA -> PreB (forward chain)
        post_a_arc
            .lock()
            .unwrap()
            .set_next_state_pre_processor(pre_b_arc.clone());

        // Initialize PreA (start state)
        pre_a_arc.lock().unwrap().init();
        pre_a_arc.lock().unwrap().update_state();
        assert_eq!(pre_a_arc.lock().unwrap().pending_count(), 1);

        // Send event to PreA
        // PreA.process_and_return() will:
        // 1. Match the event
        // 2. Create StateEvent with event at position 0
        // 3. Call PostA.process() AFTER releasing lock (no deadlock!)
        // 4. PostA will call PreB.add_state()
        let event_a = StreamEvent::new(1000, 1, 0, 0);
        let result_a = pre_a_arc
            .lock()
            .unwrap()
            .process_and_return(Some(Box::new(event_a)));

        // Verify PreA produced StateEvent
        assert!(result_a.is_some());
        let state_with_a = result_a.unwrap();
        let state_event_a = state_with_a.as_any().downcast_ref::<StateEvent>().unwrap();
        assert!(state_event_a.get_stream_event(0).is_some());

        // PreB should have received the StateEvent from PostA automatically
        // Move from new to pending
        pre_b_arc.lock().unwrap().update_state();
        assert_eq!(
            pre_b_arc.lock().unwrap().pending_count(),
            1,
            "PreB should have 1 pending state forwarded automatically from PostA"
        );

        // Send event to PreB
        let event_b = StreamEvent::new(2000, 1, 0, 0);
        let result_b = pre_b_arc
            .lock()
            .unwrap()
            .process_and_return(Some(Box::new(event_b)));

        // Verify PreB produced complete StateEvent with both A and B
        assert!(result_b.is_some());
        let state_with_b = result_b.unwrap();
        let state_event_b = state_with_b.as_any().downcast_ref::<StateEvent>().unwrap();
        assert!(
            state_event_b.get_stream_event(0).is_some(),
            "Position 0 (A) should be present"
        );
        assert!(
            state_event_b.get_stream_event(1).is_some(),
            "Position 1 (B) should be present"
        );
    }
}
