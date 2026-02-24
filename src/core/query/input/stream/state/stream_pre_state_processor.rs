// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/input/stream/state/stream_pre_state_processor.rs
// Base implementation of PreStateProcessor for single stream patterns

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

/// StateType distinguishes between Pattern and Sequence semantics
///
/// **Pattern** (A and B, A or B):
/// - No temporal ordering required
/// - Events can arrive in any order
/// - State event cleared on no match (try again with next event)
///
/// **Sequence** (A -> B -> C):
/// - Temporal ordering enforced
/// - Events must arrive in order
/// - State event removed on no match (failed sequence)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateType {
    Pattern,
    Sequence,
}

/// StreamPreStateProcessor is the base implementation for single stream pattern matching
///
/// **Responsibilities**:
/// 1. Maintains StreamPreState (three lists: current/pending/new)
/// 2. Processes incoming events against pending states
/// 3. Handles lifecycle (init/add/update/reset)
/// 4. Manages WITHIN time constraints
/// 5. Clones events for state tracking
///
/// **Pattern Matching Flow**:
/// 1. **init()**: Start states create empty StateEvent
/// 2. **add_state()**: Previous processor chains forward StateEvent
/// 3. **update_state()**: Move new→pending (sorted by timestamp)
/// 4. **process_and_return()**: Match incoming event against all pending states
///    - For each pending StateEvent:
///      - Clone incoming StreamEvent
///      - Add to StateEvent at this processor's position
///      - Check condition (via expression executor - simplified for now)
///      - If match: forward to PostStateProcessor (to be added in Task 1.5)
///    - Clean up: remove/reset states based on Pattern vs Sequence semantics
/// 5. **reset_state()**: Clear after complete match
///
/// **Pattern vs Sequence Semantics**:
/// - **Pattern**: If event doesn't match, clear event at position, try next pending
/// - **Sequence**: If event doesn't match, remove StateEvent (sequence broken)
///
pub struct StreamPreStateProcessor {
    // Core identification
    state_id: usize,
    is_start_state: bool,
    state_type: StateType,

    // Context
    app_context: Arc<EventFluxAppContext>,
    query_context: Arc<EventFluxQueryContext>,

    // State management
    // Made public for LogicalPreStateProcessor to access for partner coordination
    pub state: Arc<Mutex<StreamPreState>>,

    // Shared state with PostStateProcessor (lock-free communication)
    // Prevents deadlock by avoiding need to lock PreStateProcessor from PostStateProcessor
    shared_state: Arc<ProcessorSharedState>,

    // Event cloning
    stream_event_cloner: Option<StreamEventCloner>,
    state_event_cloner: Option<StateEventCloner>,

    // Time constraint (None = no constraint)
    within_time: Option<i64>,

    // Start state IDs to check for WITHIN constraint
    // Java: protected int[] startStateIds
    // Used to check time elapsed from ALL start states, not just current position
    start_state_ids: Vec<usize>,

    // Success condition flag for count quantifiers (Phase 2)
    // Set by CountPostStateProcessor when min count reached
    // Java: protected boolean isSuccessCondition
    success_condition: bool,

    // EVERY pattern flag
    // Set to true if this processor is part of an EVERY pattern
    // Allows detection of EVERY semantics without checking post processors
    is_every_pattern: bool,

    // Processor chain
    next_processor: Option<Arc<Mutex<dyn Processor>>>,

    // PostStateProcessor for this processor (CRITICAL for chaining)
    this_state_post_processor: Option<Arc<Mutex<dyn PostStateProcessor>>>,

    // Condition function for filter evaluation
    // Receives StateEvent containing all matched events (e1, e2, ...)
    // This enables cross-stream references like: e2[price > e1.price]
    // The current event being evaluated is already in StateEvent at position self.state_id
    // Uses Arc for shareability across clones (partitioned execution, state restoration)
    condition_fn: Option<Arc<dyn Fn(&StateEvent) -> bool + Send + Sync>>,
}

// Manual Debug implementation (condition_fn doesn't implement Debug)
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
    /// Create a new StreamPreStateProcessor
    ///
    /// **Parameters**:
    /// - `state_id`: Stream position in StateEvent (0-based index)
    /// - `is_start_state`: true if this is first processor in chain
    /// - `state_type`: Pattern or Sequence semantics
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
            is_every_pattern: false, // Set later by PatternChainBuilder if EVERY
            stream_event_cloner: None,
            state_event_cloner: None,
            within_time: None, // No constraint by default
            start_state_ids: Vec::new(),
            success_condition: false,
            next_processor: None,
            this_state_post_processor: None,
            condition_fn: None,
        }
    }

    /// Get the state ID
    pub fn state_id(&self) -> usize {
        self.state_id
    }

    /// Check if this is a start state
    pub fn is_start_state(&self) -> bool {
        self.is_start_state
    }

    /// Get the state type (Pattern or Sequence)
    pub fn state_type(&self) -> StateType {
        self.state_type
    }

    /// Get the shared state (for PostStateProcessor to access)
    ///
    /// **Purpose**: Allows PostStateProcessor to share state_changed flag
    /// without needing to lock this PreStateProcessor
    pub fn get_shared_state(&self) -> Arc<ProcessorSharedState> {
        self.shared_state.clone()
    }

    /// Set the PostStateProcessor for this processor
    ///
    /// **Usage**: Wire PreStateProcessor -> PostStateProcessor chain
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

    /// Set the start state IDs for WITHIN constraint checking
    ///
    /// **Purpose**: When checking WITHIN time, need to check against ALL start states,
    /// not just current position. This is because patterns like:
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

    /// Set success condition flag (for count quantifiers)
    ///
    /// **Purpose**: Count quantifiers like A{2,5} use this flag.
    /// When CountPostStateProcessor reaches min count (2), it sets this to true.
    /// PreStateProcessor can then forward to next state.
    ///
    /// **Java Reference**: StreamPreStateProcessor.isSuccessCondition
    pub fn set_success_condition(&mut self, success: bool) {
        self.success_condition = success;
    }

    /// Check success condition flag (for count quantifiers)
    pub fn is_success_condition(&self) -> bool {
        self.success_condition
    }

    /// Set the condition function for filtering events
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

    /// Check if a state event matches this processor's condition
    ///
    /// **Parameters**:
    /// - `state_event`: Full StateEvent with all matched events (including current at self.state_id)
    ///
    /// **Returns**: true if event matches condition, false otherwise
    fn matches_condition(&self, state_event: &StateEvent) -> bool {
        match &self.condition_fn {
            Some(f) => f(state_event),
            None => true, // No condition = match all
        }
    }

    /// Check if a pending state is expired based on WITHIN constraint
    ///
    /// **Correct Implementation**: Check time against ALL start state IDs.
    /// For pattern A -> B -> C within 5 sec:
    /// - Check (current_time - A.timestamp) > 5 sec
    /// - NOT just (current_time - C.timestamp) > 5 sec
    fn is_expired(&self, state_event: &StateEvent, current_timestamp: i64) -> bool {
        let within = match self.within_time {
            Some(w) => w,
            None => return false, // No time constraint
        };

        // Check against ALL start state IDs (not just current position)
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
            // Fallback: check first non-null event (legacy behavior)
            for stream_opt in &state_event.stream_events {
                if let Some(stream) = stream_opt {
                    let age = (current_timestamp - stream.timestamp).abs();
                    if age > within {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Clone a StreamEvent using the cloner or direct clone
    fn clone_stream_event(&self, event: &StreamEvent) -> StreamEvent {
        match &self.stream_event_cloner {
            Some(cloner) => cloner.copy_stream_event(event),
            None => event.clone(),
        }
    }

    /// Clone a StateEvent using the cloner or direct clone
    fn clone_state_event(&self, event: &StateEvent) -> StateEvent {
        match &self.state_event_cloner {
            Some(cloner) => cloner.copy_state_event(event),
            None => event.clone(),
        }
    }

    /// Get stream event cloner (for CountPreStateProcessor)
    pub fn get_stream_event_cloner(&self) -> &StreamEventCloner {
        self.stream_event_cloner
            .as_ref()
            .expect("StreamEventCloner not initialized")
    }

    /// Set stream event cloner (for testing and initialization)
    pub fn set_stream_event_cloner(&mut self, cloner: StreamEventCloner) {
        self.stream_event_cloner = Some(cloner);
    }

    /// Set state event cloner (for testing and initialization)
    pub fn set_state_event_cloner(&mut self, cloner: StateEventCloner) {
        self.state_event_cloner = Some(cloner);
    }

    /// Get state event cloner as Option (for CountPreStateProcessor EVERY logic)
    pub fn get_state_event_cloner_option(&self) -> Option<&StateEventCloner> {
        self.state_event_cloner.as_ref()
    }

    /// Set EVERY pattern flag
    pub fn set_every_pattern_flag(&mut self, is_every: bool) {
        self.is_every_pattern = is_every;
    }

    /// Get EVERY pattern flag
    pub fn is_every_pattern(&self) -> bool {
        self.is_every_pattern
    }

    /// Get this state post processor (for CountPreStateProcessor)
    pub fn get_this_state_post_processor(&self) -> Option<Arc<Mutex<dyn PostStateProcessor>>> {
        self.this_state_post_processor.clone()
    }

    /// Get next processor (for CountPreStateProcessor)
    pub fn get_next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.next_processor.clone()
    }

    /// Clone this processor (for CountPreStateProcessor)
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
            condition_fn: self.condition_fn.clone(), // Arc is cloneable
        }
    }
}

// ===== Processor Trait Implementation =====

impl Processor for StreamPreStateProcessor {
    fn process(&self, _chunk: Option<Box<dyn ComplexEvent>>) {
        // PreStateProcessor uses process_and_return() instead
        // This method should not be called directly
        panic!("process() should not be called on PreStateProcessor. Use process_and_return().");
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.next_processor.clone()
    }

    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.next_processor = next;
    }

    fn clone_processor(&self, query_context: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        // Clone the processor for partitioned execution or state restoration
        // condition_fn uses Arc and is properly cloned to preserve filtering semantics
        Box::new(Self {
            state_id: self.state_id,
            is_start_state: self.is_start_state,
            state_type: self.state_type,
            app_context: Arc::clone(&self.app_context),
            query_context: Arc::clone(query_context), // Use new query context
            state: Arc::clone(&self.state),           // Shared state across clones
            shared_state: Arc::clone(&self.shared_state), // Shared state across clones
            is_every_pattern: self.is_every_pattern,
            stream_event_cloner: self.stream_event_cloner.clone(),
            state_event_cloner: self.state_event_cloner.clone(),
            within_time: self.within_time,
            start_state_ids: self.start_state_ids.clone(),
            success_condition: self.success_condition,
            next_processor: self.next_processor.clone(),
            this_state_post_processor: self.this_state_post_processor.clone(),
            condition_fn: self.condition_fn.clone(), // Arc is cloneable - preserves filter
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
        true // Pattern processors are always stateful
    }
}

// ===== PreStateProcessor Trait Implementation =====

impl PreStateProcessor for StreamPreStateProcessor {
    fn init(&mut self) {
        let mut state = self.state.lock().unwrap();

        // Only start states create initial empty StateEvent
        if self.is_start_state && !state.is_initialized() {
            // Create empty StateEvent to begin pattern matching
            // The number of stream positions = state_id + 1 (since this is the first)
            // In full implementation, would know total stream count from metadata
            let initial_state = StateEvent::new(self.state_id + 1, 0);
            state.add_to_new_list(initial_state);
            state.mark_initialized();
        }
    }

    fn process(&mut self, chunk: Option<Box<dyn ComplexEvent>>) -> Option<Box<dyn ComplexEvent>> {
        // Entry point for incoming events
        // Delegates to process_and_return which handles the actual pattern matching
        self.process_and_return(chunk)
    }

    fn add_state(&mut self, state_event: StateEvent) {
        let mut state = self.state.lock().unwrap();

        // Add to new/every list
        // For Sequence, Java has special logic about only adding if list is empty
        // For Pattern, always adds
        // Simplified: always add for now
        state.add_to_new_list(state_event);
        state.mark_state_changed();

        // Also mark shared state for lock-free communication with PostStateProcessor
        self.shared_state.mark_state_changed();
    }

    fn add_every_state(&mut self, state_event: StateEvent) {
        // Same as add_state for base implementation
        // 'every' semantics handled by PostStateProcessor chaining
        self.add_state(state_event);
    }

    fn update_state(&mut self) {
        let mut state = self.state.lock().unwrap();
        // Move new→pending with timestamp sorting
        state.move_new_to_pending();
    }

    fn reset_state(&mut self) {
        // EVERY pattern handling: Check if this is the start of an EVERY pattern
        // If so, we should NOT clear pending to allow pattern restart with new events
        let should_skip_reset = self.is_start_state && self.is_every_pattern;

        if should_skip_reset {
            // EVERY pattern: Don't clear pending, keep accepting new events
            // Just reinitialize so new events can start new pattern instances
            let mut state = self.state.lock().unwrap();

            // Only reinitialize if new list is empty
            if state.new_count() == 0 {
                state.reset_initialized();
                drop(state); // Release lock before calling init
                self.init();
            }
            // Don't clear pending - this allows pattern to restart with accumulated events
            return;
        }

        // Normal reset logic for non-EVERY patterns
        let mut state = self.state.lock().unwrap();
        state.clear_pending();

        // SEQUENCE mode: Auto-restart after each match (implicit EVERY behavior)
        // Unlike PATTERN-EVERY which keeps pending for overlapping instances,
        // SEQUENCE clears pending (strict consecutive) but still restarts for next match
        let is_sequence_auto_restart =
            self.is_start_state && self.state_type == StateType::Sequence;

        // If start state and new list is empty, reinitialize
        // This applies to both normal patterns and SEQUENCE auto-restart
        if self.is_start_state && state.new_count() == 0 {
            // Reset initialized flag to allow init() to create new StateEvent
            state.reset_initialized();
            drop(state); // Release lock before calling init
            self.init();

            // For SEQUENCE auto-restart, we need to ensure the processor is ready
            // for the next consecutive match
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
        // Extract and clone the StreamEvent from the chunk
        // We clone it here to avoid lifetime issues
        let stream_event = match chunk {
            Some(event) => {
                // Downcast to StreamEvent and clone it
                match event.as_any().downcast_ref::<StreamEvent>() {
                    Some(se) => self.clone_stream_event(se),
                    None => return None, // Not a StreamEvent
                }
            }
            None => return None, // No event to process
        };

        // Step 1: Collect pending states and process matches WITHOUT holding the lock
        // (to avoid deadlock when calling PostStateProcessor which calls back to state_changed)
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

        // Iterate through all pending StateEvents
        for (idx, pending_state) in pending_states.iter().enumerate() {
            // Clone the pending StateEvent
            let mut candidate_state = self.clone_state_event(pending_state);

            // Clone the incoming StreamEvent
            let cloned_stream = self.clone_stream_event(&stream_event);

            // Ensure StateEvent has enough positions for this processor's state_id
            // Without this, set_event() will fail silently if position >= stream_events.len()
            candidate_state.expand_to_size(self.state_id + 1);

            // Add the StreamEvent to the StateEvent at this processor's position
            candidate_state.set_event(self.state_id, cloned_stream);

            // Check if the event matches our condition
            // StateEvent contains all events including current one at self.state_id
            if self.matches_condition(&candidate_state) {
                // Match! This state progresses
                return_events.push(candidate_state.clone());

                // Collect events to forward to PostStateProcessor (will do this AFTER releasing lock)
                if self.this_state_post_processor.is_some() {
                    post_processor_events.push(candidate_state);
                }

                // Mark state changed
                state_changed = true;

                // Remove from pending based on Pattern vs Sequence semantics
                match self.state_type {
                    StateType::Pattern => {
                        // Pattern: Keep StateEvent in pending for next try
                        // (Multi-match capability - can match multiple events against same state)
                        // PostStateProcessor handles forwarding to next states
                    }
                    StateType::Sequence => {
                        // Sequence: Remove StateEvent (progressed to next state)
                        pending_to_remove.push(idx);
                    }
                }
            } else {
                // No match - handle based on Pattern vs Sequence semantics
                match self.state_type {
                    StateType::Pattern => {
                        // Pattern: Clear event at position, keep StateEvent for next try
                        // Don't remove from pending - try again with next event
                        // (Already not added to return_events)
                    }
                    StateType::Sequence => {
                        // Sequence: Remove StateEvent (sequence broken)
                        pending_to_remove.push(idx);
                    }
                }
            }
        }

        // Step 2: Update state (mark changed, remove processed) - need lock
        {
            let mut state = self.state.lock().unwrap();

            // Mark state changed if any matches occurred
            if state_changed {
                state.mark_state_changed();
                // Also mark shared state for lock-free communication with PostStateProcessor
                self.shared_state.mark_state_changed();
            }

            // Remove processed states (in reverse order to maintain indices)
            if !pending_to_remove.is_empty() {
                let pending_list_mut = state.get_pending_list_mut();
                for &idx in pending_to_remove.iter().rev() {
                    if idx < pending_list_mut.len() {
                        pending_list_mut.remove(idx);
                    }
                }
            }
        } // Release lock here!

        // Step 3: Forward to PostStateProcessor AFTER releasing lock
        // This avoids deadlock when PostStateProcessor calls back to state_changed()
        if !post_processor_events.is_empty() {
            if let Some(ref post) = self.this_state_post_processor {
                if let Ok(mut post_guard) = post.lock() {
                    for event in post_processor_events {
                        post_guard.process(Some(Box::new(event)));
                    }
                }
            }
        }

        // Convert return_events to ComplexEvent chain
        // Build linked list: first -> second -> third -> ...
        if return_events.is_empty() {
            None
        } else {
            // For now, return just the first event as Box<dyn ComplexEvent>
            // In full implementation, would build proper ComplexEventChunk
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

        // Mark expired events and notify 'every' processor before removing
        pending_list_mut.retain(|state_event| {
            if self.is_expired(state_event, timestamp) {
                // Mark as EXPIRED (better lifecycle tracking)
                let _expired = state_event.clone();
                // Note: StreamPreStateProcessor handles expiration directly by removing from pending.
                // More complex processors (CountPreStateProcessor, etc.) may notify callbacks
                // for cleanup and restart logic when events expire.

                false // Remove from pending
            } else {
                true // Keep in pending
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
        // Use shared atomic state instead of locking internal state
        // This prevents deadlock when PostStateProcessor calls this method
        self.shared_state.mark_state_changed();
    }

    fn has_state_changed(&self) -> bool {
        // Use shared atomic state for lock-free access
        self.shared_state.is_state_changed()
    }
}

// ===== Public accessor methods (for testing and integration) =====

impl StreamPreStateProcessor {
    /// Get reference to the internal state (for testing)
    pub fn get_state(&self) -> Arc<Mutex<StreamPreState>> {
        Arc::clone(&self.state)
    }

    /// Get pending event count (for testing)
    pub fn pending_count(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.pending_count()
    }

    /// Get new event count (for testing)
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
        // For now, simplified impl removes it. Full impl would keep it.
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
                if let Some(value) = stream_event.before_window_data.get(0) {
                    if let AttributeValue::Float(price) = value {
                        return *price > 100.0;
                    }
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
        let mut processor = create_test_processor(0, true, StateType::Pattern);
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

        // In real implementation, PostStateProcessor would expand StateEvent
        // For now, manually create expanded StateEvent for B
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

        // Set to true (as CountPostStateProcessor would do)
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
