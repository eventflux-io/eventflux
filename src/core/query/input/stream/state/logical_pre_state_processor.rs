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

// src/core/query/input/stream/state/logical_pre_state_processor.rs
// LogicalPreStateProcessor for AND and OR patterns

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::state::state_event::StateEvent;
use crate::core::query::input::stream::state::pre_state_processor::PreStateProcessor;
use crate::core::query::input::stream::state::stream_pre_state_processor::{
    StateType, StreamPreStateProcessor,
};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// How the two sides of a logical pattern combine
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalType {
    /// Both sides must match (A and B)
    And,
    /// Either side is enough (A or B)
    Or,
}

/// Pre processor for one side of a logical pattern such as "A and B" or "A or B".
///
/// Two of these are built, one per side, and linked with `set_partner_processor`.
/// After linking they hold a reference to each other in `partner_processor` and use
/// the same `shared_lock`, so state changes on one side are mirrored to the other.
///
/// **State sharing**:
/// - `add_state` adds the StateEvent to this side and also to the partner's new list.
///   At a start state or in a sequence, the partner gets it only when its new list is
///   empty.
/// - `add_every_state` gives both sides a copy with their own position onward reset.
/// - `update_state` moves new states to pending on both sides.
///
/// **AND**: this side fills its position and passes the StateEvent on. The paired
/// LogicalPostStateProcessor forwards it only once the partner's position is filled too.
///
/// **OR**: a pending StateEvent whose partner position is already filled is dropped,
/// because the other side has already satisfied the OR.
///
/// The step condition is not checked here yet. Every pending StateEvent counts as a
/// match.
pub struct LogicalPreStateProcessor {
    /// Wrapped stream pre processor that holds the state lists
    base_processor: StreamPreStateProcessor,

    logical_type: LogicalType,

    /// Processor for the other side of the AND or OR
    partner_processor: Option<Arc<Mutex<LogicalPreStateProcessor>>>,

    /// Lock held by both partners around every state change, so the two sides stay
    /// in step across threads
    shared_lock: Arc<Mutex<()>>,
}

impl LogicalPreStateProcessor {
    /// Create one side of a logical pattern.
    ///
    /// # Arguments
    /// * `state_id` - Position of this side in the StateEvent
    /// * `is_start_state` - Whether this is the first step of the pattern
    /// * `logical_type` - AND or OR
    /// * `state_type` - Pattern or Sequence rules
    /// * `app_ctx` - Application context
    /// * `query_ctx` - Query context
    pub fn new(
        state_id: usize,
        is_start_state: bool,
        logical_type: LogicalType,
        state_type: StateType,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        Self {
            base_processor: StreamPreStateProcessor::new(
                state_id,
                is_start_state,
                state_type,
                app_ctx,
                query_ctx,
            ),
            logical_type,
            partner_processor: None,
            shared_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Link this side to the other side of the logical pattern.
    ///
    /// Call it on both sides. It replaces the partner's lock with this side's lock, so
    /// afterwards both use the same `shared_lock`.
    pub fn set_partner_processor(&mut self, partner: Arc<Mutex<LogicalPreStateProcessor>>) {
        let lock_to_share = Arc::clone(&self.shared_lock);

        self.partner_processor = Some(partner.clone());

        if let Ok(mut partner_guard) = partner.lock() {
            partner_guard.shared_lock = lock_to_share;
        }
    }

    pub fn logical_type(&self) -> LogicalType {
        self.logical_type
    }

    pub fn partner_processor(&self) -> Option<Arc<Mutex<LogicalPreStateProcessor>>> {
        self.partner_processor.clone()
    }

    // The methods below pass through to the base processor. Tests and the partner use them.
    pub fn new_count(&self) -> usize {
        self.base_processor.new_count()
    }

    pub fn pending_count(&self) -> usize {
        self.base_processor.pending_count()
    }

    pub fn init(&mut self) {
        self.base_processor.init();
    }

    pub fn update_state(&mut self) {
        self.base_processor.update_state();
    }

    pub fn reset_state(&mut self) {
        self.base_processor.reset_state();
    }

    pub fn process_and_return(
        &mut self,
        chunk: Option<Box<dyn crate::core::event::complex_event::ComplexEvent>>,
    ) -> Option<Box<dyn crate::core::event::complex_event::ComplexEvent>> {
        self.base_processor.process_and_return(chunk)
    }

    /// True when the new list is empty. The partner calls this from `add_state`.
    pub fn is_new_and_every_list_empty(&self) -> bool {
        self.base_processor.new_count() == 0
    }

    /// Add a StateEvent straight to the new list. The partner calls this to mirror its
    /// states. Unlike `add_state`, it does not mark the state as changed.
    pub fn add_event_to_new_list(&mut self, state_event: StateEvent) {
        let mut state = self.base_processor.state.lock().unwrap();
        state.add_to_new_list(state_event);
    }

    /// Move every state in the new list to pending. The partner calls this from
    /// `update_state`.
    pub fn move_new_to_pending_for_partner(&mut self) {
        let mut state = self.base_processor.state.lock().unwrap();
        state.move_new_to_pending();
    }

    /// Clear the pending list. The partner calls this from `reset_state`.
    pub fn clear_pending_for_partner(&mut self) {
        let mut state = self.base_processor.state.lock().unwrap();
        state.clear_pending();
    }
}

impl Debug for LogicalPreStateProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogicalPreStateProcessor")
            .field("state_id", &self.base_processor.state_id())
            .field("is_start_state", &self.base_processor.is_start_state())
            .field("logical_type", &self.logical_type)
            .field("state_type", &self.base_processor.state_type())
            .field("has_partner", &self.partner_processor.is_some())
            .finish()
    }
}

impl PreStateProcessor for LogicalPreStateProcessor {
    fn init(&mut self) {
        self.base_processor.init();
    }

    fn process(&mut self, chunk: Option<Box<dyn ComplexEvent>>) -> Option<Box<dyn ComplexEvent>> {
        self.process_and_return(chunk)
    }

    fn add_state(&mut self, state_event: StateEvent) {
        let _lock = self.shared_lock.lock().unwrap();

        self.base_processor.add_state(state_event.clone());

        // Mirror the state to the partner
        if let Some(ref partner) = self.partner_processor {
            if let Ok(mut partner_guard) = partner.lock() {
                // At a start state or in a sequence, the partner gets the state only if its
                // new list is empty
                if self.base_processor.is_start_state()
                    || self.base_processor.state_type() == StateType::Sequence
                {
                    // Meant to skip duplicates. The add above always leaves new_count at
                    // 1 or more, so this does not return today.
                    if self.base_processor.new_count() == 0 {
                        return;
                    }

                    if partner_guard.is_new_and_every_list_empty() {
                        partner_guard.add_event_to_new_list(state_event.clone());
                    }
                } else {
                    partner_guard.add_event_to_new_list(state_event);
                }
            }
        }
    }

    fn add_every_state(&mut self, state_event: StateEvent) {
        let _lock = self.shared_lock.lock().unwrap();

        // Reset this position and every later one by writing an empty StreamEvent there.
        // The slot is then filled with a blank event, not left empty.
        let mut cloned = state_event.clone();
        let state_id = self.base_processor.state_id();

        for i in state_id..cloned.stream_event_count() {
            cloned.set_event(
                i,
                crate::core::event::stream::stream_event::StreamEvent::new(0, 0, 0, 0),
            );
        }

        self.base_processor.add_every_state(cloned.clone());

        if let Some(ref partner) = self.partner_processor {
            if let Ok(mut partner_guard) = partner.lock() {
                // Reset the partner's position onward the same way
                let mut partner_cloned = cloned.clone();
                let partner_state_id = partner_guard.base_processor.state_id();
                for i in partner_state_id..partner_cloned.stream_event_count() {
                    partner_cloned.set_event(
                        i,
                        crate::core::event::stream::stream_event::StreamEvent::new(0, 0, 0, 0),
                    );
                }
                partner_guard.add_event_to_new_list(partner_cloned);
            }
        }
    }

    fn update_state(&mut self) {
        let _lock = self.shared_lock.lock().unwrap();

        // Move new states to pending on both sides
        self.base_processor.update_state();

        if let Some(ref partner) = self.partner_processor {
            if let Ok(mut partner_guard) = partner.lock() {
                partner_guard.move_new_to_pending_for_partner();
            }
        }
    }

    fn reset_state(&mut self) {
        let _lock = self.shared_lock.lock().unwrap();

        // OR always clears both sides. AND clears only when both sides have the same
        // pending count, or when there is no partner.
        let should_clear = match self.logical_type {
            LogicalType::Or => true,
            LogicalType::And => {
                if let Some(ref partner) = self.partner_processor {
                    if let Ok(partner_guard) = partner.lock() {
                        self.pending_count() == partner_guard.pending_count()
                    } else {
                        false
                    }
                } else {
                    true
                }
            }
        };

        if should_clear {
            self.base_processor.reset_state();

            if let Some(ref partner) = self.partner_processor {
                if let Ok(mut partner_guard) = partner.lock() {
                    partner_guard.clear_pending_for_partner();

                    // A start state with nothing new needs init again to accept new matches
                    if partner_guard.base_processor.is_start_state()
                        && partner_guard.base_processor.new_count() == 0
                    {
                        partner_guard.init();
                    }
                }
            }
        }
    }

    fn process_and_return(
        &mut self,
        chunk: Option<Box<dyn ComplexEvent>>,
    ) -> Option<Box<dyn ComplexEvent>> {
        let _lock = self.shared_lock.lock().unwrap();

        let stream_event = match chunk {
            Some(event) => {
                match event
                    .as_any()
                    .downcast_ref::<crate::core::event::stream::stream_event::StreamEvent>()
                {
                    Some(se) => se.clone(),
                    None => return None,
                }
            }
            None => return None,
        };

        let state_id = self.base_processor.state_id();
        let state = self.base_processor.state.lock().unwrap();
        let pending_states: Vec<StateEvent> = state.get_pending_list().iter().cloned().collect();
        drop(state); // release the state lock before the loop takes it again

        let mut return_events: Vec<StateEvent> = Vec::new();
        let mut indices_to_remove: Vec<usize> = Vec::new();

        for (idx, mut pending_state) in pending_states.into_iter().enumerate() {
            // OR: the partner already filled its position, so drop this state
            if self.logical_type == LogicalType::Or {
                if let Some(ref partner) = self.partner_processor {
                    if let Ok(partner_guard) = partner.lock() {
                        let partner_state_id = partner_guard.base_processor.state_id();
                        if pending_state.get_stream_event(partner_state_id).is_some() {
                            indices_to_remove.push(idx);
                            continue;
                        }
                    }
                }
            }

            pending_state.set_event(state_id, stream_event.clone());

            // The step condition is not checked yet, so every state matches and the
            // else branch below never runs
            let matches = true;

            if matches {
                return_events.push(pending_state.clone());
                indices_to_remove.push(idx);
            } else {
                match self.base_processor.state_type() {
                    StateType::Pattern => {
                        // A pattern may skip events. Reset this position and keep waiting.
                        pending_state.set_event(
                            state_id,
                            crate::core::event::stream::stream_event::StreamEvent::new(0, 0, 0, 0),
                        );
                    }
                    StateType::Sequence => {
                        // A sequence must match each event in turn, so this state is broken
                        indices_to_remove.push(idx);
                    }
                }
            }
        }

        // Remove in reverse so earlier indices stay valid
        if !indices_to_remove.is_empty() {
            let mut state = self.base_processor.state.lock().unwrap();
            let pending_list = state.get_pending_list_mut();
            for &idx in indices_to_remove.iter().rev() {
                if idx < pending_list.len() {
                    pending_list.remove(idx);
                }
            }
        }

        // Only the first matched state is returned. Any others are removed from pending
        // but not returned.
        if return_events.is_empty() {
            None
        } else {
            Some(Box::new(return_events.into_iter().next().unwrap()))
        }
    }

    fn set_within_time(&mut self, within_time: i64) {
        self.base_processor.set_within_time(within_time);
    }

    fn expire_events(&mut self, timestamp: i64) {
        self.base_processor.expire_events(timestamp);
    }

    fn state_id(&self) -> usize {
        self.base_processor.state_id()
    }

    fn is_start_state(&self) -> bool {
        self.base_processor.is_start_state()
    }

    fn get_shared_state(
        &self,
    ) -> Arc<crate::core::query::input::stream::state::shared_processor_state::ProcessorSharedState>
    {
        self.base_processor.get_shared_state()
    }

    fn this_state_post_processor(&self) -> Option<Arc<Mutex<dyn crate::core::query::input::stream::state::post_state_processor::PostStateProcessor>>>{
        self.base_processor.this_state_post_processor()
    }

    fn state_changed(&self) {
        self.base_processor.state_changed();
    }

    fn has_state_changed(&self) -> bool {
        self.base_processor.has_state_changed()
    }
}

// ===== Tests =====

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_new_and_processor() {
        let (app_ctx, query_ctx) = create_test_context();
        let processor = LogicalPreStateProcessor::new(
            0,
            true,
            LogicalType::And,
            StateType::Pattern,
            app_ctx,
            query_ctx,
        );

        assert_eq!(processor.logical_type(), LogicalType::And);
        assert_eq!(processor.state_id(), 0);
        assert!(processor.is_start_state());
        assert!(processor.partner_processor().is_none());
    }

    #[test]
    fn test_new_or_processor() {
        let (app_ctx, query_ctx) = create_test_context();
        let processor = LogicalPreStateProcessor::new(
            1,
            false,
            LogicalType::Or,
            StateType::Sequence,
            app_ctx,
            query_ctx,
        );

        assert_eq!(processor.logical_type(), LogicalType::Or);
        assert_eq!(processor.state_id(), 1);
        assert!(!processor.is_start_state());
    }

    // ===== Partner Coordination Tests =====

    #[test]
    fn test_set_partner_processor() {
        let (app_ctx, query_ctx) = create_test_context();
        let mut processor_a = LogicalPreStateProcessor::new(
            0,
            true,
            LogicalType::And,
            StateType::Pattern,
            app_ctx.clone(),
            query_ctx.clone(),
        );
        let processor_b = Arc::new(Mutex::new(LogicalPreStateProcessor::new(
            1,
            false,
            LogicalType::And,
            StateType::Pattern,
            app_ctx,
            query_ctx,
        )));

        processor_a.set_partner_processor(processor_b.clone());

        assert!(processor_a.partner_processor().is_some());

        // Verify shared lock
        let partner = processor_a.partner_processor().unwrap();
        let partner_guard = partner.lock().unwrap();
        assert!(Arc::ptr_eq(
            &processor_a.shared_lock,
            &partner_guard.shared_lock
        ));
    }

    #[test]
    fn test_partner_bidirectional() {
        let (app_ctx, query_ctx) = create_test_context();
        let processor_a = Arc::new(Mutex::new(LogicalPreStateProcessor::new(
            0,
            true,
            LogicalType::And,
            StateType::Pattern,
            app_ctx.clone(),
            query_ctx.clone(),
        )));
        let processor_b = Arc::new(Mutex::new(LogicalPreStateProcessor::new(
            1,
            false,
            LogicalType::And,
            StateType::Pattern,
            app_ctx,
            query_ctx,
        )));

        // Wire together
        processor_a
            .lock()
            .unwrap()
            .set_partner_processor(processor_b.clone());
        processor_b
            .lock()
            .unwrap()
            .set_partner_processor(processor_a.clone());

        // Verify both have partners
        assert!(processor_a.lock().unwrap().partner_processor().is_some());
        assert!(processor_b.lock().unwrap().partner_processor().is_some());
    }

    // ===== Debug Implementation Test =====

    #[test]
    fn test_debug_impl() {
        let (app_ctx, query_ctx) = create_test_context();
        let processor = LogicalPreStateProcessor::new(
            0,
            true,
            LogicalType::And,
            StateType::Pattern,
            app_ctx,
            query_ctx,
        );

        let debug_str = format!("{:?}", processor);
        assert!(debug_str.contains("LogicalPreStateProcessor"));
        assert!(debug_str.contains("logical_type"));
    }

    // ===== Functional Tests =====

    #[test]
    fn test_and_processor_initialization() {
        let (app_ctx, query_ctx) = create_test_context();

        let proc_a = Arc::new(Mutex::new(LogicalPreStateProcessor::new(
            0,
            true,
            LogicalType::And,
            StateType::Pattern,
            app_ctx.clone(),
            query_ctx.clone(),
        )));
        let proc_b = Arc::new(Mutex::new(LogicalPreStateProcessor::new(
            1,
            false,
            LogicalType::And,
            StateType::Pattern,
            app_ctx,
            query_ctx,
        )));

        proc_a.lock().unwrap().set_partner_processor(proc_b.clone());
        proc_b.lock().unwrap().set_partner_processor(proc_a.clone());

        proc_a.lock().unwrap().init();
        proc_b.lock().unwrap().init();

        assert_eq!(proc_a.lock().unwrap().new_count(), 1);
        assert_eq!(proc_b.lock().unwrap().new_count(), 0);
    }
}
