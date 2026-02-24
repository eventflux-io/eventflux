// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/input/stream/state/logical_pre_state_processor.rs
// LogicalPreStateProcessor for AND/OR pattern matching

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

/// Type of logical operation between two pattern streams
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalType {
    /// Both patterns must match (A and B)
    And,
    /// Either pattern can match (A or B)
    Or,
}

/// LogicalPreStateProcessor coordinates AND/OR logic between two pattern streams.
///
/// **Purpose**: Handles logical patterns like "A and B" or "A or B" where events
/// from two different streams must be coordinated according to logical rules.
///
/// **Key Architecture - Partner Processor Pattern**:
/// - Two LogicalPreStateProcessors are created (left and right sides)
/// - They share a reference to each other via `partner_state_pre_processor`
/// - They share the same lock via `shared_lock` for thread-safe coordination
/// - State changes in one processor affect the partner
///
/// **AND Logic** (both must match):
/// ```text
/// Pattern: A and B
///
/// Stream A: event_a1 arrives
/// - LogicalPreA adds StateEvent to its new list
/// - LogicalPreA also adds to LogicalPreB's new list (coordination)
///
/// Stream B: event_b1 arrives
/// - LogicalPreB adds StateEvent to its new list
/// - LogicalPreB also adds to LogicalPreA's new list (coordination)
///
/// After updateState():
/// - Both processors have StateEvents with A filled
/// - Both processors have StateEvents with B filled
///
/// When A matches: checks if B is also filled in the StateEvent
/// When B matches: checks if A is also filled in the StateEvent
/// ```
///
/// **OR Logic** (either can match):
/// ```text
/// Pattern: A or B
///
/// Stream A: event_a1 arrives and matches
/// - LogicalPreA processes match
/// - Sets StateEvent position for A
/// - Forwards to PostStateProcessor
///
/// Stream B: event_b1 arrives
/// - LogicalPreB checks: did partner (A) already match this StateEvent?
/// - If yes: skip this StateEvent (OR already satisfied)
/// - If no: process normally
/// ```
pub struct LogicalPreStateProcessor {
    /// The underlying StreamPreStateProcessor for basic pattern matching
    base_processor: StreamPreStateProcessor,

    /// Type of logical operation (AND or OR)
    logical_type: LogicalType,

    /// Reference to the partner processor (the other side of AND/OR)
    /// Both processors coordinate via this reference
    partner_processor: Option<Arc<Mutex<LogicalPreStateProcessor>>>,

    /// Shared lock for coordinating between partner processors
    /// Both partners use the same lock to ensure thread-safe coordination
    shared_lock: Arc<Mutex<()>>,
}

impl LogicalPreStateProcessor {
    /// Create a new LogicalPreStateProcessor
    ///
    /// # Arguments
    /// * `state_id` - Position in the StateEvent for this processor
    /// * `is_start_state` - Whether this is the first processor in the pattern
    /// * `logical_type` - AND or OR logic
    /// * `state_type` - Pattern or Sequence semantics
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

    /// Set the partner processor and establish shared lock coordination
    ///
    /// This must be called to wire together the two sides of the logical expression.
    /// After calling this, both processors will share the same lock.
    pub fn set_partner_processor(&mut self, partner: Arc<Mutex<LogicalPreStateProcessor>>) {
        // Share the lock with partner
        let lock_to_share = Arc::clone(&self.shared_lock);

        // Set partner reference
        self.partner_processor = Some(partner.clone());

        // Make partner use the same lock
        if let Ok(mut partner_guard) = partner.lock() {
            partner_guard.shared_lock = lock_to_share;
        }
    }

    /// Get the logical type
    pub fn logical_type(&self) -> LogicalType {
        self.logical_type
    }

    /// Get reference to partner processor
    pub fn partner_processor(&self) -> Option<Arc<Mutex<LogicalPreStateProcessor>>> {
        self.partner_processor.clone()
    }

    /// Delegate methods to base processor for testing and lifecycle management
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

    /// Check if the new/every state event list is empty
    /// Used by partner processor for coordination
    pub fn is_new_and_every_list_empty(&self) -> bool {
        self.base_processor.new_count() == 0
    }

    /// Add event to new/every list (used by partner for coordination)
    pub fn add_event_to_new_list(&mut self, state_event: StateEvent) {
        // Direct access to base processor's state
        let mut state = self.base_processor.state.lock().unwrap();
        state.add_to_new_list(state_event);
    }

    /// Move all new/every events to pending (used by partner coordination)
    pub fn move_new_to_pending_for_partner(&mut self) {
        let mut state = self.base_processor.state.lock().unwrap();
        state.move_new_to_pending();
    }

    /// Clear pending list (used by partner coordination in resetState)
    pub fn clear_pending_for_partner(&mut self) {
        let mut state = self.base_processor.state.lock().unwrap();
        state.clear_pending();
    }
}

// Manual Debug implementation
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
        // Delegate to base processor
        self.base_processor.init();
    }

    fn process(&mut self, chunk: Option<Box<dyn ComplexEvent>>) -> Option<Box<dyn ComplexEvent>> {
        // Delegate to process_and_return
        self.process_and_return(chunk)
    }

    fn add_state(&mut self, state_event: StateEvent) {
        let _lock = self.shared_lock.lock().unwrap();

        // Add to this processor's new list
        self.base_processor.add_state(state_event.clone());

        // Coordinate with partner
        if let Some(ref partner) = self.partner_processor {
            if let Ok(mut partner_guard) = partner.lock() {
                // For start states or sequences, special logic
                if self.base_processor.is_start_state()
                    || self.base_processor.state_type() == StateType::Sequence
                {
                    if self.base_processor.new_count() == 0 {
                        // Don't add duplicate
                        return;
                    }

                    if partner_guard.is_new_and_every_list_empty() {
                        partner_guard.add_event_to_new_list(state_event.clone());
                    }
                } else {
                    // Non-start states: always add to partner
                    partner_guard.add_event_to_new_list(state_event);
                }
            }
        }
    }

    fn add_every_state(&mut self, state_event: StateEvent) {
        let _lock = self.shared_lock.lock().unwrap();

        // Clone and clear this state's position and all after
        let mut cloned = state_event.clone();
        let state_id = self.base_processor.state_id();

        // Clear this position and all following positions
        for i in state_id..cloned.stream_event_count() {
            cloned.set_event(
                i,
                crate::core::event::stream::stream_event::StreamEvent::new(0, 0, 0, 0),
            );
        }

        // Add to this processor
        self.base_processor.add_every_state(cloned.clone());

        // Add to partner
        if let Some(ref partner) = self.partner_processor {
            if let Ok(mut partner_guard) = partner.lock() {
                // Also clear partner's position
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

        // Sort and move new->pending for this processor
        self.base_processor.update_state();

        // Coordinate with partner: move their new->pending as well
        if let Some(ref partner) = self.partner_processor {
            if let Ok(mut partner_guard) = partner.lock() {
                partner_guard.move_new_to_pending_for_partner();
            }
        }
    }

    fn reset_state(&mut self) {
        let _lock = self.shared_lock.lock().unwrap();

        // OR logic: always clear both processors
        // AND logic: only clear if both have same pending count (both matched)
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
            // Clear both processors' pending lists
            self.base_processor.reset_state();

            if let Some(ref partner) = self.partner_processor {
                if let Ok(mut partner_guard) = partner.lock() {
                    partner_guard.clear_pending_for_partner();

                    // Reinitialize if start state and new list is empty
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

        // Get the incoming StreamEvent
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
        let mut state = self.base_processor.state.lock().unwrap();
        let pending_states: Vec<StateEvent> = state.get_pending_list().iter().cloned().collect();
        drop(state); // Release lock before processing

        let mut return_events: Vec<StateEvent> = Vec::new();
        let mut indices_to_remove: Vec<usize> = Vec::new();

        for (idx, mut pending_state) in pending_states.into_iter().enumerate() {
            // OR logic: skip if partner already matched this StateEvent
            if self.logical_type == LogicalType::Or {
                if let Some(ref partner) = self.partner_processor {
                    if let Ok(partner_guard) = partner.lock() {
                        let partner_state_id = partner_guard.base_processor.state_id();
                        if pending_state.get_stream_event(partner_state_id).is_some() {
                            // Partner already matched, skip this
                            indices_to_remove.push(idx);
                            continue;
                        }
                    }
                }
            }

            // Set the stream event at this processor's position
            pending_state.set_event(state_id, stream_event.clone());

            // Check if condition matches (simplified - always matches for now)
            let matches = true;

            if matches {
                return_events.push(pending_state.clone());
                indices_to_remove.push(idx);
            } else {
                // Pattern vs Sequence semantics
                match self.base_processor.state_type() {
                    StateType::Pattern => {
                        // Clear this position, keep trying
                        pending_state.set_event(
                            state_id,
                            crate::core::event::stream::stream_event::StreamEvent::new(0, 0, 0, 0),
                        );
                    }
                    StateType::Sequence => {
                        // Sequence broken, remove
                        indices_to_remove.push(idx);
                    }
                }
            }
        }

        // Remove processed indices
        if !indices_to_remove.is_empty() {
            let mut state = self.base_processor.state.lock().unwrap();
            let pending_list = state.get_pending_list_mut();
            for &idx in indices_to_remove.iter().rev() {
                if idx < pending_list.len() {
                    pending_list.remove(idx);
                }
            }
        }

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
