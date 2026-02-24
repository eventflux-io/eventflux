// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/input/stream/state/logical_post_state_processor.rs
// LogicalPostStateProcessor for handling AND/OR pattern match results

use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::state::state_event::StateEvent;
use crate::core::query::input::stream::state::logical_pre_state_processor::{
    LogicalPreStateProcessor, LogicalType,
};
use crate::core::query::input::stream::state::post_state_processor::PostStateProcessor;
use crate::core::query::input::stream::state::pre_state_processor::PreStateProcessor;
use crate::core::query::input::stream::state::stream_post_state_processor::StreamPostStateProcessor;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// LogicalPostStateProcessor handles successful AND/OR pattern matches.
///
/// **Purpose**: After LogicalPreStateProcessor matches an event, it forwards the
/// StateEvent to its LogicalPostStateProcessor. This processor applies AND/OR logic
/// before forwarding to the next stage.
///
/// **AND Logic**:
/// - Only forwards if BOTH sides of the AND have matched
/// - Checks if partner's position in StateEvent is filled
/// - If partner not matched yet: marks state as changed, but doesn't forward
/// - If partner matched: forwards normally via base StreamPostStateProcessor
///
/// **OR Logic**:
/// - Always forwards (either side matching is sufficient)
/// - Special handling: if this side matches, mark partner's PostStateProcessor
///   as "event returned" too (since OR is satisfied)
///
/// **Example - AND**:
/// ```text
/// Pattern: A and B
///
/// Event arrives at Stream A:
/// - LogicalPreA matches and creates StateEvent with A filled
/// - Forwards to LogicalPostA
/// - LogicalPostA checks: is B filled in StateEvent?
///   - NO: mark state changed, don't forward yet
///
/// Event arrives at Stream B:
/// - LogicalPreB matches same StateEvent, now has both A and B
/// - Forwards to LogicalPostB
/// - LogicalPostB checks: is A filled in StateEvent?
///   - YES: forward to next processor (pattern complete!)
/// ```
///
/// **Example - OR**:
/// ```text
/// Pattern: A or B
///
/// Event arrives at Stream A:
/// - LogicalPreA matches and creates StateEvent with A filled
/// - Forwards to LogicalPostA
/// - LogicalPostA: OR satisfied, forward immediately
/// - Mark LogicalPostB as "event returned" (partner side)
///
/// Event arrives at Stream B:
/// - LogicalPreB checks: already matched by partner?
///   - YES: skip (OR already satisfied)
/// ```
pub struct LogicalPostStateProcessor {
    /// The underlying StreamPostStateProcessor for basic pattern match handling
    base_processor: StreamPostStateProcessor,

    /// Type of logical operation (AND or OR)
    logical_type: LogicalType,

    /// Reference to partner's PreStateProcessor (for AND logic checking)
    partner_pre_processor: Option<Arc<Mutex<LogicalPreStateProcessor>>>,

    /// Reference to partner's PostStateProcessor (for OR logic coordination)
    partner_post_processor: Option<Arc<Mutex<LogicalPostStateProcessor>>>,
}

impl LogicalPostStateProcessor {
    /// Create a new LogicalPostStateProcessor
    ///
    /// # Arguments
    /// * `state_id` - Position in the StateEvent for this processor
    /// * `logical_type` - AND or OR logic
    pub fn new(state_id: usize, logical_type: LogicalType) -> Self {
        Self {
            base_processor: StreamPostStateProcessor::new(state_id),
            logical_type,
            partner_pre_processor: None,
            partner_post_processor: None,
        }
    }

    /// Get the logical type
    pub fn logical_type(&self) -> LogicalType {
        self.logical_type
    }

    /// Set the partner PreStateProcessor
    /// Used for AND logic to check if partner's event is present
    pub fn set_partner_pre_processor(&mut self, partner: Arc<Mutex<LogicalPreStateProcessor>>) {
        self.partner_pre_processor = Some(partner);
    }

    /// Set the partner PostStateProcessor
    /// Used for OR logic to coordinate event returned flag
    pub fn set_partner_post_processor(&mut self, partner: Arc<Mutex<LogicalPostStateProcessor>>) {
        self.partner_post_processor = Some(partner);
    }

    /// Set the associated PreStateProcessor
    pub fn set_this_state_pre_processor(
        &mut self,
        pre_processor: Arc<Mutex<dyn PreStateProcessor>>,
    ) {
        self.base_processor
            .set_this_state_pre_processor(pre_processor);
    }

    /// Process a matched StateEvent with AND/OR logic
    fn process_with_logic(&mut self, state_event: &StateEvent) {
        match self.logical_type {
            LogicalType::And => {
                // AND: only proceed if partner's event is also present
                let mut should_process = false;

                if let Some(ref partner_pre) = self.partner_pre_processor {
                    if let Ok(partner_guard) = partner_pre.lock() {
                        let partner_state_id = partner_guard.state_id();

                        // Check if partner's position has an event
                        if state_event.get_stream_event(partner_state_id).is_some() {
                            should_process = true;
                        }
                    }
                }

                if should_process {
                    // Both sides matched, forward normally
                    self.base_processor.process_state_event(state_event);
                } else {
                    // Partner not matched yet, just mark state changed
                    // Don't forward yet - waiting for both sides
                    if let Some(this_pre) = self.base_processor.this_state_pre_processor.as_ref() {
                        if let Ok(mut guard) = this_pre.lock() {
                            guard.state_changed();
                        }
                    }
                }
            }
            LogicalType::Or => {
                // OR: always forward (either side is sufficient)
                self.base_processor.process_state_event(state_event);

                // Mark partner as "event returned" too
                if let Some(ref partner_post) = self.partner_post_processor {
                    if let Ok(mut partner_guard) = partner_post.lock() {
                        // Check if partner has output processor
                        if partner_guard.base_processor.get_next_processor().is_some() {
                            partner_guard.base_processor.is_event_returned = true;
                        }
                    }
                }
            }
        }
    }
}

// Manual Debug implementation
impl Debug for LogicalPostStateProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogicalPostStateProcessor")
            .field("state_id", &self.base_processor.state_id())
            .field("logical_type", &self.logical_type)
            .field("has_partner_pre", &self.partner_pre_processor.is_some())
            .field("has_partner_post", &self.partner_post_processor.is_some())
            .finish()
    }
}

impl PostStateProcessor for LogicalPostStateProcessor {
    fn process(&mut self, chunk: Option<Box<dyn ComplexEvent>>) -> Option<Box<dyn ComplexEvent>> {
        if let Some(event) = chunk {
            if let Some(state_event) = event.as_any().downcast_ref::<StateEvent>() {
                // Apply AND/OR logic
                self.process_with_logic(state_event);

                // If we have a next processor, forward
                if self.base_processor.get_next_processor().is_some() {
                    return Some(event);
                }
            }
        }
        None
    }

    fn set_next_processor(&mut self, processor: Arc<Mutex<dyn PostStateProcessor>>) {
        self.base_processor.set_next_processor(processor);
    }

    fn get_next_processor(&self) -> Option<Arc<Mutex<dyn PostStateProcessor>>> {
        self.base_processor.get_next_processor()
    }

    fn state_id(&self) -> usize {
        self.base_processor.state_id()
    }

    fn set_next_state_pre_processor(
        &mut self,
        next_state_pre_processor: Arc<Mutex<dyn PreStateProcessor>>,
    ) {
        self.base_processor
            .set_next_state_pre_processor(next_state_pre_processor.clone());

        // Also set for partner (coordination)
        if let Some(ref partner_post) = self.partner_post_processor {
            if let Ok(mut partner_guard) = partner_post.lock() {
                partner_guard
                    .base_processor
                    .set_next_state_pre_processor(next_state_pre_processor);
            }
        }
    }

    fn set_next_every_state_pre_processor(
        &mut self,
        next_every_state_pre_processor: Arc<Mutex<dyn PreStateProcessor>>,
    ) {
        self.base_processor
            .set_next_every_state_pre_processor(next_every_state_pre_processor.clone());

        // Also set for partner (coordination)
        if let Some(ref partner_post) = self.partner_post_processor {
            if let Ok(mut partner_guard) = partner_post.lock() {
                partner_guard
                    .base_processor
                    .set_next_every_state_pre_processor(next_every_state_pre_processor);
            }
        }
    }

    fn set_callback_pre_state_processor(
        &mut self,
        callback_pre_state_processor: Arc<Mutex<dyn PreStateProcessor>>,
    ) {
        self.base_processor
            .set_callback_pre_state_processor(callback_pre_state_processor);
    }

    fn get_next_every_state_pre_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>> {
        self.base_processor.get_next_every_state_pre_processor()
    }

    fn is_event_returned(&self) -> bool {
        self.base_processor.is_event_returned()
    }

    fn clear_processed_event(&mut self) {
        self.base_processor.clear_processed_event()
    }

    fn this_state_pre_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>> {
        self.base_processor.this_state_pre_processor()
    }
}

// ===== Tests =====

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::eventflux_app_context::EventFluxAppContext;
    use crate::core::config::eventflux_query_context::EventFluxQueryContext;
    use crate::core::query::input::stream::state::stream_pre_state_processor::StateType;

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
        let processor = LogicalPostStateProcessor::new(0, LogicalType::And);
        assert_eq!(processor.logical_type(), LogicalType::And);
        assert_eq!(processor.state_id(), 0);
    }

    #[test]
    fn test_new_or_processor() {
        let processor = LogicalPostStateProcessor::new(1, LogicalType::Or);
        assert_eq!(processor.logical_type(), LogicalType::Or);
        assert_eq!(processor.state_id(), 1);
    }

    // ===== Partner Coordination Tests =====

    #[test]
    fn test_set_partner_pre_processor() {
        let mut post_processor = LogicalPostStateProcessor::new(0, LogicalType::And);
        let (app_ctx, query_ctx) = create_test_context();
        let pre_processor = Arc::new(Mutex::new(LogicalPreStateProcessor::new(
            1,
            false,
            LogicalType::And,
            StateType::Pattern,
            app_ctx,
            query_ctx,
        )));

        post_processor.set_partner_pre_processor(pre_processor);
        assert!(post_processor.partner_pre_processor.is_some());
    }

    #[test]
    fn test_set_partner_post_processor() {
        let mut processor_a = LogicalPostStateProcessor::new(0, LogicalType::Or);
        let processor_b = Arc::new(Mutex::new(LogicalPostStateProcessor::new(
            1,
            LogicalType::Or,
        )));

        processor_a.set_partner_post_processor(processor_b);
        assert!(processor_a.partner_post_processor.is_some());
    }

    // ===== Debug Implementation Test =====

    #[test]
    fn test_debug_impl() {
        let processor = LogicalPostStateProcessor::new(0, LogicalType::And);
        let debug_str = format!("{:?}", processor);
        assert!(debug_str.contains("LogicalPostStateProcessor"));
        assert!(debug_str.contains("logical_type"));
    }
}
