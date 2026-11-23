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

// src/core/query/input/stream/state/logical_post_state_processor.rs
// LogicalPostStateProcessor, which applies the AND or OR rule to a matched StateEvent

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

/// Post processor for one side of an AND or OR pattern.
///
/// LogicalPreStateProcessor sends each matched StateEvent here. This processor
/// applies the AND or OR rule before passing it on.
///
/// **AND**:
/// - Forwards only when the partner's position in the StateEvent is also filled
/// - With no partner pre processor set, it never forwards
/// - When the partner is not filled yet, it marks this side's state as changed and
///   waits
/// - When both are filled, it forwards through the wrapped StreamPostStateProcessor
///
/// **OR**:
/// - Always forwards, since either side is enough
/// - Then, if the partner post processor has a next processor, sets the partner's
///   `is_event_returned` flag too, since the OR is already satisfied
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
    /// Wrapped stream post processor that does the forwarding
    base_processor: StreamPostStateProcessor,

    logical_type: LogicalType,

    /// Partner's pre processor. AND reads its `state_id` to find the partner position.
    partner_pre_processor: Option<Arc<Mutex<LogicalPreStateProcessor>>>,

    /// Partner's post processor. OR sets its `is_event_returned` flag, and the next
    /// state setters copy their target to it.
    partner_post_processor: Option<Arc<Mutex<LogicalPostStateProcessor>>>,
}

impl LogicalPostStateProcessor {
    /// Create one side's post processor.
    ///
    /// # Arguments
    /// * `state_id` - Position of this side in the StateEvent
    /// * `logical_type` - AND or OR
    pub fn new(state_id: usize, logical_type: LogicalType) -> Self {
        Self {
            base_processor: StreamPostStateProcessor::new(state_id),
            logical_type,
            partner_pre_processor: None,
            partner_post_processor: None,
        }
    }

    pub fn logical_type(&self) -> LogicalType {
        self.logical_type
    }

    /// Set the partner's pre processor. AND needs it to find the partner position.
    pub fn set_partner_pre_processor(&mut self, partner: Arc<Mutex<LogicalPreStateProcessor>>) {
        self.partner_pre_processor = Some(partner);
    }

    /// Set the partner's post processor. OR needs it to set the partner's
    /// `is_event_returned` flag.
    pub fn set_partner_post_processor(&mut self, partner: Arc<Mutex<LogicalPostStateProcessor>>) {
        self.partner_post_processor = Some(partner);
    }

    /// Set the pre processor paired with this post processor
    pub fn set_this_state_pre_processor(
        &mut self,
        pre_processor: Arc<Mutex<dyn PreStateProcessor>>,
    ) {
        self.base_processor
            .set_this_state_pre_processor(pre_processor);
    }

    /// Apply the AND or OR rule to a matched StateEvent
    fn process_with_logic(&mut self, state_event: &StateEvent) {
        match self.logical_type {
            LogicalType::And => {
                let mut should_process = false;

                if let Some(ref partner_pre) = self.partner_pre_processor {
                    if let Ok(partner_guard) = partner_pre.lock() {
                        let partner_state_id = partner_guard.state_id();

                        if state_event.get_stream_event(partner_state_id).is_some() {
                            should_process = true;
                        }
                    }
                }

                if should_process {
                    self.base_processor.process_state_event(state_event);
                } else {
                    // Partner side not filled yet. Mark the state changed and wait.
                    if let Some(this_pre) = self.base_processor.this_state_pre_processor.as_ref() {
                        if let Ok(guard) = this_pre.lock() {
                            guard.state_changed();
                        }
                    }
                }
            }
            LogicalType::Or => {
                self.base_processor.process_state_event(state_event);

                // The OR is satisfied, so mark the partner as returned too. Only done
                // when the partner has a next processor.
                if let Some(ref partner_post) = self.partner_post_processor {
                    if let Ok(mut partner_guard) = partner_post.lock() {
                        if partner_guard.base_processor.get_next_processor().is_some() {
                            partner_guard.base_processor.is_event_returned = true;
                        }
                    }
                }
            }
        }
    }
}

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
                self.process_with_logic(state_event);

                // Hand the chunk back only when a next processor is set. This happens
                // even when the AND rule held the event back.
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

        // Both sides lead to the same next step, so set it on the partner too
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

        // Both sides lead to the same next step, so set it on the partner too
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
