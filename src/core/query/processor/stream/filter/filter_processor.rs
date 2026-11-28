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

// src/core/query/processor/stream/filter/filter_processor.rs
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::core::query::processor::{CommonProcessorMeta, ProcessingMode, Processor};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

// The Java code has no separate filter class. There, filtering happens inside
// SingleStreamProcessor or JoinProcessor through a ConditionExpressionExecutor.
/// Stream processor that passes on only the events for which the condition is true.
///
/// A condition result of false, Null, no value or a non boolean drops the event.
#[derive(Debug)]
pub struct FilterProcessor {
    meta: CommonProcessorMeta,
    condition_executor: Box<dyn ExpressionExecutor>,
}

impl FilterProcessor {
    /// Create a filter for `condition_executor`.
    ///
    /// # Errors
    /// Returns an error message when the condition does not return BOOL.
    pub fn new(
        condition_executor: Box<dyn ExpressionExecutor>,
        eventflux_app_context: Arc<EventFluxAppContext>,
        eventflux_query_context: Arc<EventFluxQueryContext>, // holds the query name
    ) -> Result<Self, String> {
        if condition_executor.get_return_type() != crate::query_api::definition::AttributeType::BOOL
        {
            return Err(format!(
                "Filter condition executor must return BOOL, but found {:?}",
                condition_executor.get_return_type()
            ));
        }

        Ok(Self {
            meta: CommonProcessorMeta::new(eventflux_app_context, eventflux_query_context),
            condition_executor,
        })
    }

    fn passes(&self, event: &dyn ComplexEvent) -> bool {
        matches!(
            self.condition_executor.execute(Some(event)),
            Some(AttributeValue::Bool(true))
        )
    }
}

impl Processor for FilterProcessor {
    /// Send the events that pass on to the next processor, as one chunk in their
    /// original order.
    ///
    /// A `None` chunk is passed on as is, because upstream processors send it to ask
    /// for a flush, as an output rate limiter needs at shutdown. A chunk where no event
    /// passes sends nothing.
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        let Some(ref next) = self.meta.next_processor else {
            return;
        };
        if complex_event_chunk.is_none() {
            next.lock().unwrap().process(None);
            return;
        }

        // Link the passing events into a new chunk as they are checked, with no copy.
        let mut head: Option<Box<dyn ComplexEvent>> = None;
        let mut tail = &mut head;
        let mut current = complex_event_chunk;
        while let Some(mut event) = current {
            // Unlink the event so it can be checked and moved on its own
            current = event.set_next(None);
            if self.passes(event.as_ref()) {
                tail = tail.insert(event).mut_next_ref_option();
            }
        }

        if head.is_some() {
            next.lock().unwrap().process(head);
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.meta.next_processor.as_ref().map(Arc::clone)
    }

    fn set_next_processor(&mut self, next_processor: Option<Arc<Mutex<dyn Processor>>>) {
        self.meta.next_processor = next_processor;
    }

    fn clone_processor(
        &self,
        eventflux_query_context: &Arc<EventFluxQueryContext>,
    ) -> Box<dyn Processor> {
        let cloned_condition_executor = self
            .condition_executor
            .clone_executor(&eventflux_query_context.eventflux_app_context);
        Box::new(FilterProcessor {
            meta: CommonProcessorMeta::new(
                Arc::clone(&eventflux_query_context.eventflux_app_context),
                Arc::clone(eventflux_query_context),
            ),
            condition_executor: cloned_condition_executor,
        })
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }

    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.meta.get_eventflux_query_context()
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::DEFAULT
    }

    fn is_stateful(&self) -> bool {
        false
    }
}
