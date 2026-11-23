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
    // is_per_event_trace_enabled: bool, // Can get from eventflux_app_context.get_eventflux_context().get_statistics_configuration()
}

impl FilterProcessor {
    /// Create a filter for `condition_executor`.
    ///
    /// Prints the runtime settings to stdout when built.
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

        let filter_processor = Self {
            meta: CommonProcessorMeta::new(eventflux_app_context, eventflux_query_context),
            condition_executor,
        };

        filter_processor.log_configuration_info();

        Ok(filter_processor)
    }

    /// Print the main runtime settings to stdout with println!, not the logger
    fn log_configuration_info(&self) {
        let config = self.meta.eventflux_app_context.get_global_config();

        println!("FilterProcessor initialized with configuration:");
        println!(
            "  - Thread pool size: {}",
            config.eventflux.runtime.performance.thread_pool_size
        );
        println!(
            "  - Event buffer size: {}",
            config.eventflux.runtime.performance.event_buffer_size
        );
        println!(
            "  - Batch processing: {}",
            config.eventflux.runtime.performance.batch_processing
        );
        println!(
            "  - Async processing: {}",
            config.eventflux.runtime.performance.async_processing
        );
        println!("  - Runtime mode: {:?}", config.eventflux.runtime.mode);
    }

    /// True when batch processing is turned on in the app config
    fn should_use_batch_processing(&self) -> bool {
        self.meta
            .eventflux_app_context
            .is_batch_processing_enabled()
    }

    /// Configured event buffer size, doubled in distributed mode
    fn get_optimal_buffer_size(&self) -> usize {
        let base_size = self
            .meta
            .eventflux_app_context
            .get_configured_event_buffer_size();
        if self.meta.eventflux_app_context.is_distributed_mode() {
            base_size * 2
        } else {
            base_size
        }
    }
}

impl Processor for FilterProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        // The app config picks batch or one by one processing
        if self.should_use_batch_processing() {
            self.process_batch_mode(complex_event_chunk);
        } else {
            self.process_single_mode(complex_event_chunk);
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

impl FilterProcessor {
    /// Filter the chunk and send all passing events to the next processor as one chunk.
    ///
    /// Sends even when nothing passed, in which case the next processor gets None.
    fn process_single_mode(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        // Builds a new chunk from the passing events. Editing the chunk in place or using
        // an event pool could be faster.

        let mut filtered_events: Vec<Box<dyn ComplexEvent>> = Vec::new();

        let mut current_event_opt = complex_event_chunk;
        while let Some(mut current_event_box) = current_event_opt {
            // Unlink the event so it can be checked on its own
            let next_event_in_original_chunk = current_event_box.set_next(None);

            let passes_filter = match self
                .condition_executor
                .execute(Some(current_event_box.as_ref()))
            {
                Some(AttributeValue::Bool(true)) => true,
                Some(AttributeValue::Bool(false)) | Some(AttributeValue::Null) => false,
                None => false, // the condition gave no value, so drop the event
                _ => {
                    // log_error!("Filter condition did not return a boolean for event: {:?}", current_event_box);
                    false
                }
            };

            if passes_filter {
                filtered_events.push(current_event_box);
            } // otherwise the event is dropped

            current_event_opt = next_event_in_original_chunk;
        }

        // Link the passing events back into a chunk
        let mut filtered_chunk_head: Option<Box<dyn ComplexEvent>> = None;
        let mut tail_next_ref: &mut Option<Box<dyn ComplexEvent>> = &mut filtered_chunk_head;
        for event_box in filtered_events {
            *tail_next_ref = Some(event_box);
            if let Some(ref mut current_tail) = *tail_next_ref {
                tail_next_ref = current_tail.mut_next_ref_option();
            }
        }

        if let Some(ref next_proc_arc) = self.meta.next_processor {
            next_proc_arc.lock().unwrap().process(filtered_chunk_head);
        }
    }

    /// Filter the chunk and send passing events on in batches.
    ///
    /// The batch size is the buffer size from get_optimal_buffer_size, capped at 1000.
    /// Nothing is sent when no event passes.
    fn process_batch_mode(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        let batch_size = self.get_optimal_buffer_size().min(1000);
        let mut filtered_events: Vec<Box<dyn ComplexEvent>> = Vec::with_capacity(batch_size);

        let mut current_event_opt = complex_event_chunk;
        let mut batch_count = 0;

        while let Some(mut current_event_box) = current_event_opt {
            // Unlink the event so it can be checked on its own
            let next_event_in_original_chunk = current_event_box.set_next(None);

            let passes_filter = match self
                .condition_executor
                .execute(Some(current_event_box.as_ref()))
            {
                Some(AttributeValue::Bool(true)) => true,
                Some(AttributeValue::Bool(false)) | Some(AttributeValue::Null) => false,
                None => false, // the condition gave no value, so drop the event
                _ => false,
            };

            if passes_filter {
                filtered_events.push(current_event_box);
                batch_count += 1;

                if batch_count >= batch_size {
                    self.send_batch_to_next_processor(&mut filtered_events);
                    batch_count = 0;
                }
            }

            current_event_opt = next_event_in_original_chunk;
        }

        // Send whatever is left as a final, smaller batch
        if !filtered_events.is_empty() {
            self.send_batch_to_next_processor(&mut filtered_events);
        }
    }

    /// Link the events into a chunk and send it to the next processor. Empties
    /// `filtered_events`. Does nothing when it is empty.
    fn send_batch_to_next_processor(&self, filtered_events: &mut Vec<Box<dyn ComplexEvent>>) {
        if filtered_events.is_empty() {
            return;
        }

        let mut filtered_chunk_head: Option<Box<dyn ComplexEvent>> = None;
        let mut tail_next_ref: &mut Option<Box<dyn ComplexEvent>> = &mut filtered_chunk_head;

        for event_box in filtered_events.drain(..) {
            *tail_next_ref = Some(event_box);
            if let Some(ref mut current_tail) = *tail_next_ref {
                tail_next_ref = current_tail.mut_next_ref_option();
            }
        }

        if let Some(ref next_proc_arc) = self.meta.next_processor {
            next_proc_arc.lock().unwrap().process(filtered_chunk_head);
        }
    }
}
