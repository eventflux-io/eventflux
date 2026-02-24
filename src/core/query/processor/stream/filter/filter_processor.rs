// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/processor/stream/filter/filter_processor.rs
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::eventflux_query_context::EventFluxQueryContext; // For clone_processor
use crate::core::event::complex_event::ComplexEvent; // Trait
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor; // Trait
use crate::core::query::processor::{CommonProcessorMeta, ProcessingMode, Processor}; // Use CommonProcessorMeta
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

// FilterProcessor doesn't exist as a distinct class in Java EventFlux's core structure.
// Filtering is typically part of SingleStreamProcessor or JoinProcessor using a ConditionExpressionExecutor.
// This Rust struct is created as per the prompt to represent a dedicated filter.
/// A stream processor that filters events based on a condition.
#[derive(Debug)]
pub struct FilterProcessor {
    meta: CommonProcessorMeta, // Common fields like eventflux_app_context, query_name, next_processor
    condition_executor: Box<dyn ExpressionExecutor>,
    // is_per_event_trace_enabled: bool, // Can get from eventflux_app_context.get_eventflux_context().get_statistics_configuration()
}

impl FilterProcessor {
    pub fn new(
        condition_executor: Box<dyn ExpressionExecutor>,
        eventflux_app_context: Arc<EventFluxAppContext>,
        eventflux_query_context: Arc<EventFluxQueryContext>, // query_name is in here
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

        // Example: Log configuration-driven initialization
        filter_processor.log_configuration_info();

        Ok(filter_processor)
    }

    /// Example method demonstrating configuration access
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

    /// Example method showing how processors can adapt behavior based on configuration
    fn should_use_batch_processing(&self) -> bool {
        self.meta
            .eventflux_app_context
            .is_batch_processing_enabled()
    }

    /// Example method showing configuration-driven buffer sizing
    fn get_optimal_buffer_size(&self) -> usize {
        let base_size = self
            .meta
            .eventflux_app_context
            .get_configured_event_buffer_size();
        // Example: Scale buffer size for distributed mode
        if self.meta.eventflux_app_context.is_distributed_mode() {
            base_size * 2 // Larger buffer for distributed processing
        } else {
            base_size
        }
    }
}

impl Processor for FilterProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        // Configuration-driven processing: Check if batch processing should be used
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
        ProcessingMode::DEFAULT // Filter is usually a pass-through or simple mode
    }

    fn is_stateful(&self) -> bool {
        false // FilterProcessor is typically stateless
    }
}

impl FilterProcessor {
    /// Process events one by one (traditional mode)
    fn process_single_mode(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        // This simplified version processes events one by one from the input chunk
        // and builds a new chunk for the filtered events.
        // More advanced/performant versions might try to modify the chunk in-place
        // or use an event pool.

        let mut filtered_events: Vec<Box<dyn ComplexEvent>> = Vec::new();

        let mut current_event_opt = complex_event_chunk;
        while let Some(mut current_event_box) = current_event_opt {
            // Detach the current event from the original chunk to process it individually.
            let next_event_in_original_chunk = current_event_box.set_next(None);

            let passes_filter = match self
                .condition_executor
                .execute(Some(current_event_box.as_ref()))
            {
                Some(AttributeValue::Bool(true)) => true,
                Some(AttributeValue::Bool(false)) | Some(AttributeValue::Null) => false,
                None => false, // Error or no value from condition executor, filter out
                _ => {
                    // log_error!("Filter condition did not return a boolean for event: {:?}", current_event_box);
                    false
                }
            };

            if passes_filter {
                filtered_events.push(current_event_box);
            } // else: event is dropped

            current_event_opt = next_event_in_original_chunk;
        }

        // Reconstruct linked list from Vec of passed events
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

    /// Process events in batch mode for better performance
    fn process_batch_mode(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        // Get configured batch size for optimal processing
        let batch_size = self.get_optimal_buffer_size().min(1000); // Cap at reasonable limit
        let mut filtered_events: Vec<Box<dyn ComplexEvent>> = Vec::with_capacity(batch_size);

        let mut current_event_opt = complex_event_chunk;
        let mut batch_count = 0;

        while let Some(mut current_event_box) = current_event_opt {
            // Detach the current event from the original chunk to process it individually.
            let next_event_in_original_chunk = current_event_box.set_next(None);

            let passes_filter = match self
                .condition_executor
                .execute(Some(current_event_box.as_ref()))
            {
                Some(AttributeValue::Bool(true)) => true,
                Some(AttributeValue::Bool(false)) | Some(AttributeValue::Null) => false,
                None => false, // Error or no value from condition executor, filter out
                _ => false,
            };

            if passes_filter {
                filtered_events.push(current_event_box);
                batch_count += 1;

                // Process in batches for better performance
                if batch_count >= batch_size {
                    self.send_batch_to_next_processor(&mut filtered_events);
                    batch_count = 0;
                }
            }

            current_event_opt = next_event_in_original_chunk;
        }

        // Process any remaining events in the final batch
        if !filtered_events.is_empty() {
            self.send_batch_to_next_processor(&mut filtered_events);
        }
    }

    /// Helper method to send a batch of filtered events to the next processor
    fn send_batch_to_next_processor(&self, filtered_events: &mut Vec<Box<dyn ComplexEvent>>) {
        if filtered_events.is_empty() {
            return;
        }

        // Reconstruct linked list from Vec of passed events
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
