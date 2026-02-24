// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/processor/stream/window/sort_window_processor.rs
// Rust implementation of EventFlux SortWindowProcessor

use crate::core::config::{
    eventflux_app_context::EventFluxAppContext, eventflux_query_context::EventFluxQueryContext,
    ConfigValue,
};

#[cfg(test)]
use crate::core::config::eventflux_context::EventFluxContext;
use crate::core::event::complex_event::{ComplexEvent, ComplexEventType};
use crate::core::event::stream::StreamEvent;
use crate::core::query::processor::stream::window::WindowProcessor;
use crate::core::query::processor::{CommonProcessorMeta, ProcessingMode, Processor};
use crate::core::query::selector::order_by_event_comparator::OrderByEventComparator;
use crate::query_api::execution::query::input::handler::WindowHandler;
use crate::query_api::expression::{constant::ConstantValueWithFloat, Expression};

use std::sync::{Arc, Mutex};

/// A sort window maintains a fixed-size sliding window of events in sorted order
#[derive(Debug)]
pub struct SortWindowProcessor {
    /// Common processor metadata
    meta: CommonProcessorMeta,
    /// Maximum number of events to keep in the window
    length_to_keep: usize,
    /// Sorted buffer of events
    sorted_window: Arc<Mutex<Vec<Arc<StreamEvent>>>>,
    /// Comparator for sorting events
    comparator: OrderByEventComparator,
}

impl SortWindowProcessor {
    /// Create a new sort window processor
    pub fn new(
        length_to_keep: usize,
        comparator: OrderByEventComparator,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        // Configuration-driven initialization
        let effective_length = Self::calculate_effective_window_size(length_to_keep, &app_ctx);
        let initial_capacity = Self::calculate_initial_capacity(effective_length, &app_ctx);

        let processor = SortWindowProcessor {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            length_to_keep: effective_length,
            sorted_window: Arc::new(Mutex::new(Vec::with_capacity(initial_capacity))),
            comparator,
        };

        // Log configuration-driven setup
        processor.log_window_configuration();

        processor
    }

    /// Calculate effective window size based on configuration
    fn calculate_effective_window_size(
        requested_size: usize,
        app_ctx: &EventFluxAppContext,
    ) -> usize {
        let base_size = requested_size;

        // Use the configuration reader to get distributed size factor
        if let Some(config_reader) = app_ctx.get_config_reader() {
            if let Some(ConfigValue::Float(factor)) =
                config_reader.get_window_config("sort", "distributed_size_factor")
            {
                // Apply scaling factor - for now we'll use a simple approach
                return (base_size as f64 * factor).ceil() as usize;
            }
        }

        // Fallback to base size if configuration is unavailable
        base_size
    }

    /// Calculate initial capacity for the sorted buffer
    fn calculate_initial_capacity(window_size: usize, app_ctx: &EventFluxAppContext) -> usize {
        // Get multiplier from configuration
        let multiplier = if let Some(config_reader) = app_ctx.get_config_reader() {
            if let Some(ConfigValue::Float(mult)) =
                config_reader.get_window_config("sort", "initial_capacity_multiplier")
            {
                mult
            } else {
                1.2 // Default multiplier
            }
        } else {
            1.2
        };

        // Check for batch processing mode from performance config
        let batch_multiplier = if let Some(config_reader) = app_ctx.get_config_reader() {
            if let Some(ConfigValue::Boolean(true)) =
                config_reader.get_performance_config("batch_processing_enabled")
            {
                1.25 // Additional multiplier for batch mode
            } else {
                1.0
            }
        } else {
            1.0
        };

        (window_size as f64 * multiplier * batch_multiplier).ceil() as usize
    }

    /// Log configuration-driven initialization
    fn log_window_configuration(&self) {
        println!("SortWindowProcessor configured:");
        println!("  - Window size: {} events", self.length_to_keep);

        // Log configuration if available through config reader
        if let Some(config_reader) = self.meta.eventflux_app_context.get_config_reader() {
            if let Some(ConfigValue::Float(factor)) =
                config_reader.get_window_config("sort", "distributed_size_factor")
            {
                println!("  - Distributed size factor: {}", factor);
            }
            if let Some(ConfigValue::Float(multiplier)) =
                config_reader.get_window_config("sort", "initial_capacity_multiplier")
            {
                println!("  - Initial capacity multiplier: {}", multiplier);
            }
        }
    }

    /// Check if memory-optimized processing should be used
    fn should_use_memory_optimization(&self) -> bool {
        // For now, return false since we don't have direct access to runtime config
        // TODO: Implement memory optimization detection through config reader
        false
    }

    /// Create from window handler (standard factory pattern)
    pub fn from_handler(
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Self, String> {
        let params = handler.get_parameters();

        if params.is_empty() {
            return Err("Sort window requires at least a length parameter".to_string());
        }

        // First parameter: window length (required)
        let length_to_keep = match params.first() {
            Some(Expression::Constant(c)) => match &c.value {
                ConstantValueWithFloat::Int(i) if *i > 0 => *i as usize,
                ConstantValueWithFloat::Long(l) if *l > 0 => *l as usize,
                ConstantValueWithFloat::Int(_) | ConstantValueWithFloat::Long(_) => {
                    return Err("Sort window length must be positive".to_string())
                }
                _ => return Err("Sort window length must be an integer".to_string()),
            },
            _ => return Err("Sort window length must be a constant".to_string()),
        };

        // Parse remaining parameters as attribute/order pairs
        // Format: sort(length, attr1, 'asc', attr2, 'desc', ...)
        let mut executors: Vec<
            Box<dyn crate::core::executor::expression_executor::ExpressionExecutor>,
        > = Vec::new();
        let mut ascending: Vec<bool> = Vec::new();

        let remaining_params = &params[1..];
        let mut i = 0;

        while i < remaining_params.len() {
            // Each iteration should process an attribute expression
            let attr_expr = &remaining_params[i];

            // Parse the attribute expression using the context
            let executor = crate::core::util::parser::expression_parser::parse_expression(
                attr_expr, parse_ctx,
            )
            .map_err(|e| format!("Failed to parse sort attribute: {}", e))?;

            // CRITICAL VALIDATION: Only accept Variable expressions (attributes), not constants or complex expressions
            if !executor.is_variable_executor() {
                return Err(format!(
                    "Sort window requires variable expressions (stream attributes), not constants or complex expressions. \
                    Invalid parameter at position {}. Use attribute names only (e.g., 'price', 'volume').",
                    i + 2  // +2 because: +1 for 1-based indexing, +1 to skip size param
                ));
            }

            executors.push(executor);

            // Check for optional order specification ('asc' or 'desc')
            let is_ascending = if i + 1 < remaining_params.len() {
                if let Expression::Constant(c) = &remaining_params[i + 1] {
                    if let ConstantValueWithFloat::String(order_str) = &c.value {
                        let order_lower = order_str.to_lowercase();
                        if order_lower == "asc" {
                            i += 1; // Consume the order parameter
                            true
                        } else if order_lower == "desc" {
                            i += 1; // Consume the order parameter
                            false
                        } else {
                            // STRICT VALIDATION: Reject invalid order strings
                            return Err(format!(
                                "Sort window order parameter must be 'asc' or 'desc', found: '{}'. \
                                Valid usage: WINDOW('sort', size, attribute, 'asc') or WINDOW('sort', size, attribute, 'desc')",
                                order_str
                            ));
                        }
                    } else {
                        true // Default to ascending if not a string
                    }
                } else {
                    true // Default to ascending if next param is not a constant
                }
            } else {
                true // Default to ascending if no order specified
            };

            ascending.push(is_ascending);
            i += 1;
        }

        // Validate that we have at least one sort attribute
        if executors.is_empty() {
            return Err("Sort window requires at least one sort attribute".to_string());
        }

        let comparator = OrderByEventComparator::new(executors, ascending);

        Ok(Self::new(length_to_keep, comparator, app_ctx, query_ctx))
    }

    /// Process an incoming event
    fn process_event(&self, event: Arc<StreamEvent>) -> Result<Vec<Box<dyn ComplexEvent>>, String> {
        let mut sorted_buffer = self
            .sorted_window
            .lock()
            .map_err(|_| "Failed to acquire sort window lock".to_string())?;

        // Add the new event to the buffer
        // Note: We store Arc references for efficiency. Events are immutable in EventFlux,
        // so sharing via Arc is safe and more idiomatic than cloning.
        sorted_buffer.push(Arc::clone(&event));

        let mut result = Vec::new();

        // Always emit the current event first
        let mut current_stream_event = event.as_ref().clone_without_next();
        current_stream_event.set_event_type(ComplexEventType::Current);
        result.push(Box::new(current_stream_event) as Box<dyn ComplexEvent>);

        // If we exceed the window size, sort and remove the last element
        if sorted_buffer.len() > self.length_to_keep {
            // Sort using the OrderByEventComparator with attribute-based sorting
            sorted_buffer.sort_by(|a, b| self.comparator.compare(a.as_ref(), b.as_ref()));

            // Remove the last element (highest in sort order) and mark it as expired
            if let Some(expired_event) = sorted_buffer.pop() {
                let mut expired_stream_event = expired_event.as_ref().clone_without_next();
                expired_stream_event.set_event_type(ComplexEventType::Expired);

                // CRITICAL: Update timestamp on expired event to current time
                // Required for downstream processors that depend on event ordering by timestamp
                expired_stream_event.set_timestamp(event.timestamp);

                result.push(Box::new(expired_stream_event) as Box<dyn ComplexEvent>);
            }
        }

        Ok(result)
    }
}

impl Processor for SortWindowProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        if let Some(ref next) = self.meta.next_processor {
            if let Some(chunk) = complex_event_chunk {
                let mut current_opt = Some(chunk.as_ref() as &dyn ComplexEvent);
                let mut all_events: Vec<Box<dyn ComplexEvent>> = Vec::new();

                while let Some(ev) = current_opt {
                    if let Some(se) = ev.as_any().downcast_ref::<StreamEvent>() {
                        match self.process_event(Arc::new(se.clone_without_next())) {
                            Ok(events) => {
                                all_events.extend(events);
                            }
                            Err(e) => {
                                eprintln!("Error processing sort window event: {e}");
                            }
                        }
                    }
                    current_opt = ev.get_next();
                }

                // Send all events to next processor
                if !all_events.is_empty() {
                    let mut head: Option<Box<dyn ComplexEvent>> = None;
                    let mut tail = &mut head;

                    for event in all_events {
                        *tail = Some(event);
                        tail = tail.as_mut().unwrap().mut_next_ref_option();
                    }

                    next.lock().unwrap().process(head);
                }
            }
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.meta.next_processor.as_ref().map(Arc::clone)
    }

    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.meta.next_processor = next;
    }

    fn clone_processor(&self, query_ctx: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        Box::new(Self::new(
            self.length_to_keep,
            // TODO: Clone comparator properly
            OrderByEventComparator::new(Vec::new(), Vec::new()),
            Arc::clone(&self.meta.eventflux_app_context),
            Arc::clone(query_ctx),
        ))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }

    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.meta.get_eventflux_query_context()
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::SLIDE
    }

    fn is_stateful(&self) -> bool {
        true
    }
}

impl WindowProcessor for SortWindowProcessor {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort_window_creation() {
        // Basic test to ensure the structure can be created
        let executors = Vec::new();
        let ascending = vec![true];
        let comparator = OrderByEventComparator::new(executors, ascending);

        // This test mainly verifies compilation
        let eventflux_context = Arc::new(EventFluxContext::new());
        let app = Arc::new(crate::query_api::eventflux_app::EventFluxApp::new(
            "TestApp".to_string(),
        ));
        let app_ctx = Arc::new(EventFluxAppContext::new(
            eventflux_context,
            "TestApp".to_string(),
            app,
            String::new(),
        ));
        let query_ctx = Arc::new(EventFluxQueryContext::new(
            app_ctx.clone(),
            "test".to_string(),
            None,
        ));

        let _processor = SortWindowProcessor {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            length_to_keep: 3,
            sorted_window: Arc::new(Mutex::new(Vec::new())),
            comparator,
        };
    }
}
