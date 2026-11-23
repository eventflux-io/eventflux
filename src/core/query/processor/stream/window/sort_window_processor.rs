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

// src/core/query/processor/stream/window/sort_window_processor.rs
// Sort window processor, ported from Siddhi's SortWindowProcessor

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

/// Sliding window that keeps at most `length_to_keep` events. When one more arrives,
/// the events are sorted and the last one in sort order is expired.
#[derive(Debug)]
pub struct SortWindowProcessor {
    meta: CommonProcessorMeta,
    /// Most events to keep, after any config scaling
    length_to_keep: usize,
    /// Kept events. Only sorted when the window overflows.
    sorted_window: Arc<Mutex<Vec<Arc<StreamEvent>>>>,
    /// Sort order from the window's attribute and order parameters
    comparator: OrderByEventComparator,
}

impl SortWindowProcessor {
    /// Create a sort window.
    ///
    /// The length is scaled by the `distributed_size_factor` setting when set, and the
    /// buffer is sized from config. Prints the settings to stdout.
    pub fn new(
        length_to_keep: usize,
        comparator: OrderByEventComparator,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        let effective_length = Self::calculate_effective_window_size(length_to_keep, &app_ctx);
        let initial_capacity = Self::calculate_initial_capacity(effective_length, &app_ctx);

        let processor = SortWindowProcessor {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            length_to_keep: effective_length,
            sorted_window: Arc::new(Mutex::new(Vec::with_capacity(initial_capacity))),
            comparator,
        };

        processor.log_window_configuration();

        processor
    }

    /// Window length to use, which is the requested size times the sort window's
    /// `distributed_size_factor` setting, rounded up. The requested size when the
    /// setting or config reader is missing.
    fn calculate_effective_window_size(
        requested_size: usize,
        app_ctx: &EventFluxAppContext,
    ) -> usize {
        let base_size = requested_size;

        if let Some(config_reader) = app_ctx.get_config_reader() {
            if let Some(ConfigValue::Float(factor)) =
                config_reader.get_window_config("sort", "distributed_size_factor")
            {
                return (base_size as f64 * factor).ceil() as usize;
            }
        }

        base_size
    }

    /// Starting capacity for the buffer, which is the window size times
    /// `initial_capacity_multiplier` (default 1.2), times 1.25 more when
    /// `batch_processing_enabled` is true, rounded up.
    fn calculate_initial_capacity(window_size: usize, app_ctx: &EventFluxAppContext) -> usize {
        let multiplier = if let Some(config_reader) = app_ctx.get_config_reader() {
            if let Some(ConfigValue::Float(mult)) =
                config_reader.get_window_config("sort", "initial_capacity_multiplier")
            {
                mult
            } else {
                1.2 // default multiplier
            }
        } else {
            1.2
        };

        let batch_multiplier = if let Some(config_reader) = app_ctx.get_config_reader() {
            if let Some(ConfigValue::Boolean(true)) =
                config_reader.get_performance_config("batch_processing_enabled")
            {
                1.25 // extra room in batch mode
            } else {
                1.0
            }
        } else {
            1.0
        };

        (window_size as f64 * multiplier * batch_multiplier).ceil() as usize
    }

    /// Print the window size and any sort settings to stdout with println!
    fn log_window_configuration(&self) {
        println!("SortWindowProcessor configured:");
        println!("  - Window size: {} events", self.length_to_keep);

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

    /// Whether to use memory saving processing. Always false, since the runtime config
    /// is not reachable from here.
    #[allow(dead_code)]
    fn should_use_memory_optimization(&self) -> bool {
        // TODO: read this from the config reader
        false
    }

    /// Build the window from `sort(length, attr1, 'asc', attr2, 'desc', ...)`.
    ///
    /// Each order after an attribute is optional and defaults to ascending.
    ///
    /// # Errors
    /// Returns an error message when the length is missing, not a positive integer
    /// constant, when an attribute is not a plain stream attribute, when an order is
    /// not 'asc' or 'desc', or when no attribute is given.
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

        // First parameter: window length, required
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

        // The rest are attributes, each optionally followed by its order
        // Format: sort(length, attr1, 'asc', attr2, 'desc', ...)
        let mut executors: Vec<
            Box<dyn crate::core::executor::expression_executor::ExpressionExecutor>,
        > = Vec::new();
        let mut ascending: Vec<bool> = Vec::new();

        let remaining_params = &params[1..];
        let mut i = 0;

        while i < remaining_params.len() {
            // Each pass starts at an attribute expression
            let attr_expr = &remaining_params[i];

            let executor = crate::core::util::parser::expression_parser::parse_expression(
                attr_expr, parse_ctx,
            )
            .map_err(|e| format!("Failed to parse sort attribute: {}", e))?;

            // Only plain stream attributes are allowed, not constants or other expressions
            if !executor.is_variable_executor() {
                return Err(format!(
                    "Sort window requires variable expressions (stream attributes), not constants or complex expressions. \
                    Invalid parameter at position {}. Use attribute names only (e.g., 'price', 'volume').",
                    i + 2  // +1 to count from 1, +1 to skip the size parameter
                ));
            }

            executors.push(executor);

            // An optional 'asc' or 'desc' may follow the attribute
            let is_ascending = if i + 1 < remaining_params.len() {
                if let Expression::Constant(c) = &remaining_params[i + 1] {
                    if let ConstantValueWithFloat::String(order_str) = &c.value {
                        let order_lower = order_str.to_lowercase();
                        if order_lower == "asc" {
                            i += 1; // skip past the order parameter
                            true
                        } else if order_lower == "desc" {
                            i += 1; // skip past the order parameter
                            false
                        } else {
                            // Any other string is rejected
                            return Err(format!(
                                "Sort window order parameter must be 'asc' or 'desc', found: '{}'. \
                                Valid usage: WINDOW('sort', size, attribute, 'asc') or WINDOW('sort', size, attribute, 'desc')",
                                order_str
                            ));
                        }
                    } else {
                        true // not a string, so ascending. The next pass parses it.
                    }
                } else {
                    true // next parameter is not a constant, so ascending
                }
            } else {
                true // no order given, so ascending
            };

            ascending.push(is_ascending);
            i += 1;
        }

        if executors.is_empty() {
            return Err("Sort window requires at least one sort attribute".to_string());
        }

        let comparator = OrderByEventComparator::new(executors, ascending);

        Ok(Self::new(length_to_keep, comparator, app_ctx, query_ctx))
    }

    /// Add one event to the window.
    ///
    /// Returns the event as Current, followed by the expired event when the window
    /// overflowed.
    ///
    /// # Errors
    /// Returns an error message when the window lock is poisoned.
    fn process_event(&self, event: Arc<StreamEvent>) -> Result<Vec<Box<dyn ComplexEvent>>, String> {
        let mut sorted_buffer = self
            .sorted_window
            .lock()
            .map_err(|_| "Failed to acquire sort window lock".to_string())?;

        // Events do not change once created, so sharing the Arc is safe and cheaper
        // than a copy
        sorted_buffer.push(Arc::clone(&event));

        let mut result = Vec::new();

        // The new event always goes out first, as Current
        let mut current_stream_event = event.as_ref().clone_without_next();
        current_stream_event.set_event_type(ComplexEventType::Current);
        result.push(Box::new(current_stream_event) as Box<dyn ComplexEvent>);

        // Over the limit, so sort and expire the last event in sort order
        if sorted_buffer.len() > self.length_to_keep {
            sorted_buffer.sort_by(|a, b| self.comparator.compare(a.as_ref(), b.as_ref()));

            if let Some(expired_event) = sorted_buffer.pop() {
                let mut expired_stream_event = expired_event.as_ref().clone_without_next();
                expired_stream_event.set_event_type(ComplexEventType::Expired);

                // Stamp it with the new event's time. Later processors rely on events
                // arriving in timestamp order.
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

                // Link every result into one chunk for the next processor
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
            // TODO: clone the comparator. The clone gets one with no sort attributes.
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
