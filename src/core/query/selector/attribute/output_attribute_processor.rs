// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/selector/attribute/output_attribute_processor.rs
// Corresponds to io.eventflux.core.query.selector.attribute.processor.AttributeProcessor
use crate::core::config::eventflux_app_context::EventFluxAppContext; // For cloning executor
use crate::core::event::complex_event::ComplexEvent; // Trait
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor; // Trait
use crate::query_api::definition::attribute::Type as ApiAttributeType; // Import Type enum
use std::fmt::Debug;
use std::sync::Arc;

// This struct processes a single attribute in the select clause.
// It wraps an ExpressionExecutor and knows at which position its result should be placed in the output event.
#[derive(Debug)] // Manual Debug might be needed if Box<dyn ExpressionExecutor> does not derive Debug in some contexts
pub struct OutputAttributeProcessor {
    expression_executor: Box<dyn ExpressionExecutor>,
    // The output_position is where this processor's result will be placed in the ComplexEvent's output_data.
    // The output_position is no longer needed here as SelectProcessor will manage the output array construction.
    // output_position: usize,
    return_type: ApiAttributeType, // Cached from expression_executor
}

impl OutputAttributeProcessor {
    // output_position removed from constructor
    pub fn new(expression_executor: Box<dyn ExpressionExecutor>) -> Self {
        let return_type = expression_executor.get_return_type();
        Self {
            expression_executor,
            // output_position, // Removed
            return_type,
        }
    }

    // Processes the event and returns the computed AttributeValue.
    // Takes Option<&dyn ComplexEvent> to align with ExpressionExecutor::execute and SelectProcessor's call.
    pub fn process(&self, event_opt: Option<&dyn ComplexEvent>) -> AttributeValue {
        self.expression_executor
            .execute(event_opt)
            .unwrap_or(AttributeValue::Null)
    }

    pub fn get_return_type(&self) -> ApiAttributeType {
        self.return_type
    }

    // Needed for SelectProcessor::clone_processor
    // Requires ExpressionExecutor to have clone_executor method.
    pub fn clone_oap(&self, eventflux_app_context: &Arc<EventFluxAppContext>) -> Self {
        Self {
            expression_executor: self
                .expression_executor
                .clone_executor(eventflux_app_context),
            // output_position: self.output_position, // Removed
            return_type: self.return_type,
        }
    }

    /// Whether this attribute processor involves an aggregator function.
    pub fn is_aggregator(&self) -> bool {
        self.expression_executor.is_attribute_aggregator()
    }
}
