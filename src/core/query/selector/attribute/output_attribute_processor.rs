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

// src/core/query/selector/attribute/output_attribute_processor.rs
// Port of Siddhi's io.siddhi.core.query.selector.attribute.processor.AttributeProcessor
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::fmt::Debug;
use std::sync::Arc;

// Computes one attribute of the select clause by running its ExpressionExecutor.
// SelectProcessor decides where each result goes in the output event.
#[derive(Debug)]
pub struct OutputAttributeProcessor {
    expression_executor: Box<dyn ExpressionExecutor>,
    // The output position used to live here. SelectProcessor now builds the output
    // array, so it is not needed.
    // output_position: usize,
    return_type: ApiAttributeType, // taken from expression_executor at construction
}

impl OutputAttributeProcessor {
    pub fn new(expression_executor: Box<dyn ExpressionExecutor>) -> Self {
        let return_type = expression_executor.get_return_type();
        Self {
            expression_executor,
            // output_position, // Removed
            return_type,
        }
    }

    // Run the expression on the event and return its value, or Null when it gives none.
    // Takes Option<&dyn ComplexEvent> to match ExpressionExecutor::execute.
    pub fn process(&self, event_opt: Option<&dyn ComplexEvent>) -> AttributeValue {
        self.expression_executor
            .execute(event_opt)
            .unwrap_or(AttributeValue::Null)
    }

    pub fn get_return_type(&self) -> ApiAttributeType {
        self.return_type
    }

    // Copy this processor, cloning the executor with clone_executor. Used by
    // SelectProcessor::clone_processor.
    pub fn clone_oap(&self, eventflux_app_context: &Arc<EventFluxAppContext>) -> Self {
        Self {
            expression_executor: self
                .expression_executor
                .clone_executor(eventflux_app_context),
            // output_position: self.output_position, // Removed
            return_type: self.return_type,
        }
    }

    /// True when the top level expression is an aggregator function.
    pub fn is_aggregator(&self) -> bool {
        self.expression_executor.is_attribute_aggregator()
    }
}
