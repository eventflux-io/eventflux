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

// Corresponds to io.eventflux.query.api.expression.condition.Compare
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::expression::Expression;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy, Default)] // Added Eq, Hash, Copy
pub enum Operator {
    LessThan,
    GreaterThan,
    LessThanEqual,
    GreaterThanEqual,
    #[default]
    Equal,
    NotEqual,
}

// This From impl was for a placeholder Java enum, can be removed or adapted if JNI is used.
// For now, assuming it's not needed for pure Rust logic.
// impl From<io_eventflux_query_api_expression_condition_Compare_Operator> for Operator { ... }
// #[allow(non_camel_case_types)]
// enum io_eventflux_query_api_expression_condition_Compare_Operator { ... }

#[derive(Clone, Debug, PartialEq)] // Removed Default
pub struct Compare {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement

    // Compare specific fields
    pub left_expression: Box<Expression>,
    pub operator: Operator,
    pub right_expression: Box<Expression>,
}

impl Compare {
    pub fn new(
        left_expression: Expression,
        operator: Operator,
        right_expression: Expression,
    ) -> Self {
        Compare {
            eventflux_element: EventFluxElement::default(),
            left_expression: Box::new(left_expression),
            operator,
            right_expression: Box::new(right_expression),
        }
    }
}
