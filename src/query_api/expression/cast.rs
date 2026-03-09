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

//! CAST Expression
//!
//! Represents a type conversion expression: CAST(expr AS type)

use super::expression::Expression;
use crate::query_api::definition::attribute::Type as AttributeType;
use crate::query_api::eventflux_element::EventFluxElement;

/// CAST expression for type conversion
///
/// SQL syntax: `CAST(expression AS target_type)`
///
/// Supports conversions between:
/// - String to numeric types (INT, LONG, FLOAT, DOUBLE)
/// - Numeric types to String
/// - Numeric type widening (INT -> LONG, FLOAT -> DOUBLE)
/// - Numeric type narrowing (LONG -> INT, DOUBLE -> FLOAT)
#[derive(Clone, Debug, PartialEq)]
pub struct Cast {
    /// The expression to convert
    pub expression: Box<Expression>,
    /// The target type to convert to
    pub target_type: AttributeType,
    /// EventFlux element metadata
    pub eventflux_element: EventFluxElement,
}

impl Cast {
    /// Create a new Cast expression
    pub fn new(expression: Expression, target_type: AttributeType) -> Self {
        Self {
            expression: Box::new(expression),
            target_type,
            eventflux_element: EventFluxElement::default(),
        }
    }
}
