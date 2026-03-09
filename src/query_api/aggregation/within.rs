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

use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::expression::Expression;

#[derive(Clone, Debug, PartialEq)] // Default is tricky due to required fields
#[derive(Default)]
pub struct Within {
    pub eventflux_element: EventFluxElement,
    // In Java, timeRange can hold one (pattern) or two (start, end) expressions.
    // We can model this as an enum or distinct fields.
    // Using distinct fields for clarity, matching the two factory methods.
    pub pattern_expression: Option<Box<Expression>>, // For within(Expression pattern)
    pub start_expression: Option<Box<Expression>>,   // For within(Expression start, Expression end)
    pub end_expression: Option<Box<Expression>>,     // For within(Expression start, Expression end)
}

impl Within {
    // Corresponds to Java's within(Expression pattern)
    pub fn new_with_pattern(pattern: Expression) -> Self {
        Self {
            eventflux_element: EventFluxElement::default(),
            pattern_expression: Some(Box::new(pattern)),
            start_expression: None,
            end_expression: None,
        }
    }

    // Corresponds to Java's within(Expression start, Expression end)
    pub fn new_with_range(start: Expression, end: Expression) -> Self {
        Self {
            eventflux_element: EventFluxElement::default(),
            pattern_expression: None,
            start_expression: Some(Box::new(start)),
            end_expression: Some(Box::new(end)),
        }
    }

    // Getter that matches Java's getTimeRange() -> List<Expression>
    // It might be more idiomatic in Rust to provide specific getters for pattern/start/end.
    pub fn get_time_range_expressions(&self) -> Vec<&Expression> {
        let mut range = Vec::new();
        if let Some(p) = &self.pattern_expression {
            range.push(p.as_ref());
        }
        if let Some(s) = &self.start_expression {
            range.push(s.as_ref());
        }
        if let Some(e) = &self.end_expression {
            range.push(e.as_ref());
        }
        range
    }
}

// Default might not be very useful as either pattern or start/end is required.
// However, if needed for Default derive on other structs that contain Option<Within>:
