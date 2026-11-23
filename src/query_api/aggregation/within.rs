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

/// Time range used when reading from an aggregation. Holds either one pattern expression
/// or a start and end pair. The default value has all three unset.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Within {
    pub eventflux_element: EventFluxElement,
    /// Set by `new_with_pattern`.
    pub pattern_expression: Option<Box<Expression>>,
    /// Set by `new_with_range`, together with `end_expression`.
    pub start_expression: Option<Box<Expression>>,
    /// Set by `new_with_range`, together with `start_expression`.
    pub end_expression: Option<Box<Expression>>,
}

impl Within {
    /// Range given by a single pattern expression.
    pub fn new_with_pattern(pattern: Expression) -> Self {
        Self {
            eventflux_element: EventFluxElement::default(),
            pattern_expression: Some(Box::new(pattern)),
            start_expression: None,
            end_expression: None,
        }
    }

    /// Range given by a start and an end expression.
    pub fn new_with_range(start: Expression, end: Expression) -> Self {
        Self {
            eventflux_element: EventFluxElement::default(),
            pattern_expression: None,
            start_expression: Some(Box::new(start)),
            end_expression: Some(Box::new(end)),
        }
    }

    /// All set expressions in order pattern, start, end. Gives one item for a pattern
    /// range, two for a start and end range, and none for the default value.
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
