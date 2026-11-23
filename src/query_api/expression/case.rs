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

/// A CASE expression.
///
/// Covers both forms. A searched CASE (CASE WHEN condition THEN result ...) and a simple
/// CASE (CASE operand WHEN value THEN result ...).
#[derive(Clone, Debug, PartialEq)]
pub struct Case {
    /// The operand of a simple CASE. `None` for a searched CASE.
    pub operand: Option<Box<Expression>>,
    /// The WHEN clauses in order.
    pub when_clauses: Vec<WhenClause>,
    /// The ELSE result. Always present. The SQL converter fills in NULL when the query has
    /// no ELSE.
    pub else_result: Box<Expression>,
    /// Position in the query text, for error messages.
    pub eventflux_element: EventFluxElement,
}

/// One WHEN clause of a CASE expression.
///
/// `condition` is a boolean test in a searched CASE, or the value to compare with the
/// operand in a simple CASE.
#[derive(Clone, Debug, PartialEq)]
pub struct WhenClause {
    pub condition: Box<Expression>,
    pub result: Box<Expression>,
}

impl Case {
    pub fn new(
        operand: Option<Box<Expression>>,
        when_clauses: Vec<WhenClause>,
        else_result: Box<Expression>,
    ) -> Self {
        Self {
            operand,
            when_clauses,
            else_result,
            eventflux_element: EventFluxElement::default(),
        }
    }
}

impl WhenClause {
    pub fn new(condition: Box<Expression>, result: Box<Expression>) -> Self {
        Self { condition, result }
    }
}
