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

// Corresponds to io.eventflux.query.api.expression.condition.Or
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::expression::Expression;

#[derive(Clone, Debug, PartialEq)] // Removed Default
pub struct Or {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement

    pub left_expression: Box<Expression>,
    pub right_expression: Box<Expression>,
}

impl Or {
    pub fn new(left_expression: Expression, right_expression: Expression) -> Self {
        Or {
            eventflux_element: EventFluxElement::default(),
            left_expression: Box::new(left_expression),
            right_expression: Box::new(right_expression),
        }
    }
}
