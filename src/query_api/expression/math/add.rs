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

// Corresponds to io.eventflux.query.api.expression.math.Add
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::expression::Expression; // Main Expression enum

#[derive(Clone, Debug, PartialEq)] // Removed Default
pub struct Add {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement

    // Add specific fields
    // Default for Box<Expression> would be Box::new(Expression::default_variant_if_any)
    // However, these are logically required for Add.
    pub left_value: Box<Expression>,
    pub right_value: Box<Expression>,
}

impl Add {
    pub fn new(left_value: Expression, right_value: Expression) -> Self {
        Add {
            eventflux_element: EventFluxElement::default(),
            left_value: Box::new(left_value),
            right_value: Box::new(right_value),
        }
    }
}

// Deref if needed:
// impl std::ops::Deref for Add {
//     type Target = EventFluxElement;
//     fn deref(&self) -> &Self::Target { &self.eventflux_element }
// }
// impl std::ops::DerefMut for Add {
//     fn deref_mut(&mut self) -> &mut Self::Target { &mut self.eventflux_element }
// }
