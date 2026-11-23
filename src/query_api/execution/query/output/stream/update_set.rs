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

use super::set_attribute::SetAttribute;
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::expression::Expression;
use crate::query_api::expression::Variable;

/// The assignments of an UPDATE SET clause, in order.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct UpdateSet {
    pub eventflux_element: EventFluxElement,
    pub set_attributes: Vec<SetAttribute>,
}

impl UpdateSet {
    pub fn new() -> Self {
        Self {
            eventflux_element: EventFluxElement::default(),
            set_attributes: Vec::new(),
        }
    }

    /// Append the assignment `table_column = value_to_set`.
    pub fn add_set_attribute(mut self, table_column: Variable, value_to_set: Expression) -> Self {
        self.set_attributes
            .push(SetAttribute::new(table_column, value_to_set));
        self
    }

    // No ON condition here. It lives on UpdateStreamAction and UpdateOrInsertStreamAction
    // as on_update_expression.
}
