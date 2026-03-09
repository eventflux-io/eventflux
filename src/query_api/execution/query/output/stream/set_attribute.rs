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
use crate::query_api::expression::Variable; // Corrected path

#[derive(Clone, Debug, PartialEq)] // Default is tricky due to Variable and Expression
pub struct SetAttribute {
    pub eventflux_element: EventFluxElement,
    pub table_column: Variable, // Java uses Variable, not String.
    pub value_to_set: Expression,
}

impl SetAttribute {
    pub fn new(table_column: Variable, value_to_set: Expression) -> Self {
        Self {
            eventflux_element: EventFluxElement::default(),
            table_column,
            value_to_set,
        }
    }
}

// SetAttribute in Java is a EventFluxElement. So this struct also composes/implements it.
// Default derive removed as Variable and Expression don't have trivial defaults for this context.

// The UpdateSet class from Java would be a separate struct, likely in its own file (e.g., update_set.rs)
// and would contain `pub set_attribute_list: Vec<SetAttribute>`.
// For now, only defining SetAttribute as requested by the file name in the prompt.
