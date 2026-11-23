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
use crate::query_api::expression::Variable;

/// One assignment in an UPDATE SET clause, as in `price = s.price`.
///
/// Has no `Default` because both the column and the value are required.
#[derive(Clone, Debug, PartialEq)]
pub struct SetAttribute {
    pub eventflux_element: EventFluxElement,
    /// The table column to set. A Variable so it can carry a table qualifier.
    pub table_column: Variable,
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

// The list of assignments lives in UpdateSet, in update_set.rs.
