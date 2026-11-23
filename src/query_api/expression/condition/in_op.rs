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

/// An IN test against a table or window, as in `symbol IN StockTable`.
///
/// Named InOp because `in` is a Rust keyword.
#[derive(Clone, Debug, PartialEq)]
pub struct InOp {
    pub eventflux_element: EventFluxElement,

    pub expression: Box<Expression>,
    /// Name of the table or window to look in.
    pub source_id: String,
}

impl InOp {
    pub fn new(expression: Expression, source_id: String) -> Self {
        InOp {
            eventflux_element: EventFluxElement::default(),
            expression: Box::new(expression),
            source_id,
        }
    }
}
