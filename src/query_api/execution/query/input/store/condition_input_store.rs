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

use super::input_store::InputStoreTrait;
use super::store::Store;
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::expression::Expression;

/// A read from a store filtered by an ON condition. Has no `Default`.
#[derive(Clone, Debug, PartialEq)]
pub struct ConditionInputStore {
    /// Query positions of this element. Separate from the ones on `store`.
    pub eventflux_element: EventFluxElement,

    /// The table, window or aggregation being read.
    pub store: Store,
    /// Always `Some` when built with `new`.
    pub on_condition: Option<Expression>,
}

impl ConditionInputStore {
    pub fn new(store: Store, on_condition: Expression) -> Self {
        ConditionInputStore {
            eventflux_element: EventFluxElement::default(),
            store,
            on_condition: Some(on_condition),
        }
    }

    // A constructor that allows no condition, if that is ever needed.
    // pub fn new_optional_condition(store: Store, on_condition: Option<Expression>) -> Self { ... }
}

impl InputStoreTrait for ConditionInputStore {
    fn get_store_id(&self) -> &str {
        self.store.get_store_id()
    }

    fn get_store_reference_id(&self) -> Option<&str> {
        self.store.get_store_reference_id()
    }
}
