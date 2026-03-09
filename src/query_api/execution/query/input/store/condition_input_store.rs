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

// Corresponds to io.eventflux.query.api.execution.query.input.store.ConditionInputStore
use super::input_store::InputStoreTrait;
use super::store::Store; // The Store struct (wrapper around BasicSingleInputStream)
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::expression::Expression; // To implement get_store_id etc.

#[derive(Clone, Debug, PartialEq)] // Default is not straightforward due to Store and Expression
pub struct ConditionInputStore {
    pub eventflux_element: EventFluxElement, // For its own context if needed

    pub store: Store, // The underlying store (table, window, aggregation)
    pub on_condition: Option<Expression>, // Condition is optional in some Store.on() calls in Java, but required for ConditionInputStore
}

impl ConditionInputStore {
    pub fn new(store: Store, on_condition: Expression) -> Self {
        ConditionInputStore {
            eventflux_element: EventFluxElement::default(), // Or copy context from store?
            store,
            on_condition: Some(on_condition),
        }
    }

    // If on_condition can truly be optional for this type (Java constructor implies it's not)
    // pub fn new_optional_condition(store: Store, on_condition: Option<Expression>) -> Self { ... }
}

// `impl EventFluxElement for ConditionInputStore` removed.

impl InputStoreTrait for ConditionInputStore {
    fn get_store_id(&self) -> &str {
        self.store.get_store_id() // Delegate to composed Store
    }

    fn get_store_reference_id(&self) -> Option<&str> {
        self.store.get_store_reference_id() // Delegate to composed Store
    }
}
