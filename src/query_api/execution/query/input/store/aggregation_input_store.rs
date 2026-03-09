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

// Corresponds to io.eventflux.query.api.execution.query.input.store.AggregationInputStore
use super::input_store::InputStoreTrait;
use super::store::Store; // The base Store
use crate::query_api::aggregation::Within;
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::expression::Expression; // Using the actual Within struct

#[derive(Clone, Debug, PartialEq)] // Default not straightforward
pub struct AggregationInputStore {
    // In Java, it extends ConditionInputStore. We will compose Store and add fields.
    // Or compose ConditionInputStore if that's more aligned.
    // Let's compose Store directly and manage on_condition here too.
    pub eventflux_element: EventFluxElement,

    pub store: Store,
    pub on_condition: Option<Expression>, // From ConditionInputStore part

    // AggregationInputStore specific fields
    pub within: Option<Within>,
    pub per: Option<Expression>,
}

impl AggregationInputStore {
    // Constructor for when there's an ON condition
    pub fn new_with_condition(
        store: Store,
        on_condition: Expression,
        within: Within,
        per: Expression,
    ) -> Self {
        AggregationInputStore {
            eventflux_element: EventFluxElement::default(),
            store,
            on_condition: Some(on_condition),
            within: Some(within),
            per: Some(per),
        }
    }

    // Constructor for when there's no ON condition (onCondition is null in Java)
    pub fn new_no_condition(store: Store, within: Within, per: Expression) -> Self {
        AggregationInputStore {
            eventflux_element: EventFluxElement::default(),
            store,
            on_condition: None,
            within: Some(within),
            per: Some(per),
        }
    }
}

// `impl EventFluxElement for AggregationInputStore` removed.

impl InputStoreTrait for AggregationInputStore {
    fn get_store_id(&self) -> &str {
        self.store.get_store_id() // Delegate
    }

    fn get_store_reference_id(&self) -> Option<&str> {
        self.store.get_store_reference_id() // Delegate
    }
}
