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
use crate::query_api::aggregation::Within;
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::expression::Expression;

/// A read from an aggregation over a time range, with an optional ON condition.
///
/// Holds the Store and the condition itself instead of wrapping a ConditionInputStore.
/// Has no `Default`.
#[derive(Clone, Debug, PartialEq)]
pub struct AggregationInputStore {
    pub eventflux_element: EventFluxElement,

    pub store: Store,
    pub on_condition: Option<Expression>,

    /// Time range to read. Both constructors set it.
    pub within: Option<Within>,
    /// Time unit to read results at. Both constructors set it.
    pub per: Option<Expression>,
}

impl AggregationInputStore {
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

    /// Same as `new_with_condition` but with no ON condition.
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

impl InputStoreTrait for AggregationInputStore {
    fn get_store_id(&self) -> &str {
        self.store.get_store_id()
    }

    fn get_store_reference_id(&self) -> Option<&str> {
        self.store.get_store_reference_id()
    }
}
