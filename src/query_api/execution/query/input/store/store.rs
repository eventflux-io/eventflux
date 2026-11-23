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

use super::aggregation_input_store::AggregationInputStore;
use super::condition_input_store::ConditionInputStore;
use super::input_store::InputStoreTrait;
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::execution::query::input::stream::SingleInputStream;
use crate::query_api::expression::Expression;

use crate::query_api::aggregation::Within;

/// A table, window or aggregation read by an on demand query. Named through a basic
/// SingleInputStream so it can carry an alias and stream handlers.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Store {
    /// Holds the store id, its alias and any handlers such as filters.
    pub single_input_stream: SingleInputStream,
    /// Query positions of the store itself. Separate from the ones on
    /// `single_input_stream`.
    pub eventflux_element: EventFluxElement,
}

impl Store {
    /// A store with just an id. Used by `InputStore::store`.
    pub fn new_with_id(store_id: String) -> Self {
        Store {
            eventflux_element: EventFluxElement::default(),
            // Basic stream that is neither inner nor a fault stream.
            single_input_stream: SingleInputStream::new_basic(
                store_id,
                false,
                false,
                None,
                Vec::new(),
            ),
        }
    }

    /// A store with an id and an alias. Used by `InputStore::store_with_ref`.
    pub fn new_with_ref(store_reference_id: String, store_id: String) -> Self {
        Store {
            eventflux_element: EventFluxElement::default(),
            single_input_stream: SingleInputStream::new_basic(
                store_id,
                false,
                false,
                Some(store_reference_id),
                Vec::new(),
            ),
        }
    }

    // Store takes no condition itself. Add one with on_condition() or the
    // on_aggregation_* methods. The results can be wrapped in the InputStore enum.

    /// Read this store filtered by `on_condition`.
    pub fn on_condition(self, on_condition: Expression) -> ConditionInputStore {
        ConditionInputStore::new(self, on_condition)
    }

    /// Read this aggregation over `within` at the `per` time unit, filtered by
    /// `on_condition`.
    pub fn on_aggregation_condition(
        self,
        on_condition: Expression,
        within: Within,
        per: Expression,
    ) -> AggregationInputStore {
        AggregationInputStore::new_with_condition(self, on_condition, within, per)
    }

    /// Read this aggregation over `within` at the `per` time unit, with no condition.
    pub fn on_aggregation_only(self, within: Within, per: Expression) -> AggregationInputStore {
        AggregationInputStore::new_no_condition(self, within, per)
    }
}

impl InputStoreTrait for Store {
    /// The store id is the stream id of the inner SingleInputStream.
    fn get_store_id(&self) -> &str {
        self.single_input_stream.get_stream_id_str()
    }

    fn get_store_reference_id(&self) -> Option<&str> {
        self.single_input_stream.get_stream_reference_id_str()
    }
}

// Stream builder methods passed on to the inner SingleInputStream.
impl Store {
    /// Add a filter handler to the inner stream.
    pub fn filter(mut self, filter_expression: Expression) -> Self {
        self.single_input_stream = self.single_input_stream.filter(filter_expression);
        self
    }
    // TODO: Pass on function, window and as_ref from SingleInputStream too.
}
