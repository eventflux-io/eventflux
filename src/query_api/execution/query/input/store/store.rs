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

// Corresponds to io.eventflux.query.api.execution.query.input.store.Store
// Extends SingleInputStream (conceptually) and implements InputStore in Java.

use super::input_store::InputStoreTrait;
// For new_with_id constructor
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::execution::query::input::stream::SingleInputStream; // Changed
use crate::query_api::expression::Expression;
// For on() methods returning these types, which are then wrapped in InputStore enum
// These will need to be refactored to compose EventFluxElement as well.
use super::aggregation_input_store::AggregationInputStore;
use super::condition_input_store::ConditionInputStore;

// Using Within from query_api::aggregation
use crate::query_api::aggregation::Within;

#[derive(Clone, Debug, PartialEq, Default)] // Added Default
pub struct Store {
    // Composes SingleInputStream to inherit its stream-like properties
    pub single_input_stream: SingleInputStream, // Changed field name
    // Store has its own EventFluxElement context, separate from SingleInputStream's inner one.
    // However, Java Store directly uses context of SingleInputStream.
    // Let's ensure `eventflux_element` is part of `Store` directly or consistently accessed.
    // The current impl delegates EventFluxElement to basic_single_input_stream.
    // If Store needs its *own* context distinct from the stream it represents,
    // it would need its own `eventflux_element: EventFluxElement` field.
    // For now, assuming it shares context with its SingleInputStream representation.
    // This means Store does not need its own eventflux_element field if SingleInputStream has one.
    // SingleInputStream has `eventflux_element`.
    // So, Store effectively gets its context via `single_input_stream.eventflux_element`.
    // The `impl EventFluxElement for Store` was removed. Store uses its own eventflux_element field.
    pub eventflux_element: EventFluxElement, // Store itself is a EventFluxElement
}

impl Store {
    // Internal constructor, used by factories in InputStore enum.
    // Made pub(super) in previous version, now pub for direct use if needed.
    pub fn new_with_id(store_id: String) -> Self {
        Store {
            eventflux_element: EventFluxElement::default(),
            // BasicSingleInputStream::new functionality is now part of SingleInputStream::new_basic_from_id or similar.
            // Assuming a basic, non-fault, non-inner stream for a simple store definition.
            single_input_stream: SingleInputStream::new_basic(
                store_id,
                false,
                false,
                None,
                Vec::new(),
            ),
        }
    }

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

    // Constructor from prompt: `new(store_id: String, on_condition: Option<Expression>)`
    // This seems to conflate Store with ConditionInputStore.
    // Java Store is just an ID; conditions are applied via `.on()`.
    // Sticking to Java structure: Store itself doesn't take on_condition in constructor.

    // Methods corresponding to `on(Expression)` and `on(Within, Per)`
    // These return concrete types which can then be wrapped by the InputStore enum.
    pub fn on_condition(self, on_condition: Expression) -> ConditionInputStore {
        // ConditionInputStore would take `self` (a Store instance) and the condition.
        ConditionInputStore::new(self, on_condition) // Assumes ConditionInputStore::new is defined
    }

    pub fn on_aggregation_condition(
        self,
        on_condition: Expression,
        within: Within, // Changed from WithinPlaceholder
        per: Expression,
    ) -> AggregationInputStore {
        AggregationInputStore::new_with_condition(self, on_condition, within, per)
        // Assumes constructor exists
    }

    pub fn on_aggregation_only(
        self,
        within: Within, // Changed from WithinPlaceholder
        per: Expression,
    ) -> AggregationInputStore {
        AggregationInputStore::new_no_condition(self, within, per) // Assumes constructor exists
    }
}

// Implement EventFluxElement for Store.
// It should have its own context, separate from the BasicSingleInputStream it composes if it's a distinct element.
// Java's Store extends BasicSingleInputStream, so it *is* a BasicSingleInputStream and shares its context.
// Our BasicSingleInputStream gets context via its `inner: SingleInputStream`.
// The previous EventFluxElement impl for Store delegated to basic_single_input_stream.
// If Store itself is a EventFluxElement, it should use its own `eventflux_element` field.
// The prompt's example for `StoreQuery` has `element: EventFluxElement`, implying StoreQuery is the element,
// not necessarily the `Store` struct itself if `Store` is just an ID wrapper.
// Let's assume `Store` itself needs to be a `EventFluxElement`.
// `impl EventFluxElement for Store` removed. Store uses its own `eventflux_element` field.

// Implement InputStoreTrait
impl InputStoreTrait for Store {
    fn get_store_id(&self) -> &str {
        // The ID of the store is the stream_id of the composed SingleInputStream
        self.single_input_stream.get_stream_id_str()
    }

    fn get_store_reference_id(&self) -> Option<&str> {
        self.single_input_stream.get_stream_reference_id_str()
    }
}

// Delegate stream-like methods (filter, function, window, as) to SingleInputStream
impl Store {
    pub fn filter(mut self, filter_expression: Expression) -> Self {
        self.single_input_stream = self.single_input_stream.filter(filter_expression); // Assuming SingleInputStream has filter
        self
    }
    // TODO: Add other delegated methods: function, window, as_ref from SingleInputStream
}
