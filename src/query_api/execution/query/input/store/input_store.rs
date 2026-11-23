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

use super::aggregation_input_store::AggregationInputStore;
use super::condition_input_store::ConditionInputStore;
use super::store::Store;

/// Methods shared by all store inputs.
pub trait InputStoreTrait {
    /// Id of the table, window or aggregation being read.
    fn get_store_id(&self) -> &str;
    /// The alias given to the store, if any.
    fn get_store_reference_id(&self) -> Option<&str>;
}

/// The store read by an on demand query. A plain store, a store with an ON condition, or
/// an aggregation read with WITHIN and PER.
#[derive(Clone, Debug, PartialEq)]
pub enum InputStore {
    Store(Store),
    // Boxed because these variants are larger than Store.
    Condition(Box<ConditionInputStore>),
    Aggregation(Box<AggregationInputStore>),
}

impl InputStore {
    /// Build a plain Store. It is not wrapped in `InputStore::Store`.
    pub fn store(store_id: String) -> Store {
        Store::new_with_id(store_id)
    }

    /// Build a plain Store with an alias. It is not wrapped in `InputStore::Store`.
    pub fn store_with_ref(store_reference_id: String, store_id: String) -> Store {
        Store::new_with_ref(store_reference_id, store_id)
    }

    // The EventFluxElement of whichever variant this is. Unused for now.
    #[allow(dead_code)]
    fn eventflux_element_ref(&self) -> &EventFluxElement {
        match self {
            InputStore::Store(s) => &s.eventflux_element,
            InputStore::Condition(c) => &c.eventflux_element,
            InputStore::Aggregation(a) => &a.eventflux_element,
        }
    }
    #[allow(dead_code)]
    fn eventflux_element_mut_ref(&mut self) -> &mut EventFluxElement {
        match self {
            InputStore::Store(s) => &mut s.eventflux_element,
            InputStore::Condition(c) => &mut c.eventflux_element,
            InputStore::Aggregation(a) => &mut a.eventflux_element,
        }
    }
}

impl InputStoreTrait for InputStore {
    fn get_store_id(&self) -> &str {
        match self {
            InputStore::Store(s) => s.get_store_id(),
            InputStore::Condition(c) => c.get_store_id(),
            InputStore::Aggregation(a) => a.get_store_id(),
        }
    }

    fn get_store_reference_id(&self) -> Option<&str> {
        match self {
            InputStore::Store(s) => s.get_store_reference_id(),
            InputStore::Condition(c) => c.get_store_reference_id(),
            InputStore::Aggregation(a) => a.get_store_reference_id(),
        }
    }
}
