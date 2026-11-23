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

use super::on_demand_query::{OnDemandQuery, OnDemandQueryType};
use super::OutputStream;
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::execution::query::input::InputStore;
use crate::query_api::execution::query::selection::Selector;

/// What a store query does. Same variants as OnDemandQueryType. Defaults to `Select`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy, Default)]
pub enum StoreQueryType {
    Insert,
    Delete,
    Update,
    #[default]
    Select,
    UpdateOrInsert,
    Find,
}

/// A thin wrapper over OnDemandQuery. Every builder and getter passes through to the
/// inner query.
#[derive(Clone, Debug, PartialEq)]
pub struct StoreQuery {
    /// A copy of the inner query's positions, taken in `new`. Not kept in sync after that,
    /// except that `set_type` copies it back into the inner query.
    pub eventflux_element: EventFluxElement,
    pub on_demand_query: OnDemandQuery,
}

impl StoreQuery {
    pub fn new(on_demand_query: OnDemandQuery) -> Self {
        StoreQuery {
            eventflux_element: on_demand_query.eventflux_element.clone(),
            on_demand_query,
        }
    }

    /// Wrap a default OnDemandQuery. Starts a builder chain.
    pub fn query() -> Self {
        Self::new(OnDemandQuery::default())
    }

    /// The inner query's type, mapped to StoreQueryType. `None` if no type was set.
    pub fn get_type(&self) -> Option<StoreQueryType> {
        self.on_demand_query
            .on_demand_query_type
            .as_ref()
            .map(|odt| match odt {
                OnDemandQueryType::Insert => StoreQueryType::Insert,
                OnDemandQueryType::Delete => StoreQueryType::Delete,
                OnDemandQueryType::Update => StoreQueryType::Update,
                OnDemandQueryType::Select => StoreQueryType::Select,
                OnDemandQueryType::UpdateOrInsert => StoreQueryType::UpdateOrInsert,
                OnDemandQueryType::Find => StoreQueryType::Find,
            })
    }

    /// Set the inner query's type. Also overwrites the inner query's positions with this
    /// wrapper's copy.
    pub fn set_type(mut self, store_query_type: StoreQueryType) -> Self {
        let odq_type = match store_query_type {
            StoreQueryType::Insert => OnDemandQueryType::Insert,
            StoreQueryType::Delete => OnDemandQueryType::Delete,
            StoreQueryType::Update => OnDemandQueryType::Update,
            StoreQueryType::Select => OnDemandQueryType::Select,
            StoreQueryType::UpdateOrInsert => OnDemandQueryType::UpdateOrInsert,
            StoreQueryType::Find => OnDemandQueryType::Find,
        };
        let current_eventflux_element = self.eventflux_element.clone();
        self.on_demand_query = self.on_demand_query.set_type(odq_type);
        self.on_demand_query.eventflux_element = current_eventflux_element;
        self
    }

    pub fn from(mut self, input_store: InputStore) -> Self {
        self.on_demand_query = self.on_demand_query.from(input_store);
        self
    }

    pub fn select(mut self, selector: Selector) -> Self {
        self.on_demand_query = self.on_demand_query.select(selector);
        self
    }

    pub fn out_stream(mut self, output_stream: OutputStream) -> Self {
        self.on_demand_query = self.on_demand_query.out_stream(output_stream);
        self
    }

    pub fn get_input_store(&self) -> Option<&InputStore> {
        self.on_demand_query.get_input_store()
    }

    pub fn get_selector(&self) -> &Selector {
        self.on_demand_query.get_selector()
    }

    pub fn get_output_stream(&self) -> &OutputStream {
        self.on_demand_query.get_output_stream()
    }

    pub fn get_on_demand_query(&self) -> &OnDemandQuery {
        &self.on_demand_query
    }
}

impl Default for StoreQuery {
    fn default() -> Self {
        let default_odq = OnDemandQuery::default();
        StoreQuery {
            eventflux_element: default_odq.eventflux_element.clone(),
            on_demand_query: default_odq,
        }
    }
}

// StoreQuery keeps its own copy of the query positions, set from on_demand_query in new().
// It can drift from the inner query's copy. If the two should always match, an accessor
// like this could read the inner query's element instead:
// impl EventFluxElementAccess for StoreQuery {
//     fn eventflux_element(&self) -> &EventFluxElement {
//         &self.on_demand_query.eventflux_element
//     }
//     fn eventflux_element_mut(&mut self) -> &mut EventFluxElement {
//         &mut self.on_demand_query.eventflux_element
//     }
// }
