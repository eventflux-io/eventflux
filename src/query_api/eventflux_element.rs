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

/// Where a query API object starts and ends in the source query text.
///
/// Other query API structs hold this as an `eventflux_element` field. Both positions are
/// `None` when unknown, which is the default.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct EventFluxElement {
    pub query_context_start_index: Option<(i32, i32)>,
    pub query_context_end_index: Option<(i32, i32)>,
}

impl EventFluxElement {
    pub fn new(start_index: Option<(i32, i32)>, end_index: Option<(i32, i32)>) -> Self {
        EventFluxElement {
            query_context_start_index: start_index,
            query_context_end_index: end_index,
        }
    }
}

// A getter and setter trait that was considered and not used:
// pub trait EventFluxElementTrait {
//     fn query_context_start_index(&self) -> Option<(i32, i32)>;
//     fn set_query_context_start_index(&mut self, index: Option<(i32, i32)>);
//     fn query_context_end_index(&self) -> Option<(i32, i32)>;
//     fn set_query_context_end_index(&mut self, index: Option<(i32, i32)>);
// }
// impl EventFluxElementTrait for EventFluxElement { ... }
// It would help structs that hold an EventFluxElement delegate to it. The fields are
// public, so direct access works without it. Structs hold the element in an
// `eventflux_element` field and start it with `EventFluxElement::default()`.
