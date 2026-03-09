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

// Corresponds to io.eventflux.query.api.EventFluxElement

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)] // Added Eq, Hash, Default
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

// The trait approach mentioned in previous subtasks for get/set methods:
// pub trait EventFluxElementTrait {
//     fn query_context_start_index(&self) -> Option<(i32, i32)>;
//     fn set_query_context_start_index(&mut self, index: Option<(i32, i32)>);
//     fn query_context_end_index(&self) -> Option<(i32, i32)>;
//     fn set_query_context_end_index(&mut self, index: Option<(i32, i32)>);
// }
// impl EventFluxElementTrait for EventFluxElement { ... }
// This is useful if other structs embed EventFluxElement and want to delegate.
// For now, direct field access is okay as they are public.
// The prompt asks to ensure structs that *use* EventFluxElement initialize it with `EventFluxElement::default()`.
// This implies that structs will *compose* EventFluxElement.
// The existing files mostly have `query_context_start_index` fields directly,
// which is fine if they also implement `EventFluxElementTrait` or provide similar methods.
// The prompt for this task asks for `element: EventFluxElement::default()` in structs,
// so I will assume composition is the target for structs that are EventFluxElements.
// This means many files will need to change from direct fields to `element: EventFluxElement`.
