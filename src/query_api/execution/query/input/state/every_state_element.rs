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

// Corresponds to io.eventflux.query.api.execution.query.input.state.EveryStateElement
use super::state_element::StateElement;
use crate::query_api::eventflux_element::EventFluxElement; // Recursive definition
                                                           // Expression is not used here as per Java structure. 'within' is on StateInputStream.

#[derive(Clone, Debug, PartialEq)] // Default is not straightforward
pub struct EveryStateElement {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement

    // EveryStateElement fields
    pub state_element: Box<StateElement>,
    // The 'within' clause is associated with the whole pattern in StateInputStream,
    // not with individual 'every' elements in the Java API.
}

impl EveryStateElement {
    pub fn new(state_element: StateElement) -> Self {
        EveryStateElement {
            eventflux_element: EventFluxElement::default(),
            state_element: Box::new(state_element),
        }
    }
}

// No Default derive due to required Box<StateElement>.
