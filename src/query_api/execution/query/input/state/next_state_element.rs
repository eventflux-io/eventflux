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

// Corresponds to io.eventflux.query.api.execution.query.input.state.NextStateElement
use super::state_element::StateElement;
use crate::query_api::eventflux_element::EventFluxElement; // Recursive definition

#[derive(Clone, Debug, PartialEq)] // Default is not straightforward due to required Box<StateElement>
pub struct NextStateElement {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement

    // NextStateElement fields
    pub state_element: Box<StateElement>,
    pub next_state_element: Box<StateElement>, // Kept as required, not Option
}

impl NextStateElement {
    pub fn new(state_element: StateElement, next_state_element: StateElement) -> Self {
        NextStateElement {
            eventflux_element: EventFluxElement::default(),
            state_element: Box::new(state_element),
            next_state_element: Box::new(next_state_element),
        }
    }
}

// No Default derive or custom impl for now, as it requires a default StateElement.
// Similar to LogicalStateElement.
