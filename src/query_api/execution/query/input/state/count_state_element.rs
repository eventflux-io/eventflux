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

// Corresponds to io.eventflux.query.api.execution.query.input.state.CountStateElement
use super::stream_state_element::StreamStateElement;
use crate::query_api::eventflux_element::EventFluxElement;

// Constant for ANY count, from Java's CountStateElement.ANY = -1
pub const ANY_COUNT: i32 = -1;

#[derive(Clone, Debug, PartialEq)] // Default is not straightforward
pub struct CountStateElement {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement

    // CountStateElement fields
    pub stream_state_element: Box<StreamStateElement>,
    pub min_count: i32, // Using i32 and ANY_COUNT to represent optionality, like Java
    pub max_count: i32, // Using i32 and ANY_COUNT
}

impl CountStateElement {
    pub fn new(stream_state_element: StreamStateElement, min_count: i32, max_count: i32) -> Self {
        CountStateElement {
            eventflux_element: EventFluxElement::default(),
            stream_state_element: Box::new(stream_state_element),
            min_count,
            max_count,
        }
    }
}

// No Default derive due to required StreamStateElement and specific count logic.
// A user would typically construct this with specific counts or ANY_COUNT.
// Defaulting min/max to ANY_COUNT might be an option if StreamStateElement was Default.
// impl Default for CountStateElement {
//     fn default() -> Self {
//         Self {
//             eventflux_element: EventFluxElement::default(),
//             stream_state_element: Box::default(), // Requires StreamStateElement::default()
//             min_count: ANY_COUNT,
//             max_count: ANY_COUNT,
//         }
//     }
// }
