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

use super::stream_state_element::StreamStateElement;
use crate::query_api::eventflux_element::EventFluxElement;

/// Count value that means no bound on that side.
pub const ANY_COUNT: i32 = -1;

/// A pattern step that matches a stream between `min_count` and `max_count` times, as in
/// `A{2:5}`.
///
/// Has no `Default` because the stream element is required.
#[derive(Clone, Debug, PartialEq)]
pub struct CountStateElement {
    pub eventflux_element: EventFluxElement,

    pub stream_state_element: Box<StreamStateElement>,
    /// Lowest match count, or `ANY_COUNT` for no lower bound.
    pub min_count: i32,
    /// Highest match count, or `ANY_COUNT` for no upper bound.
    pub max_count: i32,
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

// A possible Default with both counts unbounded. StreamStateElement does implement
// Default, so this would compile, but an element with no real stream is of little use.
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
