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

use super::state_element::StateElement;
use crate::query_api::eventflux_element::EventFluxElement;

/// How the two sides of a logical pattern step combine. Defaults to `And`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy, Default)]
pub enum Type {
    #[default]
    And,
    Or,
    // No Not variant. NOT is modelled with AbsentStreamStateElement.
}

/// A pattern step joining two stream steps with AND or OR, as in `A AND B`.
///
/// Has no `Default` because both sides are required.
#[derive(Clone, Debug, PartialEq)]
pub struct LogicalStateElement {
    pub eventflux_element: EventFluxElement,

    pub stream_state_element_1: Box<StateElement>,
    pub logical_type: Type,
    pub stream_state_element_2: Box<StateElement>,
}

impl LogicalStateElement {
    /// # Panics
    ///
    /// Panics if either side is not a `StateElement::Stream` or `StateElement::AbsentStream`.
    pub fn new(sse1: StateElement, logical_type: Type, sse2: StateElement) -> Self {
        // Each side must stand for a single stream.
        match sse1 {
            StateElement::Stream(_) | StateElement::AbsentStream(_) => {}
            _ => panic!(
                "LogicalStateElement operand 1 must be a Stream or AbsentStream type StateElement"
            ),
        }
        match sse2 {
            StateElement::Stream(_) | StateElement::AbsentStream(_) => {}
            _ => panic!(
                "LogicalStateElement operand 2 must be a Stream or AbsentStream type StateElement"
            ),
        }

        LogicalStateElement {
            eventflux_element: EventFluxElement::default(),
            stream_state_element_1: Box::new(sse1),
            logical_type,
            stream_state_element_2: Box::new(sse2),
        }
    }
}

// No Default impl, because StateElement has no Default. If StateElement ever gets one,
// such as StateElement::Stream(StreamStateElement::default()), this would work:
// impl Default for LogicalStateElement {
//     fn default() -> Self {
//         Self {
//             eventflux_element: EventFluxElement::default(),
//             stream_state_element_1: Box::new(StateElement::default()), // Requires StateElement::default()
//             logical_type: Type::default(),
//             stream_state_element_2: Box::new(StateElement::default()), // Requires StateElement::default()
//         }
//     }
// }
