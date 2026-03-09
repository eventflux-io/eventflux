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

// Corresponds to io.eventflux.query.api.execution.query.input.state.LogicalStateElement
use super::state_element::StateElement;
use crate::query_api::eventflux_element::EventFluxElement;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy)] // Added Eq, Hash, Copy
#[derive(Default)]
pub enum Type {
    #[default]
    And,
    Or,
    // NOT is not part of Java's LogicalStateElement.Type; it's handled by AbsentStreamStateElement.
}

#[derive(Clone, Debug, PartialEq)] // Default is complex due to Box<StateElement>
pub struct LogicalStateElement {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement

    // LogicalStateElement fields
    pub stream_state_element_1: Box<StateElement>,
    pub logical_type: Type,
    pub stream_state_element_2: Box<StateElement>, // Kept as required, not Option
}

impl LogicalStateElement {
    pub fn new(sse1: StateElement, logical_type: Type, sse2: StateElement) -> Self {
        // Validation from previous step is good:
        // Ensure sse1 and sse2 are variants that represent a single effective stream.
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

// Custom Default implementation because Box<StateElement> requires StateElement to be Default,
// which is tricky for enums with non-defaultable variants or recursive structures.
// A truly useful Default for LogicalStateElement is unlikely without specific default StateElements.
// For now, omitting Default derive and custom impl unless a clear default pattern emerges for StateElement.
// If StateElement had a simple Default (e.g. StateElement::Stream(StreamStateElement::default())),
// then this could be:
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
