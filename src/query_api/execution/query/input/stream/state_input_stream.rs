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

// Corresponds to io.eventflux.query.api.execution.query.input.stream.StateInputStream
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::execution::query::input::state::{StateElement, StreamStateElement};
use crate::query_api::expression::constant::Constant as ExpressionConstant;
// StreamHandler is not directly used by StateInputStream itself, but by its contained BasicSingleInputStreams.
// use crate::query_api::execution::query::input::handler::StreamHandler;
use super::input_stream::InputStreamTrait; // For get_all_stream_ids, get_unique_stream_ids
use std::collections::HashSet;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy)] // Added Eq, Hash, Copy
#[derive(Default)]
pub enum Type {
    #[default]
    Pattern,
    Sequence,
}

#[derive(Clone, Debug, PartialEq)] // Default will be custom
pub struct StateInputStream {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement

    // StateInputStream specific fields
    pub state_type: Type,
    pub state_element: Box<StateElement>,
    pub within_time: Option<ExpressionConstant>,
}

impl StateInputStream {
    pub fn new(
        state_type: Type,
        state_element: StateElement,
        within_time: Option<ExpressionConstant>,
    ) -> Self {
        StateInputStream {
            eventflux_element: EventFluxElement::default(),
            state_type,
            state_element: Box::new(state_element),
            within_time,
        }
    }

    // Corrected collect_stream_ids_recursive
    fn collect_stream_ids_recursive(state_element: &StateElement, stream_ids: &mut Vec<String>) {
        match state_element {
            StateElement::Logical(logical) => {
                // LogicalStateElement's fields are Box<StateElement>, not StreamStateElement directly.
                Self::collect_stream_ids_recursive(
                    logical.stream_state_element_1.as_ref(),
                    stream_ids,
                );
                Self::collect_stream_ids_recursive(
                    logical.stream_state_element_2.as_ref(),
                    stream_ids,
                );
            }
            StateElement::Count(count) => {
                // CountStateElement holds StreamStateElement
                Self::collect_stream_ids_recursive_from_stream_state(
                    &count.stream_state_element,
                    stream_ids,
                );
            }
            StateElement::Every(every) => {
                Self::collect_stream_ids_recursive(every.state_element.as_ref(), stream_ids);
            }
            StateElement::Next(next) => {
                Self::collect_stream_ids_recursive(next.state_element.as_ref(), stream_ids);
                Self::collect_stream_ids_recursive(next.next_state_element.as_ref(), stream_ids);
            }
            StateElement::Stream(stream_state) => {
                Self::collect_stream_ids_recursive_from_stream_state(stream_state, stream_ids);
            }
            StateElement::AbsentStream(absent_stream_state) => {
                // AbsentStreamStateElement composes StreamStateElement
                Self::collect_stream_ids_recursive_from_stream_state(
                    &absent_stream_state.stream_state_element,
                    stream_ids,
                );
            }
        }
    }

    // Helper to get ID from the BasicSingleInputStream within a StreamStateElement
    fn collect_stream_ids_recursive_from_stream_state(
        stream_state: &StreamStateElement,
        stream_ids: &mut Vec<String>,
    ) {
        // BasicSingleInputStream is within StreamStateElement
        // And BasicSingleInputStream wraps a SingleInputStream
        // SingleInputStream has get_all_stream_ids() from InputStreamTrait
        stream_ids.extend(stream_state.basic_single_input_stream.get_all_stream_ids());
    }
}

impl Default for StateInputStream {
    fn default() -> Self {
        // Requires StateElement to be Default, which is complex due to Box and variants.
        // Creating a minimal valid default, e.g. a pattern with a default stream.
        // This might not be a very useful default.
        StateInputStream {
            eventflux_element: EventFluxElement::default(),
            state_type: Type::default(),
            // This requires StateElement to have a sensible default.
            // Assuming StateElement::Stream(StreamStateElement::default()) could be one.
            // StreamStateElement would need BasicSingleInputStream::default().
            // BasicSingleInputStream would need SingleInputStreamKind::default().
            // SingleInputStreamKind::Basic has defaults.
            state_element: Box::new(StateElement::Stream(StreamStateElement::default())),
            within_time: None,
        }
    }
}

impl InputStreamTrait for StateInputStream {
    fn get_all_stream_ids(&self) -> Vec<String> {
        let mut ids = Vec::new();
        Self::collect_stream_ids_recursive(&self.state_element, &mut ids);
        ids
    }

    fn get_unique_stream_ids(&self) -> Vec<String> {
        let all_ids = self.get_all_stream_ids();
        let set: HashSet<_> = all_ids.into_iter().collect();
        set.into_iter().collect()
    }
}

impl StateInputStream {
    pub fn pattern_stream(
        state_element: StateElement,
        within_time: Option<ExpressionConstant>,
    ) -> Self {
        Self::new(Type::Pattern, state_element, within_time)
    }
    pub fn sequence_stream(
        state_element: StateElement,
        within_time: Option<ExpressionConstant>,
    ) -> Self {
        Self::new(Type::Sequence, state_element, within_time)
    }
}
