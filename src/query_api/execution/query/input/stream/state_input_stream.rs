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

use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::execution::query::input::state::{StateElement, StreamStateElement};
use crate::query_api::expression::constant::Constant as ExpressionConstant;
// Not used here. Handlers live on the streams inside the state elements.
// use crate::query_api::execution::query::input::handler::StreamHandler;
use super::input_stream::InputStreamTrait;
use std::collections::HashSet;

/// Whether the state input is a pattern or a sequence. Defaults to `Pattern`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy, Default)]
pub enum Type {
    #[default]
    Pattern,
    Sequence,
}

/// A pattern or sequence input, as a tree of state elements.
#[derive(Clone, Debug, PartialEq)]
pub struct StateInputStream {
    pub eventflux_element: EventFluxElement,

    pub state_type: Type,
    pub state_element: Box<StateElement>,
    /// WITHIN time limit for the whole pattern, or `None` for no limit.
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

    // Walk the tree and append every stream id, left to right, repeats kept.
    fn collect_stream_ids_recursive(state_element: &StateElement, stream_ids: &mut Vec<String>) {
        match state_element {
            StateElement::Logical(logical) => {
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
                Self::collect_stream_ids_recursive_from_stream_state(
                    &absent_stream_state.stream_state_element,
                    stream_ids,
                );
            }
        }
    }

    // Append the id of the stream inside a StreamStateElement.
    fn collect_stream_ids_recursive_from_stream_state(
        stream_state: &StreamStateElement,
        stream_ids: &mut Vec<String>,
    ) {
        stream_ids.extend(stream_state.basic_single_input_stream.get_all_stream_ids());
    }
}

// StateElement has no Default, so this builds a pattern over one default stream element.
// That stream has an empty id, so the default is of little real use.
impl Default for StateInputStream {
    fn default() -> Self {
        StateInputStream {
            eventflux_element: EventFluxElement::default(),
            state_type: Type::default(),
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

    // Goes through a HashSet, so the order is not stable.
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
