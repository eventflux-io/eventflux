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
use crate::query_api::execution::query::input::stream::SingleInputStream;
use crate::query_api::expression::constant::Constant as ExpressionConstant;

/// A pattern step that matches when an event does not arrive, as in `NOT A FOR 5 sec`.
///
/// Has no `Default` because the inner stream element is required.
#[derive(Clone, Debug, PartialEq)]
pub struct AbsentStreamStateElement {
    /// The stream that must stay silent. Also holds this element's query positions.
    pub stream_state_element: StreamStateElement,

    /// How long the stream must stay silent, or `None` when no time is given.
    pub waiting_time: Option<ExpressionConstant>,
}

impl AbsentStreamStateElement {
    /// Build from the input stream. Wraps it in a new StreamStateElement.
    pub fn new(
        single_input_stream: SingleInputStream,
        waiting_time: Option<ExpressionConstant>,
    ) -> Self {
        AbsentStreamStateElement {
            stream_state_element: StreamStateElement::new(single_input_stream),
            waiting_time,
        }
    }

    /// Build from a StreamStateElement you already have.
    pub fn new_with_stream_state(
        stream_state_element: StreamStateElement,
        waiting_time: Option<ExpressionConstant>,
    ) -> Self {
        AbsentStreamStateElement {
            stream_state_element,
            waiting_time,
        }
    }

    pub fn get_single_input_stream(&self) -> &SingleInputStream {
        self.stream_state_element.get_single_input_stream()
    }

    /// The query positions, stored on the inner StreamStateElement. Used by the
    /// StateElement enum.
    pub fn eventflux_element(&self) -> &EventFluxElement {
        &self.stream_state_element.eventflux_element
    }
    pub fn eventflux_element_mut(&mut self) -> &mut EventFluxElement {
        &mut self.stream_state_element.eventflux_element
    }
}

// EventFluxElement is a struct, not a trait, so there is nothing to implement here.
// If it ever became a trait, the impl would pass each call on to the inner element, as
// below.
// impl EventFluxElement for AbsentStreamStateElement {
//     fn query_context_start_index(&self) -> Option<(i32,i32)> { self.stream_state_element.eventflux_element.query_context_start_index }
//     // ... and so on for other EventFluxElement methods, delegating to self.stream_state_element.eventflux_element
// }
