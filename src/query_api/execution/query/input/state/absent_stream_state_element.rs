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

// Corresponds to io.eventflux.query.api.execution.query.input.state.AbsentStreamStateElement
use super::stream_state_element::StreamStateElement;
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::execution::query::input::stream::SingleInputStream;
use crate::query_api::expression::constant::Constant as ExpressionConstant; // For direct composition if not delegating // Changed

#[derive(Clone, Debug, PartialEq)] // Default is not straightforward
pub struct AbsentStreamStateElement {
    // In Java, AbsentStreamStateElement extends StreamStateElement.
    // So, it should compose StreamStateElement, which in turn composes EventFluxElement.
    // The prompt suggests direct composition of eventflux_element, but that would lose
    // the basic_single_input_stream from StreamStateElement if not careful.
    // Sticking to composing StreamStateElement.
    pub stream_state_element: StreamStateElement,

    // AbsentStreamStateElement specific fields
    pub waiting_time: Option<ExpressionConstant>,
}

impl AbsentStreamStateElement {
    // Constructor takes SingleInputStream to create the inner StreamStateElement,
    // and the waiting_time.
    pub fn new(
        single_input_stream: SingleInputStream,
        waiting_time: Option<ExpressionConstant>,
    ) -> Self {
        // Changed parameter type
        AbsentStreamStateElement {
            stream_state_element: StreamStateElement::new(single_input_stream), // Pass SingleInputStream
            waiting_time,
        }
    }

    // Constructor that takes a pre-constructed StreamStateElement, as per prompt's general direction.
    // This is more flexible if StreamStateElement is already formed.
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
        // Changed method name and return type
        self.stream_state_element.get_single_input_stream()
    }

    // Expose eventflux_element for direct access if needed by StateElement enum dispatch.
    // This provides access to the EventFluxElement composed within the inner StreamStateElement.
    pub fn eventflux_element(&self) -> &EventFluxElement {
        &self.stream_state_element.eventflux_element
    }
    pub fn eventflux_element_mut(&mut self) -> &mut EventFluxElement {
        &mut self.stream_state_element.eventflux_element
    }
}

// No Default derive due to required StreamStateElement.

// No direct EventFluxElement impl for AbsentStreamStateElement.
// The StateElement enum will access the eventflux_element from the composed stream_state_element.
// If AbsentStreamStateElement needed to be passed as `dyn EventFluxElement`, it would need:
// impl EventFluxElement for AbsentStreamStateElement {
//     fn query_context_start_index(&self) -> Option<(i32,i32)> { self.stream_state_element.eventflux_element.query_context_start_index }
//     // ... and so on for other EventFluxElement methods, delegating to self.stream_state_element.eventflux_element
// }
// This is effectively what StateElement enum's EventFluxElement impl will do via the eventflux_element() helper.
