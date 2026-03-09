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

// Corresponds to io.eventflux.query.api.execution.query.input.state.State (factory class)
use super::absent_stream_state_element::AbsentStreamStateElement;
use super::count_state_element::{CountStateElement, ANY_COUNT};
use super::every_state_element::EveryStateElement;
use super::logical_state_element::{LogicalStateElement, Type as LogicalType};
use super::next_state_element::NextStateElement;
use super::state_element::StateElement;
use super::stream_state_element::StreamStateElement;

use crate::query_api::execution::query::input::stream::SingleInputStream; // Changed
use crate::query_api::expression::constant::Constant as ExpressionConstant; // Renamed TimeConstant

// Utility struct to hold factory methods
pub struct State;

impl State {
    pub fn every(state_element: StateElement) -> StateElement {
        StateElement::Every(Box::new(EveryStateElement::new(state_element)))
    }

    // Updated to use StateElement directly for sse1 and sse2 in LogicalStateElement::new
    pub fn logical_and(sse1: StreamStateElement, sse2: StreamStateElement) -> StateElement {
        StateElement::Logical(LogicalStateElement::new(
            StateElement::Stream(sse1), // Wrap in StateElement enum
            LogicalType::And,
            StateElement::Stream(sse2), // Wrap in StateElement enum
        ))
    }

    pub fn logical_or(sse1: StreamStateElement, sse2: StreamStateElement) -> StateElement {
        StateElement::Logical(LogicalStateElement::new(
            StateElement::Stream(sse1), // Wrap in StateElement enum
            LogicalType::Or,
            StateElement::Stream(sse2), // Wrap in StateElement enum
        ))
    }

    pub fn logical_not(
        stream_state_element: StreamStateElement,
        time: Option<ExpressionConstant>,
    ) -> AbsentStreamStateElement {
        // Assuming get_single_input_stream() returns a reference to the SingleInputStream composed in StreamStateElement
        if stream_state_element
            .get_single_input_stream()
            .get_stream_reference_id_str()
            .is_some()
        {
            // TODO: This should ideally return a Result or handle error more gracefully
            panic!(
                "EventFluxAppValidationException: NOT pattern cannot have reference id but found {}",
                stream_state_element
                    .get_single_input_stream()
                    .get_stream_reference_id_str()
                    .unwrap_or_default()
            );
        }
        // AbsentStreamStateElement::new expects an owned SingleInputStream.
        // StreamStateElement.basic_single_input_stream is an owned SingleInputStream.
        // We need to clone it if StreamStateElement is not consumed.
        // If StreamStateElement is consumed, we can move it.
        // For now, assuming clone() is available and appropriate.
        AbsentStreamStateElement::new(stream_state_element.basic_single_input_stream.clone(), time)
    }

    // Now this can be implemented correctly
    pub fn logical_not_and(
        absent_sse: AbsentStreamStateElement,
        sse2: StreamStateElement,
    ) -> StateElement {
        StateElement::Logical(LogicalStateElement::new(
            StateElement::AbsentStream(absent_sse), // Wrap in StateElement enum
            LogicalType::And,
            StateElement::Stream(sse2), // Wrap in StateElement enum
        ))
    }

    pub fn next(
        state_element: StateElement,
        followed_by_state_element: StateElement,
    ) -> StateElement {
        StateElement::Next(Box::new(NextStateElement::new(
            state_element,
            followed_by_state_element,
        )))
    }

    pub fn count(stream_state_element: StreamStateElement, min: i32, max: i32) -> StateElement {
        StateElement::Count(CountStateElement::new(stream_state_element, min, max))
    }

    pub fn count_more_than_equal(
        stream_state_element: StreamStateElement,
        min: i32,
    ) -> StateElement {
        Self::count(stream_state_element, min, ANY_COUNT)
    }

    pub fn count_less_than_equal(
        stream_state_element: StreamStateElement,
        max: i32,
    ) -> StateElement {
        Self::count(stream_state_element, ANY_COUNT, max)
    }

    pub fn stream(single_input_stream: SingleInputStream) -> StreamStateElement {
        // Changed parameter type
        StreamStateElement::new(single_input_stream)
    }

    pub fn stream_element(single_input_stream: SingleInputStream) -> StateElement {
        // Changed parameter type
        StateElement::Stream(Self::stream(single_input_stream))
    }

    pub fn zero_or_many(stream_state_element: StreamStateElement) -> StateElement {
        Self::count(stream_state_element, 0, ANY_COUNT)
    }

    pub fn zero_or_one(stream_state_element: StreamStateElement) -> StateElement {
        Self::count(stream_state_element, 0, 1)
    }

    pub fn one_or_many(stream_state_element: StreamStateElement) -> StateElement {
        Self::count(stream_state_element, 1, ANY_COUNT)
    }
}
