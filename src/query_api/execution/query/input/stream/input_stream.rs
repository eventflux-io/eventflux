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

// Corresponds to io.eventflux.query.api.execution.query.input.stream.InputStream (abstract class)
use crate::query_api::eventflux_element::EventFluxElement;

// Import specific stream types that will be variants of the InputStream enum
use super::join_input_stream::{
    EventTrigger as JoinEventTrigger, JoinInputStream, Type as JoinType,
};
use super::single_input_stream::SingleInputStream;
use super::state_input_stream::StateInputStream;
// BasicSingleInputStream and AnonymousInputStream were removed and functionality moved into SingleInputStream.
// Factory methods will now directly use SingleInputStream::new_basic... or SingleInputStream::new_anonymous...

// For factory methods that need these:
use crate::query_api::aggregation::Within;
use crate::query_api::execution::query::input::state::StateElement;
use crate::query_api::expression::constant::Constant as ExpressionConstant;
use crate::query_api::expression::Expression; // Actual Within struct

// Trait for common methods of InputStream (from Java's InputStream abstract class)
pub trait InputStreamTrait {
    // Removed : EventFluxElement as EventFluxElement is directly implemented by the enum
    fn get_all_stream_ids(&self) -> Vec<String>;
    fn get_unique_stream_ids(&self) -> Vec<String>;
}

#[derive(Clone, Debug, PartialEq)]
pub enum InputStream {
    Single(SingleInputStream),
    Join(Box<JoinInputStream>),
    State(Box<StateInputStream>),
}

// EventFluxElement implementation for the InputStream enum
impl InputStream {
    // These helpers assume that SingleInputStream, JoinInputStream, StateInputStream
    // will be refactored to compose `eventflux_element: EventFluxElement`.
    #[allow(dead_code)]
    fn eventflux_element_ref(&self) -> &EventFluxElement {
        match self {
            InputStream::Single(s) => &s.eventflux_element,
            InputStream::Join(j) => &j.eventflux_element,
            InputStream::State(st) => &st.eventflux_element,
        }
    }

    #[allow(dead_code)]
    fn eventflux_element_mut_ref(&mut self) -> &mut EventFluxElement {
        match self {
            InputStream::Single(s) => &mut s.eventflux_element,
            InputStream::Join(j) => &mut j.eventflux_element,
            InputStream::State(st) => &mut st.eventflux_element,
        }
    }
}

// `impl EventFluxElement for InputStream` removed.

impl InputStreamTrait for InputStream {
    fn get_all_stream_ids(&self) -> Vec<String> {
        match self {
            InputStream::Single(s) => s.get_all_stream_ids(), // Assumes SingleInputStream impls InputStreamTrait
            InputStream::Join(j) => j.get_all_stream_ids(), // Assumes JoinInputStream impls InputStreamTrait
            InputStream::State(st) => st.get_all_stream_ids(), // Assumes StateInputStream impls InputStreamTrait
        }
    }

    fn get_unique_stream_ids(&self) -> Vec<String> {
        match self {
            InputStream::Single(s) => s.get_unique_stream_ids(),
            InputStream::Join(j) => j.get_unique_stream_ids(),
            InputStream::State(st) => st.get_unique_stream_ids(),
        }
    }
}

// Factory methods from Java's InputStream abstract class
impl InputStream {
    // Basic streams
    pub fn stream(stream_id: String) -> Self {
        InputStream::Single(SingleInputStream::new_basic_from_id(stream_id))
    }
    pub fn stream_with_ref(stream_reference_id: String, stream_id: String) -> Self {
        InputStream::Single(SingleInputStream::new_basic_from_ref_id(
            stream_reference_id,
            stream_id,
        ))
    }
    pub fn inner_stream(stream_id: String) -> Self {
        InputStream::Single(SingleInputStream::new_basic_inner_from_id(stream_id))
    }
    // TODO: Add other BasicSingleInputStream factories (inner_with_ref, fault_stream, etc.)

    // Anonymous stream from Query
    pub fn stream_from_query(query: crate::query_api::execution::query::Query) -> Self {
        InputStream::Single(SingleInputStream::new_anonymous_from_query(query))
    }

    // Join streams
    pub fn join_stream(
        left_stream: SingleInputStream,
        join_type: JoinType,
        right_stream: SingleInputStream,
        on_compare: Option<Expression>,
        trigger: Option<JoinEventTrigger>, // Made Option, Java defaults to ALL
        within: Option<Within>,
        per: Option<Expression>,
    ) -> Self {
        InputStream::Join(Box::new(JoinInputStream::new(
            left_stream,
            join_type,
            right_stream,
            on_compare,
            trigger.unwrap_or(JoinEventTrigger::All), // Default from Java
            within,
            per,
        )))
    }
    // TODO: Add other overloaded JoinInputStream factories from Java

    // Pattern streams
    pub fn pattern_stream(
        pattern_element: StateElement,
        within_time: Option<ExpressionConstant>,
    ) -> Self {
        InputStream::State(Box::new(StateInputStream::pattern_stream(
            pattern_element,
            within_time,
        )))
    }

    // Sequence streams
    pub fn sequence_stream(
        sequence_element: StateElement,
        within_time: Option<ExpressionConstant>,
    ) -> Self {
        InputStream::State(Box::new(StateInputStream::sequence_stream(
            sequence_element,
            within_time,
        )))
    }
}
