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

use super::join_input_stream::{
    EventTrigger as JoinEventTrigger, JoinInputStream, Type as JoinType,
};
use super::single_input_stream::SingleInputStream;
use super::state_input_stream::StateInputStream;
// Basic and anonymous streams are both kinds of SingleInputStream. There are no separate
// BasicSingleInputStream or AnonymousInputStream types.

use crate::query_api::aggregation::Within;
use crate::query_api::execution::query::input::state::StateElement;
use crate::query_api::expression::constant::Constant as ExpressionConstant;
use crate::query_api::expression::Expression;

/// Methods shared by all input streams.
pub trait InputStreamTrait {
    /// Every stream id read, in order, with repeats.
    fn get_all_stream_ids(&self) -> Vec<String>;
    /// Every stream id read, each once. Order depends on the stream type.
    fn get_unique_stream_ids(&self) -> Vec<String>;
}

/// The FROM part of a query. A single stream, a join of two streams, or a pattern or
/// sequence.
#[derive(Clone, Debug, PartialEq)]
pub enum InputStream {
    Single(SingleInputStream),
    Join(Box<JoinInputStream>),
    State(Box<StateInputStream>),
}

impl InputStream {
    // The EventFluxElement of whichever variant this is. Unused for now.
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

impl InputStreamTrait for InputStream {
    fn get_all_stream_ids(&self) -> Vec<String> {
        match self {
            InputStream::Single(s) => s.get_all_stream_ids(),
            InputStream::Join(j) => j.get_all_stream_ids(),
            InputStream::State(st) => st.get_all_stream_ids(),
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

// Factory functions.
impl InputStream {
    pub fn stream(stream_id: String) -> Self {
        InputStream::Single(SingleInputStream::new_basic_from_id(stream_id))
    }
    pub fn stream_with_ref(stream_reference_id: String, stream_id: String) -> Self {
        InputStream::Single(SingleInputStream::new_basic_from_ref_id(
            stream_reference_id,
            stream_id,
        ))
    }
    /// An inner stream, one local to a partition (`#` prefix in the query).
    pub fn inner_stream(stream_id: String) -> Self {
        InputStream::Single(SingleInputStream::new_basic_inner_from_id(stream_id))
    }
    // TODO: Add the other basic stream factories, such as an inner stream with an alias
    // and a fault stream.

    /// An anonymous stream fed by the output of `query`. Gets a generated id.
    pub fn stream_from_query(query: crate::query_api::execution::query::Query) -> Self {
        InputStream::Single(SingleInputStream::new_anonymous_from_query(query))
    }

    /// Join two streams. A `None` trigger means `JoinEventTrigger::All`.
    pub fn join_stream(
        left_stream: SingleInputStream,
        join_type: JoinType,
        right_stream: SingleInputStream,
        on_compare: Option<Expression>,
        trigger: Option<JoinEventTrigger>,
        within: Option<Within>,
        per: Option<Expression>,
    ) -> Self {
        InputStream::Join(Box::new(JoinInputStream::new(
            left_stream,
            join_type,
            right_stream,
            on_compare,
            trigger.unwrap_or(JoinEventTrigger::All),
            within,
            per,
        )))
    }
    // TODO: Add shorter join factories that fill in the optional arguments.

    pub fn pattern_stream(
        pattern_element: StateElement,
        within_time: Option<ExpressionConstant>,
    ) -> Self {
        InputStream::State(Box::new(StateInputStream::pattern_stream(
            pattern_element,
            within_time,
        )))
    }

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
