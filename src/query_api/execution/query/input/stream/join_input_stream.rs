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

use super::input_stream::InputStreamTrait;
use super::single_input_stream::SingleInputStream;
use crate::query_api::aggregation::Within;
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::expression::Expression;

/// Kind of join. Defaults to `Join`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy, Default)]
pub enum Type {
    #[default]
    Join,
    InnerJoin,
    LeftOuterJoin,
    RightOuterJoin,
    FullOuterJoin,
}

/// Which side's arriving events start a join. Defaults to `All`, meaning both sides.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy, Default)]
pub enum EventTrigger {
    Left,
    Right,
    #[default]
    All,
}

/// A join of two single streams.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct JoinInputStream {
    pub eventflux_element: EventFluxElement,

    pub left_input_stream: Box<SingleInputStream>,
    pub join_type: Type,
    pub right_input_stream: Box<SingleInputStream>,
    /// The ON condition, or `None` for no condition.
    pub on_compare: Option<Expression>,

    pub trigger: EventTrigger,
    /// Time range, used when one side is an aggregation.
    pub within: Option<Within>,
    /// Time unit to read at, used when one side is an aggregation.
    pub per: Option<Expression>,
    /// The SQL WHERE condition, or `None` for no WHERE. Runs on each joined event,
    /// after the join, so on an outer join a WHERE on the side with no match drops the
    /// row.
    pub filter: Option<Expression>,
}

impl JoinInputStream {
    pub fn new(
        left_input_stream: SingleInputStream,
        join_type: Type,
        right_input_stream: SingleInputStream,
        on_compare: Option<Expression>,
        trigger: EventTrigger,
        within: Option<Within>,
        per: Option<Expression>,
    ) -> Self {
        JoinInputStream {
            eventflux_element: EventFluxElement::default(),
            left_input_stream: Box::new(left_input_stream),
            join_type,
            right_input_stream: Box::new(right_input_stream),
            on_compare,
            trigger,
            within,
            per,
            filter: None,
        }
    }

    /// Set the WHERE condition that runs on each joined event.
    pub fn with_filter(mut self, filter: Expression) -> Self {
        self.filter = Some(filter);
        self
    }
}

// Left side ids come first, then right side ids.
impl InputStreamTrait for JoinInputStream {
    fn get_all_stream_ids(&self) -> Vec<String> {
        let mut ids = self.left_input_stream.get_all_stream_ids();
        ids.extend(self.right_input_stream.get_all_stream_ids());
        ids
    }

    fn get_unique_stream_ids(&self) -> Vec<String> {
        let mut ids = self.left_input_stream.get_unique_stream_ids();
        for id in self.right_input_stream.get_unique_stream_ids() {
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        ids
    }
}
