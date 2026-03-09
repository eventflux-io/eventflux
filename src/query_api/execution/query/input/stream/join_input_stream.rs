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

// Corresponds to io.eventflux.query.api.execution.query.input.stream.JoinInputStream
use super::input_stream::InputStreamTrait;
use super::single_input_stream::SingleInputStream;
use crate::query_api::aggregation::Within;
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::expression::Expression; // Using the actual Within struct

#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy)] // Added Eq, Hash, Copy
#[derive(Default)]
pub enum Type {
    #[default]
    Join,
    InnerJoin,
    LeftOuterJoin,
    RightOuterJoin,
    FullOuterJoin,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy)] // Added Eq, Hash, Copy
#[derive(Default)]
pub enum EventTrigger {
    Left,
    Right,
    #[default]
    All,
}

#[derive(Clone, Debug, PartialEq)] // Default will be custom
#[derive(Default)]
pub struct JoinInputStream {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement

    pub left_input_stream: Box<SingleInputStream>,
    pub join_type: Type,
    pub right_input_stream: Box<SingleInputStream>,
    pub on_compare: Option<Expression>,

    pub trigger: EventTrigger,
    pub within: Option<Within>, // Using the actual Within struct
    pub per: Option<Expression>,
}

impl JoinInputStream {
    pub fn new(
        left_input_stream: SingleInputStream,
        join_type: Type,
        right_input_stream: SingleInputStream,
        on_compare: Option<Expression>,
        trigger: EventTrigger,
        within: Option<Within>, // Using the actual Within struct
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
        }
    }
}

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
