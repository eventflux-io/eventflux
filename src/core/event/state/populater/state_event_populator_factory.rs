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

// src/core/event/state/populater/state_event_populator_factory.rs
use super::{
    selective_state_event_populator::SelectiveStateEventPopulator,
    skip_state_event_populator::SkipStateEventPopulator,
    state_event_populator::StateEventPopulator,
    state_mapping_element::StateMappingElement,
};
use crate::core::event::state::{MetaStateEvent, MetaStateEventAttribute};
use crate::core::event::stream::meta_stream_event::MetaStreamEvent;

pub fn construct_event_populator(meta: &MetaStateEvent) -> Box<dyn StateEventPopulator> {
    if meta.stream_event_count() == 1 {
        // Single stream events don't need population
        Box::new(SkipStateEventPopulator)
    } else {
        let mut mappings = Vec::new();
        let mut to_index = 0usize;
        if let Some(attrs) = &meta.output_data_attributes {
            for attr in attrs {
                let mapping = StateMappingElement::new(attr.position.clone(), to_index);
                mappings.push(mapping);
                to_index += 1;
            }
        }
        Box::new(SelectiveStateEventPopulator::new(mappings))
    }
}
