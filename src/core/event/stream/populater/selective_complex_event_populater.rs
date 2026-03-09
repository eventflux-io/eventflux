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

// src/core/event/stream/populater/selective_complex_event_populater.rs
use super::{ComplexEventPopulater, StreamMappingElement};
use crate::core::event::{
    complex_event::ComplexEvent, state::state_event::StateEvent, stream::stream_event::StreamEvent,
    value::AttributeValue,
};

#[derive(Debug, Clone)]
pub struct SelectiveComplexEventPopulater {
    pub mappings: Vec<StreamMappingElement>,
}

impl SelectiveComplexEventPopulater {
    pub fn new(mappings: Vec<StreamMappingElement>) -> Self {
        Self { mappings }
    }

    fn populate_value(&self, ce: &mut dyn ComplexEvent, value: AttributeValue, pos: &[i32]) {
        if let Some(se) = ce.as_any_mut().downcast_mut::<StreamEvent>() {
            let _ = se.set_attribute_by_position(value, pos);
        } else if let Some(state) = ce.as_any_mut().downcast_mut::<StateEvent>() {
            let _ = state.set_attribute(value, pos);
        }
    }
}

impl ComplexEventPopulater for SelectiveComplexEventPopulater {
    fn populate_complex_event(
        &self,
        complex_event: &mut dyn ComplexEvent,
        data: &[AttributeValue],
    ) {
        for mapping in &self.mappings {
            if let Some(ref pos) = mapping.to_position {
                if let Some(val) = data.get(mapping.from_position).cloned() {
                    self.populate_value(complex_event, val, pos);
                }
            }
        }
    }
}
