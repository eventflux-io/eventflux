// SPDX-License-Identifier: MIT OR Apache-2.0

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
