// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/event/state/populater/selective_state_event_populator.rs
use super::{
    state_event_populator::StateEventPopulator, state_mapping_element::StateMappingElement,
};
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::state::state_event::StateEvent;

#[derive(Debug, Clone)]
pub struct SelectiveStateEventPopulator {
    pub mappings: Vec<StateMappingElement>,
}

impl SelectiveStateEventPopulator {
    pub fn new(mappings: Vec<StateMappingElement>) -> Self {
        Self { mappings }
    }
}

impl StateEventPopulator for SelectiveStateEventPopulator {
    fn populate_state_event(&self, complex_event: &mut dyn ComplexEvent) {
        let state_event = match complex_event.as_any_mut().downcast_mut::<StateEvent>() {
            Some(se) => se,
            None => return,
        };
        for mapping in &self.mappings {
            if let Some(val) = state_event.get_attribute(&mapping.from_position) {
                let _ = state_event.set_output_data_at_idx(val.clone(), mapping.to_position);
            }
        }
    }
}
