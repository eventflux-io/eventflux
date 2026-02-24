// SPDX-License-Identifier: MIT OR Apache-2.0

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
