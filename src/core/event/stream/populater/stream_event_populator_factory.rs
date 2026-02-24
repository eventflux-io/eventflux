// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/event/stream/populater/stream_event_populator_factory.rs
use super::{SelectiveComplexEventPopulater, StreamMappingElement};
use crate::core::event::stream::meta_stream_event::MetaStreamEvent;
use crate::core::util::eventflux_constants::{
    BEFORE_WINDOW_DATA_INDEX, ON_AFTER_WINDOW_DATA_INDEX, OUTPUT_DATA_INDEX,
};
use crate::query_api::definition::attribute::Attribute;

pub fn construct_event_populator(
    meta: &MetaStreamEvent,
    stream_event_chain_index: i32,
    attributes: &[Attribute],
) -> SelectiveComplexEventPopulater {
    let mut mappings = Vec::new();
    for (i, attr) in attributes.iter().enumerate() {
        let mut mapping = StreamMappingElement::new(i, None);
        if let Some(index) = meta
            .get_output_data()
            .iter()
            .position(|a| a.name == attr.name)
        {
            mapping.to_position = Some(vec![
                stream_event_chain_index,
                0,
                OUTPUT_DATA_INDEX as i32,
                index as i32,
            ]);
        } else if let Some(index) = meta
            .get_on_after_window_data()
            .iter()
            .position(|a| a.name == attr.name)
        {
            mapping.to_position = Some(vec![
                stream_event_chain_index,
                0,
                ON_AFTER_WINDOW_DATA_INDEX as i32,
                index as i32,
            ]);
        } else if let Some(index) = meta
            .get_before_window_data()
            .iter()
            .position(|a| a.name == attr.name)
        {
            mapping.to_position = Some(vec![
                stream_event_chain_index,
                0,
                BEFORE_WINDOW_DATA_INDEX as i32,
                index as i32,
            ]);
        }
        mappings.push(mapping);
    }
    SelectiveComplexEventPopulater::new(mappings)
}
