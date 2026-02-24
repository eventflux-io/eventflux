// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/event/state/state_event_factory.rs
// Corresponds to io.eventflux.core.event.state.StateEventFactory
use super::meta_state_event::MetaStateEvent;
use super::state_event::StateEvent;

#[derive(Debug, Clone)]
pub struct StateEventFactory {
    pub event_size: usize,
    pub output_data_size: usize,
}

impl StateEventFactory {
    pub fn new(event_size: usize, output_data_size: usize) -> Self {
        Self {
            event_size,
            output_data_size,
        }
    }

    pub fn new_from_meta(meta: &MetaStateEvent) -> Self {
        Self {
            event_size: meta.stream_event_count(),
            output_data_size: meta.get_output_data_attributes().len(),
        }
    }

    pub fn new_instance(&self) -> StateEvent {
        StateEvent::new(self.event_size, self.output_data_size)
    }
}
