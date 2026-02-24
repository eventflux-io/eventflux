// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/event/state/populater/state_event_populator.rs
use crate::core::event::complex_event::ComplexEvent;

pub trait StateEventPopulator {
    fn populate_state_event(&self, complex_event: &mut dyn ComplexEvent);
}
