// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/event/state/populater/skip_state_event_populator.rs
use super::state_event_populator::StateEventPopulator;
use crate::core::event::complex_event::ComplexEvent;

#[derive(Debug, Clone)]
pub struct SkipStateEventPopulator;

impl StateEventPopulator for SkipStateEventPopulator {
    fn populate_state_event(&self, _complex_event: &mut dyn ComplexEvent) {
        // intentionally no-op
    }
}
