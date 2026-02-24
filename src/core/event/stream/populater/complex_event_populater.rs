// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/event/stream/populater/complex_event_populater.rs
use crate::core::event::{complex_event::ComplexEvent, value::AttributeValue};

pub trait ComplexEventPopulater {
    fn populate_complex_event(&self, complex_event: &mut dyn ComplexEvent, data: &[AttributeValue]);
}
