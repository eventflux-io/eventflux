// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/event/state/meta_state_event_attribute.rs
// Corresponds to io.eventflux.core.event.state.MetaStateEventAttribute
use crate::query_api::definition::attribute::Attribute as QueryApiAttribute;

#[derive(Debug, Clone, PartialEq)]
pub struct MetaStateEventAttribute {
    pub attribute: QueryApiAttribute,
    pub position: Vec<i32>,
}

impl MetaStateEventAttribute {
    pub fn new(attribute: QueryApiAttribute, position: Vec<i32>) -> Self {
        Self {
            attribute,
            position,
        }
    }

    pub fn get_attribute(&self) -> &QueryApiAttribute {
        &self.attribute
    }

    pub fn get_position(&self) -> &[i32] {
        &self.position
    }
}
