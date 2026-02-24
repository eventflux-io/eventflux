// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/event/state/populater/state_mapping_element.rs
#[derive(Debug, Clone)]
pub struct StateMappingElement {
    pub from_position: Vec<i32>,
    pub to_position: usize,
}

impl StateMappingElement {
    pub fn new(from_position: Vec<i32>, to_position: usize) -> Self {
        Self {
            from_position,
            to_position,
        }
    }
}
