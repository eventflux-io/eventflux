/*
 * Copyright 2025-2026 EventFlux.io
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
