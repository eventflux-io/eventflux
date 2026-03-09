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
