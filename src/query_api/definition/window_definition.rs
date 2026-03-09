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

// Corresponds to io.eventflux.query.api.definition.WindowDefinition
use crate::query_api::definition::stream_definition::StreamDefinition;
use crate::query_api::execution::query::input::handler::WindowHandler; // Use the re-exported WindowHandler
use crate::query_api::execution::query::output::OutputEventType; // Actual OutputEventType enum

#[derive(Clone, Debug, PartialEq)] // Default will be custom due to nested StreamDefinition
pub struct WindowDefinition {
    // Composition for inheritance from StreamDefinition
    pub stream_definition: StreamDefinition,

    // Fields specific to WindowDefinition
    pub window_handler: Option<WindowHandler>, // Renamed from 'window' or 'window_placeholder'
    pub output_event_type: OutputEventType,    // Replaced placeholder, Java default is ALL_EVENTS
}

impl WindowDefinition {
    pub fn new(id: String) -> Self {
        WindowDefinition {
            stream_definition: StreamDefinition::new(id),
            window_handler: None,
            output_event_type: OutputEventType::AllEvents, // Default in Java
        }
    }

    // Static factory `id` from Java
    pub fn id(window_id: String) -> Self {
        Self::new(window_id)
    }

    // Builder method to set the window handler (fluent style)
    pub fn window(mut self, window_handler: WindowHandler) -> Self {
        self.window_handler = Some(window_handler);
        self
    }

    // Builder method to set output event type (fluent style)
    // Java uses `setOutputEventType`, this makes it builder style.
    pub fn output_event_type(mut self, event_type: OutputEventType) -> Self {
        self.output_event_type = event_type;
        self
    }

    pub fn annotation(mut self, annotation: crate::query_api::annotation::Annotation) -> Self {
        self.stream_definition = self.stream_definition.annotation(annotation);
        self
    }
}

// Custom Default implementation if needed, or ensure StreamDefinition::default() is sensible.
// If StreamDefinition requires an ID, WindowDefinition cannot have a simple Default.
// Let's assume for now that an ID-less StreamDefinition is not typical for a Default.
// impl Default for WindowDefinition { ... }

// Provide access to StreamDefinition, AbstractDefinition, and EventFluxElement fields
use crate::query_api::definition::abstract_definition::AbstractDefinition;
use crate::query_api::eventflux_element::EventFluxElement;

impl AsRef<StreamDefinition> for WindowDefinition {
    fn as_ref(&self) -> &StreamDefinition {
        &self.stream_definition
    }
}

impl AsMut<StreamDefinition> for WindowDefinition {
    fn as_mut(&mut self) -> &mut StreamDefinition {
        &mut self.stream_definition
    }
}

impl AsRef<AbstractDefinition> for WindowDefinition {
    fn as_ref(&self) -> &AbstractDefinition {
        self.stream_definition.as_ref()
    }
}

impl AsMut<AbstractDefinition> for WindowDefinition {
    fn as_mut(&mut self) -> &mut AbstractDefinition {
        self.stream_definition.as_mut()
    }
}

impl AsRef<EventFluxElement> for WindowDefinition {
    fn as_ref(&self) -> &EventFluxElement {
        self.stream_definition.abstract_definition.as_ref()
    }
}

impl AsMut<EventFluxElement> for WindowDefinition {
    fn as_mut(&mut self) -> &mut EventFluxElement {
        self.stream_definition.abstract_definition.as_mut()
    }
}
