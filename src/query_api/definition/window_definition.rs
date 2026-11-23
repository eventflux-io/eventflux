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

use crate::query_api::definition::stream_definition::StreamDefinition;
use crate::query_api::execution::query::input::handler::WindowHandler;
use crate::query_api::execution::query::output::OutputEventType;

/// A named window that queries can share. Holds the stream schema plus the window to apply.
///
/// Has no `Default` impl. Build it with `new` or `id`.
#[derive(Clone, Debug, PartialEq)]
pub struct WindowDefinition {
    pub stream_definition: StreamDefinition,

    pub window_handler: Option<WindowHandler>,
    /// Which events the window emits. `new` sets it to `AllEvents`.
    pub output_event_type: OutputEventType,
}

impl WindowDefinition {
    pub fn new(id: String) -> Self {
        WindowDefinition {
            stream_definition: StreamDefinition::new(id),
            window_handler: None,
            output_event_type: OutputEventType::AllEvents,
        }
    }

    /// Same as `new`. Starts a builder chain.
    pub fn id(window_id: String) -> Self {
        Self::new(window_id)
    }

    pub fn window(mut self, window_handler: WindowHandler) -> Self {
        self.window_handler = Some(window_handler);
        self
    }

    pub fn output_event_type(mut self, event_type: OutputEventType) -> Self {
        self.output_event_type = event_type;
        self
    }

    pub fn annotation(mut self, annotation: crate::query_api::annotation::Annotation) -> Self {
        self.stream_definition = self.stream_definition.annotation(annotation);
        self
    }
}

// No Default impl on purpose. A window definition without an id makes little sense.
// impl Default for WindowDefinition { ... }

// Access to the stream definition, the shared definition fields and the query positions.
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
