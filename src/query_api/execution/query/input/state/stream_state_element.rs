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

// Corresponds to io.eventflux.query.api.execution.query.input.state.StreamStateElement
use crate::query_api::eventflux_element::EventFluxElement;
// BasicSingleInputStream functionality is now part of SingleInputStream via SingleInputStreamKind::Basic
use crate::query_api::execution::query::input::stream::SingleInputStream;

#[derive(Clone, Debug, PartialEq, Default)] // Added Default
pub struct StreamStateElement {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement

    // StreamStateElement fields
    // Changed from BasicSingleInputStream to SingleInputStream.
    // This SingleInputStream instance would typically be of SingleInputStreamKind::Basic.
    pub basic_single_input_stream: SingleInputStream,
}

impl StreamStateElement {
    pub fn new(single_input_stream: SingleInputStream) -> Self {
        // Parameter type changed
        // It's up to the caller to ensure the provided SingleInputStream is appropriate
        // (e.g., of a Basic kind for most pattern stream elements).
        StreamStateElement {
            eventflux_element: EventFluxElement::default(),
            basic_single_input_stream: single_input_stream,
        }
    }

    pub fn get_single_input_stream(&self) -> &SingleInputStream {
        // Method name and return type changed
        &self.basic_single_input_stream
    }

    // Helper method for StateInputStream or other internal uses.
    #[allow(dead_code)]
    pub(crate) fn get_stream_id(&self) -> &str {
        // SingleInputStream has get_stream_id_str() directly.
        self.basic_single_input_stream.get_stream_id_str()
    }
}

// EventFluxElement is composed. Access via self.eventflux_element.
// No direct impl of EventFluxElement trait needed if using composition and public field,
// unless it needs to be passed as `dyn EventFluxElement`.
// The StateElement enum's EventFluxElement impl handles dispatching to this composed element.
