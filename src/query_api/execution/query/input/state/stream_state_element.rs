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

use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::execution::query::input::stream::SingleInputStream;

/// A single stream step in a pattern, as in `e1=StockStream[price > 10]`.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct StreamStateElement {
    pub eventflux_element: EventFluxElement,

    /// The stream to match. Usually of kind SingleInputStreamKind::Basic. The name is
    /// historical.
    pub basic_single_input_stream: SingleInputStream,
}

impl StreamStateElement {
    /// The stream kind is not checked. The caller should pass a basic stream.
    pub fn new(single_input_stream: SingleInputStream) -> Self {
        StreamStateElement {
            eventflux_element: EventFluxElement::default(),
            basic_single_input_stream: single_input_stream,
        }
    }

    pub fn get_single_input_stream(&self) -> &SingleInputStream {
        &self.basic_single_input_stream
    }

    // Id of the matched stream. Unused for now.
    #[allow(dead_code)]
    pub(crate) fn get_stream_id(&self) -> &str {
        self.basic_single_input_stream.get_stream_id_str()
    }
}

// Read the query positions through the public eventflux_element field.
