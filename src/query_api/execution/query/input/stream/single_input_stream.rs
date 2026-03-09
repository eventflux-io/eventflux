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

// Corresponds to io.eventflux.query.api.execution.query.input.stream.SingleInputStream
use super::input_stream::InputStreamTrait;
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::execution::query::input::handler::{
    Filter, StreamFunction, StreamHandler, WindowHandler,
};
use crate::query_api::execution::query::Query;
use crate::query_api::expression::Expression; // For AnonymousInputStream part

#[derive(Clone, Debug, PartialEq)]
pub enum SingleInputStreamKind {
    Basic {
        is_fault_stream: bool,
        is_inner_stream: bool,
        stream_id: String,
        stream_reference_id: Option<String>,
        stream_handlers: Vec<StreamHandler>,
    },
    Anonymous {
        // Using Box<Query> for the actual query object to avoid sizing issues if Query is large.
        query: Box<Query>,
        stream_id: String, // Generated ID
        stream_reference_id: Option<String>,
        stream_handlers: Vec<StreamHandler>,
    },
}

impl Default for SingleInputStreamKind {
    fn default() -> Self {
        // Default to a basic, empty stream
        SingleInputStreamKind::Basic {
            is_fault_stream: false,
            is_inner_stream: false,
            stream_id: String::new(), // Or a default name like "_default_single_stream"
            stream_reference_id: None,
            stream_handlers: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Default)] // Added Default
pub struct SingleInputStream {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement
    pub kind: SingleInputStreamKind,
}

impl SingleInputStream {
    // Private constructor to be used by specific factory methods
    fn new(kind: SingleInputStreamKind) -> Self {
        SingleInputStream {
            eventflux_element: EventFluxElement::default(),
            kind,
        }
    }

    // Factory for Basic kind
    pub fn new_basic(
        stream_id: String,
        is_inner: bool,
        is_fault: bool,
        stream_ref_id: Option<String>,
        handlers: Vec<StreamHandler>,
    ) -> Self {
        Self::new(SingleInputStreamKind::Basic {
            is_fault_stream: is_fault,
            is_inner_stream: is_inner,
            stream_id,
            stream_reference_id: stream_ref_id,
            stream_handlers: handlers,
        })
    }

    // Factories for variants used by InputStream enum factories
    pub(super) fn new_basic_from_id(stream_id: String) -> Self {
        Self::new_basic(stream_id, false, false, None, Vec::new())
    }
    pub(super) fn new_basic_from_ref_id(stream_ref_id: String, stream_id: String) -> Self {
        Self::new_basic(stream_id, false, false, Some(stream_ref_id), Vec::new())
    }
    pub(super) fn new_basic_inner_from_id(stream_id: String) -> Self {
        Self::new_basic(stream_id, true, false, None, Vec::new())
    }
    // TODO: Add other specific factories: new_basic_inner_from_ref_id, new_basic_fault_from_id, etc.

    // Factory for Anonymous kind
    pub fn new_anonymous(
        query: Box<Query>,
        stream_id: String,
        stream_ref_id: Option<String>,
        handlers: Vec<StreamHandler>,
    ) -> Self {
        Self::new(SingleInputStreamKind::Anonymous {
            query,
            stream_id,
            stream_reference_id: stream_ref_id,
            stream_handlers: handlers,
        })
    }

    pub(super) fn new_anonymous_from_query(query: Query) -> Self {
        // TODO: Add validation for query.output_stream being ReturnStream
        let stream_id = format!("Anonymous-{}", uuid::Uuid::new_v4());
        Self::new_anonymous(Box::new(query), stream_id, None, Vec::new())
    }

    // Accessors and builder methods
    pub fn get_stream_id_str(&self) -> &str {
        match &self.kind {
            SingleInputStreamKind::Basic { stream_id, .. } => stream_id,
            SingleInputStreamKind::Anonymous { stream_id, .. } => stream_id,
        }
    }

    pub fn get_stream_reference_id_str(&self) -> Option<&str> {
        match &self.kind {
            SingleInputStreamKind::Basic {
                stream_reference_id,
                ..
            } => stream_reference_id.as_deref(),
            SingleInputStreamKind::Anonymous {
                stream_reference_id,
                ..
            } => stream_reference_id.as_deref(),
        }
    }
    // ... other getters for is_fault_stream, is_inner_stream, stream_reference_id ...

    pub fn get_stream_handlers(&self) -> &Vec<StreamHandler> {
        match &self.kind {
            SingleInputStreamKind::Basic {
                stream_handlers, ..
            } => stream_handlers,
            SingleInputStreamKind::Anonymous {
                stream_handlers, ..
            } => stream_handlers,
        }
    }

    pub fn get_stream_handlers_mut(&mut self) -> &mut Vec<StreamHandler> {
        match &mut self.kind {
            SingleInputStreamKind::Basic {
                stream_handlers, ..
            } => stream_handlers,
            SingleInputStreamKind::Anonymous {
                stream_handlers, ..
            } => stream_handlers,
        }
    }

    pub fn filter(mut self, filter_expression: Expression) -> Self {
        self.get_stream_handlers_mut()
            .push(StreamHandler::Filter(Filter::new(filter_expression)));
        self
    }

    pub fn function(
        mut self,
        namespace: Option<String>,
        name: String,
        params: Vec<Expression>,
    ) -> Self {
        self.get_stream_handlers_mut()
            .push(StreamHandler::Function(StreamFunction::new(
                name, namespace, params,
            )));
        self
    }

    pub fn window(
        mut self,
        namespace: Option<String>,
        name: String,
        params: Vec<Expression>,
    ) -> Self {
        self.get_stream_handlers_mut()
            .push(StreamHandler::Window(Box::new(WindowHandler::new(
                name, namespace, params,
            ))));
        self
    }

    pub fn as_ref(mut self, stream_ref_id: String) -> Self {
        match &mut self.kind {
            SingleInputStreamKind::Basic {
                stream_reference_id,
                ..
            } => *stream_reference_id = Some(stream_ref_id),
            SingleInputStreamKind::Anonymous {
                stream_reference_id,
                ..
            } => *stream_reference_id = Some(stream_ref_id),
        }
        self
    }
}

// EventFluxElement is composed: access via self.eventflux_element
// InputStreamTrait implementation
impl InputStreamTrait for SingleInputStream {
    fn get_all_stream_ids(&self) -> Vec<String> {
        vec![self.get_stream_id_str().to_string()]
    }

    fn get_unique_stream_ids(&self) -> Vec<String> {
        vec![self.get_stream_id_str().to_string()]
    }
}
