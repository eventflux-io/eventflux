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

use crate::query_api::annotation::Annotation;
use crate::query_api::definition::attribute::Type as AttributeType;
use crate::query_api::eventflux_element::EventFluxElement;

/// A user defined function written in a script language.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct FunctionDefinition {
    pub eventflux_element: EventFluxElement,

    pub id: String,
    /// Script language the body is written in.
    pub language: String,
    /// Function source code.
    pub body: String,
    /// Defaults to `AttributeType::OBJECT`.
    pub return_type: AttributeType,
    pub annotations: Vec<Annotation>,
}

impl FunctionDefinition {
    pub fn new(id: String, language: String, body: String, return_type: AttributeType) -> Self {
        FunctionDefinition {
            eventflux_element: EventFluxElement::default(),
            id,
            language,
            body,
            return_type,
            annotations: Vec::new(),
        }
    }

    /// Start a builder chain with only the id set. Language and body start empty and the
    /// return type starts as `OBJECT`.
    pub fn id(id: String) -> Self {
        FunctionDefinition {
            id,
            annotations: Vec::new(),
            ..Default::default()
        }
    }

    pub fn language(mut self, language: String) -> Self {
        self.language = language;
        self
    }

    pub fn body(mut self, body: String) -> Self {
        self.body = body;
        self
    }

    // Not named `type` because that is a Rust keyword.
    pub fn return_type(mut self, return_type: AttributeType) -> Self {
        self.return_type = return_type;
        self
    }

    pub fn annotation(mut self, annotation: Annotation) -> Self {
        self.annotations.push(annotation);
        self
    }
}

// Possible Deref impls, if callers ever need direct access to the EventFluxElement fields.
// impl std::ops::Deref for FunctionDefinition {
//     type Target = EventFluxElement;
//     fn deref(&self) -> &Self::Target { &self.eventflux_element }
// }
// impl std::ops::DerefMut for FunctionDefinition {
//     fn deref_mut(&mut self) -> &mut Self::Target { &mut self.eventflux_element }
// }
