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

// Corresponds to io.eventflux.query.api.definition.FunctionDefinition
use crate::query_api::annotation::Annotation;
use crate::query_api::definition::attribute::Type as AttributeType;
use crate::query_api::eventflux_element::EventFluxElement;

#[derive(Clone, Debug, PartialEq, Default)] // Added Default
pub struct FunctionDefinition {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement

    // FunctionDefinition fields
    pub id: String,
    pub language: String,
    pub body: String,
    pub return_type: AttributeType, // Default for AttributeType::OBJECT
    pub annotations: Vec<Annotation>,
}

impl FunctionDefinition {
    // Constructor requiring all essential fields.
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

    // Builder-style methods from Java
    // These are now associated functions that construct a new FunctionDefinition
    // or modify an existing one if they take `mut self`.
    // The Java API implies these are builder steps on a new object.

    // Static factories for builder pattern start
    pub fn id(id: String) -> Self {
        // Starts the build with an ID
        FunctionDefinition {
            id,
            annotations: Vec::new(),
            ..Default::default() // Sets eventflux_element, language, body, return_type to default
        }
    }

    // Methods to modify fields (fluent interface)
    pub fn language(mut self, language: String) -> Self {
        self.language = language;
        self
    }

    pub fn body(mut self, body: String) -> Self {
        self.body = body;
        self
    }

    // Renamed from `type` in Java
    pub fn return_type(mut self, return_type: AttributeType) -> Self {
        self.return_type = return_type;
        self
    }

    pub fn annotation(mut self, annotation: Annotation) -> Self {
        self.annotations.push(annotation);
        self
    }
}

// Removed: impl From<EventFluxElement> for FunctionDefinition

// For direct field access via Deref, if desired:
// impl std::ops::Deref for FunctionDefinition {
//     type Target = EventFluxElement;
//     fn deref(&self) -> &Self::Target { &self.eventflux_element }
// }
// impl std::ops::DerefMut for FunctionDefinition {
//     fn deref_mut(&mut self) -> &mut Self::Target { &mut self.eventflux_element }
// }
