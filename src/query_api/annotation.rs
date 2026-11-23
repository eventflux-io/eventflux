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

/// One key and value pair inside an annotation.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Element {
    pub eventflux_element: EventFluxElement,
    pub key: String,
    pub value: String,
}

impl Element {
    pub fn new(key: String, value: String) -> Self {
        Element {
            eventflux_element: EventFluxElement::default(),
            key,
            value,
        }
    }
}

// Possible Deref impls, if callers ever need direct access to the EventFluxElement fields.
// impl std::ops::Deref for Element {
//     type Target = EventFluxElement;
//     fn deref(&self) -> &Self::Target { &self.eventflux_element }
// }
// impl std::ops::DerefMut for Element {
//     fn deref_mut(&mut self) -> &mut Self::Target { &mut self.eventflux_element }
// }

/// A named annotation with key and value elements and nested annotations.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Annotation {
    pub eventflux_element: EventFluxElement,

    pub name: String,
    pub elements: Vec<Element>,
    pub annotations: Vec<Annotation>,
}

impl Annotation {
    pub fn new(name: String) -> Self {
        Annotation {
            eventflux_element: EventFluxElement::default(),
            name,
            elements: Vec::new(),
            annotations: Vec::new(),
        }
    }

    pub fn name(mut self, name: String) -> Self {
        self.name = name;
        self
    }

    /// Add an element. A missing key defaults to `"value"`.
    pub fn element(mut self, key: Option<String>, value: String) -> Self {
        let actual_key = key.unwrap_or_else(|| "value".to_string());
        self.elements.push(Element::new(actual_key, value));
        self
    }

    pub fn add_element(mut self, element_obj: Element) -> Self {
        self.elements.push(element_obj);
        self
    }

    pub fn annotation(mut self, annotation: Annotation) -> Self {
        self.annotations.push(annotation);
        self
    }
}

// Annotation and Element hold an EventFluxElement field instead of implementing a trait.
// Read the query positions through it, as in
// `my_annotation.eventflux_element.query_context_start_index`.

// Old copy of Element, kept for reference. The live definition is at the top of this file.
// #[derive(Clone, Debug, PartialEq, Default)]
// pub struct Element {
//     pub eventflux_element: EventFluxElement,
//     pub key: String,
//     pub value: String,
// }
// impl Element {
//     pub fn new(key: String, value: String) -> Self {
//         Element {
//             eventflux_element: EventFluxElement::default(),
//             key,
//             value,
//         }
//     }
// }
