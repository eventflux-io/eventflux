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

// Corresponds to io.eventflux.query.api.annotation.Annotation & Element.java
use crate::query_api::eventflux_element::EventFluxElement; // The struct to be composed

#[derive(Clone, Debug, PartialEq, Default)] // Added Default
pub struct Element {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement
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

// If direct access to EventFluxElement fields is needed often:
// impl std::ops::Deref for Element {
//     type Target = EventFluxElement;
//     fn deref(&self) -> &Self::Target { &self.eventflux_element }
// }
// impl std::ops::DerefMut for Element {
//     fn deref_mut(&mut self) -> &mut Self::Target { &mut self.eventflux_element }
// }

#[derive(Clone, Debug, PartialEq, Default)] // Added Default
pub struct Annotation {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement

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

    // Builder methods from Java
    pub fn name(mut self, name: String) -> Self {
        self.name = name;
        self
    }

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

// No longer need to impl EventFluxElement for Annotation/Element as they compose it.
// Access to context indices would be via `my_annotation.eventflux_element.query_context_start_index`.
// Or via Deref/DerefMut if implemented.

// Note: The prompt had `pub element: EventFluxElement` for the composed field.
// I used `element_context` in `Element` to avoid potential naming conflicts if `Element` itself
// had methods named `element`. For `Annotation`, `eventflux_element` is fine.
// I will stick to `eventflux_element` for consistency as per the general instruction.

// Re-adjusting Element to use `eventflux_element` for the composed field name.
// (This block is part of the same overwrite operation)
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
// The above change for Element is now incorporated directly into the main struct definition for Element.
