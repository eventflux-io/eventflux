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
use crate::query_api::definition::attribute::Attribute;
use crate::query_api::eventflux_element::EventFluxElement;

/// Fields shared by every definition. An id, an ordered attribute list and annotations.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct AbstractDefinition {
    pub eventflux_element: EventFluxElement,

    pub id: String,
    /// Attributes in schema order.
    pub attribute_list: Vec<Attribute>,
    // Attribute names are not cached. attribute_name_list() builds them on each call.
    pub annotations: Vec<Annotation>,
}

impl AbstractDefinition {
    pub fn new(id: String) -> Self {
        AbstractDefinition {
            eventflux_element: EventFluxElement::default(),
            id,
            attribute_list: Vec::new(),
            annotations: Vec::new(),
        }
    }

    pub fn get_id(&self) -> &String {
        &self.id
    }

    pub fn get_attribute_list(&self) -> &Vec<Attribute> {
        &self.attribute_list
    }

    /// Attribute names in schema order. Builds a new list on every call.
    pub fn attribute_name_list(&self) -> Vec<String> {
        self.attribute_list
            .iter()
            .map(|attr| attr.name.clone())
            .collect()
    }

    // TODO: Add builder methods for attributes and annotations here. The concrete
    // definitions such as StreamDefinition and TableDefinition have their own for now.
    // pub fn attribute(mut self, attribute_name: String, attribute_type: crate::query_api::definition::attribute::Type) -> Self {
    //     // TODO: checkAttribute logic
    //     self.attribute_list.push(Attribute::new(attribute_name, attribute_type));
    //     self
    // }
    // pub fn annotation(mut self, annotation: Annotation) -> Self {
    //     self.annotations.push(annotation);
    //     self
    // }
}

// The field is public too, so `my_def.eventflux_element` works as well.
impl AsRef<EventFluxElement> for AbstractDefinition {
    fn as_ref(&self) -> &EventFluxElement {
        &self.eventflux_element
    }
}

impl AsMut<EventFluxElement> for AbstractDefinition {
    fn as_mut(&mut self) -> &mut EventFluxElement {
        &mut self.eventflux_element
    }
}
