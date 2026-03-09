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

// Corresponds to io.eventflux.query.api.definition.TableDefinition
use crate::query_api::annotation::Annotation;
use crate::query_api::definition::abstract_definition::AbstractDefinition;
use crate::query_api::definition::attribute::{Attribute, Type as AttributeType}; // Assuming Annotation is defined

#[derive(Clone, Debug, PartialEq, Default)] // Added Default
pub struct TableDefinition {
    // Composition for inheritance from AbstractDefinition
    pub abstract_definition: AbstractDefinition,

    /// Configuration properties from SQL WITH clause
    ///
    /// Stores the FlatConfig extracted during SQL parsing. These properties have
    /// the highest priority in the 4-layer configuration merge:
    ///
    /// Priority (highest to lowest):
    /// 1. SQL WITH (this field)
    /// 2. TOML table-specific
    /// 3. TOML application-wide
    /// 4. Rust defaults
    ///
    /// None if no WITH clause was specified in SQL.
    pub with_config: Option<crate::core::config::stream_config::FlatConfig>,
}

impl TableDefinition {
    // Constructor that takes an id, as per Java's `TableDefinition.id(id)`
    pub fn new(id: String) -> Self {
        TableDefinition {
            abstract_definition: AbstractDefinition::new(id),
            with_config: None,
        }
    }

    // Static factory method `id` from Java
    pub fn id(table_id: String) -> Self {
        Self::new(table_id)
    }

    // Builder-style methods, specific to TableDefinition
    pub fn attribute(mut self, attribute_name: String, attribute_type: AttributeType) -> Self {
        // Check for duplicate attribute names and warn
        if self
            .abstract_definition
            .attribute_list
            .iter()
            .any(|attr| attr.get_name() == &attribute_name)
        {
            eprintln!(
                "Warning: Duplicate attribute '{}' in table definition",
                attribute_name
            );
        }

        self.abstract_definition
            .attribute_list
            .push(Attribute::new(attribute_name, attribute_type));
        self
    }

    pub fn annotation(mut self, annotation: Annotation) -> Self {
        self.abstract_definition.annotations.push(annotation);
        self
    }

    /// Set SQL WITH configuration
    pub fn with_config(mut self, config: crate::core::config::stream_config::FlatConfig) -> Self {
        self.with_config = Some(config);
        self
    }
}

// Provide access to AbstractDefinition fields and EventFluxElement fields
impl AsRef<AbstractDefinition> for TableDefinition {
    fn as_ref(&self) -> &AbstractDefinition {
        &self.abstract_definition
    }
}

impl AsMut<AbstractDefinition> for TableDefinition {
    fn as_mut(&mut self) -> &mut AbstractDefinition {
        &mut self.abstract_definition
    }
}

// Through AbstractDefinition, can access EventFluxElement
use crate::query_api::eventflux_element::EventFluxElement;
impl AsRef<EventFluxElement> for TableDefinition {
    fn as_ref(&self) -> &EventFluxElement {
        self.abstract_definition.as_ref()
    }
}

impl AsMut<EventFluxElement> for TableDefinition {
    fn as_mut(&mut self) -> &mut EventFluxElement {
        self.abstract_definition.as_mut()
    }
}
