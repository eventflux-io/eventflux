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
use crate::query_api::definition::abstract_definition::AbstractDefinition;
use crate::query_api::definition::attribute::{Attribute, Type as AttributeType};

/// A table with a unique id, a list of attributes and optional SQL WITH properties.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct TableDefinition {
    pub abstract_definition: AbstractDefinition,

    /// Properties from the SQL WITH clause, collected during parsing.
    ///
    /// These win over every other config source when the runtime merges them.
    ///
    /// Priority (highest to lowest):
    /// 1. SQL WITH (this field)
    /// 2. CLI `--set` overrides, already applied to the YAML config
    /// 3. YAML config file
    /// 4. Code defaults
    ///
    /// `None` when the SQL has no WITH clause.
    pub with_config: Option<crate::core::config::stream_config::FlatConfig>,
}

impl TableDefinition {
    pub fn new(id: String) -> Self {
        TableDefinition {
            abstract_definition: AbstractDefinition::new(id),
            with_config: None,
        }
    }

    /// Same as `new`. Starts a builder chain.
    pub fn id(table_id: String) -> Self {
        Self::new(table_id)
    }

    /// Add an attribute.
    ///
    /// A duplicate name is still added. It only prints a warning to stderr.
    pub fn attribute(mut self, attribute_name: String, attribute_type: AttributeType) -> Self {
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

    /// Set the SQL WITH clause properties, replacing any set before.
    pub fn with_config(mut self, config: crate::core::config::stream_config::FlatConfig) -> Self {
        self.with_config = Some(config);
        self
    }
}

// Access to the shared definition fields and the query positions.
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

// The EventFluxElement lives inside AbstractDefinition.
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
