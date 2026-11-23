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

// Simplified port of the Java MetaStreamEvent. Attribute lookup covers only the main input
// stream. The Java version also tracks several input definitions, output definitions and more.
use crate::query_api::definition::{
    attribute::Type as ApiAttributeType, Attribute as ApiAttribute,
    StreamDefinition as ApiStreamDefinition,
};
use std::collections::HashMap;
use std::sync::Arc;

// Port of the Java MetaStreamEvent.EventType (TABLE, WINDOW, AGGREGATE, DEFAULT).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy, Default)]
pub enum MetaStreamEventType {
    #[default]
    DEFAULT,
    TABLE,
    WINDOW,
    AGGREGATE,
}

#[derive(Debug, Clone)]
pub struct MetaStreamEvent {
    // The one input stream assumed to provide every attribute.
    pub input_stream_definition: Arc<ApiStreamDefinition>,

    // Attribute name to (index in the event data array, attribute type), built once from
    // input_stream_definition. Assumes the event data array follows that definition's
    // attribute order, shifted by any apply_attribute_offset call.
    attribute_info: HashMap<String, (usize, ApiAttributeType)>,
    pub before_window_data: Vec<ApiAttribute>,
    pub on_after_window_data: Vec<ApiAttribute>,
    pub output_data: Vec<ApiAttribute>,
    pub input_definitions: Vec<Arc<ApiStreamDefinition>>,
    pub input_reference_id: Option<String>,
    pub output_stream_definition: Option<Arc<ApiStreamDefinition>>,
    pub event_type: MetaStreamEventType,
    pub multi_value: bool,
}

impl MetaStreamEvent {
    // Builds a meta event for one input stream. Output data starts as a copy of the
    // stream's attributes.
    pub fn new_for_single_input(input_stream_def: Arc<ApiStreamDefinition>) -> Self {
        let mut attribute_info = HashMap::new();
        for (index, api_attr) in input_stream_def
            .abstract_definition
            .attribute_list
            .iter()
            .enumerate()
        {
            attribute_info.insert(api_attr.name.clone(), (index, api_attr.attribute_type));
        }
        Self {
            input_stream_definition: input_stream_def.clone(),
            attribute_info,
            before_window_data: Vec::new(),
            on_after_window_data: Vec::new(),
            output_data: input_stream_def.abstract_definition.attribute_list.clone(),
            input_definitions: vec![input_stream_def],
            input_reference_id: None,
            output_stream_definition: None,
            event_type: MetaStreamEventType::default(),
            multi_value: false,
        }
    }

    // Looks up an attribute of the input stream by name.
    // Returns (index in the data array, attribute type), or None if not found.
    pub fn find_attribute_info(&self, attribute_name: &str) -> Option<&(usize, ApiAttributeType)> {
        self.attribute_info.get(attribute_name)
    }

    /// Adds `offset` to every stored attribute index.
    ///
    /// Used for joins and other combined events, where the attributes of several
    /// streams sit in one data array. Shifting one stream's indexes lets the same
    /// variable lookup work over the combined array.
    pub fn apply_attribute_offset(&mut self, offset: usize) {
        self.attribute_info = self
            .attribute_info
            .iter()
            .map(|(k, (idx, t))| (k.clone(), (idx + offset, *t)))
            .collect();
    }

    pub fn get_before_window_data(&self) -> &[ApiAttribute] {
        &self.before_window_data
    }

    pub fn get_on_after_window_data(&self) -> &[ApiAttribute] {
        &self.on_after_window_data
    }

    pub fn get_output_data(&self) -> &[ApiAttribute] {
        &self.output_data
    }

    pub fn add_input_definition(&mut self, def: Arc<ApiStreamDefinition>) {
        self.input_definitions.push(def);
    }

    pub fn set_output_definition(&mut self, def: ApiStreamDefinition) {
        self.output_stream_definition = Some(Arc::new(def));
    }

    pub fn get_output_stream_definition(&self) -> Option<&Arc<ApiStreamDefinition>> {
        self.output_stream_definition.as_ref()
    }

    pub fn add_output_data_allowing_duplicate(&mut self, attr: ApiAttribute) {
        self.output_data.push(attr);
    }

    pub fn clone_meta_stream_event(&self) -> Self {
        Self {
            input_stream_definition: self.input_stream_definition.clone(),
            attribute_info: self.attribute_info.clone(),
            before_window_data: self.before_window_data.clone(),
            on_after_window_data: self.on_after_window_data.clone(),
            output_data: self.output_data.clone(),
            input_definitions: self.input_definitions.clone(),
            input_reference_id: self.input_reference_id.clone(),
            output_stream_definition: self.output_stream_definition.clone(),
            event_type: self.event_type,
            multi_value: self.multi_value,
        }
    }

    // Returns (name, type) for each attribute of the input stream, in definition order.
    pub fn get_input_attribute_name_type_list(&self) -> Vec<(String, ApiAttributeType)> {
        self.input_stream_definition
            .abstract_definition
            .attribute_list
            .iter()
            .map(|attr| (attr.name.clone(), attr.attribute_type))
            .collect()
    }

    // TODO: Port the rest of the Java MetaStreamEvent as needed, mainly addData. The Java
    // addData is complex. It fills beforeWindowData and onAfterWindowData and decides
    // where an attribute is first seen.
}

// Builds an empty MetaStreamEvent with a default stream definition. It is not usable
// for real lookups until set up further.
impl Default for MetaStreamEvent {
    fn default() -> Self {
        Self {
            input_stream_definition: Arc::new(ApiStreamDefinition::default()),
            attribute_info: HashMap::new(),
            before_window_data: Vec::new(),
            on_after_window_data: Vec::new(),
            output_data: Vec::new(),
            input_definitions: Vec::new(),
            input_reference_id: None,
            output_stream_definition: None,
            event_type: MetaStreamEventType::default(),
            multi_value: false,
        }
    }
}
