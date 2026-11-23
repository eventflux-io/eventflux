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
use crate::query_api::definition::{
    AggregationDefinition, FunctionDefinition, StreamDefinition, TableDefinition,
    TriggerDefinition, WindowDefinition,
};
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::execution::ExecutionElement;
use std::collections::HashMap;
use std::sync::Arc;

/// A parsed EventFlux app. Holds its definitions, queries, partitions and annotations.
///
/// Each definition map is keyed by the definition id.
#[derive(Debug, Clone, Default)]
pub struct EventFluxApp {
    pub eventflux_element: EventFluxElement,
    pub name: String,

    pub stream_definition_map: HashMap<String, Arc<StreamDefinition>>,
    pub table_definition_map: HashMap<String, Arc<TableDefinition>>,
    pub window_definition_map: HashMap<String, Arc<WindowDefinition>>,
    pub trigger_definition_map: HashMap<String, Arc<TriggerDefinition>>,
    pub aggregation_definition_map: HashMap<String, Arc<AggregationDefinition>>,
    pub function_definition_map: HashMap<String, Arc<FunctionDefinition>>,

    /// Queries and partitions in the order they were added.
    pub execution_element_list: Vec<ExecutionElement>,
    pub annotations: Vec<Annotation>,
    // Execution element names are not stored. Duplicate names are not checked here.
}

impl EventFluxApp {
    pub fn new(name: String) -> Self {
        Self {
            eventflux_element: EventFluxElement::default(),
            name,
            stream_definition_map: HashMap::new(),
            table_definition_map: HashMap::new(),
            window_definition_map: HashMap::new(),
            trigger_definition_map: HashMap::new(),
            aggregation_definition_map: HashMap::new(),
            function_definition_map: HashMap::new(),
            execution_element_list: Vec::new(),
            annotations: Vec::new(),
        }
    }

    // --- Add definitions. Each replaces any earlier definition with the same id. ---
    pub fn add_stream_definition(&mut self, stream_def: StreamDefinition) {
        self.stream_definition_map
            .insert(stream_def.get_id().to_string(), Arc::new(stream_def));
    }
    pub fn add_table_definition(&mut self, table_def: TableDefinition) {
        self.table_definition_map
            .insert(table_def.get_id().to_string(), Arc::new(table_def));
    }
    pub fn add_window_definition(&mut self, window_def: WindowDefinition) {
        self.window_definition_map
            .insert(window_def.get_id().to_string(), Arc::new(window_def));
    }
    pub fn add_trigger_definition(&mut self, trigger_def: TriggerDefinition) {
        self.trigger_definition_map
            .insert(trigger_def.id.clone(), Arc::new(trigger_def));
    }
    pub fn add_aggregation_definition(&mut self, agg_def: AggregationDefinition) {
        self.aggregation_definition_map
            .insert(agg_def.get_id().to_string(), Arc::new(agg_def));
    }
    pub fn add_function_definition(&mut self, func_def: FunctionDefinition) {
        self.function_definition_map
            .insert(func_def.id.clone(), Arc::new(func_def));
    }

    // --- Read definitions ---
    pub fn get_stream_definition_map(&self) -> &HashMap<String, Arc<StreamDefinition>> {
        &self.stream_definition_map
    }
    pub fn get_stream_definition(&self, stream_id: &str) -> Option<Arc<StreamDefinition>> {
        self.stream_definition_map.get(stream_id).cloned()
    }
    // TODO: Add getters like these for the other definition maps if needed. Their fields
    // are public for now.

    // --- Execution elements ---
    pub fn add_execution_element(&mut self, exec_element: ExecutionElement) {
        self.execution_element_list.push(exec_element);
    }
    pub fn get_execution_elements(&self) -> &Vec<ExecutionElement> {
        &self.execution_element_list
    }

    // --- Annotations ---
    pub fn add_annotation(&mut self, annotation: Annotation) {
        self.annotations.push(annotation);
    }
    pub fn get_annotations(&self) -> &Vec<Annotation> {
        &self.annotations
    }
}

// Private id getters used by the add_* methods above.
impl StreamDefinition {
    fn get_id(&self) -> &str {
        &self.abstract_definition.id
    }
}
impl TableDefinition {
    fn get_id(&self) -> &str {
        &self.abstract_definition.id
    }
}
impl WindowDefinition {
    fn get_id(&self) -> &str {
        &self.stream_definition.abstract_definition.id
    }
}
impl AggregationDefinition {
    fn get_id(&self) -> &str {
        &self.abstract_definition.id
    }
}
// FunctionDefinition and TriggerDefinition need none. Their `id` field is public.
