// SPDX-License-Identifier: MIT OR Apache-2.0

// src/query_api/eventflux_app.rs
use crate::query_api::annotation::Annotation;
use crate::query_api::definition::{
    AggregationDefinition, FunctionDefinition, StreamDefinition, TableDefinition,
    TriggerDefinition, WindowDefinition,
};
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::execution::ExecutionElement; // This should be the enum
use std::collections::HashMap;
use std::sync::Arc;

/// Represents a EventFlux Application, containing stream definitions, queries, etc.
#[derive(Debug, Clone, Default)] // Added Debug, Clone, Default
pub struct EventFluxApp {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement (as per current design)
    pub name: String, // Name for the EventFlux Application, from @info(name='...')

    pub stream_definition_map: HashMap<String, Arc<StreamDefinition>>,
    pub table_definition_map: HashMap<String, Arc<TableDefinition>>,
    pub window_definition_map: HashMap<String, Arc<WindowDefinition>>,
    pub trigger_definition_map: HashMap<String, Arc<TriggerDefinition>>,
    pub aggregation_definition_map: HashMap<String, Arc<AggregationDefinition>>,
    pub function_definition_map: HashMap<String, Arc<FunctionDefinition>>,

    pub execution_element_list: Vec<ExecutionElement>, // query_api::execution::ExecutionElement enum
    pub annotations: Vec<Annotation>,
    // execution_element_name_list is not typically stored directly; validated during parsing/adding.
}

impl EventFluxApp {
    pub fn new(name: String) -> Self {
        Self {
            eventflux_element: EventFluxElement::default(), // Initialize with default start/end context
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

    // --- Add definition methods ---
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

    // --- Get definition methods ---
    pub fn get_stream_definition_map(&self) -> &HashMap<String, Arc<StreamDefinition>> {
        &self.stream_definition_map
    }
    pub fn get_stream_definition(&self, stream_id: &str) -> Option<Arc<StreamDefinition>> {
        self.stream_definition_map.get(stream_id).cloned()
    }
    // TODO: Add similar getters for other definition maps if needed.

    // --- ExecutionElement methods ---
    pub fn add_execution_element(&mut self, exec_element: ExecutionElement) {
        self.execution_element_list.push(exec_element);
    }
    pub fn get_execution_elements(&self) -> &Vec<ExecutionElement> {
        &self.execution_element_list
    }

    // --- Annotation methods ---
    pub fn add_annotation(&mut self, annotation: Annotation) {
        self.annotations.push(annotation);
    }
    pub fn get_annotations(&self) -> &Vec<Annotation> {
        &self.annotations
    }
}

// Helper method for StreamDefinition to get its ID (assuming it's via AbstractDefinition)
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
// FunctionDefinition and TriggerDefinition have public `id` field.
