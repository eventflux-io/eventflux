// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/event/stream/meta_stream_event.rs
// Simplified for initial ExpressionParser - focuses on a single input stream.
// The full MetaStreamEvent in Java handles multiple input definitions, output defs, etc.
use crate::query_api::definition::{
    attribute::Type as ApiAttributeType, // Import the Type enum
    Attribute as ApiAttribute,
    StreamDefinition as ApiStreamDefinition,
};
use std::collections::HashMap;
use std::sync::Arc;

// MetaStreamEvent in Java has an EventType enum (TABLE, WINDOW, AGGREGATE, DEFAULT)
// This was defined in subtask 0015, turn 10.
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
    // For simple parsing, assumes one input stream providing all attributes.
    pub input_stream_definition: Arc<ApiStreamDefinition>,

    // Pre-calculated map for quick attribute lookup from the input_stream_definition.
    // Maps attribute name to (index_in_event_data_array, attribute_type).
    // This assumes that the 'data' array in a core::Event or core::StreamEvent
    // directly corresponds to the attributes in this input_stream_definition.
    attribute_info: HashMap<String, (usize, ApiAttributeType)>, // Use ApiAttributeType
    pub before_window_data: Vec<ApiAttribute>,
    pub on_after_window_data: Vec<ApiAttribute>,
    pub output_data: Vec<ApiAttribute>,
    pub input_definitions: Vec<Arc<ApiStreamDefinition>>,
    pub input_reference_id: Option<String>,
    pub output_stream_definition: Option<Arc<ApiStreamDefinition>>,
    pub event_type: MetaStreamEventType, // From Java
    pub multi_value: bool,
}

impl MetaStreamEvent {
    // Constructor for the simplified single input stream case.
    pub fn new_for_single_input(input_stream_def: Arc<ApiStreamDefinition>) -> Self {
        let mut attribute_info = HashMap::new();
        // Assuming get_attribute_list() is available on ApiStreamDefinition (likely via AbstractDefinition)
        for (index, api_attr) in input_stream_def
            .abstract_definition
            .attribute_list
            .iter()
            .enumerate()
        {
            // Assuming get_name() and get_type() are available on ApiAttribute
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

    // Finds attribute info from the (single) input_stream_definition it holds.
    // Returns (index_in_data_array, attribute_type)
    pub fn find_attribute_info(&self, attribute_name: &str) -> Option<&(usize, ApiAttributeType)> {
        // Use ApiAttributeType
        self.attribute_info.get(attribute_name)
    }

    /// Offsets all stored attribute index positions by the given amount.
    ///
    /// This is useful when building composite events such as joins where
    /// attributes from multiple streams are packed into a single event data
    /// array.  By applying an offset to one stream's `MetaStreamEvent`
    /// we can reuse the variable resolution logic over the combined array.
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

    // This method was in the prompt. It might be useful for constructing output events
    // or for a VariableExpressionExecutor that needs to know the output structure if it's
    // different from the input (e.g., after projections).
    // For a simple VEE based on input, this specific list might not be directly used by VEE.execute(),
    // but could be used during VEE construction to determine its return type and index.
    pub fn get_input_attribute_name_type_list(&self) -> Vec<(String, ApiAttributeType)> {
        // Use ApiAttributeType
        self.input_stream_definition
            .abstract_definition
            .attribute_list
            .iter()
            .map(|attr| (attr.name.clone(), attr.attribute_type))
            .collect()
    }

    // TODO: Add other methods from Java MetaStreamEvent as needed for more complex parsing,
    // e.g., addData, addOutputData, addInputDefinition, setOutputDefinition, etc.
    // The Java version's addData method is particularly complex as it manages
    // beforeWindowData, onAfterWindowData, and determines where an attribute is "first" seen.
}

// Default implementation might not be very useful if input_stream_definition is mandatory.
// However, if used in Option fields, Default could be:
impl Default for MetaStreamEvent {
    fn default() -> Self {
        // Creates an empty MetaStreamEvent, likely invalid for actual use without further setup.
        // This requires ApiStreamDefinition to be Default.
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
