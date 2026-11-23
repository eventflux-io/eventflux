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

/// A reference to an attribute, optionally qualified by a stream or a function.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Variable {
    pub eventflux_element: EventFluxElement,

    /// Stream id or alias that qualifies the attribute, as in `s.price`.
    pub stream_id: Option<String>,
    pub is_inner_stream: bool,
    /// Position of the event when the stream matched more than once in a pattern.
    pub stream_index: Option<i32>,
    pub function_id: Option<String>,
    pub function_index: Option<i32>,
    pub attribute_name: String,
}

impl Variable {
    /// An unqualified attribute. All other fields start unset.
    pub fn new(attribute_name: String) -> Self {
        Variable {
            eventflux_element: EventFluxElement::default(),
            attribute_name,
            stream_id: None,
            is_inner_stream: false,
            stream_index: None,
            function_id: None,
            function_index: None,
        }
    }

    // The of_stream builders below set or clear is_inner_stream. Those without an index
    // leave any earlier stream_index in place.
    pub fn of_stream(mut self, stream_id: String) -> Self {
        self.stream_id = Some(stream_id);
        self.is_inner_stream = false;
        self
    }

    pub fn of_inner_stream(mut self, stream_id: String) -> Self {
        self.stream_id = Some(stream_id);
        self.is_inner_stream = true;
        self
    }

    pub fn of_stream_with_index(mut self, stream_id: String, stream_index: i32) -> Self {
        self.stream_id = Some(stream_id);
        self.stream_index = Some(stream_index);
        self.is_inner_stream = false;
        self
    }

    pub fn of_inner_stream_with_index(mut self, stream_id: String, stream_index: i32) -> Self {
        self.stream_id = Some(stream_id);
        self.stream_index = Some(stream_index);
        self.is_inner_stream = true;
        self
    }

    pub fn of_function(mut self, function_id: String) -> Self {
        self.function_id = Some(function_id);
        self
    }

    pub fn of_function_with_index(mut self, function_id: String, function_index: i32) -> Self {
        self.function_id = Some(function_id);
        self.function_index = Some(function_index);
        self
    }

    pub fn get_attribute_name(&self) -> &String {
        &self.attribute_name
    }

    pub fn get_stream_id(&self) -> Option<&String> {
        self.stream_id.as_ref()
    }

    pub fn is_inner_stream(&self) -> bool {
        self.is_inner_stream
    }

    pub fn get_stream_index(&self) -> Option<i32> {
        self.stream_index
    }

    pub fn get_function_id(&self) -> Option<&String> {
        self.function_id.as_ref()
    }

    pub fn get_function_index(&self) -> Option<i32> {
        self.function_index
    }
}

/// Index value that means the last event. Same value as `constants::LAST_INDEX`.
pub const LAST_INDEX: i32 = -2;

// Possible Deref impls, if callers ever need direct access to the EventFluxElement fields.
// impl std::ops::Deref for Variable {
//     type Target = EventFluxElement;
//     fn deref(&self) -> &Self::Target { &self.eventflux_element }
// }
// impl std::ops::DerefMut for Variable {
//     fn deref_mut(&mut self) -> &mut Self::Target { &mut self.eventflux_element }
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_variable_creation() {
        let var = Variable::new("attr1".to_string());
        assert_eq!(var.get_attribute_name(), "attr1");
        assert_eq!(var.get_stream_id(), None);
        assert!(!var.is_inner_stream());
        assert_eq!(var.get_stream_index(), None);
        assert_eq!(var.get_function_id(), None);
        assert_eq!(var.get_function_index(), None);
        assert_eq!(var.eventflux_element.query_context_start_index, None);
    }

    #[test]
    fn test_variable_of_stream() {
        let var = Variable::new("attr2".to_string()).of_stream("StockStream".to_string());
        assert_eq!(var.get_attribute_name(), "attr2");
        assert_eq!(var.get_stream_id().unwrap(), "StockStream");
        assert!(!var.is_inner_stream());
    }

    #[test]
    fn test_variable_of_inner_stream_with_index() {
        let var = Variable::new("attr3".to_string())
            .of_inner_stream_with_index("TempStream".to_string(), 1);
        assert_eq!(var.get_attribute_name(), "attr3");
        assert_eq!(var.get_stream_id().unwrap(), "TempStream");
        assert!(var.is_inner_stream());
        assert_eq!(var.get_stream_index(), Some(1));
    }

    #[test]
    fn test_variable_of_function() {
        let var = Variable::new("countVal".to_string()).of_function("countEvents".to_string());
        assert_eq!(var.get_attribute_name(), "countVal");
        assert_eq!(var.get_function_id().unwrap(), "countEvents");
    }

    #[test]
    fn test_variable_of_function_with_index() {
        let var =
            Variable::new("val".to_string()).of_function_with_index("customFunc".to_string(), 0);
        assert_eq!(var.get_attribute_name(), "val");
        assert_eq!(var.get_function_id().unwrap(), "customFunc");
        assert_eq!(var.get_function_index(), Some(0));
    }
}
