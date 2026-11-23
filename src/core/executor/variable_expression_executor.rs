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

use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::state::state_event::StateEvent;
use crate::core::event::stream::stream_event::StreamEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::core::util::eventflux_constants::{
    BEFORE_WINDOW_DATA_INDEX, ON_AFTER_WINDOW_DATA_INDEX, OUTPUT_DATA_INDEX,
    STATE_OUTPUT_DATA_INDEX, STREAM_ATTRIBUTE_INDEX_IN_TYPE, STREAM_ATTRIBUTE_TYPE_INDEX,
    STREAM_EVENT_CHAIN_INDEX, STREAM_EVENT_INDEX_IN_CHAIN,
};
use crate::query_api::definition::attribute::Type as ApiAttributeType;

/// Reads a variable (attribute) value from an event.
///
/// For a StreamEvent it reads the section and index from `position`, falling back to
/// `output_data`. For a StateEvent it uses `StateEvent::get_attribute`. For any other event
/// it reads `get_output_data` at the attribute index. No event gives `None`.
#[derive(Debug, Clone)]
pub struct VariableExpressionExecutor {
    /// EventFlux position array that locates the attribute in a `ComplexEvent`.
    ///
    /// `position[STREAM_EVENT_CHAIN_INDEX]`  - index of the stream event in a `StateEvent` chain.
    /// `position[STREAM_EVENT_INDEX_IN_CHAIN]` - index within the chain when the stream has
    ///   several events. Only used for StateEvents.
    /// `position[STREAM_ATTRIBUTE_TYPE_INDEX]` - which data section to read (before window,
    ///   output, and so on).
    /// `position[STREAM_ATTRIBUTE_INDEX_IN_TYPE]` - attribute index within that section.
    pub position: [i32; 4],
    pub return_type: ApiAttributeType,
    pub attribute_name_for_debug: String,
}

impl VariableExpressionExecutor {
    pub fn new(
        position: [i32; 4],
        return_type: ApiAttributeType,
        attribute_name_for_debug: String,
    ) -> Self {
        Self {
            position,
            return_type,
            attribute_name_for_debug,
        }
    }

    /// Returns the current EventFlux position array for this variable.
    pub fn get_position(&self) -> [i32; 4] {
        self.position
    }

    /// Updates the position. As in the Java version, `position` is either two elements
    /// (attribute type and index) for simple stream queries, or all four for state events
    /// and table lookups. A slice of any other length is ignored.
    pub fn set_position(&mut self, position: &[i32]) {
        if position.len() == 2 {
            self.position[STREAM_ATTRIBUTE_TYPE_INDEX] = position[0];
            self.position[STREAM_ATTRIBUTE_INDEX_IN_TYPE] = position[1];
        } else if position.len() == 4 {
            self.position[STREAM_EVENT_CHAIN_INDEX] = position[0];
            self.position[STREAM_EVENT_INDEX_IN_CHAIN] = position[1];
            self.position[STREAM_ATTRIBUTE_TYPE_INDEX] = position[2];
            self.position[STREAM_ATTRIBUTE_INDEX_IN_TYPE] = position[3];
        }
    }

    /// Reads the attribute from a plain row of values instead of a [`ComplexEvent`]. Used
    /// for table lookups. Only the attribute index is used.
    pub fn execute_on_row(&self, values: &[AttributeValue]) -> Option<AttributeValue> {
        values
            .get(self.position[STREAM_ATTRIBUTE_INDEX_IN_TYPE] as usize)
            .cloned()
    }
}

impl ExpressionExecutor for VariableExpressionExecutor {
    fn execute(&self, event_opt: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let complex_event = event_opt?;

        if let Some(stream_event) = complex_event.as_any().downcast_ref::<StreamEvent>() {
            let idx = self.position[STREAM_ATTRIBUTE_INDEX_IN_TYPE] as usize;
            let attr = match self.position[STREAM_ATTRIBUTE_TYPE_INDEX] as usize {
                BEFORE_WINDOW_DATA_INDEX => stream_event.before_window_data.get(idx),
                OUTPUT_DATA_INDEX | STATE_OUTPUT_DATA_INDEX => {
                    stream_event.output_data.as_ref().and_then(|v| v.get(idx))
                }
                ON_AFTER_WINDOW_DATA_INDEX => stream_event.on_after_window_data.get(idx),
                _ => None,
            }
            .cloned();

            if attr.is_some() {
                return attr;
            }

            // The parser's MetaStreamEvent can put all attributes in `output_data` while the
            // position points at `before_window_data`. So fall back to output_data when the
            // value is not in the requested section.
            return stream_event
                .output_data
                .as_ref()
                .and_then(|v| v.get(idx))
                .cloned();
        }

        if let Some(state_event) = complex_event.as_any().downcast_ref::<StateEvent>() {
            return state_event.get_attribute(&self.position).cloned();
        }

        // Any other event type. Use whatever it exposes through `get_output_data`.
        complex_event
            .get_output_data()
            .and_then(|d| d.get(self.position[STREAM_ATTRIBUTE_INDEX_IN_TYPE] as usize))
            .cloned()
    }

    fn get_return_type(&self) -> ApiAttributeType {
        self.return_type
    }

    fn clone_executor(
        &self,
        _eventflux_app_context: &std::sync::Arc<
            crate::core::config::eventflux_app_context::EventFluxAppContext,
        >,
    ) -> Box<dyn ExpressionExecutor> {
        // Holds no context, so a plain clone is enough. If it stored one, it would copy it:
        // eventflux_app_context: self.eventflux_app_context.as_ref().map(Arc::clone),
        Box::new(self.clone())
    }

    /// Always true here. Used for validation.
    fn is_variable_executor(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::complex_event::{ComplexEvent, ComplexEventType};
    use crate::core::event::stream::stream_event::StreamEvent; // A concrete ComplexEvent
    use crate::core::event::value::AttributeValue;
    use crate::core::util::eventflux_constants::{
        BEFORE_WINDOW_DATA_INDEX, STREAM_ATTRIBUTE_INDEX_IN_TYPE,
    };
    // ApiAttributeType comes from the parent module through super::*
    use crate::core::config::eventflux_app_context::EventFluxAppContext;
    use std::sync::Arc;

    // StreamEvent with only output_data set
    fn create_test_stream_event_with_output_data(
        output_data_vec: Vec<AttributeValue>,
    ) -> StreamEvent {
        let mut se = StreamEvent::new(0, 0, 0, output_data_vec.len()); // ts, before, onafter, output_len
        se.output_data = Some(output_data_vec);
        se
    }

    // StreamEvent with only before_window_data set
    fn create_test_stream_event_with_before_window_data(
        data_array: Vec<AttributeValue>,
    ) -> StreamEvent {
        let mut se = StreamEvent::new(0, data_array.len(), 0, 0); // timestamp, before_len, on_after_len, output_len
        se.before_window_data = data_array;
        se
    }

    #[test]
    fn test_variable_access_from_before_window_data_string() {
        let exec = VariableExpressionExecutor::new(
            [0, 0, BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::STRING,
            "test_attr".to_string(),
        );
        let event_data = vec![AttributeValue::String("val1".to_string())];
        let test_event = create_test_stream_event_with_before_window_data(event_data);

        let result = exec.execute(Some(&test_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::String("val1".to_string())));
    }

    #[test]
    fn test_variable_access_from_before_window_data_int_out_of_bounds() {
        let exec = VariableExpressionExecutor::new(
            [0, 0, BEFORE_WINDOW_DATA_INDEX as i32, 1],
            ApiAttributeType::INT,
            "test_attr_int".to_string(),
        );
        let event_data = vec![AttributeValue::Int(123)]; // Only one element, at index 0
        let test_event = create_test_stream_event_with_before_window_data(event_data);

        let result = exec.execute(Some(&test_event as &dyn ComplexEvent));
        // Index 1 is out of bounds in before_window_data, and output_data has no values, so
        // the fallback finds nothing either
        assert_eq!(result, None);
    }

    #[test]
    fn test_variable_access_output_data_for_non_stream_event_fallback() {
        // Checks the last branch, for events that are neither StreamEvent nor StateEvent.
        // Those read get_output_data(). A StreamEvent cannot reach that branch, so this uses
        // a small mock event that only has output_data.

        #[derive(Debug)]
        struct MockComplexEvent {
            output_data: Option<Vec<AttributeValue>>,
            next: Option<Box<dyn ComplexEvent>>,
            event_type: ComplexEventType,
        }
        impl ComplexEvent for MockComplexEvent {
            fn get_next(&self) -> Option<&dyn ComplexEvent> {
                self.next.as_deref()
            }
            fn set_next(
                &mut self,
                next_event: Option<Box<dyn ComplexEvent>>,
            ) -> Option<Box<dyn ComplexEvent>> {
                std::mem::replace(&mut self.next, next_event)
            }
            fn mut_next_ref_option(&mut self) -> &mut Option<Box<dyn ComplexEvent>> {
                &mut self.next
            }

            fn get_output_data(&self) -> Option<&[AttributeValue]> {
                self.output_data.as_deref()
            }
            fn set_output_data(&mut self, data: Option<Vec<AttributeValue>>) {
                self.output_data = data;
            }

            fn get_timestamp(&self) -> i64 {
                0
            }
            fn set_timestamp(&mut self, _timestamp: i64) {}

            fn get_event_type(&self) -> ComplexEventType {
                self.event_type
            }
            fn set_event_type(&mut self, event_type: ComplexEventType) {
                self.event_type = event_type;
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
                self
            }
        }

        let exec = VariableExpressionExecutor::new(
            [0, 0, BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::STRING,
            "fallback_attr".to_string(),
        );
        let mock_event_data = vec![AttributeValue::String("fallback_val".to_string())];
        let mock_event = MockComplexEvent {
            output_data: Some(mock_event_data),
            next: None,
            event_type: ComplexEventType::Current,
        };

        let result = exec.execute(Some(&mock_event as &dyn ComplexEvent));
        assert_eq!(
            result,
            Some(AttributeValue::String("fallback_val".to_string()))
        );
    }

    #[test]
    fn test_variable_access_no_event() {
        let exec = VariableExpressionExecutor::new(
            [0, 0, BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::STRING,
            "test_attr".to_string(),
        );
        let result = exec.execute(None);
        assert_eq!(result, None);
    }

    #[test]
    fn test_variable_clone() {
        let exec = VariableExpressionExecutor::new(
            [0, 0, BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::STRING,
            "test_attr_clone".to_string(),
        );
        let app_ctx_placeholder = Arc::new(EventFluxAppContext::default_for_testing());
        let cloned_exec = exec.clone_executor(&app_ctx_placeholder);

        assert_eq!(cloned_exec.get_return_type(), ApiAttributeType::STRING);

        let event_data_before = vec![AttributeValue::String("clone_val_before".to_string())];
        let test_event_before = create_test_stream_event_with_before_window_data(event_data_before);
        let result_before = cloned_exec.execute(Some(&test_event_before as &dyn ComplexEvent));
        assert_eq!(
            result_before,
            Some(AttributeValue::String("clone_val_before".to_string()))
        );
    }

    #[test]
    fn test_set_position_and_execute_on_row() {
        let mut exec = VariableExpressionExecutor::new(
            [0, 0, BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::INT,
            "row_attr".to_string(),
        );
        exec.set_position(&[OUTPUT_DATA_INDEX as i32, 1]);
        let row = vec![AttributeValue::Int(10), AttributeValue::Int(20)];
        let val = exec.execute_on_row(&row);
        assert_eq!(val, Some(AttributeValue::Int(20)));
        assert_eq!(exec.get_position()[STREAM_ATTRIBUTE_INDEX_IN_TYPE], 1);
    }

    #[test]
    fn test_state_output_index_on_stream_event() {
        let exec = VariableExpressionExecutor::new(
            [0, 0, STATE_OUTPUT_DATA_INDEX as i32, 0],
            ApiAttributeType::INT,
            "out".to_string(),
        );
        let se = create_test_stream_event_with_output_data(vec![AttributeValue::Int(5)]);
        let val = exec.execute(Some(&se as &dyn ComplexEvent));
        assert_eq!(val, Some(AttributeValue::Int(5)));
    }
}
