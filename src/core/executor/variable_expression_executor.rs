// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/variable_expression_executor.rs
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

/// Executor that retrieves a variable's value from an event. Current impl is simplified.
#[derive(Debug, Clone)]
pub struct VariableExpressionExecutor {
    /// EventFlux position array for locating the attribute within a `ComplexEvent`.
    ///
    /// `position[STREAM_EVENT_CHAIN_INDEX]`  - index of the stream event in a `StateEvent` chain.
    /// `position[STREAM_EVENT_INDEX_IN_CHAIN]` - index within the chain if the stream has multiple
    ///   events (not commonly used in this simplified implementation).
    /// `position[STREAM_ATTRIBUTE_TYPE_INDEX]` - which data section to access (before window,
    ///   output, etc.).
    /// `position[STREAM_ATTRIBUTE_INDEX_IN_TYPE]` - attribute index within the selected section.
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

    /// Return the current EventFlux position array describing this variable.
    pub fn get_position(&self) -> [i32; 4] {
        self.position
    }

    /// Update the executor's position information.  This mirrors the behaviour
    /// of the Java implementation where a position can be specified either as a
    /// two element array (attribute type and index) for simple stream queries or
    /// a full four element array for state events and table lookups.
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

    /// Convenience method used when performing table lookups where the data is
    /// provided as a plain attribute array rather than wrapped in a
    /// [`ComplexEvent`].
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

            // MetaStreamEvent in the simplified parser may place all attributes
            // in `output_data` while positions point at `before_window_data`.
            // Fall back to output_data when the requested section is empty.
            return stream_event
                .output_data
                .as_ref()
                .and_then(|v| v.get(idx))
                .cloned();
        }

        if let Some(state_event) = complex_event.as_any().downcast_ref::<StateEvent>() {
            return state_event.get_attribute(&self.position).cloned();
        }

        // Fallback to whatever data the ComplexEvent exposes via `get_output_data`.
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
        // If eventflux_app_context was stored on self, it would be cloned:
        // eventflux_app_context: self.eventflux_app_context.as_ref().map(Arc::clone),
        Box::new(self.clone())
    }

    /// Returns true for VariableExpressionExecutor - used for validation
    fn is_variable_executor(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::complex_event::{ComplexEvent, ComplexEventType};
    use crate::core::event::stream::stream_event::StreamEvent; // Using StreamEvent as a concrete ComplexEvent
    use crate::core::event::value::AttributeValue;
    use crate::core::util::eventflux_constants::{
        BEFORE_WINDOW_DATA_INDEX, STREAM_ATTRIBUTE_INDEX_IN_TYPE,
    };
    // ApiAttributeType is imported in the outer scope
    use crate::core::config::eventflux_app_context::EventFluxAppContext;
    use std::sync::Arc;

    // Mock ComplexEvent or use StreamEvent for testing
    // This StreamEvent has its output_data set for testing VEE.
    fn create_test_stream_event_with_output_data(
        output_data_vec: Vec<AttributeValue>,
    ) -> StreamEvent {
        let mut se = StreamEvent::new(0, 0, 0, output_data_vec.len()); // ts, before, onafter, output_len
        se.output_data = Some(output_data_vec);
        se
    }

    // New helper for testing VEE with before_window_data
    fn create_test_stream_event_with_before_window_data(
        data_array: Vec<AttributeValue>,
    ) -> StreamEvent {
        let mut se = StreamEvent::new(0, data_array.len(), 0, 0); // timestamp, before_len, on_after_len, output_len
        se.before_window_data = data_array;
        se
    }

    #[test]
    fn test_variable_access_from_before_window_data_string() {
        // Renamed and adapted
        let exec = VariableExpressionExecutor::new(
            [0, 0, BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::STRING,
            "test_attr".to_string(),
        );
        let event_data = vec![AttributeValue::String("val1".to_string())];
        // Use the new helper that populates before_window_data
        let test_event = create_test_stream_event_with_before_window_data(event_data);

        let result = exec.execute(Some(&test_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::String("val1".to_string())));
    }

    #[test]
    fn test_variable_access_from_before_window_data_int_out_of_bounds() {
        // Renamed and adapted
        let exec = VariableExpressionExecutor::new(
            [0, 0, BEFORE_WINDOW_DATA_INDEX as i32, 1],
            ApiAttributeType::INT,
            "test_attr_int".to_string(),
        );
        let event_data = vec![AttributeValue::Int(123)]; // Only one element at index 0
                                                         // Use the new helper that populates before_window_data
        let test_event = create_test_stream_event_with_before_window_data(event_data);

        let result = exec.execute(Some(&test_event as &dyn ComplexEvent));
        // Now expects None because it only checks before_window_data for StreamEvent
        // and the index is out of bounds for that array.
        assert_eq!(result, None);
    }

    #[test]
    fn test_variable_access_output_data_for_non_stream_event_fallback() {
        // This test checks the fallback behavior for non-StreamEvent types (or failed downcast)
        // For this, we need a mock ComplexEvent that is not a StreamEvent but has output_data.
        // Or, we assume that if it's not a StreamEvent, it uses get_output_data().
        // The current code will use get_output_data() if the downcast to StreamEvent fails.
        // We can simulate this by using a StreamEvent but relying on the VEE to *not* find it
        // in before_window_data if we make before_window_data empty, and then it would hit the else branch.
        // However, the modified VEE execute for StreamEvent now returns None if not in before_window_data.
        // So, to test the `else` branch of `if let Some(stream_event) = ...`, we need a non-StreamEvent.
        // Let's create a simple mock for that.

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
        // This test needs to be re-evaluated based on new execute logic
        let exec = VariableExpressionExecutor::new(
            [0, 0, BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::STRING,
            "test_attr_clone".to_string(),
        );
        let app_ctx_placeholder = Arc::new(EventFluxAppContext::default_for_testing());
        let cloned_exec = exec.clone_executor(&app_ctx_placeholder);

        assert_eq!(cloned_exec.get_return_type(), ApiAttributeType::STRING);

        // Test with before_window_data
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
