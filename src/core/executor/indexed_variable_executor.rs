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

//! Executor for indexed variable access in pattern event collections: e[0].attr, e[last].attr
//!
//! This executor enables accessing specific events within an event collection matched by
//! count quantifiers in pattern queries. For example, with `e1=FailedLogin{3,5}`, you can:
//! - Access `e1[0].userId` - first failed login's userId
//! - Access `e1[last].timestamp` - last failed login's timestamp
//! - Access `e1[2].ipAddress` - third failed login's IP address

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::state::state_event::StateEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::core::util::eventflux_constants::{
    BEFORE_WINDOW_DATA_INDEX, ON_AFTER_WINDOW_DATA_INDEX, OUTPUT_DATA_INDEX,
    STATE_OUTPUT_DATA_INDEX,
};
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use crate::query_api::expression::indexed_variable::EventIndex;
use std::sync::Arc;

/// Executor for indexed variable access: e[0].attr, e[last].attr
///
/// # Architecture
///
/// This executor requires a `StateEvent` (not just `StreamEvent`) to function, because:
/// 1. Pattern queries build StateEvents that contain multiple stream positions
/// 2. Each position can have a collection of events (from count quantifiers)
/// 3. IndexedVariableExecutor accesses a specific event from a specific collection
///
/// # Execution Flow
///
/// ```text
/// 1. Downcast ComplexEvent to StateEvent (required)
/// 2. Get event chain at state_position (e1=0, e2=1, ...)
/// 3. Resolve index (0, 1, 2, ... or "last")
/// 4. Get event at resolved index from chain
/// 5. Extract attribute from that event
/// 6. Return value (or None if out of bounds)
/// ```
///
/// # Example Usage
///
/// ```ignore
/// // For pattern: e1=FailedLogin{3,5}
/// // SELECT e1[0].userId, e1[last].timestamp
///
/// let executor_first = IndexedVariableExecutor::new(
///     0, // state_position (e1 is at position 0)
///     EventIndex::Numeric(0), // e1[0]
///     [BEFORE_WINDOW_DATA_INDEX as i32, 0], // userId is at index 0
///     ApiAttributeType::STRING,
///     "userId".to_string(),
/// );
///
/// let executor_last = IndexedVariableExecutor::new(
///     0, // state_position (e1 is at position 0)
///     EventIndex::Last, // e1[last]
///     [BEFORE_WINDOW_DATA_INDEX as i32, 1], // timestamp is at index 1
///     ApiAttributeType::LONG,
///     "timestamp".to_string(),
/// );
/// ```
#[derive(Debug, Clone)]
pub struct IndexedVariableExecutor {
    /// Position in StateEvent.stream_events[] array (e1=0, e2=1, e3=2, ...)
    pub state_position: usize,

    /// Index into the event chain (0, 1, 2, ... or "last")
    pub index: EventIndex,

    /// Attribute position within event's data arrays
    /// [0] = data_type_index (BEFORE_WINDOW_DATA_INDEX, OUTPUT_DATA_INDEX, etc.)
    /// [1] = attribute_index (index within that data array)
    pub attribute_position: [i32; 2],

    /// Return type of the attribute
    pub return_type: ApiAttributeType,

    /// Attribute name for debugging purposes
    pub attribute_name_for_debug: String,
}

impl IndexedVariableExecutor {
    /// Create new indexed variable executor
    ///
    /// # Arguments
    /// * `state_position` - Position in StateEvent (e1=0, e2=1, ...)
    /// * `index` - Event index in collection (0, 1, 2, ... or Last)
    /// * `attribute_position` - [data_type_index, attribute_index]
    /// * `return_type` - Type of attribute
    /// * `attribute_name_for_debug` - Attribute name for debugging
    pub fn new(
        state_position: usize,
        index: EventIndex,
        attribute_position: [i32; 2],
        return_type: ApiAttributeType,
        attribute_name_for_debug: String,
    ) -> Self {
        Self {
            state_position,
            index,
            attribute_position,
            return_type,
            attribute_name_for_debug,
        }
    }

    /// Get the state position
    pub fn get_state_position(&self) -> usize {
        self.state_position
    }

    /// Get the event index
    pub fn get_index(&self) -> &EventIndex {
        &self.index
    }

    /// Get the attribute position
    pub fn get_attribute_position(&self) -> [i32; 2] {
        self.attribute_position
    }
}

impl ExpressionExecutor for IndexedVariableExecutor {
    fn execute(&self, event_opt: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let complex_event = event_opt?;

        // CRITICAL: Must be StateEvent for indexed array access
        // Pattern queries build StateEvents, while simple stream queries use StreamEvents
        let state_event = complex_event.as_any().downcast_ref::<StateEvent>()?;

        // Get event chain at the specified position (e1=0, e2=1, ...)
        let event_chain = state_event.get_event_chain(self.state_position);

        if event_chain.is_empty() {
            // No events at this position - count quantifier min not reached,
            // or this stream position has no events yet
            return None;
        }

        // Resolve index: numeric (0, 1, 2, ...) or "last"
        let resolved_index = match &self.index {
            EventIndex::Numeric(idx) => *idx,
            EventIndex::Last => {
                // "last" dynamically resolves to the last event in the chain
                event_chain.len().saturating_sub(1)
            }
        };

        // Get event at resolved index
        // Returns None if index is out of bounds (e.g., e[100] with only 3 events)
        let stream_event = event_chain.get(resolved_index)?;

        // Extract attribute from the stream event
        // Use the same logic as VariableExpressionExecutor for consistency
        let attr_idx = self.attribute_position[1] as usize;
        let attr = match self.attribute_position[0] as usize {
            BEFORE_WINDOW_DATA_INDEX => stream_event.before_window_data.get(attr_idx),
            OUTPUT_DATA_INDEX | STATE_OUTPUT_DATA_INDEX => stream_event
                .output_data
                .as_ref()
                .and_then(|v| v.get(attr_idx)),
            ON_AFTER_WINDOW_DATA_INDEX => stream_event.on_after_window_data.get(attr_idx),
            _ => None,
        }
        .cloned();

        if attr.is_some() {
            return attr;
        }

        // Fallback to output_data if attribute not found in specified section
        // This mirrors VariableExpressionExecutor behavior for consistency
        stream_event
            .output_data
            .as_ref()
            .and_then(|v| v.get(attr_idx))
            .cloned()
    }

    fn get_return_type(&self) -> ApiAttributeType {
        self.return_type
    }

    fn clone_executor(
        &self,
        _eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::state::state_event::StateEvent;
    use crate::core::event::stream::stream_event::StreamEvent;
    use crate::core::event::value::AttributeValue;
    use crate::query_api::expression::indexed_variable::EventIndex;

    /// Helper to create a StreamEvent with before_window_data
    fn create_stream_event_with_data(data: Vec<AttributeValue>) -> StreamEvent {
        let mut event = StreamEvent::new(0, data.len(), 0, 0);
        event.before_window_data = data;
        event
    }

    /// Helper to create a StateEvent with events at a specific position
    fn create_state_event_with_events(position: usize, events: Vec<StreamEvent>) -> StateEvent {
        let mut state_event = StateEvent::new(position + 1, 0); // stream_events_size, output_size
        for event in events {
            state_event.add_event(position, event);
        }
        state_event
    }

    #[test]
    fn test_indexed_access_first_event() {
        let executor = IndexedVariableExecutor::new(
            0,                      // e1 at position 0
            EventIndex::Numeric(0), // e[0]
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::STRING,
            "userId".to_string(),
        );

        let events = vec![
            create_stream_event_with_data(vec![AttributeValue::String("user1".to_string())]),
            create_stream_event_with_data(vec![AttributeValue::String("user2".to_string())]),
            create_stream_event_with_data(vec![AttributeValue::String("user3".to_string())]),
        ];

        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::String("user1".to_string())));
    }

    #[test]
    fn test_indexed_access_second_event() {
        let executor = IndexedVariableExecutor::new(
            0,
            EventIndex::Numeric(1), // e[1] - second event
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::STRING,
            "userId".to_string(),
        );

        let events = vec![
            create_stream_event_with_data(vec![AttributeValue::String("user1".to_string())]),
            create_stream_event_with_data(vec![AttributeValue::String("user2".to_string())]),
            create_stream_event_with_data(vec![AttributeValue::String("user3".to_string())]),
        ];

        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::String("user2".to_string())));
    }

    #[test]
    fn test_indexed_access_last_keyword() {
        let executor = IndexedVariableExecutor::new(
            0,
            EventIndex::Last, // e[last]
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::STRING,
            "userId".to_string(),
        );

        let events = vec![
            create_stream_event_with_data(vec![AttributeValue::String("user1".to_string())]),
            create_stream_event_with_data(vec![AttributeValue::String("user2".to_string())]),
            create_stream_event_with_data(vec![AttributeValue::String("user3".to_string())]),
        ];

        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::String("user3".to_string()))); // Last event
    }

    #[test]
    fn test_indexed_access_last_with_single_event() {
        let executor = IndexedVariableExecutor::new(
            0,
            EventIndex::Last,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::INT,
            "value".to_string(),
        );

        let events = vec![create_stream_event_with_data(vec![AttributeValue::Int(42)])];

        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Int(42))); // Only event is also last
    }

    #[test]
    fn test_indexed_access_out_of_bounds() {
        let executor = IndexedVariableExecutor::new(
            0,
            EventIndex::Numeric(100), // Way out of bounds
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::STRING,
            "userId".to_string(),
        );

        let events = vec![create_stream_event_with_data(vec![AttributeValue::String(
            "user1".to_string(),
        )])];

        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, None); // Out of bounds returns None
    }

    #[test]
    fn test_indexed_access_empty_event_chain() {
        let executor = IndexedVariableExecutor::new(
            0,
            EventIndex::Numeric(0),
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::STRING,
            "userId".to_string(),
        );

        let state_event = StateEvent::new(1, 0); // stream_events_size=1, output_size=0, no events added

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, None); // Empty chain returns None
    }

    #[test]
    fn test_indexed_access_different_attribute_types() {
        // Test INT
        let executor_int = IndexedVariableExecutor::new(
            0,
            EventIndex::Numeric(0),
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::INT,
            "count".to_string(),
        );

        let events_int = vec![create_stream_event_with_data(vec![AttributeValue::Int(42)])];
        let state_event_int = create_state_event_with_events(0, events_int);
        let result_int = executor_int.execute(Some(&state_event_int as &dyn ComplexEvent));
        assert_eq!(result_int, Some(AttributeValue::Int(42)));

        // Test LONG
        let executor_long = IndexedVariableExecutor::new(
            0,
            EventIndex::Numeric(0),
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::LONG,
            "timestamp".to_string(),
        );

        let events_long = vec![create_stream_event_with_data(vec![AttributeValue::Long(
            1000,
        )])];
        let state_event_long = create_state_event_with_events(0, events_long);
        let result_long = executor_long.execute(Some(&state_event_long as &dyn ComplexEvent));
        assert_eq!(result_long, Some(AttributeValue::Long(1000)));

        // Test DOUBLE
        let executor_double = IndexedVariableExecutor::new(
            0,
            EventIndex::Numeric(0),
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
            "price".to_string(),
        );

        let events_double = vec![create_stream_event_with_data(vec![AttributeValue::Double(
            99.99,
        )])];
        let state_event_double = create_state_event_with_events(0, events_double);
        let result_double = executor_double.execute(Some(&state_event_double as &dyn ComplexEvent));
        assert_eq!(result_double, Some(AttributeValue::Double(99.99)));
    }

    #[test]
    fn test_indexed_access_multiple_positions() {
        // Test e1[0] and e2[1] in same StateEvent
        let executor_e1 = IndexedVariableExecutor::new(
            0, // e1 at position 0
            EventIndex::Numeric(0),
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::STRING,
            "userId".to_string(),
        );

        let executor_e2 = IndexedVariableExecutor::new(
            1, // e2 at position 1
            EventIndex::Numeric(1),
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::STRING,
            "action".to_string(),
        );

        let mut state_event = StateEvent::new(2, 0); // stream_events_size=2, output_size=0

        // Add events to position 0 (e1)
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::String("user1".to_string())]),
        );

        // Add events to position 1 (e2)
        state_event.add_event(
            1,
            create_stream_event_with_data(vec![AttributeValue::String("login".to_string())]),
        );
        state_event.add_event(
            1,
            create_stream_event_with_data(vec![AttributeValue::String("access".to_string())]),
        );

        let result_e1 = executor_e1.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result_e1, Some(AttributeValue::String("user1".to_string())));

        let result_e2 = executor_e2.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(
            result_e2,
            Some(AttributeValue::String("access".to_string()))
        ); // e2[1]
    }

    #[test]
    fn test_indexed_access_fallback_to_output_data() {
        let executor = IndexedVariableExecutor::new(
            0,
            EventIndex::Numeric(0),
            [OUTPUT_DATA_INDEX as i32, 0], // Looking in output_data
            ApiAttributeType::STRING,
            "result".to_string(),
        );

        let mut event = StreamEvent::new(0, 0, 0, 1);
        event.output_data = Some(vec![AttributeValue::String("output_val".to_string())]);

        let state_event = create_state_event_with_events(0, vec![event]);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(
            result,
            Some(AttributeValue::String("output_val".to_string()))
        );
    }

    #[test]
    fn test_indexed_access_returns_none_for_non_state_event() {
        let executor = IndexedVariableExecutor::new(
            0,
            EventIndex::Numeric(0),
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::STRING,
            "userId".to_string(),
        );

        // Pass a StreamEvent instead of StateEvent
        let stream_event =
            create_stream_event_with_data(vec![AttributeValue::String("user1".to_string())]);

        let result = executor.execute(Some(&stream_event as &dyn ComplexEvent));
        assert_eq!(result, None); // Should return None because it's not a StateEvent
    }

    #[test]
    fn test_indexed_access_no_event() {
        let executor = IndexedVariableExecutor::new(
            0,
            EventIndex::Numeric(0),
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::STRING,
            "userId".to_string(),
        );

        let result = executor.execute(None);
        assert_eq!(result, None); // None event should return None
    }

    #[test]
    fn test_clone_executor() {
        let executor = IndexedVariableExecutor::new(
            0,
            EventIndex::Last,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::STRING,
            "userId".to_string(),
        );

        let app_ctx = Arc::new(EventFluxAppContext::default_for_testing());
        let cloned_exec = executor.clone_executor(&app_ctx);

        assert_eq!(cloned_exec.get_return_type(), ApiAttributeType::STRING);

        // Test that cloned executor works
        let events = vec![
            create_stream_event_with_data(vec![AttributeValue::String("user1".to_string())]),
            create_stream_event_with_data(vec![AttributeValue::String("user2".to_string())]),
        ];
        let state_event = create_state_event_with_events(0, events);

        let result = cloned_exec.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::String("user2".to_string()))); // Last event
    }
}
