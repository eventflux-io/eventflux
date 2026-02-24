// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/processor/stream/window/changelog_helpers.rs

//! Common helpers for applying changelog operations to window state holders
//!
//! This module provides shared functionality for applying state operations
//! across different window types, reducing code duplication.

use crate::core::event::stream::stream_event::StreamEvent;
use crate::core::persistence::state_holder::{StateError, StateOperation};
use std::sync::Arc;

/// Check if two stream events match based on timestamp and data
///
/// This is the standard matching logic used across all window types
/// to identify events during delete and update operations.
#[inline]
fn events_match(event: &StreamEvent, target: &StreamEvent) -> bool {
    event.timestamp == target.timestamp && event.before_window_data == target.before_window_data
}

/// Apply a StateOperation to a simple window with a VecDeque buffer
///
/// This is a complete implementation for simple windows (length, time, external_time)
/// that use a single VecDeque<Arc<StreamEvent>> buffer.
pub fn apply_operation_to_simple_window(
    buffer: &mut std::collections::VecDeque<Arc<StreamEvent>>,
    operation: &StateOperation,
    deserialize_event: &dyn Fn(&[u8]) -> Result<StreamEvent, StateError>,
) -> Result<(), StateError> {
    match operation {
        StateOperation::Insert { key: _, value } => {
            // We need to clone the closure context, so use a wrapper
            match deserialize_event(value) {
                Ok(event) => {
                    buffer.push_back(Arc::new(event));
                    Ok(())
                }
                Err(e) => {
                    log::warn!(
                        "Failed to deserialize event during changelog apply: {:?}",
                        e
                    );
                    Err(e)
                }
            }
        }

        StateOperation::Delete { key: _, old_value } => {
            if let Ok(event_to_remove) = deserialize_event(old_value) {
                buffer.retain(|e| !events_match(e, &event_to_remove));
            }
            Ok(())
        }

        StateOperation::Update {
            key: _,
            old_value,
            new_value,
        } => {
            // Remove old event
            if let Ok(old_event) = deserialize_event(old_value) {
                buffer.retain(|e| !events_match(e, &old_event));
            }

            // Insert new event
            if let Ok(new_event) = deserialize_event(new_value) {
                buffer.push_back(Arc::new(new_event));
            }

            Ok(())
        }

        StateOperation::Clear => {
            buffer.clear();
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::value::AttributeValue;
    use std::collections::VecDeque;

    fn create_test_event(timestamp: i64, value: i32) -> StreamEvent {
        let mut event = StreamEvent::new(timestamp, 1, 0, 0);
        event.before_window_data = vec![AttributeValue::Int(value)];
        event
    }

    #[test]
    fn test_events_match() {
        let event1 = create_test_event(100, 42);
        let event2 = create_test_event(100, 42);
        let event3 = create_test_event(100, 43);
        let event4 = create_test_event(101, 42);

        assert!(events_match(&event1, &event2));
        assert!(!events_match(&event1, &event3));
        assert!(!events_match(&event1, &event4));
    }

    #[test]
    fn test_apply_operation_to_simple_window_insert() {
        let mut buffer = VecDeque::new();
        let event = create_test_event(100, 42);
        let serialized = bincode::serialize(&event).unwrap();

        let deserialize_fn = |data: &[u8]| {
            bincode::deserialize::<StreamEvent>(data).map_err(|_| {
                StateError::DeserializationError {
                    message: "Failed".to_string(),
                }
            })
        };

        let op = StateOperation::Insert {
            key: vec![],
            value: serialized,
        };

        apply_operation_to_simple_window(&mut buffer, &op, &deserialize_fn).unwrap();

        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0].timestamp, 100);
    }

    #[test]
    fn test_apply_operation_to_simple_window_delete() {
        let mut buffer = VecDeque::new();
        let event = create_test_event(100, 42);
        buffer.push_back(Arc::new(event.clone()));

        let serialized = bincode::serialize(&event).unwrap();

        let deserialize_fn = |data: &[u8]| {
            bincode::deserialize::<StreamEvent>(data).map_err(|_| {
                StateError::DeserializationError {
                    message: "Failed".to_string(),
                }
            })
        };

        let op = StateOperation::Delete {
            key: vec![],
            old_value: serialized,
        };

        apply_operation_to_simple_window(&mut buffer, &op, &deserialize_fn).unwrap();

        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_apply_operation_to_simple_window_clear() {
        let mut buffer = VecDeque::new();
        buffer.push_back(Arc::new(create_test_event(100, 42)));
        buffer.push_back(Arc::new(create_test_event(101, 43)));

        let deserialize_fn = |_data: &[u8]| {
            Err(StateError::DeserializationError {
                message: "Should not be called".to_string(),
            })
        };

        let op = StateOperation::Clear;

        apply_operation_to_simple_window(&mut buffer, &op, &deserialize_fn).unwrap();

        assert_eq!(buffer.len(), 0);
    }
}
