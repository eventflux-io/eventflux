// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/event/state/state_event_cloner.rs
// Simplified port of io.eventflux.core.event.state.StateEventCloner
use super::{
    meta_state_event::MetaStateEvent, state_event::StateEvent,
    state_event_factory::StateEventFactory,
};

#[derive(Debug, Clone)]
pub struct StateEventCloner {
    stream_event_size: usize,
    output_data_size: usize,
    state_event_factory: StateEventFactory,
}

impl StateEventCloner {
    pub fn new(meta: &MetaStateEvent, factory: StateEventFactory) -> Self {
        Self {
            stream_event_size: meta.stream_event_count(),
            output_data_size: meta.get_output_data_attributes().len(),
            state_event_factory: factory,
        }
    }

    /// Create a cloner directly from a StateEvent (for pattern processing)
    /// This is useful when you don't have MetaStateEvent available
    pub fn from_state_event(event: &StateEvent) -> Self {
        let stream_event_size = event.stream_events.len();
        let output_data_size = event.output_data.as_ref().map_or(0, |v| v.len());

        let factory = StateEventFactory::new(stream_event_size, output_data_size);

        Self {
            stream_event_size,
            output_data_size,
            state_event_factory: factory,
        }
    }

    pub fn copy_state_event(&self, state_event: &StateEvent) -> StateEvent {
        let mut new_event = self.state_event_factory.new_instance();
        if self.output_data_size > 0 {
            if let (Some(src), Some(dest)) = (&state_event.output_data, &mut new_event.output_data)
            {
                for i in 0..self.output_data_size {
                    dest[i] = src[i].clone();
                }
            }
        }
        if self.stream_event_size > 0 {
            for i in 0..self.stream_event_size {
                // This clone() will recursively clone event chains via StreamEvent's Clone impl
                new_event.stream_events[i] = state_event.stream_events[i].clone();
            }
        }
        new_event.event_type = state_event.event_type;
        new_event.timestamp = state_event.timestamp;
        new_event.id = state_event.id;
        new_event
    }

    /// Clone StateEvent for 'every' pattern (creates NEW ID, clears future positions)
    ///
    /// **Purpose**: 'every' patterns need to restart matching with a fresh StateEvent.
    /// This means:
    /// 1. Generate NEW ID (not preserve original ID)
    /// 2. Clear positions >= clear_from_position (reset future matching)
    ///
    /// **Example**:
    /// ```text
    /// Pattern: every (A -> B -> C)
    /// After A -> B -> C completes, restart with:
    /// - NEW StateEvent ID (different from previous match)
    /// - Keep A (position 0)
    /// - Clear B and C (positions 1, 2)
    /// ```
    ///
    /// StateEventFactory generates new IDs for each cloned 'every' pattern event
    pub fn copy_state_event_for_every(
        &self,
        state_event: &StateEvent,
        clear_from_position: usize,
    ) -> StateEvent {
        let mut new_event = self.copy_state_event(state_event);

        // Generate NEW ID for 'every' (don't preserve original)
        new_event.id = StateEvent::next_id();

        // Clear positions >= clear_from_position
        for i in clear_from_position..self.stream_event_size {
            new_event.stream_events[i] = None;
        }

        new_event
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::complex_event::{ComplexEvent, ComplexEventType};
    use crate::core::event::stream::StreamEvent;
    use crate::core::event::value::AttributeValue;

    // ===== StateEventCloner Tests =====

    #[test]
    fn test_clone_empty_state() {
        let state = StateEvent::new(2, 3);
        let cloner = StateEventCloner::from_state_event(&state);
        let cloned = cloner.copy_state_event(&state);

        assert_eq!(cloned.stream_events.len(), 2);
        assert_eq!(cloned.output_data.as_ref().unwrap().len(), 3);
        assert_eq!(cloned.timestamp, state.timestamp);
    }

    #[test]
    fn test_clone_with_events() {
        let mut state = StateEvent::new(2, 0);
        let stream_event = StreamEvent::new(1000, 1, 0, 0);
        state.set_event(0, stream_event);

        let cloner = StateEventCloner::from_state_event(&state);
        let cloned = cloner.copy_state_event(&state);

        assert!(cloned.get_stream_event(0).is_some());
        assert!(cloned.get_stream_event(1).is_none());
    }

    #[test]
    fn test_clone_with_chains() {
        let mut state = StateEvent::new(1, 0);

        // Add a chain of 3 events
        let event1 = StreamEvent::new(1000, 1, 0, 0);
        let event2 = StreamEvent::new(2000, 1, 0, 0);
        let event3 = StreamEvent::new(3000, 1, 0, 0);

        state.add_event(0, event1);
        state.add_event(0, event2);
        state.add_event(0, event3);

        assert_eq!(state.count_events_at(0), 3);

        let cloner = StateEventCloner::from_state_event(&state);
        let cloned = cloner.copy_state_event(&state);

        // Verify the chain was cloned
        assert_eq!(cloned.count_events_at(0), 3);

        let original_chain = state.get_event_chain(0);
        let cloned_chain = cloned.get_event_chain(0);

        assert_eq!(original_chain.len(), cloned_chain.len());
        for i in 0..original_chain.len() {
            assert_eq!(original_chain[i].timestamp, cloned_chain[i].timestamp);
        }
    }

    #[test]
    fn test_deep_clone_independence() {
        let mut state = StateEvent::new(1, 0);

        // Add events with data
        let mut event1 = StreamEvent::new(1000, 1, 0, 0);
        event1.before_window_data[0] = AttributeValue::Int(100);
        state.set_event(0, event1);

        let cloner = StateEventCloner::from_state_event(&state);
        let mut cloned = cloner.copy_state_event(&state);

        // Modify the cloned event directly through public field
        if let Some(Some(cloned_event)) = cloned.stream_events.get_mut(0) {
            cloned_event.before_window_data[0] = AttributeValue::Int(999);
        }

        // Verify original is unchanged
        if let Some(original_event) = state.get_stream_event(0) {
            assert_eq!(
                original_event.before_window_data[0],
                AttributeValue::Int(100)
            );
        }

        // Verify clone was modified
        if let Some(cloned_event) = cloned.get_stream_event(0) {
            assert_eq!(cloned_event.before_window_data[0], AttributeValue::Int(999));
        }
    }

    #[test]
    fn test_clone_preserves_timestamp() {
        let mut state = StateEvent::new(1, 0);
        state.set_timestamp(99999);

        let cloner = StateEventCloner::from_state_event(&state);
        let cloned = cloner.copy_state_event(&state);

        assert_eq!(cloned.get_timestamp(), 99999);
    }

    #[test]
    fn test_clone_preserves_type() {
        let mut state = StateEvent::new(1, 0);
        state.set_event_type(ComplexEventType::Expired);

        let cloner = StateEventCloner::from_state_event(&state);
        let cloned = cloner.copy_state_event(&state);

        assert_eq!(cloned.get_event_type(), ComplexEventType::Expired);
    }

    #[test]
    fn test_clone_output_data() {
        let mut state = StateEvent::new(1, 3);

        // Set some output data
        if let Some(output) = state.output_data.as_mut() {
            output[0] = AttributeValue::Int(10);
            output[1] = AttributeValue::String("test".to_string());
            output[2] = AttributeValue::Float(3.14);
        }

        let cloner = StateEventCloner::from_state_event(&state);
        let cloned = cloner.copy_state_event(&state);

        // Verify output data was cloned
        assert!(cloned.output_data.is_some());
        if let Some(cloned_output) = cloned.output_data.as_ref() {
            assert_eq!(cloned_output[0], AttributeValue::Int(10));
            assert_eq!(cloned_output[1], AttributeValue::String("test".to_string()));
            assert_eq!(cloned_output[2], AttributeValue::Float(3.14));
        }
    }

    #[test]
    fn test_clone_chain_independence() {
        let mut state = StateEvent::new(1, 0);

        // Build a chain of 2 events
        let mut event1 = StreamEvent::new(1000, 1, 0, 0);
        event1.before_window_data[0] = AttributeValue::Int(100);
        let mut event2 = StreamEvent::new(2000, 1, 0, 0);
        event2.before_window_data[0] = AttributeValue::Int(200);

        state.add_event(0, event1);
        state.add_event(0, event2);

        let cloner = StateEventCloner::from_state_event(&state);
        let mut cloned = cloner.copy_state_event(&state);

        // Modify cloned chain
        cloned.remove_last_event(0);

        // Original should still have 2 events
        assert_eq!(state.count_events_at(0), 2);
        // Clone should have 1 event
        assert_eq!(cloned.count_events_at(0), 1);
    }

    #[test]
    fn test_copy_state_event_for_every() {
        // Test 'every' pattern cloning: new ID + clear future positions
        let mut state = StateEvent::new(3, 0);

        // Fill all 3 positions
        let mut event_a = StreamEvent::new(1000, 1, 0, 0);
        event_a.before_window_data[0] = AttributeValue::Int(100);
        let mut event_b = StreamEvent::new(2000, 1, 0, 0);
        event_b.before_window_data[0] = AttributeValue::Int(200);
        let mut event_c = StreamEvent::new(3000, 1, 0, 0);
        event_c.before_window_data[0] = AttributeValue::Int(300);

        state.set_event(0, event_a);
        state.set_event(1, event_b);
        state.set_event(2, event_c);

        let original_id = state.id;

        let cloner = StateEventCloner::from_state_event(&state);

        // Clear from position 1 (keep position 0, clear 1 and 2)
        let cloned = cloner.copy_state_event_for_every(&state, 1);

        // Verify NEW ID was generated
        assert_ne!(cloned.id, original_id);

        // Verify position 0 is preserved
        assert!(cloned.get_stream_event(0).is_some());
        if let Some(event) = cloned.get_stream_event(0) {
            assert_eq!(event.before_window_data[0], AttributeValue::Int(100));
        }

        // Verify positions 1 and 2 are cleared
        assert!(cloned.get_stream_event(1).is_none());
        assert!(cloned.get_stream_event(2).is_none());

        // Verify original is unchanged
        assert_eq!(state.id, original_id);
        assert!(state.get_stream_event(0).is_some());
        assert!(state.get_stream_event(1).is_some());
        assert!(state.get_stream_event(2).is_some());
    }

    #[test]
    fn test_copy_state_event_for_every_clear_from_zero() {
        // Test clearing from position 0 (clear all positions)
        let mut state = StateEvent::new(2, 0);

        let event_a = StreamEvent::new(1000, 1, 0, 0);
        let event_b = StreamEvent::new(2000, 1, 0, 0);

        state.set_event(0, event_a);
        state.set_event(1, event_b);

        let cloner = StateEventCloner::from_state_event(&state);
        let cloned = cloner.copy_state_event_for_every(&state, 0);

        // All positions should be cleared
        assert!(cloned.get_stream_event(0).is_none());
        assert!(cloned.get_stream_event(1).is_none());

        // New ID generated
        assert_ne!(cloned.id, state.id);
    }

    #[test]
    fn test_copy_state_event_for_every_clear_from_last() {
        // Test clearing from last position (keep all but last)
        let mut state = StateEvent::new(3, 0);

        let event_a = StreamEvent::new(1000, 1, 0, 0);
        let event_b = StreamEvent::new(2000, 1, 0, 0);
        let event_c = StreamEvent::new(3000, 1, 0, 0);

        state.set_event(0, event_a);
        state.set_event(1, event_b);
        state.set_event(2, event_c);

        let cloner = StateEventCloner::from_state_event(&state);
        let cloned = cloner.copy_state_event_for_every(&state, 2);

        // Positions 0 and 1 preserved
        assert!(cloned.get_stream_event(0).is_some());
        assert!(cloned.get_stream_event(1).is_some());

        // Position 2 cleared
        assert!(cloned.get_stream_event(2).is_none());

        // New ID generated
        assert_ne!(cloned.id, state.id);
    }

    #[test]
    fn test_copy_state_event_for_every_preserves_metadata() {
        // Test that metadata (timestamp, type, output_data) is preserved
        let mut state = StateEvent::new(2, 2);
        state.timestamp = 5000;
        state.event_type = ComplexEventType::Current;

        if let Some(output) = state.output_data.as_mut() {
            output[0] = AttributeValue::Int(99);
            output[1] = AttributeValue::String("test".to_string());
        }

        let event_a = StreamEvent::new(1000, 1, 0, 0);
        state.set_event(0, event_a);

        let cloner = StateEventCloner::from_state_event(&state);
        let cloned = cloner.copy_state_event_for_every(&state, 1);

        // Metadata preserved
        assert_eq!(cloned.timestamp, 5000);
        assert_eq!(cloned.event_type, ComplexEventType::Current);

        // Output data preserved
        if let Some(cloned_output) = cloned.output_data.as_ref() {
            assert_eq!(cloned_output[0], AttributeValue::Int(99));
            assert_eq!(cloned_output[1], AttributeValue::String("test".to_string()));
        }

        // But ID is different
        assert_ne!(cloned.id, state.id);
    }
}
