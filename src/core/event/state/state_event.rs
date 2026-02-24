// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/event/state/state_event.rs
// Corresponds to io.eventflux.core.event.state.StateEvent
use crate::core::event::complex_event::{ComplexEvent, ComplexEventType};
use crate::core::event::stream::StreamEvent; // StateEvent holds StreamEvents
use crate::core::event::value::AttributeValue;
// use crate::core::util::eventflux_constants::{
//     BEFORE_WINDOW_DATA_INDEX, CURRENT, LAST, ON_AFTER_WINDOW_DATA_INDEX, OUTPUT_DATA_INDEX,
//     STATE_OUTPUT_DATA_INDEX, STREAM_ATTRIBUTE_INDEX_IN_TYPE, STREAM_ATTRIBUTE_TYPE_INDEX,
//     STREAM_EVENT_CHAIN_INDEX, STREAM_EVENT_INDEX_IN_CHAIN,
// }; // TODO: Will be used when implementing state event operations
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_STATE_EVENT_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Default, Serialize, Deserialize)] // Default is placeholder
pub struct StateEvent {
    // Fields from Java StateEvent
    pub stream_events: Vec<Option<StreamEvent>>, // Java: StreamEvent[], can have nulls
    pub timestamp: i64,
    pub event_type: ComplexEventType, // Corresponds to type field
    pub output_data: Option<Vec<AttributeValue>>, // Corresponds to outputData field

    // For ComplexEvent linked list
    #[serde(default, skip_serializing, skip_deserializing)]
    pub next: Option<Box<dyn ComplexEvent>>,

    // Java StateEvent has an 'id' field too (long)
    pub id: u64, // Using u64 to match Event.id, though Java StateEvent.id is not static atomic
}

impl Clone for StateEvent {
    fn clone(&self) -> Self {
        StateEvent {
            stream_events: self.stream_events.clone(),
            timestamp: self.timestamp,
            event_type: self.event_type,
            output_data: self.output_data.clone(),
            next: self
                .next
                .as_ref()
                .map(|n| crate::core::event::complex_event::clone_box_complex_event(n.as_ref())),
            id: self.id,
        }
    }
}

impl StateEvent {
    /// Generate a new unique ID for StateEvent
    ///
    /// **Used by**: StateEventCloner for 'every' patterns (need fresh IDs)
    pub fn next_id() -> u64 {
        NEXT_STATE_EVENT_ID.fetch_add(1, Ordering::Relaxed)
    }

    pub fn clone_without_next(&self) -> Self {
        StateEvent {
            stream_events: self.stream_events.clone(),
            timestamp: self.timestamp,
            event_type: self.event_type,
            output_data: self.output_data.clone(),
            next: None,
            id: self.id,
        }
    }
    pub fn new(stream_events_size: usize, output_size: usize) -> Self {
        // Initialize stream_events with None
        let mut stream_events_vec = Vec::with_capacity(stream_events_size);
        for _ in 0..stream_events_size {
            stream_events_vec.push(None);
        }

        Self {
            stream_events: stream_events_vec,
            timestamp: -1, // Default timestamp
            event_type: ComplexEventType::default(),
            output_data: if output_size > 0 {
                Some(vec![AttributeValue::default(); output_size])
            } else {
                None
            },
            next: None,
            id: NEXT_STATE_EVENT_ID.fetch_add(1, Ordering::Relaxed),
        }
    }
    pub fn get_stream_event(&self, position: usize) -> Option<&StreamEvent> {
        self.stream_events.get(position)?.as_ref()
    }

    pub fn get_stream_events(&self) -> &Vec<Option<StreamEvent>> {
        &self.stream_events
    }

    /// Get the number of stream event positions in this StateEvent
    pub fn stream_event_count(&self) -> usize {
        self.stream_events.len()
    }

    pub fn set_event(&mut self, position: usize, event: StreamEvent) {
        if position < self.stream_events.len() {
            self.stream_events[position] = Some(event);
        }
    }

    /// Expand the StateEvent to have at least `min_size` positions
    /// Used when forwarding StateEvents to next processors that need more positions
    pub fn expand_to_size(&mut self, min_size: usize) {
        while self.stream_events.len() < min_size {
            self.stream_events.push(None);
        }
    }

    /// Add event to chain at position (for count quantifiers)
    /// Properly builds a chain: first -> second -> third -> ...
    pub fn add_event(&mut self, position: usize, stream_event: StreamEvent) {
        if position >= self.stream_events.len() {
            return;
        }

        match &mut self.stream_events[position] {
            None => {
                // First event at this position
                self.stream_events[position] = Some(stream_event);
            }
            Some(existing) => {
                // Find the last event in chain and append
                let mut current = existing;
                loop {
                    let has_next = current.next.is_some();
                    if !has_next {
                        current.next = Some(Box::new(stream_event));
                        break;
                    }

                    // Get the next stream event
                    if let Some(ref mut next_box) = current.next {
                        if let Some(stream_event_ref) =
                            next_box.as_any_mut().downcast_mut::<StreamEvent>()
                        {
                            current = stream_event_ref;
                        } else {
                            // Not a StreamEvent, break
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    }

    /// Retrieve a stream event based on EventFlux position array logic.
    pub fn get_stream_event_by_position(&self, position: &[i32]) -> Option<&StreamEvent> {
        use crate::core::util::eventflux_constants::{
            CURRENT, LAST, STREAM_EVENT_CHAIN_INDEX, STREAM_EVENT_INDEX_IN_CHAIN,
        };
        let mut stream_event = self
            .stream_events
            .get(*position.get(STREAM_EVENT_CHAIN_INDEX)? as usize)?
            .as_ref()?;
        let mut current = stream_event as &dyn ComplexEvent;
        let idx = *position.get(STREAM_EVENT_INDEX_IN_CHAIN)?;
        if idx >= 0 {
            for _ in 1..=idx {
                current = current.get_next()?;
                stream_event = current.as_any().downcast_ref::<StreamEvent>()?;
            }
        } else if idx == CURRENT {
            while let Some(next) = current.get_next() {
                current = next;
                stream_event = current.as_any().downcast_ref::<StreamEvent>()?;
            }
        } else if idx == LAST {
            current.get_next()?;
            while let Some(next_next) = current.get_next().and_then(|n| n.get_next()) {
                current = current.get_next()?;
                stream_event = current.as_any().downcast_ref::<StreamEvent>()?;
                if next_next.get_next().is_none() {
                    break;
                }
            }
        } else {
            let mut list = Vec::new();
            let mut tmp: Option<&dyn ComplexEvent> = Some(current);
            while let Some(ev) = tmp {
                list.push(ev.as_any().downcast_ref::<StreamEvent>()?);
                tmp = ev.get_next();
            }
            let index = list.len() as i32 + idx;
            if index < 0 {
                return None;
            }
            stream_event = list.get(index as usize)?;
        }
        Some(stream_event)
    }

    /// Remove last event from chain at position (backtracking for count quantifiers)
    pub fn remove_last_event(&mut self, position: usize) -> Option<StreamEvent> {
        if position >= self.stream_events.len() {
            return None;
        }

        let slot = &mut self.stream_events[position];

        match slot.as_mut() {
            None => None,
            Some(first) => {
                if first.next.is_none() {
                    // Only one event in chain, remove it
                    slot.take()
                } else {
                    // Find second-to-last event
                    let mut current = first;
                    loop {
                        let next_is_last = if let Some(ref next_box) = current.next {
                            if let Some(next_stream) =
                                next_box.as_any().downcast_ref::<StreamEvent>()
                            {
                                next_stream.next.is_none()
                            } else {
                                false
                            }
                        } else {
                            false
                        };

                        if next_is_last {
                            // Take the last event
                            if let Some(last_box) = current.next.take() {
                                return last_box.as_any().downcast_ref::<StreamEvent>().cloned();
                            }
                            return None;
                        }

                        // Move to next
                        if let Some(ref mut next_box) = current.next {
                            if let Some(next_stream) =
                                next_box.as_any_mut().downcast_mut::<StreamEvent>()
                            {
                                current = next_stream;
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    None
                }
            }
        }
    }

    /// Get all events in chain at position
    pub fn get_event_chain(&self, position: usize) -> Vec<&StreamEvent> {
        let mut result = Vec::new();

        if let Some(first) = self.get_stream_event(position) {
            result.push(first);
            let mut current: &dyn ComplexEvent = first;

            while let Some(next) = current.get_next() {
                if let Some(stream_event) = next.as_any().downcast_ref::<StreamEvent>() {
                    result.push(stream_event);
                    current = stream_event;
                } else {
                    break;
                }
            }
        }

        result
    }

    /// Count events in chain at position
    pub fn count_events_at(&self, position: usize) -> usize {
        self.get_event_chain(position).len()
    }
}

// Inherent methods for StateEvent
impl StateEvent {
    pub fn set_output_data_at_idx(
        &mut self,
        value: AttributeValue,
        index: usize,
    ) -> Result<(), String> {
        if let Some(data) = self.output_data.as_mut() {
            if index < data.len() {
                data[index] = value;
                Ok(())
            } else {
                Err(format!(
                    "Index {} out of bounds for output_data with len {}",
                    index,
                    data.len()
                ))
            }
        } else if index == 0 {
            let mut new_data = vec![AttributeValue::default(); index + 1];
            new_data[index] = value;
            self.output_data = Some(new_data);
            Ok(())
        } else {
            Err("output_data is None and index is not 0, cannot set value".to_string())
        }
    }

    pub fn set_output_data_vec(&mut self, data: Option<Vec<AttributeValue>>) {
        self.output_data = data;
    }

    pub fn get_output_data(&self) -> Option<&[AttributeValue]> {
        self.output_data.as_deref()
    }

    pub fn get_attribute(&self, position: &[i32]) -> Option<&AttributeValue> {
        use crate::core::util::eventflux_constants::{
            BEFORE_WINDOW_DATA_INDEX, ON_AFTER_WINDOW_DATA_INDEX, OUTPUT_DATA_INDEX,
            STATE_OUTPUT_DATA_INDEX, STREAM_ATTRIBUTE_INDEX_IN_TYPE, STREAM_ATTRIBUTE_TYPE_INDEX,
        };
        if *position.get(STREAM_ATTRIBUTE_TYPE_INDEX)? as usize == STATE_OUTPUT_DATA_INDEX {
            return self
                .output_data
                .as_ref()
                .and_then(|d| d.get(*position.get(STREAM_ATTRIBUTE_INDEX_IN_TYPE)? as usize));
        }
        let se = self.get_stream_event_by_position(position)?;
        match *position.get(STREAM_ATTRIBUTE_TYPE_INDEX)? as usize {
            BEFORE_WINDOW_DATA_INDEX => se
                .before_window_data
                .get(*position.get(STREAM_ATTRIBUTE_INDEX_IN_TYPE)? as usize),
            OUTPUT_DATA_INDEX => se
                .output_data
                .as_ref()?
                .get(*position.get(STREAM_ATTRIBUTE_INDEX_IN_TYPE)? as usize),
            ON_AFTER_WINDOW_DATA_INDEX => se
                .on_after_window_data
                .get(*position.get(STREAM_ATTRIBUTE_INDEX_IN_TYPE)? as usize),
            _ => None,
        }
    }

    pub fn set_attribute(&mut self, value: AttributeValue, position: &[i32]) -> Result<(), String> {
        use crate::core::util::eventflux_constants::{
            BEFORE_WINDOW_DATA_INDEX, ON_AFTER_WINDOW_DATA_INDEX, OUTPUT_DATA_INDEX,
            STATE_OUTPUT_DATA_INDEX, STREAM_ATTRIBUTE_INDEX_IN_TYPE, STREAM_ATTRIBUTE_TYPE_INDEX,
        };
        if *position.get(STREAM_ATTRIBUTE_TYPE_INDEX).ok_or("pos")? as usize
            == STATE_OUTPUT_DATA_INDEX
        {
            if let Some(ref mut vec) = self.output_data {
                let idx = *position.get(STREAM_ATTRIBUTE_INDEX_IN_TYPE).ok_or("pos")? as usize;
                if idx < vec.len() {
                    vec[idx] = value;
                    return Ok(());
                } else {
                    return Err("index out of bounds".into());
                }
            } else {
                return Err("output_data is None".into());
            }
        }
        let se_position = *position
            .get(crate::core::util::eventflux_constants::STREAM_EVENT_CHAIN_INDEX)
            .ok_or("pos")? as usize;
        if se_position >= self.stream_events.len() {
            return Err("stream position out of bounds".into());
        }
        let se = self.stream_events[se_position]
            .as_mut()
            .ok_or("no stream event")?;
        let idx = *position.get(STREAM_ATTRIBUTE_INDEX_IN_TYPE).ok_or("pos")? as usize;
        match *position.get(STREAM_ATTRIBUTE_TYPE_INDEX).ok_or("pos")? as usize {
            BEFORE_WINDOW_DATA_INDEX => {
                if idx < se.before_window_data.len() {
                    se.before_window_data[idx] = value;
                    Ok(())
                } else {
                    Err("index out".into())
                }
            }
            OUTPUT_DATA_INDEX => {
                if let Some(ref mut out) = se.output_data {
                    if idx < out.len() {
                        out[idx] = value;
                        Ok(())
                    } else {
                        Err("index out".into())
                    }
                } else {
                    Err("output_data None".into())
                }
            }
            ON_AFTER_WINDOW_DATA_INDEX => {
                if idx < se.on_after_window_data.len() {
                    se.on_after_window_data[idx] = value;
                    Ok(())
                } else {
                    Err("index out".into())
                }
            }
            _ => Err("invalid type".into()),
        }
    }
}

impl ComplexEvent for StateEvent {
    fn get_next(&self) -> Option<&dyn ComplexEvent> {
        self.next.as_deref()
    }

    fn set_next(
        &mut self,
        next_event: Option<Box<dyn ComplexEvent>>,
    ) -> Option<Box<dyn ComplexEvent>> {
        let old_next = self.next.take();
        self.next = next_event;
        old_next
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
        self.timestamp
    }

    fn set_timestamp(&mut self, timestamp: i64) {
        self.timestamp = timestamp;
    }

    fn get_event_type(&self) -> ComplexEventType {
        self.event_type
    }

    fn set_event_type(&mut self, event_type: ComplexEventType) {
        self.event_type = event_type;
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===== DAY 1 TESTS: StateEvent Basic Functionality =====

    #[test]
    fn test_new_state_event() {
        let event = StateEvent::new(3, 5);
        assert_eq!(event.stream_events.len(), 3);
        assert_eq!(event.output_data.as_ref().unwrap().len(), 5);
        assert_eq!(event.timestamp, -1);
        assert_eq!(event.event_type, ComplexEventType::Current);
    }

    #[test]
    fn test_set_get_event() {
        let mut state = StateEvent::new(2, 0);
        let stream_event = StreamEvent::new(1000, 1, 0, 0);

        state.set_event(0, stream_event);
        assert!(state.get_stream_event(0).is_some());
        assert!(state.get_stream_event(1).is_none());
    }

    #[test]
    fn test_multiple_positions() {
        let mut state = StateEvent::new(3, 0);

        let event1 = StreamEvent::new(1000, 1, 0, 0);
        let event2 = StreamEvent::new(2000, 2, 0, 0);
        let event3 = StreamEvent::new(3000, 3, 0, 0);

        state.set_event(0, event1);
        state.set_event(1, event2);
        state.set_event(2, event3);

        assert!(state.get_stream_event(0).is_some());
        assert!(state.get_stream_event(1).is_some());
        assert!(state.get_stream_event(2).is_some());

        assert_eq!(state.get_stream_event(0).unwrap().timestamp, 1000);
        assert_eq!(state.get_stream_event(1).unwrap().timestamp, 2000);
        assert_eq!(state.get_stream_event(2).unwrap().timestamp, 3000);
    }

    #[test]
    fn test_out_of_bounds() {
        let mut state = StateEvent::new(2, 0);
        let stream_event = StreamEvent::new(1000, 1, 0, 0);

        // Should not panic when setting position >= len
        state.set_event(10, stream_event);

        // Should return None for out of bounds
        assert!(state.get_stream_event(10).is_none());
    }

    #[test]
    fn test_timestamp_type() {
        let mut state = StateEvent::new(1, 0);

        state.set_timestamp(12345);
        assert_eq!(state.get_timestamp(), 12345);

        state.set_event_type(ComplexEventType::Expired);
        assert_eq!(state.get_event_type(), ComplexEventType::Expired);

        state.set_event_type(ComplexEventType::Current);
        assert_eq!(state.get_event_type(), ComplexEventType::Current);
    }

    // ===== DAY 2 TESTS: Event Chain Operations =====

    #[test]
    fn test_add_event_chain() {
        let mut state = StateEvent::new(1, 0);

        let event1 = StreamEvent::new(100, 1, 0, 0);
        let event2 = StreamEvent::new(200, 1, 0, 0);

        state.add_event(0, event1);
        state.add_event(0, event2);

        assert_eq!(state.count_events_at(0), 2);

        let chain = state.get_event_chain(0);
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0].timestamp, 100);
        assert_eq!(chain[1].timestamp, 200);
    }

    #[test]
    fn test_remove_last_event() {
        let mut state = StateEvent::new(1, 0);

        let event1 = StreamEvent::new(100, 1, 0, 0);
        let event2 = StreamEvent::new(200, 1, 0, 0);

        state.add_event(0, event1);
        state.add_event(0, event2);

        assert_eq!(state.count_events_at(0), 2);

        let removed = state.remove_last_event(0);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().timestamp, 200);

        assert_eq!(state.count_events_at(0), 1);
        let chain = state.get_event_chain(0);
        assert_eq!(chain[0].timestamp, 100);
    }

    #[test]
    fn test_get_event_chain() {
        let mut state = StateEvent::new(1, 0);

        let event1 = StreamEvent::new(100, 1, 0, 0);
        let event2 = StreamEvent::new(200, 1, 0, 0);
        let event3 = StreamEvent::new(300, 1, 0, 0);

        state.add_event(0, event1);
        state.add_event(0, event2);
        state.add_event(0, event3);

        let chain = state.get_event_chain(0);
        assert_eq!(chain.len(), 3);
        assert_eq!(chain[0].timestamp, 100);
        assert_eq!(chain[1].timestamp, 200);
        assert_eq!(chain[2].timestamp, 300);
    }

    #[test]
    fn test_count_events() {
        let mut state = StateEvent::new(1, 0);
        assert_eq!(state.count_events_at(0), 0);

        state.add_event(0, StreamEvent::new(100, 1, 0, 0));
        assert_eq!(state.count_events_at(0), 1);

        state.add_event(0, StreamEvent::new(200, 1, 0, 0));
        assert_eq!(state.count_events_at(0), 2);

        state.add_event(0, StreamEvent::new(300, 1, 0, 0));
        assert_eq!(state.count_events_at(0), 3);
    }

    #[test]
    fn test_empty_chain() {
        let state = StateEvent::new(1, 0);
        assert_eq!(state.count_events_at(0), 0);
        assert!(state.get_event_chain(0).is_empty());
    }

    #[test]
    fn test_single_event_chain() {
        let mut state = StateEvent::new(1, 0);
        state.add_event(0, StreamEvent::new(100, 1, 0, 0));

        let chain = state.get_event_chain(0);
        assert_eq!(chain.len(), 1);
    }

    #[test]
    fn test_multiple_event_chain() {
        let mut state = StateEvent::new(1, 0);

        // Add 5 events
        for i in 0..5 {
            let event = StreamEvent::new(((i + 1) * 100) as i64, 1, 0, 0);
            state.add_event(0, event);
        }

        let chain = state.get_event_chain(0);
        assert_eq!(chain.len(), 5);

        for (i, event) in chain.iter().enumerate() {
            assert_eq!(event.timestamp, ((i + 1) * 100) as i64);
        }
    }

    #[test]
    fn test_remove_from_empty() {
        let mut state = StateEvent::new(1, 0);
        assert!(state.remove_last_event(0).is_none());
    }
}
