// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/event/stream/stream_event.rs
// Corresponds to io.eventflux.core.event.stream.StreamEvent
use crate::core::event::complex_event::{ComplexEvent, ComplexEventType};
use crate::core::event::value::AttributeValue;
use crate::core::util::eventflux_constants::{
    BEFORE_WINDOW_DATA_INDEX, ON_AFTER_WINDOW_DATA_INDEX, OUTPUT_DATA_INDEX,
    STREAM_ATTRIBUTE_INDEX_IN_TYPE, STREAM_ATTRIBUTE_TYPE_INDEX,
};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::Debug;

/// A concrete implementation of ComplexEvent for stream processing.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct StreamEvent {
    pub timestamp: i64,
    pub output_data: Option<Vec<AttributeValue>>,
    pub event_type: ComplexEventType, // Using the canonical ComplexEventType

    pub before_window_data: Vec<AttributeValue>,
    pub on_after_window_data: Vec<AttributeValue>,

    #[serde(default, skip_serializing, skip_deserializing)]
    pub next: Option<Box<dyn ComplexEvent>>,
}

impl Clone for StreamEvent {
    fn clone(&self) -> Self {
        StreamEvent {
            timestamp: self.timestamp,
            output_data: self.output_data.clone(),
            event_type: self.event_type,
            before_window_data: self.before_window_data.clone(),
            on_after_window_data: self.on_after_window_data.clone(),
            next: self
                .next
                .as_ref()
                .map(|n| crate::core::event::complex_event::clone_box_complex_event(n.as_ref())),
        }
    }
}

impl StreamEvent {
    pub fn new(
        timestamp: i64,
        before_window_data_size: usize,
        on_after_window_data_size: usize,
        output_data_size: usize,
    ) -> Self {
        StreamEvent {
            timestamp,
            output_data: if output_data_size > 0 {
                Some(vec![AttributeValue::default(); output_data_size])
            } else {
                None
            },
            event_type: ComplexEventType::default(), // Defaults to Current
            before_window_data: vec![AttributeValue::default(); before_window_data_size],
            on_after_window_data: vec![AttributeValue::default(); on_after_window_data_size],
            next: None,
        }
    }

    /// Create a new StreamEvent with specific data
    pub fn new_with_data(timestamp: i64, data: Vec<AttributeValue>) -> Self {
        StreamEvent {
            timestamp,
            output_data: None,
            event_type: ComplexEventType::Current,
            before_window_data: data,
            on_after_window_data: Vec::new(),
            next: None,
        }
    }

    /// Retrieve an attribute using the EventFlux position array convention.
    /// Only `position[STREAM_ATTRIBUTE_TYPE_INDEX]` and
    /// `position[STREAM_ATTRIBUTE_INDEX_IN_TYPE]` are respected.
    pub fn get_attribute_by_position(&self, position: &[i32]) -> Option<&AttributeValue> {
        let attr_index = *position.get(STREAM_ATTRIBUTE_INDEX_IN_TYPE)? as usize;
        match position.get(STREAM_ATTRIBUTE_TYPE_INDEX).copied()? as usize {
            BEFORE_WINDOW_DATA_INDEX => self.before_window_data.get(attr_index),
            OUTPUT_DATA_INDEX => self.output_data.as_ref().and_then(|v| v.get(attr_index)),
            ON_AFTER_WINDOW_DATA_INDEX => self.on_after_window_data.get(attr_index),
            _ => None,
        }
    }

    /// Set an attribute value using a EventFlux style position array.
    pub fn set_attribute_by_position(
        &mut self,
        value: AttributeValue,
        position: &[i32],
    ) -> Result<(), String> {
        let attr_index = *position
            .get(STREAM_ATTRIBUTE_INDEX_IN_TYPE)
            .ok_or("position array too short")? as usize;
        match position
            .get(STREAM_ATTRIBUTE_TYPE_INDEX)
            .copied()
            .ok_or("position array too short")? as usize
        {
            BEFORE_WINDOW_DATA_INDEX => {
                if attr_index < self.before_window_data.len() {
                    self.before_window_data[attr_index] = value;
                    Ok(())
                } else {
                    Err("index out of bounds".into())
                }
            }
            OUTPUT_DATA_INDEX => {
                if let Some(ref mut vec) = self.output_data {
                    if attr_index < vec.len() {
                        vec[attr_index] = value;
                        Ok(())
                    } else {
                        Err("index out of bounds".into())
                    }
                } else {
                    Err("output_data is None".into())
                }
            }
            ON_AFTER_WINDOW_DATA_INDEX => {
                if attr_index < self.on_after_window_data.len() {
                    self.on_after_window_data[attr_index] = value;
                    Ok(())
                } else {
                    Err("index out of bounds".into())
                }
            }
            _ => Err("unknown attribute type".into()),
        }
    }

    pub fn set_output_data_at_idx(
        &mut self,
        value: AttributeValue,
        index: usize,
    ) -> Result<(), String> {
        match self.output_data {
            Some(ref mut vec) if index < vec.len() => {
                vec[index] = value;
                Ok(())
            }
            Some(_) => Err("index out of bounds".into()),
            None => Err("output_data is None".into()),
        }
    }

    pub fn set_before_window_data_at_idx(
        &mut self,
        value: AttributeValue,
        index: usize,
    ) -> Result<(), String> {
        if index < self.before_window_data.len() {
            self.before_window_data[index] = value;
            Ok(())
        } else {
            Err("index out of bounds".into())
        }
    }

    pub fn set_on_after_window_data_at_idx(
        &mut self,
        value: AttributeValue,
        index: usize,
    ) -> Result<(), String> {
        if index < self.on_after_window_data.len() {
            self.on_after_window_data[index] = value;
            Ok(())
        } else {
            Err("index out of bounds".into())
        }
    }

    pub fn has_next(&self) -> bool {
        self.next.is_some()
    }

    /// Create a shallow clone of this `StreamEvent` without cloning the `next`
    /// pointer.  This mirrors the behavior of `EventVariableFunctionExecutor`
    /// in the Java implementation which extracts a single `StreamEvent` from a
    /// `StateEvent` without its chain.
    pub fn clone_without_next(&self) -> Self {
        StreamEvent {
            timestamp: self.timestamp,
            output_data: self.output_data.clone(),
            event_type: self.event_type,
            before_window_data: self.before_window_data.clone(),
            on_after_window_data: self.on_after_window_data.clone(),
            next: None,
        }
    }
}

impl ComplexEvent for StreamEvent {
    fn get_next(&self) -> Option<&dyn ComplexEvent> {
        self.next.as_deref()
    }
    fn set_next(
        &mut self,
        next_event: Option<Box<dyn ComplexEvent>>,
    ) -> Option<Box<dyn ComplexEvent>> {
        // Return the old next, as per typical linked list operations or Java's setNext.
        // However, Java's ComplexEvent.setNext is void. For easier list manipulation in Rust,
        // returning the old value can be useful if taking ownership.
        // Given the prompt changes `mut_next_ref_option`, this `set_next` should just set.
        // Let's make it return the old next to allow taking it.
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
    // fn get_output_data_mut(&mut self) -> Option<&mut Vec<AttributeValue>> {
    //     self.output_data.as_mut()
    // }

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

    // is_expired() and set_expired() use default trait methods from ComplexEvent.

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
