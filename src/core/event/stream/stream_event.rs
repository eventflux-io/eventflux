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

// Port of the Java StreamEvent.
use crate::core::event::complex_event::{ComplexEvent, ComplexEventType};
use crate::core::event::value::AttributeValue;
use crate::core::util::eventflux_constants::{
    BEFORE_WINDOW_DATA_INDEX, ON_AFTER_WINDOW_DATA_INDEX, OUTPUT_DATA_INDEX,
    STREAM_ATTRIBUTE_INDEX_IN_TYPE, STREAM_ATTRIBUTE_TYPE_INDEX,
};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::Debug;

/// The [`ComplexEvent`] used for single stream processing.
///
/// Cloning also clones every event after it in the chain.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct StreamEvent {
    pub timestamp: i64,
    pub output_data: Option<Vec<AttributeValue>>,
    pub event_type: ComplexEventType,

    pub before_window_data: Vec<AttributeValue>,
    pub on_after_window_data: Vec<AttributeValue>,

    // Next event in the chunk. Not serialized, so it is None after deserializing.
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
            event_type: ComplexEventType::default(), // Current
            before_window_data: vec![AttributeValue::default(); before_window_data_size],
            on_after_window_data: vec![AttributeValue::default(); on_after_window_data_size],
            next: None,
        }
    }

    /// Builds a Current StreamEvent that holds `data` as its before window data.
    ///
    /// Output data is None and on after window data is empty.
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

    /// Reads an attribute using a position array.
    ///
    /// Only `position[STREAM_ATTRIBUTE_TYPE_INDEX]` and
    /// `position[STREAM_ATTRIBUTE_INDEX_IN_TYPE]` are used. Returns `None` if the array
    /// is too short, the type is unknown, or the index is out of range.
    pub fn get_attribute_by_position(&self, position: &[i32]) -> Option<&AttributeValue> {
        let attr_index = *position.get(STREAM_ATTRIBUTE_INDEX_IN_TYPE)? as usize;
        match position.get(STREAM_ATTRIBUTE_TYPE_INDEX).copied()? as usize {
            BEFORE_WINDOW_DATA_INDEX => self.before_window_data.get(attr_index),
            OUTPUT_DATA_INDEX => self.output_data.as_ref().and_then(|v| v.get(attr_index)),
            ON_AFTER_WINDOW_DATA_INDEX => self.on_after_window_data.get(attr_index),
            _ => None,
        }
    }

    /// Writes an attribute using a position array.
    ///
    /// Uses the same two entries as [`Self::get_attribute_by_position`]. Errors if the
    /// array is too short, the type is unknown, output data is None, or the index is out
    /// of range.
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

    /// Clones this `StreamEvent` alone. The copy has no `next` event.
    ///
    /// The data arrays are still cloned. This matches the Java
    /// `EventVariableFunctionExecutor`, which takes a single `StreamEvent` out of a
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
        // Java setNext returns nothing. Returning the old next lets the caller take
        // ownership of the rest of the chain.
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
