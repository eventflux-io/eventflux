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

// Port of the Java Event class.
use super::complex_event::{ComplexEvent, ComplexEventType};
use super::value::AttributeValue;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

// Global counter that gives each event built by the constructors below a unique id.
// The Java Event has no id field. This one is a Rust addition.
static NEXT_EVENT_ID: AtomicU64 = AtomicU64::new(0);

/// A single data event with a timestamp and a data payload.
///
/// `Default` gives id 0, timestamp 0, empty data and not expired. It does not take
/// a fresh id from the global counter.
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct Event {
    pub id: u64,                   // unique per constructed event, not in the Java Event
    pub timestamp: i64,            // set by the caller, the Java default is -1
    pub data: Vec<AttributeValue>, // Java Object[] data
    pub is_expired: bool,          // false unless set
                                   // There is no expiry time field. Only the flag above
                                   // marks an event as expired.
}

impl Event {
    // Builds an event with the given data. Port of Event(long timestamp, Object[] data).
    pub fn new_with_data(timestamp: i64, data: Vec<AttributeValue>) -> Self {
        Event {
            id: NEXT_EVENT_ID.fetch_add(1, Ordering::Relaxed), // only uniqueness matters here
            timestamp,
            data,
            is_expired: false,
        }
    }

    // Builds an event with `data_len` Null values. Port of Event(int dataSize).
    pub fn new_with_size(timestamp: i64, data_len: usize) -> Self {
        Event {
            id: NEXT_EVENT_ID.fetch_add(1, Ordering::Relaxed),
            timestamp,
            data: vec![AttributeValue::default(); data_len],
            is_expired: false,
        }
    }

    // Builds an event with no data. Port of the no argument Event().
    pub fn new_empty(timestamp: i64) -> Self {
        Event {
            id: NEXT_EVENT_ID.fetch_add(1, Ordering::Relaxed),
            timestamp,
            data: Vec::new(),
            is_expired: false,
        }
    }

    // --- Methods ported from the Java Event ---
    pub fn get_timestamp(&self) -> i64 {
        self.timestamp
    }

    pub fn set_timestamp(&mut self, timestamp: i64) {
        self.timestamp = timestamp;
    }

    pub fn get_data(&self) -> &Vec<AttributeValue> {
        &self.data
    }

    pub fn get_data_mut(&mut self) -> &mut Vec<AttributeValue> {
        &mut self.data
    }

    pub fn set_data(&mut self, data: Vec<AttributeValue>) {
        self.data = data;
    }

    pub fn get_data_at_idx(&self, i: usize) -> Option<&AttributeValue> {
        self.data.get(i)
    }

    pub fn set_data_at_idx(&mut self, i: usize, value: AttributeValue) -> Result<(), String> {
        if i < self.data.len() {
            self.data[i] = value;
            Ok(())
        } else {
            Err(format!(
                "Index {} out of bounds for event data with len {}",
                i,
                self.data.len()
            ))
        }
    }

    pub fn is_expired(&self) -> bool {
        self.is_expired
    }

    pub fn set_is_expired(&mut self, is_expired: bool) {
        self.is_expired = is_expired;
    }

    // Copies timestamp, expired flag and data from `other_event`. Keeps its own id.
    pub fn copy_from(&mut self, other_event: &Event) {
        self.timestamp = other_event.timestamp;
        self.is_expired = other_event.is_expired;
        // Cloning an AttributeValue::Object gives Object(None), so object payloads are lost.
        self.data = other_event.data.clone();
    }

    /// Copies timestamp, expired status and output data from a [`ComplexEvent`].
    ///
    /// Keeps the id of `self`. The event counts as expired only when its type is
    /// `Expired`. If the complex event has no output data, `data` is cleared.
    pub fn copy_from_complex(&mut self, complex_event: &dyn ComplexEvent) {
        self.timestamp = complex_event.get_timestamp();
        self.is_expired = complex_event.get_event_type() == ComplexEventType::Expired;
        if let Some(out) = complex_event.get_output_data() {
            self.data = out.to_vec();
        } else {
            self.data.clear();
        }
    }
}

use std::fmt;

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Event{{id={}, timestamp={}, data={:?}, is_expired={}}}",
            self.id, self.timestamp, self.data, self.is_expired
        )
    }
}
