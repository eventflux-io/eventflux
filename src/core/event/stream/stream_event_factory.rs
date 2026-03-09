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

// src/core/event/stream/stream_event_factory.rs
// Corresponds to io.eventflux.core.event.stream.StreamEventFactory
use super::meta_stream_event::MetaStreamEvent;
use super::stream_event::StreamEvent;
use crate::core::event::complex_event::ComplexEventType;
use crate::core::event::value::AttributeValue;
use std::sync::Mutex;

#[derive(Debug)]
pub struct StreamEventFactory {
    pub before_window_data_size: usize,
    pub on_after_window_data_size: usize,
    pub output_data_size: usize,
    pool: Mutex<Vec<StreamEvent>>,
}

impl StreamEventFactory {
    pub fn new(
        before_window_data_size: usize,
        on_after_window_data_size: usize,
        output_data_size: usize,
    ) -> Self {
        Self {
            before_window_data_size,
            on_after_window_data_size,
            output_data_size,
            pool: Mutex::new(Vec::new()),
        }
    }

    pub fn from_meta(meta: &MetaStreamEvent) -> Self {
        Self {
            before_window_data_size: meta.get_before_window_data().len(),
            on_after_window_data_size: meta.get_on_after_window_data().len(),
            output_data_size: meta.get_output_data().len(),
            pool: Mutex::new(Vec::new()),
        }
    }

    pub fn new_instance(&self) -> StreamEvent {
        if let Some(mut ev) = self.pool.lock().unwrap().pop() {
            ev.timestamp = 0;
            ev.event_type = ComplexEventType::default();
            for v in &mut ev.before_window_data {
                *v = AttributeValue::default();
            }
            for v in &mut ev.on_after_window_data {
                *v = AttributeValue::default();
            }
            if let Some(ref mut out) = ev.output_data {
                for v in out {
                    *v = AttributeValue::default();
                }
            }
            ev.next = None;
            ev
        } else {
            StreamEvent::new(
                0,
                self.before_window_data_size,
                self.on_after_window_data_size,
                self.output_data_size,
            )
        }
    }

    pub fn release(&self, event: StreamEvent) {
        self.pool.lock().unwrap().push(event);
    }

    pub fn pool_size(&self) -> usize {
        self.pool.lock().unwrap().len()
    }
}
