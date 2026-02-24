// SPDX-License-Identifier: MIT OR Apache-2.0

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
