// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/event/stream/stream_event_cloner.rs
// Utility for cloning StreamEvents similar to io.eventflux.core.event.stream.StreamEventCloner

use super::{
    meta_stream_event::MetaStreamEvent, stream_event::StreamEvent,
    stream_event_factory::StreamEventFactory,
};

#[derive(Debug)]
pub struct StreamEventCloner {
    before_window_data_size: usize,
    on_after_window_data_size: usize,
    output_data_size: usize,
    event_factory: StreamEventFactory,
}

impl Clone for StreamEventCloner {
    fn clone(&self) -> Self {
        Self {
            before_window_data_size: self.before_window_data_size,
            on_after_window_data_size: self.on_after_window_data_size,
            output_data_size: self.output_data_size,
            event_factory: StreamEventFactory::new(
                self.before_window_data_size,
                self.on_after_window_data_size,
                self.output_data_size,
            ),
        }
    }
}

impl StreamEventCloner {
    pub fn new(meta: &MetaStreamEvent, factory: StreamEventFactory) -> Self {
        Self {
            before_window_data_size: meta.get_before_window_data().len(),
            on_after_window_data_size: meta.get_on_after_window_data().len(),
            output_data_size: meta.get_output_data().len(),
            event_factory: factory,
        }
    }

    /// Create a cloner with explicit sizes (useful when MetaStreamEvent doesn't have correct sizes)
    pub fn new_with_sizes(
        before_window_data_size: usize,
        on_after_window_data_size: usize,
        output_data_size: usize,
        factory: StreamEventFactory,
    ) -> Self {
        Self {
            before_window_data_size,
            on_after_window_data_size,
            output_data_size,
            event_factory: factory,
        }
    }

    pub fn from_event(event: &StreamEvent) -> Self {
        Self {
            before_window_data_size: event.before_window_data.len(),
            on_after_window_data_size: event.on_after_window_data.len(),
            output_data_size: event.output_data.as_ref().map_or(0, |v| v.len()),
            event_factory: StreamEventFactory::new(
                event.before_window_data.len(),
                event.on_after_window_data.len(),
                event.output_data.as_ref().map_or(0, |v| v.len()),
            ),
        }
    }

    pub fn copy_stream_event(&self, stream_event: &StreamEvent) -> StreamEvent {
        let mut new_event = self.event_factory.new_instance();
        for i in 0..self.before_window_data_size {
            if let Some(val) = stream_event.before_window_data.get(i) {
                new_event.before_window_data[i] = val.clone();
            }
        }
        for i in 0..self.on_after_window_data_size {
            if let Some(val) = stream_event.on_after_window_data.get(i) {
                new_event.on_after_window_data[i] = val.clone();
            }
        }
        if self.output_data_size > 0 {
            if let (Some(src), Some(dest)) = (
                stream_event.output_data.as_ref(),
                new_event.output_data.as_mut(),
            ) {
                for i in 0..self.output_data_size {
                    if let Some(v) = src.get(i) {
                        dest[i] = v.clone();
                    }
                }
            }
        }
        new_event.event_type = stream_event.event_type;
        new_event.timestamp = stream_event.timestamp;
        new_event
    }
}
