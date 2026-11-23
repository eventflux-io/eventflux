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

// Copies StreamEvents. Port of the Java StreamEventCloner.

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

// The clone gets a fresh factory with an empty pool, since the factory is not Clone.
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

    /// Builds a cloner with explicit sizes, for when the MetaStreamEvent sizes are wrong.
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

    // Copies data, type and timestamp into a new event from the factory. The copy has
    // no `next`. Values past the source length stay Null, and source values past the
    // cloner sizes are dropped.
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
                #[allow(clippy::needless_range_loop)]
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
