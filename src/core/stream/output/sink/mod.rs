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

pub mod log_sink;
pub mod rabbitmq_sink;
pub mod sink_factory;
pub mod sink_trait;
pub mod websocket_sink;

use crate::core::event::event::Event;
use crate::core::stream::output::mapper::SinkMapper;
use crate::core::stream::output::stream_callback::StreamCallback;
use std::sync::{Arc, Mutex};

pub use log_sink::LogSink;
pub use sink_factory::{create_sink_from_stream_config, SinkFactoryRegistry};
pub use sink_trait::Sink;

/// Adapter that connects the new Sink architecture (publish bytes) with old StreamCallback interface
///
/// This adapter implements the clean architecture flow:
/// ```text
/// Events → SinkMapper::map() → Vec<u8> → Sink::publish() → External System
/// ```
///
/// The adapter receives Events via StreamCallback, uses the mapper to format them into bytes,
/// and delivers the bytes to the Sink for transport.
#[derive(Debug)]
pub struct SinkCallbackAdapter {
    pub sink: Arc<Mutex<Box<dyn Sink>>>,
    pub mapper: Arc<Mutex<Box<dyn SinkMapper>>>,
}

impl StreamCallback for SinkCallbackAdapter {
    fn receive_events(&self, events: &[Event]) {
        // Transform Events → bytes via mapper
        let payload = match self.mapper.lock().unwrap().map(events) {
            Ok(bytes) => bytes,
            Err(e) => {
                log::error!("Mapper failed to serialize events: {}", e);
                return; // Drop events on mapping failure
            }
        };

        // Publish bytes to sink
        if let Err(e) = self.sink.lock().unwrap().publish(&payload) {
            log::error!("Sink publish failed: {}", e);
        }
    }
}
