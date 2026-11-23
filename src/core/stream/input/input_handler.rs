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

use std::sync::{Arc, Mutex};

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::event::Event;

/// A processor that takes events from an `InputHandler`. `stream_index` tells it
/// which input stream the events came from.
pub trait InputProcessor: Send + Sync + std::fmt::Debug {
    fn send_event_with_data(
        &mut self,
        timestamp: i64,
        data: Vec<crate::core::event::value::AttributeValue>,
        stream_index: usize,
    ) -> Result<(), String>;

    fn send_single_event(&mut self, event: Event, stream_index: usize) -> Result<(), String>;
    fn send_multiple_events(
        &mut self,
        events: Vec<Event>,
        stream_index: usize,
    ) -> Result<(), String>;
}

#[derive(Debug, Clone)]
pub struct InputHandler {
    stream_id: String,
    stream_index: usize,
    eventflux_app_context: Arc<EventFluxAppContext>,
    input_processor: Option<Arc<Mutex<dyn InputProcessor>>>,
    paused_input_publisher: Arc<Mutex<dyn InputProcessor>>, // restored by connect and resume
}

impl InputHandler {
    pub fn new(
        stream_id: String,
        stream_index: usize,
        input_processor: Arc<Mutex<dyn InputProcessor>>,
        eventflux_app_context: Arc<EventFluxAppContext>,
    ) -> Self {
        Self {
            stream_id,
            stream_index,
            eventflux_app_context,
            input_processor: Some(input_processor.clone()),
            paused_input_publisher: input_processor,
        }
    }

    pub fn get_stream_id(&self) -> &str {
        &self.stream_id
    }

    pub fn get_stream_index(&self) -> usize {
        self.stream_index
    }

    fn ensure_processor(&self) -> Result<Arc<Mutex<dyn InputProcessor>>, String> {
        self.input_processor
            .as_ref()
            .cloned()
            .ok_or_else(|| "EventFlux app is not running, cannot send event".to_string())
    }

    pub fn send_data(
        &self,
        data: Vec<crate::core::event::value::AttributeValue>,
    ) -> Result<(), String> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("System clock before Unix epoch - check system time configuration")
            .as_millis() as i64;
        self.ensure_processor()?
            .lock()
            .map_err(|_| "processor mutex poisoned".to_string())?
            .send_event_with_data(timestamp, data, self.stream_index)
    }

    pub fn send_event_with_timestamp(
        &self,
        timestamp: i64,
        data: Vec<crate::core::event::value::AttributeValue>,
    ) -> Result<(), String> {
        if self.eventflux_app_context.is_playback {
            // TODO: in playback mode, feed the event timestamp to the timestamp
            // generator. It should track the largest event timestamp so events made
            // inside the engine, such as timers, use replay time and not system time.
            // Not done until the playback and replay feature is picked up (M3 or later).
        }

        // Enter the ThreadBarrier so events do not run while state is being restored
        if let Some(barrier) = self.eventflux_app_context.get_thread_barrier() {
            barrier.enter();
            let result = self
                .ensure_processor()?
                .lock()
                .map_err(|_| "processor mutex poisoned".to_string())?
                .send_event_with_data(timestamp, data, self.stream_index);
            barrier.exit();
            result
        } else {
            self.ensure_processor()?
                .lock()
                .map_err(|_| "processor mutex poisoned".to_string())?
                .send_event_with_data(timestamp, data, self.stream_index)
        }
    }

    pub fn send_single_event(&self, event: Event) -> Result<(), String> {
        if self.eventflux_app_context.is_playback {
            // TODO: feed event.timestamp to the timestamp generator in playback mode.
            // See the note in send_event_with_timestamp(). Not done until M3 or later.
        }
        self.ensure_processor()?
            .lock()
            .map_err(|_| "processor mutex poisoned".to_string())?
            .send_single_event(event, self.stream_index)
    }

    pub fn send_multiple_events(&self, events: Vec<Event>) -> Result<(), String> {
        if self.eventflux_app_context.is_playback && !events.is_empty() {
            // TODO: feed the largest event timestamp to the timestamp generator in
            // playback mode. See the note in send_event_with_timestamp(). Not done
            // until M3 or later.
        }
        self.ensure_processor()?
            .lock()
            .map_err(|_| "processor mutex poisoned".to_string())?
            .send_multiple_events(events, self.stream_index)
    }

    pub fn connect(&mut self) {
        self.input_processor = Some(self.paused_input_publisher.clone());
    }

    pub fn disconnect(&mut self) {
        self.input_processor = None;
    }

    pub fn resume(&mut self) {
        self.input_processor = Some(self.paused_input_publisher.clone());
    }
}
