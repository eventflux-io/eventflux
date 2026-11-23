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

// Matches io.eventflux.core.stream.output.StreamCallback in the Java version
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::event::Event;
use crate::query_api::definition::AbstractDefinition as AbstractDefinitionApi;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

// In Java, StreamCallback is an abstract class that implements
// StreamJunction.Receiver. Here it is a trait with its own methods. The Receiver
// pattern is not used.
pub trait StreamCallback: Debug + Send + Sync {
    // Matches receive(Event[] events) in Java. The one method every callback
    // must write.
    fn receive_events(&self, events: &[Event]);

    // Java keeps stream id, definition, and context in private fields. Here the
    // structs that implement StreamCallback keep their own. The lines below
    // sketch trait methods that could do it instead.
    // fn get_stream_id_cb(&self) -> &str; // Example if these were trait methods
    // fn get_stream_definition_cb(&self) -> Option<Arc<AbstractDefinitionApi>>;
    // fn get_eventflux_app_context_cb(&self) -> Option<Arc<EventFluxAppContext>>;
    // fn set_stream_id_cb(&mut self, stream_id: String);
    // fn set_stream_definition_cb(&mut self, def: Arc<AbstractDefinitionApi>);
    // fn set_eventflux_app_context_cb(&mut self, context: Arc<EventFluxAppContext>);

    // Default versions of the StreamJunction.Receiver methods. Each one turns
    // its input into events and calls receive_events.

    /// Turns a linked chunk of complex events into events and passes them to
    /// `receive_events` in one call. Takes the chunk, leaving `None` behind.
    /// Does not call `receive_events` for an empty chunk.
    fn default_receive_complex_event_chunk(
        &self,
        complex_event_chunk: &mut Option<Box<dyn ComplexEvent>>,
    ) {
        let mut event_buffer = Vec::new();
        let mut current_opt = complex_event_chunk.take(); // take the head of the chunk

        while let Some(mut current_complex_event) = current_opt {
            // Pick the data. Use output_data first (events that went through a
            // selector), then before_window_data (events straight from a source).
            let data = if let Some(stream_event) = current_complex_event
                .as_any()
                .downcast_ref::<crate::core::event::stream::StreamEvent>(
            ) {
                // StreamEvent: output_data when not empty, else before_window_data
                if let Some(ref output) = stream_event.output_data {
                    if !output.is_empty() {
                        output.clone()
                    } else {
                        stream_event.before_window_data.clone()
                    }
                } else {
                    stream_event.before_window_data.clone()
                }
            } else if let Some(state_event) = current_complex_event
                .as_any()
                .downcast_ref::<crate::core::event::state::StateEvent>(
            ) {
                // StateEvent: output_data, or empty when there is none
                state_event.output_data.clone().unwrap_or_default()
            } else {
                log::warn!("[StreamCallback] Unknown event type, using get_output_data()");
                current_complex_event
                    .get_output_data()
                    .map(|d| d.to_vec())
                    .unwrap_or_default()
            };

            let event = Event {
                id: 0, // CallbackProcessor gives out real ids
                timestamp: current_complex_event.get_timestamp(),
                data,
                is_expired: current_complex_event.is_expired(),
            };

            event_buffer.push(event);
            current_opt = current_complex_event.set_next(None); // unlink and move to the next
        }
        if !event_buffer.is_empty() {
            self.receive_events(&event_buffer);
        }
        // The chunk was taken above, so its head is already None. The line below
        // would only make that explicit.
        // *complex_event_chunk = None;
    }

    /// Passes one event to `receive_events`
    fn default_receive_single_event(&self, event: Event) {
        self.receive_events(&[event]);
    }

    /// Passes a Vec of events to `receive_events`
    fn default_receive_event_vec(&self, events: Vec<Event>) {
        self.receive_events(&events);
    }

    /// Builds one event from a timestamp and data and passes it to `receive_events`
    fn default_receive_timestamp_data(
        &self,
        timestamp: i64,
        data: Vec<crate::core::event::value::AttributeValue>,
    ) {
        self.receive_events(&[Event::new_with_data(timestamp, data)]);
    }

    /// Passes a slice of events to `receive_events`. Takes a slice where Java
    /// takes an array.
    fn default_receive_event_array(&self, events: &[Event]) {
        self.receive_events(events);
    }

    /// Turns an event into a map of attribute name to value, plus a
    /// `_timestamp` entry. Returns `None` when the event has a different number
    /// of values than the definition has attributes.
    fn to_map(
        &self,
        event: &Event,
        definition: &AbstractDefinitionApi,
    ) -> Option<HashMap<String, Option<crate::core::event::value::AttributeValue>>> {
        let mut map = HashMap::new();
        let attrs = &definition.attribute_list;
        if attrs.len() != event.data.len() {
            return None;
        }
        map.insert(
            "_timestamp".to_string(),
            Some(crate::core::event::value::AttributeValue::Long(
                event.timestamp,
            )),
        );
        for (attr, value) in attrs.iter().zip(event.data.iter()) {
            map.insert(attr.name.clone(), Some(value.clone()));
        }
        Some(map)
    }

    /// Runs `to_map` on each event. Events that do not match the definition are
    /// skipped, so this always returns `Some`.
    fn to_map_array(
        &self,
        events: &[Event],
        definition: &AbstractDefinitionApi,
    ) -> Option<Vec<HashMap<String, Option<crate::core::event::value::AttributeValue>>>> {
        let mut vec = Vec::new();
        for e in events {
            if let Some(m) = self.to_map(e, definition) {
                vec.push(m);
            }
        }
        Some(vec)
    }

    // Java's toMap reads the callback's own stream definition. If callback
    // structs kept their definition, a version like this could use it.
    // fn to_map(&self, event: &Event) -> Option<HashMap<String, Option<Box<dyn std::any::Any>>>> { ... }

    fn start_processing(&self) { /* does nothing by default */
    }
    fn stop_processing(&self) { /* does nothing by default */
    }
}

/// Simple StreamCallback that logs each batch of events at info level
#[derive(Debug, Clone)]
pub struct LogStreamCallback {
    pub stream_id: String,
    pub stream_definition: Option<Arc<AbstractDefinitionApi>>, // for to_map
    pub eventflux_app_context: Option<Arc<EventFluxAppContext>>,
}

impl LogStreamCallback {
    /// Creates a callback with only a stream id. Used in tests and examples.
    pub fn new(stream_id: String) -> Self {
        Self {
            stream_id,
            stream_definition: None,
            eventflux_app_context: None,
        }
    }
}
impl StreamCallback for LogStreamCallback {
    fn receive_events(&self, events: &[Event]) {
        log::info!("[{}] Received events: {:?}", self.stream_id, events);
    }
}
