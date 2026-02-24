// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/stream/output/stream_callback.rs
// Corresponds to io.eventflux.core.stream.output.StreamCallback
use crate::core::config::eventflux_app_context::EventFluxAppContext; // For setContext
use crate::core::event::complex_event::ComplexEvent; // Trait
use crate::core::event::event::Event; // Using our core Event struct
use crate::query_api::definition::AbstractDefinition as AbstractDefinitionApi; // From query_api
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc; // For toMap method

// StreamCallback in Java is an abstract class implementing StreamJunction.Receiver.
// In Rust, we define it as a trait with its own methods (Receiver pattern not used in optimized implementation).
pub trait StreamCallback: Debug + Send + Sync {
    // Abstract method from Java StreamCallback
    fn receive_events(&self, events: &[Event]); // Corresponds to receive(Event[] events)

    // Methods to hold context, similar to Java private fields
    // These might be better handled by having structs that impl StreamCallback store them.
    // For a trait, we can define associated types or require methods if state is needed.
    // For now, assuming implementing structs will manage their own stream_id, definition, context.
    // fn get_stream_id_cb(&self) -> &str; // Example if these were trait methods
    // fn get_stream_definition_cb(&self) -> Option<Arc<AbstractDefinitionApi>>;
    // fn get_eventflux_app_context_cb(&self) -> Option<Arc<EventFluxAppContext>>;
    // fn set_stream_id_cb(&mut self, stream_id: String);
    // fn set_stream_definition_cb(&mut self, def: Arc<AbstractDefinitionApi>);
    // fn set_eventflux_app_context_cb(&mut self, context: Arc<EventFluxAppContext>);

    // Default implementations for methods from StreamJunction.Receiver,
    // which then call the primary receive_events method.
    fn default_receive_complex_event_chunk(
        &self,
        complex_event_chunk: &mut Option<Box<dyn ComplexEvent>>,
    ) {
        let mut event_buffer = Vec::new();
        let mut current_opt = complex_event_chunk.take(); // Take ownership of the head

        while let Some(mut current_complex_event) = current_opt {
            // Convert ComplexEvent to Event by extracting data
            // Try output_data first (for processed events with selectors),
            // then fall back to before_window_data (for source events)
            let data = if let Some(stream_event) = current_complex_event
                .as_any()
                .downcast_ref::<crate::core::event::stream::StreamEvent>(
            ) {
                // For StreamEvent, prefer output_data if non-empty, else use before_window_data
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
                // For StateEvent, use output_data or empty
                state_event.output_data.clone().unwrap_or_default()
            } else {
                log::warn!("[StreamCallback] Unknown event type, using get_output_data()");
                // Unknown event type, try output_data
                current_complex_event
                    .get_output_data()
                    .map(|d| d.to_vec())
                    .unwrap_or_default()
            };

            let event = Event {
                id: 0, // Note: ID generation handled by CallbackProcessor
                timestamp: current_complex_event.get_timestamp(),
                data,
                is_expired: current_complex_event.is_expired(),
            };

            event_buffer.push(event);
            current_opt = current_complex_event.set_next(None); // Detach and get next
        }
        if !event_buffer.is_empty() {
            self.receive_events(&event_buffer);
        }
        // Put back the (now None) chunk head if necessary, or it's consumed.
        // *complex_event_chunk = None;
    }

    fn default_receive_single_event(&self, event: Event) {
        self.receive_events(&[event]);
    }

    fn default_receive_event_vec(&self, events: Vec<Event>) {
        self.receive_events(&events);
    }

    fn default_receive_timestamp_data(
        &self,
        timestamp: i64,
        data: Vec<crate::core::event::value::AttributeValue>,
    ) {
        self.receive_events(&[Event::new_with_data(timestamp, data)]);
    }

    fn default_receive_event_array(&self, events: &[Event]) {
        // Changed from Vec to slice
        self.receive_events(events);
    }

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

    // Default provided methods from Java StreamCallback (if any beyond Receiver impl)
    // e.g., toMap - this requires streamDefinition to be accessible.
    // If StreamCallback structs store their StreamDefinition:
    // fn to_map(&self, event: &Event) -> Option<HashMap<String, Option<Box<dyn std::any::Any>>>> { ... }

    fn start_processing(&self) { /* Default no-op */
    }
    fn stop_processing(&self) { /* Default no-op */
    }
}

// Example of a concrete StreamCallback struct
#[derive(Debug, Clone)] // Clone if the callback logic is cloneable
pub struct LogStreamCallback {
    pub stream_id: String,
    pub stream_definition: Option<Arc<AbstractDefinitionApi>>, // For toMap
    pub eventflux_app_context: Option<Arc<EventFluxAppContext>>,
}

impl LogStreamCallback {
    /// Simple constructor used in tests and examples.
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
