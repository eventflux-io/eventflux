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
/// Events → per event: SinkMapper::map_event() → Vec<u8> → Sink::publish() → External System
/// ```
///
/// The adapter receives Events via StreamCallback and iterates the batch:
/// one event = one payload = one publish. This matches the per-record
/// semantics of real transports (Kafka record, AMQP message, WebSocket frame)
/// and guarantees no event in a batch is silently dropped.
///
/// A mapper or publish failure for one event is logged and does not affect
/// the remaining events in the batch.
#[derive(Debug)]
pub struct SinkCallbackAdapter {
    pub sink: Arc<Mutex<Box<dyn Sink>>>,
    pub mapper: Arc<Mutex<Box<dyn SinkMapper>>>,
}

impl StreamCallback for SinkCallbackAdapter {
    fn receive_events(&self, events: &[Event]) {
        let mapper = self.mapper.lock().unwrap();
        let sink = self.sink.lock().unwrap();

        for event in events {
            // Transform ONE event → bytes via mapper
            let payload = match mapper.map_event(event) {
                Ok(bytes) => bytes,
                Err(e) => {
                    log::error!("Mapper failed to serialize event: {}", e);
                    continue; // Skip this event, keep processing the batch
                }
            };

            // Publish bytes to sink
            if let Err(e) = sink.publish(&payload) {
                log::error!("Sink publish failed: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::value::AttributeValue;
    use crate::core::exception::EventFluxError;

    /// Sink that records every published payload
    #[derive(Debug, Clone)]
    struct RecordingSink {
        published: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl Sink for RecordingSink {
        fn publish(&self, payload: &[u8]) -> Result<(), EventFluxError> {
            self.published.lock().unwrap().push(payload.to_vec());
            Ok(())
        }

        fn clone_box(&self) -> Box<dyn Sink> {
            Box::new(self.clone())
        }
    }

    /// Mapper that serializes the first attribute as a string, failing on
    /// events whose first attribute equals "fail"
    #[derive(Debug, Clone)]
    struct TestMapper;

    impl SinkMapper for TestMapper {
        fn map_event(&self, event: &Event) -> Result<Vec<u8>, EventFluxError> {
            match event.data.first() {
                Some(AttributeValue::String(s)) if s == "fail" => {
                    Err(EventFluxError::MappingFailed {
                        message: "intentional test failure".to_string(),
                        source: None,
                    })
                }
                Some(AttributeValue::String(s)) => Ok(s.clone().into_bytes()),
                _ => Ok(Vec::new()),
            }
        }

        fn clone_box(&self) -> Box<dyn SinkMapper> {
            Box::new(self.clone())
        }
    }

    fn string_event(s: &str) -> Event {
        Event::new_with_data(0, vec![AttributeValue::String(s.to_string())])
    }

    fn make_adapter(published: Arc<Mutex<Vec<Vec<u8>>>>) -> SinkCallbackAdapter {
        SinkCallbackAdapter {
            sink: Arc::new(Mutex::new(Box::new(RecordingSink { published }))),
            mapper: Arc::new(Mutex::new(Box::new(TestMapper))),
        }
    }

    #[test]
    fn test_batch_publishes_one_message_per_event_in_order() {
        let published = Arc::new(Mutex::new(Vec::new()));
        let adapter = make_adapter(Arc::clone(&published));

        let events = [
            string_event("one"),
            string_event("two"),
            string_event("three"),
        ];
        adapter.receive_events(&events);

        let published = published.lock().unwrap();
        assert_eq!(
            *published,
            vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()],
            "each event in a batch must be published as its own message, in order"
        );
    }

    #[test]
    fn test_mapper_failure_does_not_drop_rest_of_batch() {
        let published = Arc::new(Mutex::new(Vec::new()));
        let adapter = make_adapter(Arc::clone(&published));

        let events = [
            string_event("one"),
            string_event("fail"),
            string_event("three"),
        ];
        adapter.receive_events(&events);

        let published = published.lock().unwrap();
        assert_eq!(
            *published,
            vec![b"one".to_vec(), b"three".to_vec()],
            "a mapping failure for one event must not affect the remaining events"
        );
    }

    #[test]
    fn test_empty_batch_publishes_nothing() {
        let published = Arc::new(Mutex::new(Vec::new()));
        let adapter = make_adapter(Arc::clone(&published));

        adapter.receive_events(&[]);

        assert!(
            published.lock().unwrap().is_empty(),
            "an empty batch must not produce a publish call"
        );
    }
}
