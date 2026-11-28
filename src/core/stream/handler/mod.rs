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

//! # Stream Handler Module
//!
//! Starts and stops source and sink streams.
//!
//! ## Architecture
//!
//! - **SourceStreamHandler**: owns a source, its optional mapper, and the input
//!   handler that receives the mapped events
//! - **SinkStreamHandler**: owns a sink and its optional mapper
//! - Both handlers have start and stop calls that are safe to repeat
//!
//! ## Thread Safety
//!
//! Handlers are meant to be shared across threads behind an `Arc`. The running
//! flag is atomic and the source, sink, and mappers sit behind a `Mutex`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::core::exception::EventFluxError;
use crate::core::stream::input::input_handler::InputHandler;
use crate::core::stream::input::mapper::{PassthroughMapper, SourceMapper};
use crate::core::stream::input::source::{Source, SourceCallbackAdapter};
use crate::core::stream::output::mapper::SinkMapper;
use crate::core::stream::output::sink::Sink;

/// Starts and stops one source stream.
///
/// Holds the source, its optional mapper, and the input handler that receives
/// the mapped events. Start and stop are thread safe.
#[derive(Debug)]
pub struct SourceStreamHandler {
    source: Arc<Mutex<Box<dyn Source>>>,
    mapper: Option<Arc<Mutex<Box<dyn SourceMapper>>>>,
    input_handler: Arc<Mutex<InputHandler>>,
    stream_id: String,
    is_running: AtomicBool,
}

impl SourceStreamHandler {
    /// Creates a source stream handler in the stopped state.
    ///
    /// # Arguments
    ///
    /// * `source` - The source to run
    /// * `mapper` - Mapper that turns source bytes into events. When `None`,
    ///   `start` uses a `PassthroughMapper`
    /// * `input_handler` - Receives the mapped events
    /// * `stream_id` - Id of this stream
    pub fn new(
        source: Box<dyn Source>,
        mapper: Option<Box<dyn SourceMapper>>,
        input_handler: Arc<Mutex<InputHandler>>,
        stream_id: String,
    ) -> Self {
        Self {
            source: Arc::new(Mutex::new(source)),
            mapper: mapper.map(|m| Arc::new(Mutex::new(m))),
            input_handler,
            stream_id,
            is_running: AtomicBool::new(false),
        }
    }

    /// Starts the source and wires its output through the mapper into the input
    /// handler.
    ///
    /// Safe to repeat. Calling it on a running source does nothing and returns
    /// `Ok`. It currently never returns an error.
    pub fn start(&self) -> Result<(), EventFluxError> {
        log::info!(
            "[SourceStreamHandler] start() called for stream '{}'",
            self.stream_id
        );

        if self.is_running.load(Ordering::SeqCst) {
            log::debug!(
                "[SourceStreamHandler] Stream '{}' already running, skipping",
                self.stream_id
            );
            return Ok(());
        }

        // The swap makes sure only one caller starts the source
        if self.is_running.swap(true, Ordering::SeqCst) {
            // Another thread started it between the check above and this swap
            log::debug!(
                "[SourceStreamHandler] Stream '{}' was started by another thread",
                self.stream_id
            );
            return Ok(());
        }

        // Use the configured mapper. With no format set, fall back to the
        // PassthroughMapper, which reads the default binary encoding.
        let mapper = self.mapper.clone().unwrap_or_else(|| {
            Arc::new(Mutex::new(
                Box::new(PassthroughMapper::new()) as Box<dyn SourceMapper>
            ))
        });

        // Data flow: source bytes go to the callback, the mapper turns them into
        // events, and the events go to the input handler
        let callback = Arc::new(SourceCallbackAdapter::new(
            mapper,
            Arc::clone(&self.input_handler),
        ));

        log::info!(
            "[SourceStreamHandler] Starting source for stream '{}'",
            self.stream_id
        );
        self.source.lock().unwrap().start(callback);
        Ok(())
    }

    /// Stops the source. Does nothing if it is not running.
    pub fn stop(&self) {
        if self.is_running.swap(false, Ordering::SeqCst) {
            self.source.lock().unwrap().stop();
        }
    }

    /// Returns true while the source is running.
    #[inline]
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }

    /// Returns the stream id.
    #[inline]
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    /// Returns a shared handle to the source.
    #[inline]
    pub fn source(&self) -> Arc<Mutex<Box<dyn Source>>> {
        Arc::clone(&self.source)
    }

    /// Returns a shared handle to the configured mapper, or `None` if there is none.
    #[inline]
    pub fn mapper(&self) -> Option<Arc<Mutex<Box<dyn SourceMapper>>>> {
        self.mapper.as_ref().map(Arc::clone)
    }

    /// Returns a shared handle to the input handler.
    #[inline]
    pub fn input_handler(&self) -> Arc<Mutex<InputHandler>> {
        Arc::clone(&self.input_handler)
    }
}

/// Starts and stops one sink stream.
///
/// Holds the sink and its optional mapper. Start and stop are thread safe.
#[derive(Debug)]
pub struct SinkStreamHandler {
    sink: Arc<Mutex<Box<dyn Sink>>>,
    mapper: Option<Arc<Mutex<Box<dyn SinkMapper>>>>,
    stream_id: String,
    is_running: AtomicBool,
}

impl SinkStreamHandler {
    /// Creates a sink stream handler in the stopped state.
    ///
    /// # Arguments
    ///
    /// * `sink` - The sink to run
    /// * `mapper` - Optional mapper that turns events into sink bytes
    /// * `stream_id` - Id of this stream
    pub fn new(
        sink: Box<dyn Sink>,
        mapper: Option<Box<dyn SinkMapper>>,
        stream_id: String,
    ) -> Self {
        Self {
            sink: Arc::new(Mutex::new(sink)),
            mapper: mapper.map(|m| Arc::new(Mutex::new(m))),
            stream_id,
            is_running: AtomicBool::new(false),
        }
    }

    /// Starts the sink. Does nothing if it is already running.
    pub fn start(&self) {
        if !self.is_running.swap(true, Ordering::SeqCst) {
            self.sink.lock().unwrap().start();
        }
    }

    /// Stops the sink. Does nothing if it is not running. Any flushing of
    /// pending events is up to the sink's own `stop`.
    pub fn stop(&self) {
        if self.is_running.swap(false, Ordering::SeqCst) {
            self.sink.lock().unwrap().stop();
        }
    }

    /// Returns true while the sink is running.
    #[inline]
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }

    /// Returns the stream id.
    #[inline]
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    /// Returns a shared handle to the sink.
    #[inline]
    pub fn sink(&self) -> Arc<Mutex<Box<dyn Sink>>> {
        Arc::clone(&self.sink)
    }

    /// Returns a shared handle to the mapper, or `None` if there is none.
    #[inline]
    pub fn mapper(&self) -> Option<Arc<Mutex<Box<dyn SinkMapper>>>> {
        self.mapper.as_ref().map(Arc::clone)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::event::Event;
    use crate::core::stream::input::input_handler::InputProcessor;
    use crate::core::stream::input::source::timer_source::TimerSource;
    use crate::core::stream::output::sink::log_sink::LogSink;

    /// InputProcessor that accepts and drops every event.
    #[derive(Debug)]
    struct MockInputProcessor;

    impl InputProcessor for MockInputProcessor {
        fn send_event_with_data(
            &mut self,
            _timestamp: i64,
            _data: Vec<crate::core::event::value::AttributeValue>,
            _stream_index: usize,
        ) -> Result<(), String> {
            Ok(())
        }

        fn send_single_event(&mut self, _event: Event, _stream_index: usize) -> Result<(), String> {
            Ok(())
        }

        fn send_multiple_events(
            &mut self,
            _events: Vec<Event>,
            _stream_index: usize,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    /// Builds an InputHandler backed by `MockInputProcessor`.
    fn create_test_input_handler(stream_id: &str) -> Arc<Mutex<InputHandler>> {
        crate::core::stream::input::input_handler::test_input_handler(
            stream_id,
            Arc::new(Mutex::new(MockInputProcessor)),
        )
    }

    #[test]
    fn test_source_handler_creation() {
        let source = Box::new(TimerSource::new(100));
        let input_handler = create_test_input_handler("TestStream");

        let handler =
            SourceStreamHandler::new(source, None, input_handler, "TestStream".to_string());

        assert_eq!(handler.stream_id(), "TestStream");
        assert!(!handler.is_running());
    }

    #[test]
    fn test_source_handler_start_stop() {
        let source = Box::new(TimerSource::new(100));
        let input_handler = create_test_input_handler("TestStream");

        let handler =
            SourceStreamHandler::new(source, None, input_handler, "TestStream".to_string());

        assert!(handler.start().is_ok());
        assert!(handler.is_running());

        // A second start does nothing and still returns Ok
        assert!(handler.start().is_ok());
        assert!(handler.is_running());

        handler.stop();
        assert!(!handler.is_running());
    }

    #[test]
    fn test_sink_handler_creation() {
        let sink = Box::new(LogSink::new());

        let handler = SinkStreamHandler::new(sink, None, "TestSink".to_string());

        assert_eq!(handler.stream_id(), "TestSink");
        assert!(!handler.is_running());
    }

    #[test]
    fn test_sink_handler_start_stop() {
        let sink = Box::new(LogSink::new());

        let handler = SinkStreamHandler::new(sink, None, "TestSink".to_string());

        handler.start();
        assert!(handler.is_running());

        handler.stop();
        assert!(!handler.is_running());
    }
}
