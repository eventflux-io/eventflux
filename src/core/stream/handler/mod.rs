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
//! Provides lifecycle management for source and sink streams with proper
//! initialization, startup, and shutdown handling.
//!
//! ## Architecture
//!
//! - **SourceStreamHandler**: Manages source streams with mapper integration
//! - **SinkStreamHandler**: Manages sink streams with mapper integration
//! - Both handlers provide start/stop lifecycle control
//!
//! ## Thread Safety
//!
//! All handlers are designed to be used behind Arc for shared ownership across
//! multiple threads during query processing.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::core::exception::EventFluxError;
use crate::core::stream::input::input_handler::InputHandler;
use crate::core::stream::input::mapper::{PassthroughMapper, SourceMapper};
use crate::core::stream::input::source::{Source, SourceCallbackAdapter};
use crate::core::stream::output::mapper::SinkMapper;
use crate::core::stream::output::sink::Sink;

/// Handler for source streams with lifecycle management
///
/// Manages a source with its associated mapper and input handler.
/// Provides thread-safe start/stop operations.
#[derive(Debug)]
pub struct SourceStreamHandler {
    source: Arc<Mutex<Box<dyn Source>>>,
    mapper: Option<Arc<Mutex<Box<dyn SourceMapper>>>>,
    input_handler: Arc<Mutex<InputHandler>>,
    stream_id: String,
    is_running: AtomicBool,
}

impl SourceStreamHandler {
    /// Create a new source stream handler
    ///
    /// # Arguments
    ///
    /// * `source` - The source implementation
    /// * `mapper` - Optional source mapper for data transformation
    /// * `input_handler` - Input handler for processing events
    /// * `stream_id` - Unique identifier for this stream
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

    /// Start the source stream
    ///
    /// Begins processing events from the source. This operation is idempotent;
    /// calling start() on an already-running source is a no-op and returns Ok.
    pub fn start(&self) -> Result<(), EventFluxError> {
        log::info!(
            "[SourceStreamHandler] start() called for stream '{}'",
            self.stream_id
        );

        // Check if already running - if so, this is a no-op
        if self.is_running.load(Ordering::SeqCst) {
            log::debug!(
                "[SourceStreamHandler] Stream '{}' already running, skipping",
                self.stream_id
            );
            return Ok(());
        }

        // Atomically transition to running state
        if self.is_running.swap(true, Ordering::SeqCst) {
            // Another thread started it between our check and swap
            log::debug!(
                "[SourceStreamHandler] Stream '{}' was started by another thread",
                self.stream_id
            );
            return Ok(());
        }

        // Get mapper from handler (custom format) or use PassthroughMapper (binary default)
        let mapper = self.mapper.clone().unwrap_or_else(|| {
            // No format specified - use efficient binary passthrough
            Arc::new(Mutex::new(
                Box::new(PassthroughMapper::new()) as Box<dyn SourceMapper>
            ))
        });

        // Create adapter: Source → bytes → SourceCallback → SourceMapper → Events → InputHandler
        let callback = Arc::new(SourceCallbackAdapter::new(
            mapper,
            Arc::clone(&self.input_handler),
        ));

        log::info!(
            "[SourceStreamHandler] Starting source for stream '{}'",
            self.stream_id
        );
        // Start the source with callback
        self.source.lock().unwrap().start(callback);
        Ok(())
    }

    /// Stop the source stream
    ///
    /// Gracefully stops event processing from the source.
    pub fn stop(&self) {
        if self.is_running.swap(false, Ordering::SeqCst) {
            self.source.lock().unwrap().stop();
        }
    }

    /// Check if the source is currently running
    #[inline]
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }

    /// Get the stream identifier
    #[inline]
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    /// Get reference to the source
    #[inline]
    pub fn source(&self) -> Arc<Mutex<Box<dyn Source>>> {
        Arc::clone(&self.source)
    }

    /// Get reference to the mapper (if any)
    #[inline]
    pub fn mapper(&self) -> Option<Arc<Mutex<Box<dyn SourceMapper>>>> {
        self.mapper.as_ref().map(Arc::clone)
    }

    /// Get reference to the input handler
    #[inline]
    pub fn input_handler(&self) -> Arc<Mutex<InputHandler>> {
        Arc::clone(&self.input_handler)
    }
}

/// Handler for sink streams with lifecycle management
///
/// Manages a sink with its associated mapper.
/// Provides thread-safe start/stop operations.
#[derive(Debug)]
pub struct SinkStreamHandler {
    sink: Arc<Mutex<Box<dyn Sink>>>,
    mapper: Option<Arc<Mutex<Box<dyn SinkMapper>>>>,
    stream_id: String,
    is_running: AtomicBool,
}

impl SinkStreamHandler {
    /// Create a new sink stream handler
    ///
    /// # Arguments
    ///
    /// * `sink` - The sink implementation
    /// * `mapper` - Optional sink mapper for data transformation
    /// * `stream_id` - Unique identifier for this stream
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

    /// Start the sink stream
    ///
    /// Begins accepting events at the sink.
    pub fn start(&self) {
        if !self.is_running.swap(true, Ordering::SeqCst) {
            self.sink.lock().unwrap().start();
        }
    }

    /// Stop the sink stream
    ///
    /// Gracefully stops the sink and flushes any pending events.
    pub fn stop(&self) {
        if self.is_running.swap(false, Ordering::SeqCst) {
            self.sink.lock().unwrap().stop();
        }
    }

    /// Check if the sink is currently running
    #[inline]
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }

    /// Get the stream identifier
    #[inline]
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    /// Get reference to the sink
    #[inline]
    pub fn sink(&self) -> Arc<Mutex<Box<dyn Sink>>> {
        Arc::clone(&self.sink)
    }

    /// Get reference to the mapper (if any)
    #[inline]
    pub fn mapper(&self) -> Option<Arc<Mutex<Box<dyn SinkMapper>>>> {
        self.mapper.as_ref().map(Arc::clone)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::eventflux_app_context::EventFluxAppContext;
    use crate::core::event::event::Event;
    use crate::core::stream::input::input_handler::InputProcessor;
    use crate::core::stream::input::source::timer_source::TimerSource;
    use crate::core::stream::output::sink::log_sink::LogSink;

    /// Mock InputProcessor for testing
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

    /// Helper to create a test InputHandler
    fn create_test_input_handler(stream_id: String) -> Arc<Mutex<InputHandler>> {
        use crate::core::config::eventflux_context::EventFluxContext;
        use crate::query_api::eventflux_app::EventFluxApp;

        // Create minimal EventFluxContext
        let eventflux_context = Arc::new(EventFluxContext::default());

        // Create minimal EventFluxApp
        let eventflux_app = Arc::new(EventFluxApp::default());

        // Create EventFluxAppContext
        let app_context = Arc::new(EventFluxAppContext::new(
            eventflux_context,
            "TestApp".to_string(),
            eventflux_app,
            String::new(), // empty app string for tests
        ));

        let processor: Arc<Mutex<dyn InputProcessor>> = Arc::new(Mutex::new(MockInputProcessor));
        Arc::new(Mutex::new(InputHandler::new(
            stream_id,
            0,
            processor,
            app_context,
        )))
    }

    #[test]
    fn test_source_handler_creation() {
        let source = Box::new(TimerSource::new(100));
        let input_handler = create_test_input_handler("TestStream".to_string());

        let handler =
            SourceStreamHandler::new(source, None, input_handler, "TestStream".to_string());

        assert_eq!(handler.stream_id(), "TestStream");
        assert!(!handler.is_running());
    }

    #[test]
    fn test_source_handler_start_stop() {
        let source = Box::new(TimerSource::new(100));
        let input_handler = create_test_input_handler("TestStream".to_string());

        let handler =
            SourceStreamHandler::new(source, None, input_handler, "TestStream".to_string());

        assert!(handler.start().is_ok());
        assert!(handler.is_running());

        // Starting again should be idempotent (no-op, returns Ok)
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
