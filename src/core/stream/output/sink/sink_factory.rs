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

//! Sink Factory Implementation
//!
//! Provides automatic sink creation from configuration

use crate::core::config::{types::application_config::SinkConfig, ProcessorConfigReader};
use crate::core::stream::output::mapper::PassthroughMapper;
use crate::core::stream::output::sink::{LogSink, SinkCallbackAdapter};
use crate::core::stream::output::stream_callback::StreamCallback;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Registry for sink factories
pub struct SinkFactoryRegistry {
    factories: HashMap<String, Box<dyn SinkFactoryTrait>>,
}

impl Default for SinkFactoryRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SinkFactoryRegistry {
    /// Create a new sink factory registry with default factories
    pub fn new() -> Self {
        let mut registry = Self {
            factories: HashMap::new(),
        };

        // Register default sink factories
        registry.register("log", Box::new(LogSinkFactory));
        // Future: registry.register("kafka", Box::new(KafkaSinkFactory));
        // Future: registry.register("http", Box::new(HttpSinkFactory));

        registry
    }

    /// Register a new sink factory
    pub fn register(&mut self, sink_type: &str, factory: Box<dyn SinkFactoryTrait>) {
        self.factories.insert(sink_type.to_string(), factory);
    }

    /// Create a sink from configuration
    pub fn create_sink(
        &self,
        sink_config: &SinkConfig,
        config_reader: Option<Arc<ProcessorConfigReader>>,
        sink_name: &str,
    ) -> Result<Box<dyn StreamCallback>, String> {
        let factory = self
            .factories
            .get(&sink_config.sink_type)
            .ok_or_else(|| format!("Unknown sink type: {}", sink_config.sink_type))?;

        factory.create_from_config(sink_config, config_reader, sink_name)
    }
}

/// Trait for sink factories
pub trait SinkFactoryTrait: Send + Sync {
    /// Create a sink from configuration
    fn create_from_config(
        &self,
        sink_config: &SinkConfig,
        config_reader: Option<Arc<ProcessorConfigReader>>,
        sink_name: &str,
    ) -> Result<Box<dyn StreamCallback>, String>;
}

/// Factory for LogSink
struct LogSinkFactory;

impl SinkFactoryTrait for LogSinkFactory {
    fn create_from_config(
        &self,
        _sink_config: &SinkConfig,
        config_reader: Option<Arc<ProcessorConfigReader>>,
        sink_name: &str,
    ) -> Result<Box<dyn StreamCallback>, String> {
        // Create LogSink with configuration support
        let log_sink = LogSink::new_with_config(config_reader, sink_name.to_string());

        // Wrap in adapter with PassthroughMapper for binary serialization
        let adapter = SinkCallbackAdapter {
            sink: Arc::new(Mutex::new(Box::new(log_sink))),
            mapper: Arc::new(Mutex::new(Box::new(PassthroughMapper::new()))),
        };

        Ok(Box::new(adapter))
    }
}

/// Helper function to create a sink from a stream configuration
pub fn create_sink_from_stream_config(
    stream_name: &str,
    sink_config: &SinkConfig,
    config_reader: Option<Arc<ProcessorConfigReader>>,
) -> Result<Box<dyn StreamCallback>, String> {
    // Use the global registry (in a real implementation, this would be passed in)
    let registry = SinkFactoryRegistry::new();
    registry.create_sink(sink_config, config_reader, stream_name)
}
