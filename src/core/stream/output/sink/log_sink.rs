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

use crate::core::config::{ConfigValue, ProcessorConfigReader};
use crate::core::event::event::Event;
use crate::core::exception::EventFluxError;
use crate::core::stream::output::mapper::PassthroughMapper;
use crate::core::stream::output::sink::sink_trait::Sink;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// LogSink - Debug sink that logs events to stdout/log system
///
/// This sink deserializes binary event payloads and logs them for debugging.
/// Used primarily for development and testing.
#[derive(Debug, Clone)]
pub struct LogSink {
    pub events: Arc<Mutex<Vec<Event>>>,
    pub config_reader: Option<Arc<ProcessorConfigReader>>,
    pub sink_name: String,
}

impl Default for LogSink {
    fn default() -> Self {
        Self::new()
    }
}

impl LogSink {
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            config_reader: None,
            sink_name: "log".to_string(),
        }
    }

    /// Create a new LogSink with configuration reader support
    pub fn new_with_config(
        config_reader: Option<Arc<ProcessorConfigReader>>,
        sink_name: String,
    ) -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            config_reader,
            sink_name,
        }
    }

    /// Get the configured prefix for this sink
    fn get_prefix(&self) -> String {
        if let Some(ref config_reader) = self.config_reader {
            if let Some(ConfigValue::String(prefix)) =
                config_reader.get_sink_config(&self.sink_name, "prefix")
            {
                return prefix;
            }
        }
        "[LOG]".to_string() // Default prefix
    }
}

impl Sink for LogSink {
    fn publish(&self, payload: &[u8]) -> Result<(), EventFluxError> {
        let prefix = self.get_prefix();

        // Check if payload looks like JSON (starts with '{' or '[' after optional whitespace)
        // This handles JSON mapper output which should be logged as text
        if let Ok(text) = std::str::from_utf8(payload) {
            let trimmed = text.trim_start();
            if trimmed.starts_with('{') || trimmed.starts_with('[') {
                // JSON format - log as-is without deserialization
                log::info!("{} {}", prefix, text);
                return Ok(());
            }
        }

        // Fall back to binary deserialization for bincode format
        let events = PassthroughMapper::deserialize(payload).map_err(|e| {
            EventFluxError::app_runtime(format!("LogSink deserialization failed: {}", e))
        })?;

        for e in &events {
            log::info!("{} {:?}", prefix, e);
            self.events.lock().unwrap().push(e.clone());
        }

        Ok(())
    }

    fn clone_box(&self) -> Box<dyn Sink> {
        Box::new(self.clone())
    }
}
