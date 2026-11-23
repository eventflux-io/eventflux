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

/// Debug sink that writes events to the log at info level.
///
/// A payload that is UTF-8 and starts with `{` or `[` is logged as text. Any
/// other payload is decoded from the binary PassthroughMapper format, and each
/// event is logged and kept in `events`. Mostly used for development and tests.
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

    /// Creates a LogSink that reads its `prefix` setting from the config
    /// reader, under `sink_name`
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

    /// Returns the configured log prefix for this sink, or `[LOG]` when none is set
    fn get_prefix(&self) -> String {
        if let Some(ref config_reader) = self.config_reader {
            if let Some(ConfigValue::String(prefix)) =
                config_reader.get_sink_config(&self.sink_name, "prefix")
            {
                return prefix;
            }
        }
        "[LOG]".to_string()
    }
}

impl Sink for LogSink {
    fn publish(&self, payload: &[u8]) -> Result<(), EventFluxError> {
        let prefix = self.get_prefix();

        // A payload that looks like JSON (starts with '{' or '[' after any
        // leading whitespace) comes from the JSON mapper. Log it as text. It is
        // not added to `events`.
        if let Ok(text) = std::str::from_utf8(payload) {
            let trimmed = text.trim_start();
            if trimmed.starts_with('{') || trimmed.starts_with('[') {
                log::info!("{} {}", prefix, text);
                return Ok(());
            }
        }

        // Otherwise decode the binary bincode format. Anything else fails.
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
