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

//! # Startup ordering regression test (#136)
//!
//! Sources must not begin consuming until every sink is attached and
//! started. The bug: `attach_source_common` started sources during the
//! attachment phase, so a source's first events could flow through a sink
//! stream's junction before the sink subscribed — every such event was
//! silently dropped (0 subscribers). A source that emits synchronously
//! inside `start()` makes the failure deterministic instead of a race.

use eventflux::core::eventflux_manager::EventFluxManager;
use eventflux::core::exception::EventFluxError;
use eventflux::core::extension::{SinkFactory, SourceFactory};
use eventflux::core::stream::input::source::{Source, SourceCallback};
use eventflux::core::stream::output::sink::Sink;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Source that emits its payloads synchronously inside `start()` — the
/// earliest moment any source is allowed to deliver events.
#[derive(Debug, Clone)]
struct ImmediateSource {
    payloads: Vec<Vec<u8>>,
}

impl Source for ImmediateSource {
    fn start(&mut self, callback: Arc<dyn SourceCallback>) {
        for payload in &self.payloads {
            callback
                .on_data(payload)
                .expect("immediate delivery must succeed");
        }
    }

    fn stop(&mut self) {}

    fn clone_box(&self) -> Box<dyn Source> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
struct ImmediateSourceFactory;

impl SourceFactory for ImmediateSourceFactory {
    fn name(&self) -> &'static str {
        "immediate"
    }

    fn supported_formats(&self) -> &[&str] {
        &["json"]
    }

    fn required_parameters(&self) -> &[&str] {
        &[]
    }

    fn optional_parameters(&self) -> &[&str] {
        &[]
    }

    fn create_initialized(
        &self,
        _config: &HashMap<String, String>,
    ) -> Result<Box<dyn Source>, EventFluxError> {
        Ok(Box::new(ImmediateSource {
            payloads: vec![
                br#"{"v": 1}"#.to_vec(),
                br#"{"v": 2}"#.to_vec(),
                br#"{"v": 3}"#.to_vec(),
            ],
        }))
    }

    fn clone_box(&self) -> Box<dyn SourceFactory> {
        Box::new(self.clone())
    }
}

/// Sink recording every published payload into a shared buffer.
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

#[derive(Debug, Clone)]
struct RecordingSinkFactory {
    published: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl SinkFactory for RecordingSinkFactory {
    fn name(&self) -> &'static str {
        "recording"
    }

    fn supported_formats(&self) -> &[&str] {
        &["json"]
    }

    fn required_parameters(&self) -> &[&str] {
        &[]
    }

    fn optional_parameters(&self) -> &[&str] {
        &[]
    }

    fn create_initialized(
        &self,
        _config: &HashMap<String, String>,
    ) -> Result<Box<dyn Sink>, EventFluxError> {
        Ok(Box::new(RecordingSink {
            published: Arc::clone(&self.published),
        }))
    }

    fn clone_box(&self) -> Box<dyn SinkFactory> {
        Box::new(self.clone())
    }
}

#[tokio::test]
async fn test_source_events_emitted_at_start_reach_the_sink() {
    let published = Arc::new(Mutex::new(Vec::new()));

    let manager = EventFluxManager::new();
    manager.add_source_factory("immediate".to_string(), Box::new(ImmediateSourceFactory));
    manager.add_sink_factory(
        "recording".to_string(),
        Box::new(RecordingSinkFactory {
            published: Arc::clone(&published),
        }),
    );

    let sql = r#"
        CREATE STREAM In (v INT) WITH (
            type = 'source', extension = 'immediate', format = 'json'
        );
        CREATE STREAM Out (v INT) WITH (
            type = 'sink', extension = 'recording', format = 'json'
        );
        INSERT INTO Out SELECT v FROM In;
    "#;

    let runtime = manager
        .create_eventflux_app_runtime_from_string(sql)
        .await
        .expect("runtime");
    runtime.start().expect("start");

    // The source delivered synchronously inside start(), so the events are
    // either already at the sink or lost — a short deadline absorbs mapper
    // scheduling only.
    let deadline = Instant::now() + Duration::from_secs(5);
    while published.lock().unwrap().len() < 3 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    runtime.shutdown();

    let published = published.lock().unwrap();
    assert_eq!(
        published.len(),
        3,
        "events emitted the moment a source starts must reach the sink — \
         sinks must be attached and started before any source runs (#136)"
    );
}
