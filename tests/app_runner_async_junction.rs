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

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// The @app:async annotation is not part of the SQL syntax in M1, so this test turns on
// async mode for the whole app through YAML config instead.
#[tokio::test]
async fn async_junction_concurrent_dispatch() {
    use eventflux::core::config::ConfigManager;
    use eventflux::core::eventflux_manager::EventFluxManager;

    let config_manager = ConfigManager::from_file("tests/fixtures/app-async-enabled.yaml");
    let manager = EventFluxManager::new_with_config_manager(config_manager);

    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out SELECT v FROM In;\n";

    let runner = Arc::new(AppRunner::new_with_manager(manager, app, "Out").await);
    let mut handles = Vec::new();
    for i in 0..4 {
        let r = Arc::clone(&runner);
        handles.push(thread::spawn(move || {
            for k in 0..500 {
                r.send("In", vec![AttributeValue::Int(i * 500 + k)]);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    thread::sleep(Duration::from_millis(300));
    let runner = Arc::try_unwrap(runner).unwrap();
    let out = runner.shutdown();
    assert_eq!(out.len(), 2000);
}

/// Regression test for two bugs.
///
/// YAML `eventflux.application.async_default` must reach junction creation.
/// set_default_async_mode used to have no callers, so the fixture quietly gave
/// sync junctions.
///
/// An app whose junctions are ALL async must still deliver events. Consumer loops
/// used to run on the shared executor, where the consumers of two async junctions
/// filled the pool and left no thread for any subscriber dispatch.
#[tokio::test]
async fn yaml_async_default_enables_async_junctions() {
    use eventflux::core::config::ConfigManager;
    use eventflux::core::eventflux_manager::EventFluxManager;

    let config_manager = ConfigManager::from_file("tests/fixtures/app-async-enabled.yaml");
    let manager = EventFluxManager::new_with_config_manager(config_manager);

    // No WITH on the streams, so async must come from the YAML default
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out SELECT v FROM In;\n";

    let runner = AppRunner::new_with_manager(manager, app, "Out").await;
    let rt = runner.runtime();
    for (stream_id, junction) in rt.stream_junction_map.iter() {
        assert!(
            junction.lock().unwrap().get_performance_metrics().is_async,
            "stream '{stream_id}' must be async via YAML async_default"
        );
    }

    runner.send("In", vec![AttributeValue::Int(7)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![AttributeValue::Int(7)]],
        "event must flow through two async junctions (executor starvation regression)"
    );
}

/// Second subscriber on Out. It sleeps 1 ms per event to keep the executor busy,
/// so there is always a backlog of subscriber dispatches when shutdown() runs.
#[derive(Debug)]
struct SlowCallback;

impl eventflux::core::stream::output::stream_callback::StreamCallback for SlowCallback {
    fn receive_events(&self, events: &[eventflux::core::event::event::Event]) {
        for _ in events {
            thread::sleep(Duration::from_millis(1));
        }
    }
}

/// Regression test for #138. shutdown() must drain async junctions before
/// stopping sinks. Sends the same 4 x 500 burst as async_junction_concurrent_dispatch
/// but with NO sleep before shutdown. Every accepted event must still reach the output.
#[tokio::test]
async fn async_junction_shutdown_drains_queued_events() {
    use eventflux::core::eventflux_manager::EventFluxManager;

    let manager = EventFluxManager::new();

    // Both junctions are async, so the drain must cascade. Events drained from In
    // flow through the query into Out, which then drains too.
    let app = "\
        CREATE STREAM In (v INT) WITH ('async.enabled' = 'true', 'async.buffer_size' = '4096');\n\
        CREATE STREAM Out (v INT) WITH ('async.enabled' = 'true', 'async.buffer_size' = '4096');\n\
        INSERT INTO Out SELECT v FROM In;\n";

    let runner = Arc::new(AppRunner::new_with_manager(manager, app, "Out").await);
    runner
        .runtime()
        .add_callback("Out", Box::new(SlowCallback))
        .expect("add slow callback");
    let mut handles = Vec::new();
    for i in 0..4 {
        let r = Arc::clone(&runner);
        handles.push(thread::spawn(move || {
            for k in 0..500 {
                r.send("In", vec![AttributeValue::Int(i * 500 + k)]);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    // Shut down right away. Events are still queued in the async junctions and in
    // flight on the executor. The drain must deliver all of them.
    let runner = Arc::try_unwrap(runner).unwrap();
    let out = runner.shutdown();
    assert_eq!(out.len(), 2000);
}
