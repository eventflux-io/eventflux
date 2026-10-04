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

// TODO: NOT PART OF M1 - Requires @app:async annotation support in SQL compiler
// This test uses @app:async annotation which is not part of core SQL syntax.
// M1 covers: Basic queries, Windows, Joins, GROUP BY, HAVING, ORDER BY, LIMIT
// Async annotations and advanced app-level annotations will be implemented in Phase 2.
// See feat/grammar/GRAMMAR_STATUS.md for M1 feature list.
#[tokio::test]
async fn async_junction_concurrent_dispatch() {
    use eventflux::core::config::ConfigManager;
    use eventflux::core::eventflux_manager::EventFluxManager;

    // MIGRATED: Use YAML configuration for global async mode
    let config_manager = ConfigManager::from_file("tests/fixtures/app-async-enabled.yaml");
    let manager = EventFluxManager::new_with_config_manager(config_manager);

    // MIGRATED: Old EventFluxQL replaced with SQL
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

/// Second subscriber on Out whose per-event sleep keeps the executor busy, so
/// a backlog of subscriber dispatches reliably exists when shutdown() runs.
#[derive(Debug)]
struct SlowCallback;

impl eventflux::core::stream::output::stream_callback::StreamCallback for SlowCallback {
    fn receive_events(&self, events: &[eventflux::core::event::event::Event]) {
        for _ in events {
            thread::sleep(Duration::from_millis(1));
        }
    }
}

/// Regression test for #138: shutdown() must drain async junctions before
/// stopping sinks. Identical burst to the test above but with NO sleep before
/// shutdown — every accepted event must still reach the output.
#[tokio::test]
async fn async_junction_shutdown_drains_queued_events() {
    use eventflux::core::eventflux_manager::EventFluxManager;

    let manager = EventFluxManager::new();

    // NOTE: async must be requested per-stream via SQL WITH — the YAML
    // `async_default` fixture is not wired through to junction creation.
    // Only In is async: two async junctions starve the shared executor
    // (each junction spawns num_cpus/2 consumers), a pre-existing bug
    // independent of the shutdown drain.
    let app = "\
        CREATE STREAM In (v INT) WITH ('async.enabled' = 'true', 'async.buffer_size' = '4096');\n\
        CREATE STREAM Out (v INT);\n\
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
    // Shutdown immediately — events are still queued in the async junction
    // and in-flight on the executor. The drain must deliver all of them.
    let runner = Arc::try_unwrap(runner).unwrap();
    let out = runner.shutdown();
    assert_eq!(out.len(), 2000);
}
