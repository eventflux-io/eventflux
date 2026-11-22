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

//! Runtime-level drain-timeout path: with a tiny drain budget and a slow
//! subscriber, shutdown() must take the halt → settle → abandon branch and
//! uphold the ledger invariant — every accepted event is either delivered or
//! counted dropped, never both, and nothing keeps delivering after shutdown
//! returns.
//!
//! Lives in its own file (own test process) because it sets the
//! EVENTFLUX_JUNCTION_DRAIN_TIMEOUT_MS env var, which must not leak into
//! tests running in parallel threads of another binary.

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Second subscriber on Out whose per-event sleep keeps the executor busy so a
/// dispatch backlog reliably exists when the (tiny) drain budget expires.
#[derive(Debug)]
struct SlowCallback;

impl eventflux::core::stream::output::stream_callback::StreamCallback for SlowCallback {
    fn receive_events(&self, events: &[eventflux::core::event::event::Event]) {
        for _ in events {
            thread::sleep(Duration::from_millis(1));
        }
    }
}

#[tokio::test]
async fn drain_timeout_abandons_without_delivering_after_shutdown() {
    use eventflux::core::eventflux_manager::EventFluxManager;

    // Own-process env var (sole test in this binary): 10ms drain budget
    // guarantees the timeout branch under the 1ms/event slow callback.
    std::env::set_var("EVENTFLUX_JUNCTION_DRAIN_TIMEOUT_MS", "10");

    let manager = EventFluxManager::new();
    // One async junction: In's tracked dispatches run the query and deliver
    // into the sync Out junction inline, so every accepted event is either
    // delivered or still queued in In when the budget expires.
    let app = "\
        CREATE STREAM In (v INT) WITH ('async.enabled' = 'true', 'async.buffer_size' = '4096');\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out SELECT v FROM In;\n";

    let runner = Arc::new(AppRunner::new_with_manager(manager, app, "Out").await);
    runner
        .runtime()
        .add_callback("Out", Box::new(SlowCallback))
        .expect("add slow callback");

    const TOTAL: usize = 2000;
    {
        let r = Arc::clone(&runner);
        for k in 0..TOTAL {
            r.send("In", vec![AttributeValue::Int(k as i32)]);
        }
    }

    let runtime = runner.runtime();
    let runner = Arc::try_unwrap(runner).unwrap();
    // Immediate shutdown: the 10ms budget cannot drain 2000 events at
    // 1ms/event, so the halt → settle → abandon branch runs.
    let delivered = runner.shutdown();

    let dropped: u64 = runtime
        .stream_junction_map
        .values()
        .map(|j| j.lock().unwrap().get_performance_metrics().events_dropped)
        .sum();

    assert!(
        dropped > 0,
        "a 10ms budget must abandon part of a 2000-event backlog"
    );
    assert_eq!(
        delivered.len() as u64 + dropped,
        TOTAL as u64,
        "every accepted event must be exactly one of delivered or dropped \
         (delivered {}, dropped {dropped})",
        delivered.len()
    );
}
