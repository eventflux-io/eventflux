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

// tests/redis_vs_memory_comparison.rs

//! Runs the same persist and restore steps against the InMemory and Redis
//! persistence stores, so a failure points at the store rather than the runtime.
//!
//! All tests use M1 SQL syntax (CREATE STREAM, INSERT INTO, WINDOW). The Redis
//! test skips itself when Redis is not reachable.

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::distributed::RedisConfig;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::persistence::{
    InMemoryPersistenceStore, PersistenceStore, RedisPersistenceStore,
};
use std::sync::Arc;

#[tokio::test]
async fn test_memory_store_simple_works() {
    // Persist and restore within the same runtime
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out SELECT v FROM In WINDOW('length', 2);\n";

    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    runner.send("In", vec![AttributeValue::Int(2)]);
    let rev = runner.persist();
    runner.send("In", vec![AttributeValue::Int(3)]);
    // Go back to the state before event 3
    runner.restore_revision(&rev);
    runner.send("In", vec![AttributeValue::Int(4)]);
    let out = runner.shutdown();

    assert_eq!(out.last().unwrap(), &vec![AttributeValue::Int(4)]);
}

#[tokio::test]
async fn test_memory_store_with_count() {
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT, count BIGINT);\n\
        INSERT INTO Out SELECT v, COUNT() as count FROM In WINDOW('length', 3);\n";

    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;

    // Fill the window with 3 events
    runner.send("In", vec![AttributeValue::Int(1)]); // Window [1] COUNT=1
    runner.send("In", vec![AttributeValue::Int(2)]); // Window [1,2] COUNT=2
    runner.send("In", vec![AttributeValue::Int(3)]); // Window [1,2,3] COUNT=3

    let rev = runner.persist();

    // After the checkpoint the window slides to [2,3,4] COUNT=3
    runner.send("In", vec![AttributeValue::Int(4)]);

    // Restore the checkpoint, back to window [1,2,3] with count=3
    runner.restore_revision(&rev);

    // The window becomes [2,3,5], COUNT=3
    runner.send("In", vec![AttributeValue::Int(5)]);

    let out = runner.shutdown();

    // Expected outputs
    // Events 1, 2 and 3 give 1 output each, with count 1, 2 and 3.
    // Event 4 gives 2 outputs, event 1 expiring with count 2, then event 4 with count 3.
    // After the restore, event 5 gives the same 2 outputs, event 1 expiring with
    // count 2, then event 5 with count 3.
    assert_eq!(out.len(), 7, "Should have 7 output events");

    let last_event = out.last().expect("Expected at least one output event");
    assert_eq!(
        last_event.get(1),
        Some(&AttributeValue::Long(3)),
        "Count should be 3 for length(3) window"
    );

    // The restore worked if event 5 makes event 1 expire again
    let event_5_idx = out
        .iter()
        .position(|e| e.first() == Some(&AttributeValue::Int(5)))
        .expect("Event 5 should be in output");

    // The output just before event 5 must be event 1 expiring with count 2
    if event_5_idx > 0 {
        let prev_event = &out[event_5_idx - 1];
        assert_eq!(
            prev_event.first(),
            Some(&AttributeValue::Int(1)),
            "Event 1 should expire before event 5 enters"
        );
        assert_eq!(
            prev_event.get(1),
            Some(&AttributeValue::Long(2)),
            "Count should be 2 when event 1 expires"
        );
    }
}

#[tokio::test]
async fn test_count_aggregator_multiple_checkpoints() {
    // COUNT must stay correct across several checkpoints and a restore to an older one
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT, count BIGINT);\n\
        INSERT INTO Out SELECT v, COUNT() as count FROM In WINDOW('length', 3);\n";

    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;

    // Phase 1. Fill the window [10,20,30]
    runner.send("In", vec![AttributeValue::Int(10)]);
    runner.send("In", vec![AttributeValue::Int(20)]);
    runner.send("In", vec![AttributeValue::Int(30)]);
    let checkpoint1 = runner.persist();

    // Phase 2. Slide the window to [30,40,50]
    runner.send("In", vec![AttributeValue::Int(40)]);
    runner.send("In", vec![AttributeValue::Int(50)]);
    let _checkpoint2 = runner.persist();

    // Phase 3. Keep sliding
    runner.send("In", vec![AttributeValue::Int(60)]);

    // Restore checkpoint1, which puts the window back to [10,20,30]
    runner.restore_revision(&checkpoint1);

    // New events behave as if checkpoint2 and phase 3 never happened
    runner.send("In", vec![AttributeValue::Int(70)]); // Window: [20,30,70]
    runner.send("In", vec![AttributeValue::Int(80)]); // Window: [30,70,80]

    let out = runner.shutdown();

    // Event 80 must have count=3 because the window is [30,70,80]
    let event_80_idx = out
        .iter()
        .rposition(|e| e.first() == Some(&AttributeValue::Int(80)))
        .expect("Event 80 should be in output");

    assert_eq!(
        out[event_80_idx].get(1),
        Some(&AttributeValue::Long(3)),
        "Event 80 should have count=3"
    );
}

#[tokio::test]
async fn test_redis_store_with_count() {
    let config = RedisConfig {
        url: "redis://localhost:6379".to_string(),
        max_connections: 5,
        connection_timeout_ms: 1000,
        key_prefix: "test:debug:".to_string(),
        ttl_seconds: Some(300),
    };

    let store = match RedisPersistenceStore::new_with_config(config) {
        Ok(store) => store,
        Err(_) => {
            println!("Redis not available, skipping test");
            return;
        }
    };

    // `new_with_config` does not connect yet in async contexts, on purpose. Check the
    // connection first so this test skips cleanly when Redis is not reachable.
    if store.test_connection().is_err() {
        println!("Redis not reachable, skipping test");
        return;
    }

    let store: Arc<dyn PersistenceStore> = Arc::new(store);

    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT, count BIGINT);\n\
        INSERT INTO Out SELECT v, COUNT() as count FROM In WINDOW('length', 3);\n";

    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;

    // Fill the window with 3 events
    runner.send("In", vec![AttributeValue::Int(1)]);
    runner.send("In", vec![AttributeValue::Int(2)]);
    runner.send("In", vec![AttributeValue::Int(3)]);

    let rev = runner.persist();

    // After the checkpoint the window slides to [2,3,4]
    runner.send("In", vec![AttributeValue::Int(4)]);

    // Restore the checkpoint, back to window [1,2,3]
    runner.restore_revision(&rev);

    // The window becomes [2,3,5], COUNT=3
    runner.send("In", vec![AttributeValue::Int(5)]);

    let out = runner.shutdown();

    // The final count must be 3 for a length(3) window
    let last_event = out.last().expect("Expected at least one output event");
    assert_eq!(
        last_event.get(1),
        Some(&AttributeValue::Long(3)),
        "Count should be 3 for length(3) window"
    );
}
