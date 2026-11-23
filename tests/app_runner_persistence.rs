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
use eventflux::core::persistence::{InMemoryPersistenceStore, PersistenceStore};
use std::sync::Arc;

#[tokio::test]
async fn persist_restore_no_error() {
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out\n\
        SELECT v FROM In WINDOW('length', 2);\n";
    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    let rev = runner.persist();
    runner.send("In", vec![AttributeValue::Int(2)]);
    // restore must not panic
    runner.restore_revision(&rev);
    let _ = runner.shutdown();
    assert!(!rev.is_empty());
}

#[tokio::test]
async fn length_window_restore_state() {
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out\n\
        SELECT v FROM In WINDOW('length', 2);\n";
    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    runner.send("In", vec![AttributeValue::Int(2)]);
    let rev = runner.persist();
    runner.send("In", vec![AttributeValue::Int(3)]);
    runner.restore_revision(&rev);
    runner.send("In", vec![AttributeValue::Int(4)]);
    let out = runner.shutdown();
    assert_eq!(out.last().unwrap(), &vec![AttributeValue::Int(4)]);
}

// ============================================================================
// Aggregator State Persistence Tests
//
// These tests run the full persist → restore cycle for each aggregator.
// Steps are send events → persist → change state → restore → check that the
// output uses the restored aggregator state, not the newer state from before restore.
// ============================================================================

#[tokio::test]
async fn sum_aggregator_restore_state() {
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (total LONG);\n\
        INSERT INTO Out\n\
        SELECT sum(v) as total FROM In WINDOW('length', 2);\n";
    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;
    // sum=10, window=[10]
    runner.send("In", vec![AttributeValue::Int(10)]);
    let rev = runner.persist();
    // Change state, sum=30 then sum=50
    runner.send("In", vec![AttributeValue::Int(20)]);
    runner.send("In", vec![AttributeValue::Int(30)]);
    // Restore to sum=10, window=[10]
    runner.restore_revision(&rev);
    // After restore, sum is 10+1=11, window=[10,1]
    runner.send("In", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.last().unwrap(), &vec![AttributeValue::Long(11)]);
}

#[tokio::test]
async fn count_aggregator_restore_state() {
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (cnt LONG);\n\
        INSERT INTO Out\n\
        SELECT count() as cnt FROM In WINDOW('length', 3);\n";
    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;
    // count=1, window=[10]
    runner.send("In", vec![AttributeValue::Int(10)]);
    let rev = runner.persist();
    // Change state, count grows to 3
    runner.send("In", vec![AttributeValue::Int(20)]);
    runner.send("In", vec![AttributeValue::Int(30)]);
    runner.send("In", vec![AttributeValue::Int(40)]);
    // Restore to count=1, window=[10]
    runner.restore_revision(&rev);
    // After restore, count is 2, window=[10, 5]
    runner.send("In", vec![AttributeValue::Int(5)]);
    let out = runner.shutdown();
    assert_eq!(out.last().unwrap(), &vec![AttributeValue::Long(2)]);
}

#[tokio::test]
async fn avg_aggregator_restore_state() {
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (average DOUBLE);\n\
        INSERT INTO Out\n\
        SELECT avg(v) as average FROM In WINDOW('length', 2);\n";
    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;
    // avg=10.0, window=[10]
    runner.send("In", vec![AttributeValue::Int(10)]);
    let rev = runner.persist();
    // Change state
    runner.send("In", vec![AttributeValue::Int(100)]);
    runner.send("In", vec![AttributeValue::Int(200)]);
    // Restore to avg=10.0, window=[10]
    runner.restore_revision(&rev);
    // After restore, avg=(10+20)/2=15.0, window=[10, 20]
    runner.send("In", vec![AttributeValue::Int(20)]);
    let out = runner.shutdown();
    assert_eq!(out.last().unwrap(), &vec![AttributeValue::Double(15.0)]);
}

#[tokio::test]
async fn min_aggregator_restore_state() {
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (minimum INT);\n\
        INSERT INTO Out\n\
        SELECT min(v) as minimum FROM In WINDOW('length', 2);\n";
    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;
    // min=10, window=[10]
    runner.send("In", vec![AttributeValue::Int(10)]);
    let rev = runner.persist();
    // Change state, min becomes 1
    runner.send("In", vec![AttributeValue::Int(1)]);
    runner.send("In", vec![AttributeValue::Int(2)]);
    // Restore to min=10, window=[10]
    runner.restore_revision(&rev);
    // After restore, min=min(10,20)=10, window=[10, 20]
    runner.send("In", vec![AttributeValue::Int(20)]);
    let out = runner.shutdown();
    assert_eq!(out.last().unwrap(), &vec![AttributeValue::Int(10)]);
}

#[tokio::test]
async fn max_aggregator_restore_state() {
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (maximum INT);\n\
        INSERT INTO Out\n\
        SELECT max(v) as maximum FROM In WINDOW('length', 2);\n";
    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;
    // max=10, window=[10]
    runner.send("In", vec![AttributeValue::Int(10)]);
    let rev = runner.persist();
    // Change state, max becomes 999
    runner.send("In", vec![AttributeValue::Int(999)]);
    runner.send("In", vec![AttributeValue::Int(888)]);
    // Restore to max=10, window=[10]
    runner.restore_revision(&rev);
    // After restore, max=max(10,5)=10, window=[10, 5]
    runner.send("In", vec![AttributeValue::Int(5)]);
    let out = runner.shutdown();
    assert_eq!(out.last().unwrap(), &vec![AttributeValue::Int(10)]);
}

#[tokio::test]
async fn stddev_aggregator_restore_state() {
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (sd DOUBLE);\n\
        INSERT INTO Out\n\
        SELECT stddev(v) as sd FROM In WINDOW('length', 3);\n";
    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;
    // stddev uses Welford's running method. Add 10 → count=1, mean=10, m2=0
    runner.send("In", vec![AttributeValue::Int(10)]);
    // Add 20 → count=2, mean=15, m2=50, stddev=5.0
    runner.send("In", vec![AttributeValue::Int(20)]);
    let rev = runner.persist();
    // Change state significantly
    runner.send("In", vec![AttributeValue::Int(100)]);
    runner.send("In", vec![AttributeValue::Int(200)]);
    // Restore to count=2, mean=15, m2=50
    runner.restore_revision(&rev);
    // After restore, add 30 → count=3, mean=20, m2=200, stddev=sqrt(200/3)≈8.165
    runner.send("In", vec![AttributeValue::Int(30)]);
    let out = runner.shutdown();
    let last = out.last().unwrap();
    match &last[0] {
        AttributeValue::Double(v) => {
            let expected = (200.0_f64 / 3.0).sqrt(); // ≈ 8.16496580927726
            assert!(
                (v - expected).abs() < 0.001,
                "stddev after restore should be ~{expected}, got {v}"
            );
        }
        other => panic!("Expected Double, got {other:?}"),
    }
}

#[tokio::test]
async fn last_aggregator_restore_state() {
    // last always returns the most recently added value. The next process_add
    // overwrites the restored value at once, so a wrong restored state cannot be
    // seen in the output. This test only checks that persist and restore work
    // without error and that the aggregator still gives correct results after.
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (l INT);\n\
        INSERT INTO Out\n\
        SELECT last(v) as l FROM In WINDOW('length', 2);\n";
    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;
    runner.send("In", vec![AttributeValue::Int(10)]);
    let rev = runner.persist();
    runner.send("In", vec![AttributeValue::Int(20)]);
    runner.send("In", vec![AttributeValue::Int(30)]);
    runner.restore_revision(&rev);
    runner.send("In", vec![AttributeValue::Int(42)]);
    let out = runner.shutdown();
    assert_eq!(out.last().unwrap(), &vec![AttributeValue::Int(42)]);
}

#[tokio::test]
async fn distinctcount_aggregator_restore_state() {
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (dc LONG);\n\
        INSERT INTO Out\n\
        SELECT distinctCount(v) as dc FROM In WINDOW('length', 3);\n";
    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;
    // Send two equal values, distinctCount=1, window=[1,1]
    runner.send("In", vec![AttributeValue::Int(1)]);
    runner.send("In", vec![AttributeValue::Int(1)]);
    let rev = runner.persist();
    // Change state with different values
    runner.send("In", vec![AttributeValue::Int(2)]);
    runner.send("In", vec![AttributeValue::Int(3)]);
    // Restore to distinctCount=1, map={Int(1):2}, window=[1,1]
    runner.restore_revision(&rev);
    // After restore, window=[1,1,1] and distinctCount=1 since 1 is the only value
    runner.send("In", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.last().unwrap(), &vec![AttributeValue::Long(1)]);
}

#[tokio::test]
async fn first_aggregator_restore_state() {
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (f INT);\n\
        INSERT INTO Out\n\
        SELECT first(v) as f FROM In WINDOW('length', 2);\n";
    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;
    // first=10, values=[10], window=[10]
    runner.send("In", vec![AttributeValue::Int(10)]);
    let rev = runner.persist();
    // Change state, first becomes 20 after 10 expires
    runner.send("In", vec![AttributeValue::Int(20)]);
    runner.send("In", vec![AttributeValue::Int(30)]);
    // Restore to first=10, values=[10], window=[10]
    runner.restore_revision(&rev);
    // After restore, values=[10,99], first=10, window=[10,99]
    runner.send("In", vec![AttributeValue::Int(99)]);
    let out = runner.shutdown();
    assert_eq!(out.last().unwrap(), &vec![AttributeValue::Int(10)]);
}

#[tokio::test]
async fn and_aggregator_restore_state() {
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (b BOOL);\n\
        CREATE STREAM Out (result BOOL);\n\
        INSERT INTO Out\n\
        SELECT and(b) as result FROM In WINDOW('length', 2);\n";
    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;
    // tc=1, fc=0 → and=true, window=[true]
    runner.send("In", vec![AttributeValue::Bool(true)]);
    let rev = runner.persist();
    // Change state by adding false values
    runner.send("In", vec![AttributeValue::Bool(false)]);
    runner.send("In", vec![AttributeValue::Bool(false)]);
    // Restore to tc=1, fc=0, window=[true]
    runner.restore_revision(&rev);
    // After restore, tc=2, fc=0, window=[true, true] → and=true
    runner.send("In", vec![AttributeValue::Bool(true)]);
    let out = runner.shutdown();
    assert_eq!(out.last().unwrap(), &vec![AttributeValue::Bool(true)]);
}

#[tokio::test]
async fn or_aggregator_restore_state() {
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (b BOOL);\n\
        CREATE STREAM Out (result BOOL);\n\
        INSERT INTO Out\n\
        SELECT or(b) as result FROM In WINDOW('length', 2);\n";
    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;
    // tc=0, window=[false]
    runner.send("In", vec![AttributeValue::Bool(false)]);
    let rev = runner.persist();
    // Change state by adding true values → tc=2
    runner.send("In", vec![AttributeValue::Bool(true)]);
    runner.send("In", vec![AttributeValue::Bool(true)]);
    // Restore to tc=0, window=[false]
    runner.restore_revision(&rev);
    // After restore, tc=0, window=[false, false] → or=false
    runner.send("In", vec![AttributeValue::Bool(false)]);
    let out = runner.shutdown();
    assert_eq!(out.last().unwrap(), &vec![AttributeValue::Bool(false)]);
}

// Restoring a revision in a new runtime needs both runtimes to have the same app
// name, because revisions are stored under it. SQL has no @app:name annotation, and
// a runtime without a name gets a different generated one each time. So this test
// sets the name through YAML config (app-with-name.yaml) for both runtimes.
#[tokio::test]
async fn persist_shutdown_restore_state() {
    use eventflux::core::config::ConfigManager;
    use eventflux::core::eventflux_manager::EventFluxManager;

    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());

    let config_manager = ConfigManager::from_file("tests/fixtures/app-with-name.yaml");
    let manager = EventFluxManager::new_with_config_manager(config_manager);
    manager.set_persistence_store(Arc::clone(&store)).unwrap();

    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out\n\
        SELECT v FROM In WINDOW('length', 2);\n";

    let runner = AppRunner::new_with_manager(manager, app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    runner.send("In", vec![AttributeValue::Int(2)]);
    let rev = runner.persist();
    runner.send("In", vec![AttributeValue::Int(3)]);
    let _ = runner.shutdown();

    // Second runtime with the same config, and so the same app name
    let config_manager2 = ConfigManager::from_file("tests/fixtures/app-with-name.yaml");
    let manager2 = EventFluxManager::new_with_config_manager(config_manager2);
    manager2.set_persistence_store(Arc::clone(&store)).unwrap();

    let runner2 = AppRunner::new_with_manager(manager2, app, "Out").await;
    runner2.restore_revision(&rev);
    runner2.send("In", vec![AttributeValue::Int(4)]);
    let out = runner2.shutdown();
    assert_eq!(out.last().unwrap(), &vec![AttributeValue::Int(4)]);
}
