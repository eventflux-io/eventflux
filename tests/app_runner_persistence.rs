// SPDX-License-Identifier: MIT OR Apache-2.0

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
    // restore should succeed
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

// TODO: NOT PART OF M1 - App naming in SQL syntax for persistence across restarts
// This test fails because SQL syntax doesn't support @app:name annotations.
// Each new runtime gets a different auto-generated name, so the revision can't be found
// when restoring in a new runtime instance. This requires either:
// 1. SQL syntax support for app naming (e.g., CREATE APPLICATION or similar)
// 2. Alternative persistence key scheme that doesn't depend on app name
// The other two persistence tests (restore within same runtime) work fine.
// This is an implementation detail for cross-restart persistence.
// See feat/grammar/GRAMMAR_STATUS.md for M1 feature list.
#[tokio::test]
async fn persist_shutdown_restore_state() {
    use eventflux::core::config::ConfigManager;
    use eventflux::core::eventflux_manager::EventFluxManager;

    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());

    // MIGRATED: Use YAML configuration for app naming
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

    // Second instance with same config
    let config_manager2 = ConfigManager::from_file("tests/fixtures/app-with-name.yaml");
    let manager2 = EventFluxManager::new_with_config_manager(config_manager2);
    manager2.set_persistence_store(Arc::clone(&store)).unwrap();

    let runner2 = AppRunner::new_with_manager(manager2, app, "Out").await;
    runner2.restore_revision(&rev);
    runner2.send("In", vec![AttributeValue::Int(4)]);
    let out = runner2.shutdown();
    assert_eq!(out.last().unwrap(), &vec![AttributeValue::Int(4)]);
}
