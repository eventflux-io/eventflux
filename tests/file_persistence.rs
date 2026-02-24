// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::persistence::{
    FilePersistenceStore, PersistenceStore, SqlitePersistenceStore,
};
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn persist_restore_file_store() {
    let dir = tempdir().unwrap();
    let store: Arc<dyn PersistenceStore> = Arc::new(FilePersistenceStore::new(dir.path()).unwrap());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out\n\
        SELECT v FROM In WINDOW('length', 2);\n";
    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    let rev = runner.persist();
    runner.send("In", vec![AttributeValue::Int(2)]);
    runner.restore_revision(&rev);
    let _ = runner.shutdown();
    assert!(!rev.is_empty());
}

#[tokio::test]
async fn persist_restore_sqlite_store() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let store: Arc<dyn PersistenceStore> =
        Arc::new(SqlitePersistenceStore::new(file.path()).unwrap());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out\n\
        SELECT v FROM In WINDOW('length', 2);\n";
    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    let rev = runner.persist();
    runner.send("In", vec![AttributeValue::Int(2)]);
    runner.restore_revision(&rev);
    let _ = runner.shutdown();
    assert!(!rev.is_empty());
}
