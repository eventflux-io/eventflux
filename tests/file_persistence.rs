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
