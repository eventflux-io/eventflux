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

use std::collections::HashMap;
use std::sync::Mutex;

use log::error;

/// Trait for simple persistence stores that save full snapshots.
pub trait PersistenceStore: Send + Sync {
    fn save(&self, eventflux_app_id: &str, revision: &str, snapshot: &[u8]);
    fn load(&self, eventflux_app_id: &str, revision: &str) -> Option<Vec<u8>>;
    fn get_last_revision(&self, eventflux_app_id: &str) -> Option<String>;
    fn clear_all_revisions(&self, eventflux_app_id: &str);
}

/// Trait for incremental persistence stores.
pub trait IncrementalPersistenceStore: Send + Sync {
    fn save(&self, revision: &str, snapshot: &[u8]);
    fn load(&self, revision: &str) -> Option<Vec<u8>>;
    fn get_last_revision(&self, eventflux_app_id: &str) -> Option<String>;
    fn clear_all_revisions(&self, eventflux_app_id: &str);
}

/// Very small in-memory implementation useful for tests.
#[derive(Default)]
pub struct InMemoryPersistenceStore {
    inner: Mutex<HashMap<String, HashMap<String, Vec<u8>>>>,
    last_revision: Mutex<HashMap<String, String>>,
}

impl InMemoryPersistenceStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PersistenceStore for InMemoryPersistenceStore {
    fn save(&self, eventflux_app_id: &str, revision: &str, snapshot: &[u8]) {
        let mut m = self.inner.lock().unwrap();
        let entry = m.entry(eventflux_app_id.to_string()).or_default();
        entry.insert(revision.to_string(), snapshot.to_vec());
        self.last_revision
            .lock()
            .unwrap()
            .insert(eventflux_app_id.to_string(), revision.to_string());
    }

    fn load(&self, eventflux_app_id: &str, revision: &str) -> Option<Vec<u8>> {
        self.inner
            .lock()
            .unwrap()
            .get(eventflux_app_id)
            .and_then(|m| m.get(revision).cloned())
    }

    fn get_last_revision(&self, eventflux_app_id: &str) -> Option<String> {
        self.last_revision
            .lock()
            .unwrap()
            .get(eventflux_app_id)
            .cloned()
    }

    fn clear_all_revisions(&self, eventflux_app_id: &str) {
        self.inner.lock().unwrap().remove(eventflux_app_id);
        self.last_revision.lock().unwrap().remove(eventflux_app_id);
    }
}

impl IncrementalPersistenceStore for InMemoryPersistenceStore {
    fn save(&self, revision: &str, snapshot: &[u8]) {
        // For tests we treat incremental same as full with eventflux_app_id="default"
        <Self as PersistenceStore>::save(self, "default", revision, snapshot);
    }

    fn load(&self, revision: &str) -> Option<Vec<u8>> {
        <Self as PersistenceStore>::load(self, "default", revision)
    }

    fn get_last_revision(&self, eventflux_app_id: &str) -> Option<String> {
        PersistenceStore::get_last_revision(self, eventflux_app_id)
    }

    fn clear_all_revisions(&self, eventflux_app_id: &str) {
        PersistenceStore::clear_all_revisions(self, eventflux_app_id)
    }
}

// Simple file-based persistence store storing each snapshot as a file
use std::fs;
use std::path::{Path, PathBuf};

pub struct FilePersistenceStore {
    base: PathBuf,
    last_revision: Mutex<HashMap<String, String>>,
}

impl FilePersistenceStore {
    pub fn new<P: Into<PathBuf>>(path: P) -> std::io::Result<Self> {
        let p = path.into();
        fs::create_dir_all(&p)?;
        Ok(Self {
            base: p,
            last_revision: Mutex::new(HashMap::new()),
        })
    }

    fn file_path(&self, app: &str, rev: &str) -> PathBuf {
        self.base.join(app).join(rev)
    }
}

impl PersistenceStore for FilePersistenceStore {
    fn save(&self, eventflux_app_id: &str, revision: &str, snapshot: &[u8]) {
        let dir = self.base.join(eventflux_app_id);
        if let Err(e) = fs::create_dir_all(&dir) {
            error!("FilePersistenceStore: cannot create dir: {e}");
            return;
        }
        let path = self.file_path(eventflux_app_id, revision);
        match fs::write(&path, snapshot) {
            Ok(_) => {
                self.last_revision
                    .lock()
                    .unwrap()
                    .insert(eventflux_app_id.to_string(), revision.to_string());
            }
            Err(e) => {
                error!("FilePersistenceStore: write failed: {e}");
            }
        }
    }

    fn load(&self, eventflux_app_id: &str, revision: &str) -> Option<Vec<u8>> {
        let path = self.file_path(eventflux_app_id, revision);
        fs::read(path).ok()
    }

    fn get_last_revision(&self, eventflux_app_id: &str) -> Option<String> {
        self.last_revision
            .lock()
            .unwrap()
            .get(eventflux_app_id)
            .cloned()
    }

    fn clear_all_revisions(&self, eventflux_app_id: &str) {
        let dir = self.base.join(eventflux_app_id);
        let _ = fs::remove_dir_all(&dir);
        self.last_revision.lock().unwrap().remove(eventflux_app_id);
    }
}

use rusqlite::{params, Connection};

pub struct SqlitePersistenceStore {
    conn: Mutex<Connection>,
}

impl SqlitePersistenceStore {
    pub fn new<P: AsRef<Path>>(path: P) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS snapshots (app TEXT, rev TEXT, data BLOB, PRIMARY KEY(app, rev))",
            [],
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

impl PersistenceStore for SqlitePersistenceStore {
    fn save(&self, eventflux_app_id: &str, revision: &str, snapshot: &[u8]) {
        let conn = self.conn.lock().unwrap();
        if let Err(e) = conn.execute(
            "INSERT OR REPLACE INTO snapshots(app, rev, data) VALUES (?1, ?2, ?3)",
            params![eventflux_app_id, revision, snapshot],
        ) {
            error!("SqlitePersistenceStore: write failed: {e}");
        }
    }

    fn load(&self, eventflux_app_id: &str, revision: &str) -> Option<Vec<u8>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT data FROM snapshots WHERE app=?1 AND rev=?2",
            params![eventflux_app_id, revision],
            |row| row.get(0),
        )
        .ok()
    }

    fn get_last_revision(&self, eventflux_app_id: &str) -> Option<String> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT rev FROM snapshots WHERE app=?1 ORDER BY rowid DESC LIMIT 1",
            params![eventflux_app_id],
            |row| row.get(0),
        )
        .ok()
    }

    fn clear_all_revisions(&self, eventflux_app_id: &str) {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute(
            "DELETE FROM snapshots WHERE app=?1",
            params![eventflux_app_id],
        );
    }
}

/// Redis-backed persistence store for distributed state management
pub struct RedisPersistenceStore {
    backend: Arc<tokio::sync::Mutex<crate::core::distributed::RedisBackend>>,
    runtime: Option<Arc<tokio::runtime::Runtime>>,
}

impl RedisPersistenceStore {
    /// Create a new Redis persistence store with default configuration
    pub fn new() -> Result<Self, String> {
        let backend = crate::core::distributed::RedisBackend::new();
        Self::new_with_backend(backend)
    }

    /// Create a new Redis persistence store with custom Redis configuration
    pub fn new_with_config(config: crate::core::distributed::RedisConfig) -> Result<Self, String> {
        let backend = crate::core::distributed::RedisBackend::with_config(config);
        Self::new_with_backend(backend)
    }

    fn new_with_backend(backend: crate::core::distributed::RedisBackend) -> Result<Self, String> {
        // Check if we're in an async runtime context
        let runtime = if tokio::runtime::Handle::try_current().is_ok() {
            // We're in an async context, don't create a new runtime
            None
        } else {
            // Create a dedicated runtime for Redis operations
            Some(Arc::new(tokio::runtime::Runtime::new().map_err(|e| {
                format!("Failed to create async runtime: {}", e)
            })?))
        };

        // Initialize the backend - handle runtime context properly
        if runtime.is_some() {
            // Use the dedicated runtime (we'll initialize later)
        } else {
            // Skip initialization in async context - let it be lazy initialized
            // This avoids the "runtime within runtime" issue for tests
        }

        Ok(Self {
            backend: Arc::new(tokio::sync::Mutex::new(backend)),
            runtime,
        })
    }

    /// Test Redis connectivity by initializing the backend and performing a PING
    ///
    /// Returns Ok(()) if Redis is available and responding, Err otherwise.
    /// This is useful for test setup to skip tests when Redis is not available.
    pub fn test_connection(&self) -> Result<(), String> {
        let backend = Arc::clone(&self.backend);

        if let Some(ref runtime) = self.runtime {
            // Use dedicated runtime
            runtime.block_on(async move {
                let mut backend = backend.lock().await;
                backend.initialize().await.map_err(|e| e.to_string())
            })
        } else {
            // We're in an async context, use spawn_blocking
            let handle = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new()
                    .map_err(|e| format!("Failed to create runtime: {}", e))?;
                rt.block_on(async move {
                    let mut backend = backend.lock().await;
                    backend.initialize().await.map_err(|e| e.to_string())
                })
            });
            handle.join().map_err(|_| "Thread panicked".to_string())?
        }
    }

    /// Get the revision key for Redis
    fn revision_key(eventflux_app_id: &str, revision: &str) -> String {
        format!("eventflux:app:{}:revision:{}", eventflux_app_id, revision)
    }

    /// Get the last revision key for Redis
    fn last_revision_key(eventflux_app_id: &str) -> String {
        format!("eventflux:app:{}:last_revision", eventflux_app_id)
    }
}

impl PersistenceStore for RedisPersistenceStore {
    fn save(&self, eventflux_app_id: &str, revision: &str, snapshot: &[u8]) {
        let backend = Arc::clone(&self.backend);
        let revision_key = Self::revision_key(eventflux_app_id, revision);
        let last_rev_key = Self::last_revision_key(eventflux_app_id);
        let snapshot = snapshot.to_vec();
        let revision = revision.to_string();

        if let Some(ref runtime) = self.runtime {
            // Use dedicated runtime
            runtime.block_on(async move {
                let mut backend = backend.lock().await;

                // Prepare atomic multi-set operation
                let kvs = vec![
                    (revision_key.clone(), snapshot.clone()),
                    (last_rev_key.clone(), revision.into_bytes()),
                ];

                // Try atomic multi-set, initialize if needed
                if let Err(_) = backend.set_multi(kvs.clone()).await {
                    // Initialize and retry
                    if let Err(e) = backend.initialize().await {
                        error!("Failed to initialize Redis backend: {}", e);
                        return;
                    }
                    if let Err(e) = backend.set_multi(kvs).await {
                        error!("Failed to atomically save snapshot to Redis: {}", e);
                        return;
                    }
                }
            });
        } else {
            // We're in an async context, use spawn_blocking
            let handle = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async move {
                    let mut backend = backend.lock().await;

                    // Prepare atomic multi-set operation
                    let kvs = vec![
                        (revision_key.clone(), snapshot.clone()),
                        (last_rev_key.clone(), revision.into_bytes()),
                    ];

                    // Try atomic multi-set, initialize if needed
                    if let Err(_) = backend.set_multi(kvs.clone()).await {
                        // Initialize and retry
                        if let Err(e) = backend.initialize().await {
                            error!("Failed to initialize Redis backend: {}", e);
                            return;
                        }
                        if let Err(e) = backend.set_multi(kvs).await {
                            error!("Failed to atomically save snapshot to Redis: {}", e);
                            return;
                        }
                    }
                })
            });
            let _ = handle.join();
        }
    }

    fn load(&self, eventflux_app_id: &str, revision: &str) -> Option<Vec<u8>> {
        let backend = Arc::clone(&self.backend);
        let revision_key = Self::revision_key(eventflux_app_id, revision);

        if let Some(ref runtime) = self.runtime {
            // Use dedicated runtime
            runtime.block_on(async move {
                let mut backend = backend.lock().await;

                // Try to get data, initialize if needed
                match backend.get(&revision_key).await {
                    Ok(data) => data,
                    Err(_) => {
                        // Initialize and retry
                        if let Err(e) = backend.initialize().await {
                            error!("Failed to initialize Redis backend: {}", e);
                            return None;
                        }
                        match backend.get(&revision_key).await {
                            Ok(data) => data,
                            Err(e) => {
                                error!("Failed to load snapshot from Redis: {}", e);
                                None
                            }
                        }
                    }
                }
            })
        } else {
            // We're in an async context, use spawn_blocking
            let handle = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async move {
                    let mut backend = backend.lock().await;

                    // Try to get data, initialize if needed
                    match backend.get(&revision_key).await {
                        Ok(data) => data,
                        Err(_) => {
                            // Initialize and retry
                            if let Err(e) = backend.initialize().await {
                                error!("Failed to initialize Redis backend: {}", e);
                                return None;
                            }
                            match backend.get(&revision_key).await {
                                Ok(data) => data,
                                Err(e) => {
                                    error!("Failed to load snapshot from Redis: {}", e);
                                    None
                                }
                            }
                        }
                    }
                })
            });
            handle.join().unwrap_or(None)
        }
    }

    fn get_last_revision(&self, eventflux_app_id: &str) -> Option<String> {
        let backend = Arc::clone(&self.backend);
        let last_rev_key = Self::last_revision_key(eventflux_app_id);

        if let Some(ref runtime) = self.runtime {
            // Use dedicated runtime
            runtime.block_on(async move {
                let mut backend = backend.lock().await;

                // Try to get data, initialize if needed
                match backend.get(&last_rev_key).await {
                    Ok(Some(data)) => String::from_utf8(data).ok(),
                    Ok(None) => None,
                    Err(_) => {
                        // Initialize and retry
                        if let Err(e) = backend.initialize().await {
                            error!("Failed to initialize Redis backend: {}", e);
                            return None;
                        }
                        match backend.get(&last_rev_key).await {
                            Ok(Some(data)) => String::from_utf8(data).ok(),
                            Ok(None) => None,
                            Err(e) => {
                                error!("Failed to get last revision from Redis: {}", e);
                                None
                            }
                        }
                    }
                }
            })
        } else {
            // We're in an async context, use spawn_blocking
            let handle = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async move {
                    let mut backend = backend.lock().await;

                    // Try to get data, initialize if needed
                    match backend.get(&last_rev_key).await {
                        Ok(Some(data)) => String::from_utf8(data).ok(),
                        Ok(None) => None,
                        Err(_) => {
                            // Initialize and retry
                            if let Err(e) = backend.initialize().await {
                                error!("Failed to initialize Redis backend: {}", e);
                                return None;
                            }
                            match backend.get(&last_rev_key).await {
                                Ok(Some(data)) => String::from_utf8(data).ok(),
                                Ok(None) => None,
                                Err(e) => {
                                    error!("Failed to get last revision from Redis: {}", e);
                                    None
                                }
                            }
                        }
                    }
                })
            });
            handle.join().unwrap_or(None)
        }
    }

    fn clear_all_revisions(&self, eventflux_app_id: &str) {
        let backend = Arc::clone(&self.backend);
        let _app_pattern = format!("eventflux:app:{}:*", eventflux_app_id);

        if let Some(ref runtime) = self.runtime {
            // Use dedicated runtime
            runtime.block_on(async move {
                let mut backend = backend.lock().await;

                // Try to delete, initialize if needed
                let last_rev_key = Self::last_revision_key(eventflux_app_id);
                if let Err(_) = backend.delete(&last_rev_key).await {
                    // Initialize and retry
                    if let Err(e) = backend.initialize().await {
                        error!("Failed to initialize Redis backend: {}", e);
                        return;
                    }
                    if let Err(e) = backend.delete(&last_rev_key).await {
                        error!("Failed to delete last revision from Redis: {}", e);
                    }
                }

                // TODO: Implement pattern-based deletion for all revisions
                // This would require iterating through keys matching the pattern
            });
        } else {
            // We're in an async context, use spawn_blocking
            let eventflux_app_id = eventflux_app_id.to_string(); // Convert to owned string
            let handle = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async move {
                    let mut backend = backend.lock().await;

                    // Try to delete, initialize if needed
                    let last_rev_key = Self::last_revision_key(&eventflux_app_id);
                    if let Err(_) = backend.delete(&last_rev_key).await {
                        // Initialize and retry
                        if let Err(e) = backend.initialize().await {
                            error!("Failed to initialize Redis backend: {}", e);
                            return;
                        }
                        if let Err(e) = backend.delete(&last_rev_key).await {
                            error!("Failed to delete last revision from Redis: {}", e);
                        }
                    }

                    // TODO: Implement pattern-based deletion for all revisions
                    // This would require iterating through keys matching the pattern
                })
            });
            let _ = handle.join();
        }
    }
}

use crate::core::distributed::StateBackend;
use std::sync::Arc;
