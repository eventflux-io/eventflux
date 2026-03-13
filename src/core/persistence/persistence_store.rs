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
use aes_gcm::{Aes256Gcm, Key, Nonce};
use aes_gcm::aead::{Aead, NewAead};
use rand::Rng;
use crate::core::util::audit::{AuditEntry, AuditLogger};

/// Configuration for encryption
#[derive(Clone)]
pub struct EncryptionConfig {
    pub key: Vec<u8>, // 32 bytes for AES-256
    pub enabled: bool,
}

impl EncryptionConfig {
    pub fn new(key: Vec<u8>) -> Self {
        Self { key, enabled: true }
    }

    pub fn disabled() -> Self {
        Self { key: vec![], enabled: false }
    }
}

/// Encrypt data using AES-256-GCM
pub fn encrypt_data(data: &[u8], config: &EncryptionConfig) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    if !config.enabled {
        return Ok(data.to_vec());
    }
    let key = Key::from_slice(&config.key);
    let cipher = Aes256Gcm::new(key);
    let mut nonce_bytes = [0u8; 12];
    rand::thread_rng().fill(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher.encrypt(nonce, data)?;
    let mut result = nonce_bytes.to_vec();
    result.extend(ciphertext);
    Ok(result)
}

/// Decrypt data using AES-256-GCM
pub fn decrypt_data(data: &[u8], config: &EncryptionConfig) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    if !config.enabled {
        return Ok(data.to_vec());
    }
    if data.len() < 12 {
        return Err("Invalid encrypted data".into());
    }
    let nonce = Nonce::from_slice(&data[0..12]);
    let ciphertext = &data[12..];
    let key = Key::from_slice(&config.key);
    let cipher = Aes256Gcm::new(key);
    let plaintext = cipher.decrypt(nonce, ciphertext)?;
    Ok(plaintext)
}

/// Trait for simple persistence stores that save full snapshots.
pub trait PersistenceStore: Send + Sync {
    fn save(&self, eventflux_app_id: &str, revision: &str, snapshot: &[u8], config: &EncryptionConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn load(&self, eventflux_app_id: &str, revision: &str, config: &EncryptionConfig) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>>;
    fn get_last_revision(&self, eventflux_app_id: &str) -> Option<String>;
    fn clear_all_revisions(&self, eventflux_app_id: &str);
    fn delete_revision(&self, eventflux_app_id: &str, revision: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Trait for incremental persistence stores.
pub trait IncrementalPersistenceStore: Send + Sync {
    fn save(&self, revision: &str, snapshot: &[u8], config: &EncryptionConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn load(&self, revision: &str, config: &EncryptionConfig) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>>;
    fn get_last_revision(&self, eventflux_app_id: &str) -> Option<String>;
    fn clear_all_revisions(&self, eventflux_app_id: &str);
    fn delete_revision(&self, eventflux_app_id: &str, revision: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
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
    fn save(&self, eventflux_app_id: &str, revision: &str, snapshot: &[u8], config: &EncryptionConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let encrypted = encrypt_data(snapshot, config)?;
        let mut m = self.inner.lock().unwrap();
        let entry = m.entry(eventflux_app_id.to_string()).or_default();
        entry.insert(revision.to_string(), encrypted);
        self.last_revision
            .lock()
            .unwrap()
            .insert(eventflux_app_id.to_string(), revision.to_string());

        // Audit log
        let audit_entry = AuditEntry::new("save", &format!("{}/{}", eventflux_app_id, revision))
            .with_detail("size", &snapshot.len().to_string());
        // Assume global logger or pass in; for now, just log
        log::info!("Audit: {:?}", audit_entry);

        Ok(())
    }

    fn load(&self, eventflux_app_id: &str, revision: &str, config: &EncryptionConfig) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        let data = self.inner
            .lock()
            .unwrap()
            .get(eventflux_app_id)
            .and_then(|m| m.get(revision).cloned());
        match data {
            Some(d) => Ok(Some(decrypt_data(&d, config)?)),
            None => Ok(None),
        }
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

    fn delete_revision(&self, eventflux_app_id: &str, revision: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut m = self.inner.lock().unwrap();
        if let Some(entry) = m.get_mut(eventflux_app_id) {
            entry.remove(revision);
        }

        // Audit log
        let audit_entry = AuditEntry::new("delete", &format!("{}/{}", eventflux_app_id, revision));
        log::info!("Audit: {:?}", audit_entry);

        Ok(())
    }
}

impl IncrementalPersistenceStore for InMemoryPersistenceStore {
    fn save(&self, revision: &str, snapshot: &[u8], config: &EncryptionConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // For tests we treat incremental same as full with eventflux_app_id="default"
        <Self as PersistenceStore>::save(self, "default", revision, snapshot, config)
    }

    fn load(&self, revision: &str, config: &EncryptionConfig) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        <Self as PersistenceStore>::load(self, "default", revision, config)
    }

    fn get_last_revision(&self, eventflux_app_id: &str) -> Option<String> {
        PersistenceStore::get_last_revision(self, eventflux_app_id)
    }

    fn clear_all_revisions(&self, eventflux_app_id: &str) {
        PersistenceStore::clear_all_revisions(self, eventflux_app_id)
    }

    fn delete_revision(&self, eventflux_app_id: &str, revision: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        PersistenceStore::delete_revision(self, "default", revision)
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
    fn save(&self, eventflux_app_id: &str, revision: &str, snapshot: &[u8], config: &EncryptionConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let dir = self.base.join(eventflux_app_id);
        fs::create_dir_all(&dir)?;
        let path = self.file_path(eventflux_app_id, revision);
        let encrypted = encrypt_data(snapshot, config)?;
        fs::write(&path, encrypted)?;
        self.last_revision
            .lock()
            .unwrap()
            .insert(eventflux_app_id.to_string(), revision.to_string());
        Ok(())
    }

    fn load(&self, eventflux_app_id: &str, revision: &str, config: &EncryptionConfig) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        let path = self.file_path(eventflux_app_id, revision);
        match fs::read(&path) {
            Ok(data) => Ok(Some(decrypt_data(&data, config)?)),
            Err(_) => Ok(None),
        }
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

    fn delete_revision(&self, eventflux_app_id: &str, revision: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let path = self.file_path(eventflux_app_id, revision);
        fs::remove_file(path)?;
        Ok(())
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
    fn save(&self, eventflux_app_id: &str, revision: &str, snapshot: &[u8], config: &EncryptionConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.conn.lock().unwrap();
        let encrypted = encrypt_data(snapshot, config)?;
        conn.execute(
            "INSERT OR REPLACE INTO snapshots(app, rev, data) VALUES (?1, ?2, ?3)",
            params![eventflux_app_id, revision, &encrypted],
        )?;
        Ok(())
    }

    fn load(&self, eventflux_app_id: &str, revision: &str, config: &EncryptionConfig) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.conn.lock().unwrap();
        let data: Option<Vec<u8>> = conn.query_row(
            "SELECT data FROM snapshots WHERE app=?1 AND rev=?2",
            params![eventflux_app_id, revision],
            |row| row.get(0),
        ).ok();
        match data {
            Some(d) => Ok(Some(decrypt_data(&d, config)?)),
            None => Ok(None),
        }
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

    fn delete_revision(&self, eventflux_app_id: &str, revision: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM snapshots WHERE app=?1 AND rev=?2",
            params![eventflux_app_id, revision],
        )?;
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt_enabled() {
        let key = vec![0u8; 32]; // Dummy key for test
        let config = EncryptionConfig::new(key);
        let data = b"Hello, World!";
        let encrypted = encrypt_data(data, &config).unwrap();
        let decrypted = decrypt_data(&encrypted, &config).unwrap();
        assert_eq!(data.to_vec(), decrypted);
    }

    #[test]
    fn test_encrypt_decrypt_disabled() {
        let config = EncryptionConfig::disabled();
        let data = b"Hello, World!";
        let encrypted = encrypt_data(data, &config).unwrap();
        let decrypted = decrypt_data(&encrypted, &config).unwrap();
        assert_eq!(data.to_vec(), decrypted);
        assert_eq!(data.to_vec(), encrypted); // Should be unchanged
    }

    #[test]
    fn test_in_memory_store_with_encryption() {
        let store = InMemoryPersistenceStore::new();
        let config = EncryptionConfig::new(vec![1u8; 32]);
        let data = b"test data";
        store.save("app1", "rev1", data, &config).unwrap();
        let loaded = store.load("app1", "rev1", &config).unwrap().unwrap();
        assert_eq!(data.to_vec(), loaded);
    }

    #[test]
    fn test_delete_revision() {
        let store = InMemoryPersistenceStore::new();
        let config = EncryptionConfig::disabled();
        store.save("app1", "rev1", b"data", &config).unwrap();
        assert!(store.load("app1", "rev1", &config).unwrap().is_some());
        store.delete_revision("app1", "rev1").unwrap();
        assert!(store.load("app1", "rev1", &config).unwrap().is_none());
    }
}
