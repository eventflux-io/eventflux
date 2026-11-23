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

// src/core/persistence/incremental/persistence_backend.rs

//! Persistence backends.
//!
//! Places to store full and incremental checkpoints. The file backend writes
//! each checkpoint atomically through a temporary file and keeps a JSON metadata
//! index. The memory backend is for tests. The distributed backend is a stand in
//! that stores nothing, and the cloud backend does not exist yet.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::{IncrementalCheckpoint, PersistenceBackend, PersistenceBackendConfig};
use crate::core::persistence::state_holder::{CheckpointId, StateError, StateSnapshot};
use crate::core::util::{from_bytes, to_bytes};

/// Stores each checkpoint as `checkpoint_<id>.ckpt` in one folder.
///
/// Files are written to a `.tmp` file and then renamed, so a reader never sees a
/// half written checkpoint. An index of checkpoint metadata is kept in
/// `checkpoint_metadata.json` and rewritten after every store or cleanup.
pub struct FilePersistenceBackend {
    base_path: PathBuf,

    /// fsync checkpoint and metadata files after writing.
    sync_writes: bool,

    /// In memory copy of the metadata index.
    metadata_cache: Arc<RwLock<HashMap<CheckpointId, CheckpointMetadata>>>,

    /// One lock per checkpoint id, so two writes of the same id cannot overlap.
    /// Entries are never removed.
    file_locks: Arc<Mutex<HashMap<CheckpointId, Arc<Mutex<()>>>>>,

    stats: Arc<Mutex<BackendStatistics>>,
}

/// In memory backend, for tests. `cleanup_checkpoints` removes nothing.
pub struct MemoryPersistenceBackend {
    full_checkpoints: Arc<RwLock<HashMap<CheckpointId, StateSnapshot>>>,

    incremental_checkpoints: Arc<RwLock<HashMap<CheckpointId, IncrementalCheckpoint>>>,

    stats: Arc<Mutex<BackendStatistics>>,
}

/// Stand in for an etcd or Consul backend.
///
/// Stores nothing. A store only prints a message and returns a location, a load
/// always returns `CheckpointNotFound`, and the list is always empty.
#[allow(dead_code)]
pub struct DistributedPersistenceBackend {
    /// Cluster endpoints. The first one is used in returned locations.
    endpoints: Vec<String>,

    /// Key prefix for checkpoints.
    prefix: String,

    replication_factor: usize,

    /// Copy of the endpoints. No real connections are made.
    _connection_pool: Vec<String>,

    stats: Arc<Mutex<BackendStatistics>>,
}

/// Index entry for one stored checkpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct CheckpointMetadata {
    checkpoint_id: CheckpointId,

    checkpoint_type: CheckpointType,

    /// When it was stored, in seconds since the Unix epoch.
    created_at: u64,

    /// Snapshot data size, or the incremental `total_size`, in bytes.
    size_bytes: usize,

    /// File path.
    storage_location: String,

    /// Snapshot checksum. Always 0 for incremental checkpoints.
    checksum: u64,

    /// Checkpoints this one builds on. Empty for full checkpoints.
    dependencies: Vec<CheckpointId>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum CheckpointType {
    Full,
    Incremental { base: CheckpointId },
}

/// Backend counters.
///
/// `failed_operations` and `cache_hit_rate` are never updated. The memory
/// backend does not track latency.
#[derive(Debug, Default)]
#[allow(dead_code)]
struct BackendStatistics {
    pub total_stored: u64,

    pub total_loaded: u64,

    pub bytes_written: u64,

    pub bytes_read: u64,

    pub avg_write_latency_ms: f64,

    pub avg_read_latency_ms: f64,

    pub failed_operations: u64,

    pub cache_hit_rate: f64,
}

/// What a checkpoint file holds.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct CheckpointFile {
    /// File format version, `CHECKPOINT_FILE_VERSION`.
    version: u32,

    metadata: CheckpointMetadata,

    payload: CheckpointPayload,

    /// Always the fixed value `0x12345678`, checked on read. Not a real checksum,
    /// so it does not catch corruption.
    file_checksum: u64,
}

/// A full snapshot or an incremental checkpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum CheckpointPayload {
    Full(StateSnapshot),
    Incremental(IncrementalCheckpoint),
}

const CHECKPOINT_FILE_VERSION: u32 = 1;
const METADATA_FILE_NAME: &str = "checkpoint_metadata.json";

impl FilePersistenceBackend {
    /// Create a backend in `base_path`, creating the folder if needed, and load
    /// any existing metadata index.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStateData` if the folder cannot be created or the index
    /// cannot be read, and `DeserializationError` if the index is not valid JSON.
    pub fn new<P: AsRef<Path>>(base_path: P, sync_writes: bool) -> Result<Self, StateError> {
        let base_path = base_path.as_ref().to_path_buf();

        std::fs::create_dir_all(&base_path).map_err(|e| StateError::InvalidStateData {
            message: format!("Failed to create checkpoint directory: {e}"),
        })?;

        let backend = Self {
            base_path,
            sync_writes,
            metadata_cache: Arc::new(RwLock::new(HashMap::new())),
            file_locks: Arc::new(Mutex::new(HashMap::new())),
            stats: Arc::new(Mutex::new(BackendStatistics::default())),
        };

        backend.load_metadata_cache()?;

        Ok(backend)
    }

    /// Load the metadata index from disk. A missing or empty file leaves the
    /// cache empty.
    fn load_metadata_cache(&self) -> Result<(), StateError> {
        let metadata_path = self.base_path.join(METADATA_FILE_NAME);

        if !metadata_path.exists() {
            return Ok(());
        }

        let mut file = File::open(&metadata_path).map_err(|e| StateError::InvalidStateData {
            message: format!("Failed to open metadata file: {e}"),
        })?;

        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .map_err(|e| StateError::InvalidStateData {
                message: format!("Failed to read metadata file: {e}"),
            })?;

        if contents.trim().is_empty() {
            return Ok(());
        }

        let metadata_map: HashMap<CheckpointId, CheckpointMetadata> =
            serde_json::from_str(&contents).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to parse metadata: {e}"),
            })?;

        let mut cache = self.metadata_cache.write().unwrap();
        *cache = metadata_map;

        Ok(())
    }

    /// Write the metadata index to disk as pretty JSON.
    ///
    /// Overwrites the file in place, not through a temporary file, so a crash
    /// mid write can leave it truncated.
    fn save_metadata_cache(&self) -> Result<(), StateError> {
        let metadata_path = self.base_path.join(METADATA_FILE_NAME);

        let cache = self.metadata_cache.read().unwrap();
        let json =
            serde_json::to_string_pretty(&*cache).map_err(|e| StateError::SerializationError {
                message: format!("Failed to serialize metadata: {e}"),
            })?;

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&metadata_path)
            .map_err(|e| StateError::InvalidStateData {
                message: format!("Failed to create metadata file: {e}"),
            })?;

        file.write_all(json.as_bytes())
            .map_err(|e| StateError::InvalidStateData {
                message: format!("Failed to write metadata: {e}"),
            })?;

        if self.sync_writes {
            file.sync_all().map_err(|e| StateError::InvalidStateData {
                message: format!("Failed to sync metadata file: {e}"),
            })?;
        }

        Ok(())
    }

    fn checkpoint_file_path(&self, checkpoint_id: CheckpointId) -> PathBuf {
        self.base_path
            .join(format!("checkpoint_{checkpoint_id}.ckpt"))
    }

    /// Write a checkpoint file atomically.
    ///
    /// Writes to a `.tmp` file next to it, flushes, fsyncs when `sync_writes` is
    /// set, then renames it over the target.
    fn write_checkpoint_file(
        &self,
        file_path: &Path,
        checkpoint_file: &CheckpointFile,
    ) -> Result<(), StateError> {
        let temp_path = file_path.with_extension("tmp");

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .map_err(|e| StateError::InvalidStateData {
                message: format!("Failed to create checkpoint file: {e}"),
            })?;

        let mut writer = BufWriter::new(file);

        let data = to_bytes(checkpoint_file).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize checkpoint: {e}"),
        })?;

        writer
            .write_all(&data)
            .map_err(|e| StateError::InvalidStateData {
                message: format!("Failed to write checkpoint data: {e}"),
            })?;

        writer.flush().map_err(|e| StateError::InvalidStateData {
            message: format!("Failed to flush checkpoint data: {e}"),
        })?;

        if self.sync_writes {
            writer
                .get_ref()
                .sync_all()
                .map_err(|e| StateError::InvalidStateData {
                    message: format!("Failed to sync checkpoint file: {e}"),
                })?;
        }

        drop(writer);

        // The rename is atomic, so readers see the old file or the whole new one.
        std::fs::rename(&temp_path, file_path).map_err(|e| StateError::InvalidStateData {
            message: format!("Failed to rename checkpoint file: {e}"),
        })?;

        Ok(())
    }

    /// Read and deserialize a checkpoint file.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStateData` if the file cannot be read,
    /// `DeserializationError` if it does not parse, and `ChecksumMismatch` if
    /// `file_checksum` is not the fixed value.
    fn read_checkpoint_file(&self, file_path: &Path) -> Result<CheckpointFile, StateError> {
        let file = File::open(file_path).map_err(|e| StateError::InvalidStateData {
            message: format!("Failed to open checkpoint file: {e}"),
        })?;

        let mut reader = BufReader::new(file);
        let mut data = Vec::new();

        reader
            .read_to_end(&mut data)
            .map_err(|e| StateError::InvalidStateData {
                message: format!("Failed to read checkpoint file: {e}"),
            })?;

        let checkpoint_file: CheckpointFile =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize checkpoint: {e}"),
            })?;

        // Only checks the fixed marker value. This is not a real integrity check.
        if checkpoint_file.file_checksum != 0x12345678 {
            return Err(StateError::ChecksumMismatch);
        }

        Ok(checkpoint_file)
    }

    /// Returns a `DefaultHasher` hash of `data`. Not used yet.
    #[allow(dead_code)]
    fn calculate_file_checksum(&self, data: &[u8]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        data.hash(&mut hasher);
        hasher.finish()
    }

    /// Returns the lock for a checkpoint id, creating it on first use.
    fn get_file_lock(&self, checkpoint_id: CheckpointId) -> Arc<Mutex<()>> {
        let mut locks = self.file_locks.lock().unwrap();
        locks
            .entry(checkpoint_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

impl PersistenceBackend for FilePersistenceBackend {
    fn store_full_checkpoint(
        &self,
        checkpoint_id: CheckpointId,
        snapshot: &StateSnapshot,
    ) -> Result<String, StateError> {
        let start_time = Instant::now();
        let file_lock = self.get_file_lock(checkpoint_id);
        let _lock = file_lock.lock().unwrap();

        let file_path = self.checkpoint_file_path(checkpoint_id);

        let metadata = CheckpointMetadata {
            checkpoint_id,
            checkpoint_type: CheckpointType::Full,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            size_bytes: snapshot.data.len(),
            storage_location: file_path.to_string_lossy().to_string(),
            checksum: snapshot.checksum,
            dependencies: Vec::new(),
        };

        let checkpoint_file = CheckpointFile {
            version: CHECKPOINT_FILE_VERSION,
            metadata: metadata.clone(),
            payload: CheckpointPayload::Full(snapshot.clone()),
            file_checksum: 0, // replaced below
        };

        // A fixed marker stands in for a real checksum of the content.
        let final_checkpoint_file = CheckpointFile {
            file_checksum: 0x12345678, // fixed marker, not a checksum
            ..checkpoint_file
        };

        self.write_checkpoint_file(&file_path, &final_checkpoint_file)?;

        {
            let mut cache = self.metadata_cache.write().unwrap();
            cache.insert(checkpoint_id, metadata);
        }

        self.save_metadata_cache()?;

        {
            let mut stats = self.stats.lock().unwrap();
            stats.total_stored += 1;
            stats.bytes_written += snapshot.data.len() as u64;
            let latency = start_time.elapsed().as_millis() as f64;
            stats.avg_write_latency_ms =
                (stats.avg_write_latency_ms * (stats.total_stored - 1) as f64 + latency)
                    / stats.total_stored as f64;
        }

        Ok(file_path.to_string_lossy().to_string())
    }

    fn store_incremental_checkpoint(
        &self,
        checkpoint: &IncrementalCheckpoint,
    ) -> Result<String, StateError> {
        let start_time = Instant::now();
        let file_lock = self.get_file_lock(checkpoint.checkpoint_id);
        let _lock = file_lock.lock().unwrap();

        let file_path = self.checkpoint_file_path(checkpoint.checkpoint_id);

        let metadata = CheckpointMetadata {
            checkpoint_id: checkpoint.checkpoint_id,
            checkpoint_type: CheckpointType::Incremental {
                base: checkpoint.base_checkpoint_id,
            },
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            size_bytes: checkpoint.total_size,
            storage_location: file_path.to_string_lossy().to_string(),
            checksum: 0, // not computed for incremental checkpoints
            dependencies: vec![checkpoint.base_checkpoint_id],
        };

        let checkpoint_file = CheckpointFile {
            version: CHECKPOINT_FILE_VERSION,
            metadata: metadata.clone(),
            payload: CheckpointPayload::Incremental(checkpoint.clone()),
            file_checksum: 0,
        };

        // A fixed marker stands in for a real checksum of the content.
        let final_checkpoint_file = CheckpointFile {
            file_checksum: 0x12345678, // fixed marker, not a checksum
            ..checkpoint_file
        };

        self.write_checkpoint_file(&file_path, &final_checkpoint_file)?;

        {
            let mut cache = self.metadata_cache.write().unwrap();
            cache.insert(checkpoint.checkpoint_id, metadata);
        }

        self.save_metadata_cache()?;

        {
            let mut stats = self.stats.lock().unwrap();
            stats.total_stored += 1;
            stats.bytes_written += checkpoint.total_size as u64;
            let latency = start_time.elapsed().as_millis() as f64;
            stats.avg_write_latency_ms =
                (stats.avg_write_latency_ms * (stats.total_stored - 1) as f64 + latency)
                    / stats.total_stored as f64;
        }

        Ok(file_path.to_string_lossy().to_string())
    }

    fn load_full_checkpoint(
        &self,
        checkpoint_id: CheckpointId,
    ) -> Result<StateSnapshot, StateError> {
        let start_time = Instant::now();
        let file_path = self.checkpoint_file_path(checkpoint_id);

        let checkpoint_file = self.read_checkpoint_file(&file_path)?;

        match checkpoint_file.payload {
            CheckpointPayload::Full(snapshot) => {
                {
                    let mut stats = self.stats.lock().unwrap();
                    stats.total_loaded += 1;
                    stats.bytes_read += snapshot.data.len() as u64;
                    let latency = start_time.elapsed().as_millis() as f64;
                    stats.avg_read_latency_ms =
                        (stats.avg_read_latency_ms * (stats.total_loaded - 1) as f64 + latency)
                            / stats.total_loaded as f64;
                }

                Ok(snapshot)
            }
            CheckpointPayload::Incremental(_) => Err(StateError::InvalidStateData {
                message: format!("Checkpoint {checkpoint_id} is incremental, not full"),
            }),
        }
    }

    fn load_incremental_checkpoint(
        &self,
        checkpoint_id: CheckpointId,
    ) -> Result<IncrementalCheckpoint, StateError> {
        let start_time = Instant::now();
        let file_path = self.checkpoint_file_path(checkpoint_id);

        let checkpoint_file = self.read_checkpoint_file(&file_path)?;

        match checkpoint_file.payload {
            CheckpointPayload::Incremental(checkpoint) => {
                {
                    let mut stats = self.stats.lock().unwrap();
                    stats.total_loaded += 1;
                    stats.bytes_read += checkpoint.total_size as u64;
                    let latency = start_time.elapsed().as_millis() as f64;
                    stats.avg_read_latency_ms =
                        (stats.avg_read_latency_ms * (stats.total_loaded - 1) as f64 + latency)
                            / stats.total_loaded as f64;
                }

                Ok(checkpoint)
            }
            CheckpointPayload::Full(_) => Err(StateError::InvalidStateData {
                message: format!("Checkpoint {checkpoint_id} is full, not incremental"),
            }),
        }
    }

    fn list_checkpoints(&self) -> Result<Vec<CheckpointId>, StateError> {
        let cache = self.metadata_cache.read().unwrap();
        let mut checkpoint_ids: Vec<_> = cache.keys().copied().collect();
        checkpoint_ids.sort();
        Ok(checkpoint_ids)
    }

    /// Delete checkpoints stored at or before `before` and return how many files
    /// were deleted.
    ///
    /// Compares in whole seconds, so a checkpoint stored in the same second as
    /// the cutoff is also deleted. Does not check whether other checkpoints still
    /// build on a deleted one.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStateData` if a file cannot be deleted, which stops the
    /// cleanup part way, or the index cannot be saved.
    fn cleanup_checkpoints(&self, before: Instant) -> Result<usize, StateError> {
        // Turn the Instant into epoch seconds to compare with metadata.created_at.
        let now = Instant::now();
        let now_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let before_timestamp = if now >= before {
            now_epoch.saturating_sub((now - before).as_secs())
        } else {
            now_epoch + (before - now).as_secs()
        };
        let mut removed_count = 0;

        let mut to_remove = Vec::new();

        {
            let cache = self.metadata_cache.read().unwrap();
            for (checkpoint_id, metadata) in cache.iter() {
                // Use <= because created_at is in whole seconds. With < a
                // checkpoint stored less than a second before the cutoff would be
                // missed.
                if metadata.created_at <= before_timestamp {
                    to_remove.push(*checkpoint_id);
                }
            }
        }

        for checkpoint_id in to_remove {
            let file_path = self.checkpoint_file_path(checkpoint_id);

            if file_path.exists() {
                std::fs::remove_file(&file_path).map_err(|e| StateError::InvalidStateData {
                    message: format!("Failed to remove checkpoint file: {e}"),
                })?;
                removed_count += 1;
            }

            // Dropped from the index even when the file was already gone.
            let mut cache = self.metadata_cache.write().unwrap();
            cache.remove(&checkpoint_id);
        }

        // Only saved when a file was deleted, so index entries for files that
        // were already gone are dropped from memory but stay on disk.
        if removed_count > 0 {
            self.save_metadata_cache()?;
        }

        Ok(removed_count)
    }
}

impl Default for MemoryPersistenceBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryPersistenceBackend {
    pub fn new() -> Self {
        Self {
            full_checkpoints: Arc::new(RwLock::new(HashMap::new())),
            incremental_checkpoints: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(Mutex::new(BackendStatistics::default())),
        }
    }
}

impl PersistenceBackend for MemoryPersistenceBackend {
    fn store_full_checkpoint(
        &self,
        checkpoint_id: CheckpointId,
        snapshot: &StateSnapshot,
    ) -> Result<String, StateError> {
        let mut checkpoints = self.full_checkpoints.write().unwrap();
        checkpoints.insert(checkpoint_id, snapshot.clone());

        {
            let mut stats = self.stats.lock().unwrap();
            stats.total_stored += 1;
            stats.bytes_written += snapshot.data.len() as u64;
        }

        Ok(format!("memory://full/{checkpoint_id}"))
    }

    fn store_incremental_checkpoint(
        &self,
        checkpoint: &IncrementalCheckpoint,
    ) -> Result<String, StateError> {
        let mut checkpoints = self.incremental_checkpoints.write().unwrap();
        checkpoints.insert(checkpoint.checkpoint_id, checkpoint.clone());

        {
            let mut stats = self.stats.lock().unwrap();
            stats.total_stored += 1;
            stats.bytes_written += checkpoint.total_size as u64;
        }

        Ok(format!("memory://incremental/{}", checkpoint.checkpoint_id))
    }

    fn load_full_checkpoint(
        &self,
        checkpoint_id: CheckpointId,
    ) -> Result<StateSnapshot, StateError> {
        let checkpoints = self.full_checkpoints.read().unwrap();

        match checkpoints.get(&checkpoint_id) {
            Some(snapshot) => {
                {
                    let mut stats = self.stats.lock().unwrap();
                    stats.total_loaded += 1;
                    stats.bytes_read += snapshot.data.len() as u64;
                }

                Ok(snapshot.clone())
            }
            None => Err(StateError::CheckpointNotFound { checkpoint_id }),
        }
    }

    fn load_incremental_checkpoint(
        &self,
        checkpoint_id: CheckpointId,
    ) -> Result<IncrementalCheckpoint, StateError> {
        let checkpoints = self.incremental_checkpoints.read().unwrap();

        match checkpoints.get(&checkpoint_id) {
            Some(checkpoint) => {
                {
                    let mut stats = self.stats.lock().unwrap();
                    stats.total_loaded += 1;
                    stats.bytes_read += checkpoint.total_size as u64;
                }

                Ok(checkpoint.clone())
            }
            None => Err(StateError::CheckpointNotFound { checkpoint_id }),
        }
    }

    fn list_checkpoints(&self) -> Result<Vec<CheckpointId>, StateError> {
        let full_checkpoints = self.full_checkpoints.read().unwrap();
        let incremental_checkpoints = self.incremental_checkpoints.read().unwrap();

        let mut all_ids = Vec::new();
        all_ids.extend(full_checkpoints.keys());
        all_ids.extend(incremental_checkpoints.keys());
        all_ids.sort();
        all_ids.dedup();

        Ok(all_ids.into_iter().copied().collect())
    }

    fn cleanup_checkpoints(&self, _before: Instant) -> Result<usize, StateError> {
        // No cleanup yet. Could evict by age, least recent use, or total size.
        Ok(0)
    }
}

impl DistributedPersistenceBackend {
    /// Create the stand in backend. No connection is made.
    pub fn new(endpoints: Vec<String>, prefix: String, replication_factor: usize) -> Self {
        Self {
            endpoints: endpoints.clone(),
            prefix,
            replication_factor,
            _connection_pool: endpoints, // no real pool yet
            stats: Arc::new(Mutex::new(BackendStatistics::default())),
        }
    }
}

// Every method is a stand in. A real version would talk to etcd, Consul or
// similar. The store methods index `endpoints[0]` and panic if no endpoints
// were given.
impl PersistenceBackend for DistributedPersistenceBackend {
    fn store_full_checkpoint(
        &self,
        checkpoint_id: CheckpointId,
        _snapshot: &StateSnapshot,
    ) -> Result<String, StateError> {
        // Nothing is stored.
        println!("Storing full checkpoint {checkpoint_id} to distributed backend");

        Ok(format!(
            "distributed://{}/{}/full/{}",
            self.endpoints[0], self.prefix, checkpoint_id
        ))
    }

    fn store_incremental_checkpoint(
        &self,
        checkpoint: &IncrementalCheckpoint,
    ) -> Result<String, StateError> {
        // Nothing is stored.
        println!(
            "Storing incremental checkpoint {} to distributed backend",
            checkpoint.checkpoint_id
        );

        Ok(format!(
            "distributed://{}/{}/incremental/{}",
            self.endpoints[0], self.prefix, checkpoint.checkpoint_id
        ))
    }

    fn load_full_checkpoint(
        &self,
        checkpoint_id: CheckpointId,
    ) -> Result<StateSnapshot, StateError> {
        Err(StateError::CheckpointNotFound { checkpoint_id })
    }

    fn load_incremental_checkpoint(
        &self,
        checkpoint_id: CheckpointId,
    ) -> Result<IncrementalCheckpoint, StateError> {
        Err(StateError::CheckpointNotFound { checkpoint_id })
    }

    fn list_checkpoints(&self) -> Result<Vec<CheckpointId>, StateError> {
        Ok(Vec::new())
    }

    fn cleanup_checkpoints(&self, _before: Instant) -> Result<usize, StateError> {
        Ok(0)
    }
}

/// Create a backend from settings.
///
/// # Errors
///
/// Returns `InvalidStateData` for `Cloud`, which is not built yet. Also returns
/// any error from creating the file backend.
pub fn create_backend(
    config: &PersistenceBackendConfig,
) -> Result<Box<dyn PersistenceBackend>, StateError> {
    match config {
        PersistenceBackendConfig::LocalFile {
            base_path,
            sync_writes,
        } => Ok(Box::new(FilePersistenceBackend::new(
            base_path,
            *sync_writes,
        )?)),
        PersistenceBackendConfig::Distributed {
            endpoints,
            prefix,
            replication_factor,
        } => Ok(Box::new(DistributedPersistenceBackend::new(
            endpoints.clone(),
            prefix.clone(),
            *replication_factor,
        ))),
        PersistenceBackendConfig::Cloud { .. } => Err(StateError::InvalidStateData {
            message: "Cloud persistence backend not yet implemented".to_string(),
        }),
        PersistenceBackendConfig::Memory => Ok(Box::new(MemoryPersistenceBackend::new())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::persistence::state_holder::{CompressionType, SchemaVersion, StateMetadata};
    use tempfile::TempDir;

    fn create_test_snapshot(checkpoint_id: CheckpointId) -> StateSnapshot {
        let data = vec![1, 2, 3, 4, 5];
        let checksum = StateSnapshot::calculate_checksum(&data);
        StateSnapshot {
            version: SchemaVersion::new(1, 0, 0),
            checkpoint_id,
            data,
            compression: CompressionType::None,
            checksum,
            metadata: StateMetadata::new("test".to_string(), "TestComponent".to_string()),
        }
    }

    fn create_test_incremental(
        checkpoint_id: CheckpointId,
        base_id: CheckpointId,
    ) -> IncrementalCheckpoint {
        IncrementalCheckpoint {
            checkpoint_id,
            base_checkpoint_id: base_id,
            sequence_number: 1,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            component_changes: HashMap::new(),
            total_size: 100,
            compression_ratio: 1.0,
            wal_offset_range: (0, 10),
        }
    }

    #[test]
    fn test_memory_backend() {
        let backend = MemoryPersistenceBackend::new();
        let snapshot = create_test_snapshot(1);

        // Store checkpoint
        let location = backend.store_full_checkpoint(1, &snapshot).unwrap();
        assert!(location.contains("memory://"));

        // Load checkpoint
        let loaded = backend.load_full_checkpoint(1).unwrap();
        assert_eq!(loaded.checkpoint_id, snapshot.checkpoint_id);
        assert_eq!(loaded.data, snapshot.data);

        // List checkpoints
        let checkpoints = backend.list_checkpoints().unwrap();
        assert_eq!(checkpoints, vec![1]);
    }

    #[test]
    fn test_file_backend() {
        let temp_dir = TempDir::new().unwrap();
        let backend = FilePersistenceBackend::new(temp_dir.path(), true).unwrap();

        let snapshot = create_test_snapshot(1);

        // Store checkpoint
        let location = backend.store_full_checkpoint(1, &snapshot).unwrap();
        assert!(location.contains("checkpoint_1.ckpt"));

        // Load checkpoint
        let loaded = backend.load_full_checkpoint(1).unwrap();
        assert_eq!(loaded.checkpoint_id, snapshot.checkpoint_id);
        assert_eq!(loaded.data, snapshot.data);

        // List checkpoints
        let checkpoints = backend.list_checkpoints().unwrap();
        assert_eq!(checkpoints, vec![1]);
    }

    #[test]
    fn test_file_backend_incremental() {
        let temp_dir = TempDir::new().unwrap();
        let backend = FilePersistenceBackend::new(temp_dir.path(), true).unwrap();

        let incremental = create_test_incremental(2, 1);

        // Store incremental checkpoint
        let location = backend.store_incremental_checkpoint(&incremental).unwrap();
        assert!(location.contains("checkpoint_2.ckpt"));

        // Load incremental checkpoint
        let loaded = backend.load_incremental_checkpoint(2).unwrap();
        assert_eq!(loaded.checkpoint_id, incremental.checkpoint_id);
        assert_eq!(loaded.base_checkpoint_id, incremental.base_checkpoint_id);
    }

    #[test]
    fn test_file_backend_persistence() {
        let temp_dir = TempDir::new().unwrap();

        // Create backend and store checkpoint
        {
            let backend = FilePersistenceBackend::new(temp_dir.path(), true).unwrap();
            let snapshot = create_test_snapshot(1);
            backend.store_full_checkpoint(1, &snapshot).unwrap();
        }

        // Create new backend instance and verify persistence
        {
            let backend = FilePersistenceBackend::new(temp_dir.path(), true).unwrap();
            let checkpoints = backend.list_checkpoints().unwrap();
            assert_eq!(checkpoints, vec![1]);

            let loaded = backend.load_full_checkpoint(1).unwrap();
            assert_eq!(loaded.checkpoint_id, 1);
        }
    }

    #[test]
    fn test_backend_factory() {
        let memory_config = PersistenceBackendConfig::Memory;
        let backend = create_backend(&memory_config).unwrap();

        let snapshot = create_test_snapshot(1);
        let location = backend.store_full_checkpoint(1, &snapshot).unwrap();
        assert!(location.contains("memory://"));
    }

    #[test]
    fn test_file_backend_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let backend = FilePersistenceBackend::new(temp_dir.path(), true).unwrap();

        // Store some checkpoints
        for i in 1..=3 {
            let snapshot = create_test_snapshot(i);
            backend.store_full_checkpoint(i, &snapshot).unwrap();
        }

        // Put the cutoff a little after the stores. Times are in whole seconds,
        // so this relies on the <= comparison in cleanup.
        std::thread::sleep(std::time::Duration::from_millis(10));
        let cutoff = Instant::now();

        // Every checkpoint was stored before the cutoff, so all are removed.
        let removed = backend.cleanup_checkpoints(cutoff).unwrap();
        assert_eq!(removed, 3);

        let remaining = backend.list_checkpoints().unwrap();
        assert!(remaining.is_empty());
    }
}
