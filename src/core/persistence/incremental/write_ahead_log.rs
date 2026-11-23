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

// src/core/persistence/incremental/write_ahead_log.rs

//! Write ahead log (WAL).
//!
//! An append only log of state changes, written before the changes are
//! checkpointed so they can be replayed after a crash. The log is split into
//! segment files named `wal-<id>.log` in one folder. Each entry is stored as a 4
//! byte little endian length followed by the serialized entry, and carries a
//! header and a checksum.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use super::{LogEntry, LogOffset, WriteAheadLog};
use crate::core::persistence::state_holder::StateError;
use crate::core::util::to_bytes;

/// Write ahead log split into segment files.
///
/// Offsets count entries, not bytes. The first entry is offset 0.
pub struct SegmentedWAL {
    /// Folder holding the segment files.
    base_dir: PathBuf,

    /// Size in bytes at which the active segment is closed and a new one started.
    /// Checked before each write, so a segment can grow past it by one entry.
    segment_size: usize,

    /// Segment that new entries go to.
    active_segment: Arc<Mutex<WALSegment>>,

    /// Closed segments, keyed by segment id.
    completed_segments: Arc<RwLock<HashMap<u64, WALSegment>>>,

    /// Offset the next entry will get.
    global_offset: Arc<Mutex<LogOffset>>,

    /// Id the next segment will get.
    segment_sequence: Arc<Mutex<u64>>,

    config: WALConfig,

    metrics: Arc<Mutex<WALMetrics>>,
}

/// WAL settings.
#[derive(Debug, Clone)]
pub struct WALConfig {
    /// Call fsync on the active segment after each batch. Default true.
    pub enable_sync: bool,

    /// Write buffer size in bytes. Default 64 KB. Not applied yet.
    pub buffer_size: usize,

    /// Most entries per batch. Default 1000. Not enforced yet.
    pub max_batch_size: usize,

    /// Closed segments to keep when cleaning up. Default 10.
    pub retention_segments: usize,

    /// Default false. Not applied yet.
    pub enable_compression: bool,
}

impl Default for WALConfig {
    fn default() -> Self {
        Self {
            enable_sync: true,
            buffer_size: 64 * 1024, // 64 KB
            max_batch_size: 1000,
            retention_segments: 10,
            enable_compression: false,
        }
    }
}

/// One WAL segment file.
pub struct WALSegment {
    pub segment_id: u64,

    /// Buffered writer. Writes may sit in the buffer until it is flushed.
    file: BufWriter<File>,

    /// Size in bytes, including length prefixes.
    current_size: usize,

    entry_count: usize,

    /// Offset of the first entry in this segment.
    start_offset: LogOffset,

    /// Offset after the last entry in this segment.
    end_offset: LogOffset,

    file_path: PathBuf,

    /// When true, the segment rejects writes.
    is_sealed: bool,
}

/// WAL counters.
///
/// Only `total_entries`, `avg_batch_size`, `sync_count` and `last_sync` are
/// updated. The rest stay at zero.
#[derive(Debug, Default)]
pub struct WALMetrics {
    pub total_entries: u64,

    pub total_bytes: u64,

    /// Average entries per batch.
    pub avg_batch_size: f64,

    /// Entries per second.
    pub write_throughput: f64,

    /// Calls to `sync`. Syncs done inside `append_batch` are not counted.
    pub sync_count: u64,

    pub active_segments: usize,

    /// Time of the last `sync` call, in seconds since the Unix epoch.
    pub last_sync: u64,
}

/// An entry as stored on disk.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SerializedLogEntry {
    pub header: EntryHeader,

    /// The serialized `LogEntry`.
    pub payload: Vec<u8>,

    /// Checksum of `payload`.
    pub checksum: u32,
}

/// Header stored with each entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct EntryHeader {
    /// Must equal `WAL_MAGIC`. Used to spot data that is not a WAL entry.
    pub magic: u32,

    /// Must equal `WAL_VERSION`.
    pub version: u8,

    /// Payload size in bytes.
    pub payload_length: u32,

    /// Write time in microseconds since the Unix epoch.
    pub timestamp: u64,

    /// Hash of the component id.
    pub component_hash: u64,

    pub sequence: u64,
}

const WAL_MAGIC: u32 = 0x57414C00; // "WAL\0"
const WAL_VERSION: u8 = 1;

impl SegmentedWAL {
    /// Create an empty WAL in `base_dir`, creating the folder if needed.
    ///
    /// Starts segment `wal-0000000000.log`. If that file already exists, it is
    /// opened for append but its old entries are not counted. Use
    /// [`recover`](Self::recover) to reopen an existing log.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStateData` if the folder or the first segment file cannot
    /// be created.
    pub fn new<P: AsRef<Path>>(
        base_dir: P,
        segment_size: usize,
        config: WALConfig,
    ) -> Result<Self, StateError> {
        let base_dir = base_dir.as_ref().to_path_buf();

        std::fs::create_dir_all(&base_dir).map_err(|e| StateError::InvalidStateData {
            message: format!("Failed to create WAL directory: {e}"),
        })?;

        let segment_id = 0;
        let segment_path = base_dir.join(format!("wal-{segment_id:010}.log"));

        let active_segment = WALSegment::new(segment_id, &segment_path, 0)?;

        Ok(Self {
            base_dir,
            segment_size,
            active_segment: Arc::new(Mutex::new(active_segment)),
            completed_segments: Arc::new(RwLock::new(HashMap::new())),
            global_offset: Arc::new(Mutex::new(0)),
            segment_sequence: Arc::new(Mutex::new(1)),
            config,
            metrics: Arc::new(Mutex::new(WALMetrics::default())),
        })
    }

    /// Reopen a WAL from the segment files in `base_dir`.
    ///
    /// Segments are ordered by id. The last one becomes the active segment and
    /// the others are closed segments. Offsets are rebuilt by counting entries.
    /// Falls back to [`new`](Self::new) when the folder or the segments are
    /// missing.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStateData` if the folder or a segment file cannot be read.
    pub fn recover<P: AsRef<Path>>(
        base_dir: P,
        segment_size: usize,
        config: WALConfig,
    ) -> Result<Self, StateError> {
        let base_dir = base_dir.as_ref().to_path_buf();

        if !base_dir.exists() {
            return Self::new(base_dir, segment_size, config);
        }

        let mut segments = Vec::new();
        for entry in std::fs::read_dir(&base_dir).map_err(|e| StateError::InvalidStateData {
            message: format!("Failed to read WAL directory: {e}"),
        })? {
            let entry = entry.map_err(|e| StateError::InvalidStateData {
                message: format!("Failed to read directory entry: {e}"),
            })?;

            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();

            if file_name.starts_with("wal-") && file_name.ends_with(".log") {
                if let Some(segment_id_str) = file_name
                    .strip_prefix("wal-")
                    .and_then(|s| s.strip_suffix(".log"))
                {
                    if let Ok(segment_id) = segment_id_str.parse::<u64>() {
                        segments.push((segment_id, entry.path()));
                    }
                }
            }
        }

        segments.sort_by_key(|(id, _)| *id);

        if segments.is_empty() {
            return Self::new(base_dir, segment_size, config);
        }

        let mut completed_segments = HashMap::new();
        let mut global_offset = 0;
        let mut next_segment_id = 0;

        for (segment_id, segment_path) in &segments[..segments.len().saturating_sub(1)] {
            let segment = WALSegment::recover(*segment_id, segment_path, global_offset)?;
            global_offset = segment.end_offset;
            completed_segments.insert(*segment_id, segment);
            next_segment_id = segment_id + 1;
        }

        let (last_segment_id, last_segment_path) = &segments[segments.len() - 1];
        let active_segment =
            WALSegment::recover(*last_segment_id, last_segment_path, global_offset)?;

        if active_segment.end_offset > global_offset {
            global_offset = active_segment.end_offset;
        }

        if last_segment_id + 1 > next_segment_id {
            next_segment_id = last_segment_id + 1;
        }

        Ok(Self {
            base_dir,
            segment_size,
            active_segment: Arc::new(Mutex::new(active_segment)),
            completed_segments: Arc::new(RwLock::new(completed_segments)),
            global_offset: Arc::new(Mutex::new(global_offset)),
            segment_sequence: Arc::new(Mutex::new(next_segment_id)),
            config,
            metrics: Arc::new(Mutex::new(WALMetrics::default())),
        })
    }

    /// Seal the active segment, move it to the closed segments, and start a new
    /// one at the current offset.
    fn rotate_segment(&self) -> Result<(), StateError> {
        let mut active_segment = self.active_segment.lock().unwrap();
        let mut completed_segments = self.completed_segments.write().unwrap();
        let mut segment_sequence = self.segment_sequence.lock().unwrap();

        active_segment.seal()?;

        let old_segment_id = active_segment.segment_id;

        let new_segment_id = *segment_sequence;
        *segment_sequence += 1;

        let segment_path = self.base_dir.join(format!("wal-{new_segment_id:010}.log"));
        let start_offset = *self.global_offset.lock().unwrap();
        let new_segment = WALSegment::new(new_segment_id, &segment_path, start_offset)?;

        let old_segment = std::mem::replace(&mut *active_segment, new_segment);
        completed_segments.insert(old_segment_id, old_segment);

        Ok(())
    }

    /// Delete the oldest closed segments so that at most `retention_segments`
    /// remain, and return how many files were deleted.
    ///
    /// Does not check whether the deleted entries were checkpointed. A file that
    /// fails to delete is logged to stderr and dropped from the list anyway.
    pub fn cleanup_old_segments(&self) -> Result<usize, StateError> {
        let mut completed_segments = self.completed_segments.write().unwrap();
        let retention_count = self.config.retention_segments;

        if completed_segments.len() <= retention_count {
            return Ok(0);
        }

        let mut segment_ids: Vec<_> = completed_segments.keys().copied().collect();
        segment_ids.sort();

        let segments_to_remove = segment_ids.len() - retention_count;
        let mut removed_count = 0;

        for &segment_id in &segment_ids[..segments_to_remove] {
            if let Some(segment) = completed_segments.remove(&segment_id) {
                if let Err(e) = std::fs::remove_file(&segment.file_path) {
                    eprintln!(
                        "Warning: Failed to delete segment file {:?}: {}",
                        segment.file_path, e
                    );
                } else {
                    removed_count += 1;
                }
            }
        }

        Ok(removed_count)
    }
}

impl WriteAheadLog for SegmentedWAL {
    fn append(&self, entry: LogEntry) -> Result<LogOffset, StateError> {
        let results = self.append_batch(vec![entry])?;
        Ok(results[0])
    }

    /// Write entries in order and return their offsets.
    ///
    /// Not atomic. If an entry fails, the ones before it stay written. The locks
    /// are released while rotating segments, so a concurrent batch can slip in
    /// between entries.
    fn append_batch(&self, entries: Vec<LogEntry>) -> Result<Vec<LogOffset>, StateError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::with_capacity(entries.len());
        let mut active_segment = self.active_segment.lock().unwrap();
        let mut global_offset = self.global_offset.lock().unwrap();

        let entries_len = entries.len();
        for entry in entries {
            if active_segment.current_size >= self.segment_size {
                // rotate_segment takes these locks itself.
                drop(active_segment);
                drop(global_offset);
                self.rotate_segment()?;
                active_segment = self.active_segment.lock().unwrap();
                global_offset = self.global_offset.lock().unwrap();
            }

            let serialized = self.serialize_entry(&entry)?;

            let offset = *global_offset;
            active_segment.write_entry(&serialized)?;

            *global_offset += 1;
            results.push(offset);
        }

        if self.config.enable_sync {
            active_segment.sync()?;
        }

        // Running average of entries per batch.
        {
            let mut metrics = self.metrics.lock().unwrap();
            metrics.total_entries += entries_len as u64;
            metrics.avg_batch_size = (metrics.avg_batch_size
                * (metrics.total_entries - entries_len as u64) as f64
                + entries_len as f64)
                / metrics.total_entries as f64;
        }

        Ok(results)
    }

    /// Read up to `limit` entries starting at `offset`.
    ///
    /// Reads closed segments first, then the active segment. Closed segments
    /// are visited in map order, not by offset, so a read that spans several
    /// closed segments may stop early. Entries still in the write buffer are not
    /// seen.
    fn read_from(&self, offset: LogOffset, limit: usize) -> Result<Vec<LogEntry>, StateError> {
        let mut results = Vec::new();
        let mut current_offset = offset;

        {
            let completed_segments = self.completed_segments.read().unwrap();

            for (_, segment) in completed_segments.iter() {
                if current_offset >= segment.start_offset && current_offset < segment.end_offset {
                    let segment_entries =
                        segment.read_entries_from(current_offset, limit - results.len())?;
                    for entry in segment_entries {
                        results.push(self.deserialize_entry(&entry)?);
                        current_offset += 1;

                        if results.len() >= limit {
                            return Ok(results);
                        }
                    }
                }
            }
        }

        if results.len() < limit {
            let active_segment = self.active_segment.lock().unwrap();
            if current_offset >= active_segment.start_offset {
                let segment_entries =
                    active_segment.read_entries_from(current_offset, limit - results.len())?;
                for entry in segment_entries {
                    results.push(self.deserialize_entry(&entry)?);
                }
            }
        }

        Ok(results)
    }

    fn tail_offset(&self) -> Result<LogOffset, StateError> {
        let global_offset = self.global_offset.lock().unwrap();
        Ok(*global_offset)
    }

    /// Delete every closed segment whose entries all sit before `offset`. The
    /// active segment is never trimmed. Delete failures are logged to stderr.
    fn trim_to(&self, offset: LogOffset) -> Result<(), StateError> {
        let mut completed_segments = self.completed_segments.write().unwrap();
        let mut segments_to_remove = Vec::new();

        for (&segment_id, segment) in completed_segments.iter() {
            if segment.end_offset <= offset {
                segments_to_remove.push(segment_id);
            }
        }

        for segment_id in segments_to_remove {
            if let Some(segment) = completed_segments.remove(&segment_id) {
                if let Err(e) = std::fs::remove_file(&segment.file_path) {
                    eprintln!("Warning: Failed to delete segment file during trim: {e}");
                }
            }
        }

        Ok(())
    }

    fn sync(&self) -> Result<(), StateError> {
        let active_segment = self.active_segment.lock().unwrap();
        active_segment.sync()?;

        {
            let mut metrics = self.metrics.lock().unwrap();
            metrics.sync_count += 1;
            metrics.last_sync = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
        }

        Ok(())
    }
}

impl SegmentedWAL {
    /// Serialize an entry with its header and payload checksum.
    fn serialize_entry(&self, entry: &LogEntry) -> Result<Vec<u8>, StateError> {
        let payload = to_bytes(entry).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize log entry: {e}"),
        })?;

        let header = EntryHeader {
            magic: WAL_MAGIC,
            version: WAL_VERSION,
            payload_length: payload.len() as u32,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            component_hash: self.hash_component_id(&entry.component_id),
            sequence: entry.sequence,
        };

        let checksum = self.calculate_checksum(&payload);

        let serialized_entry = SerializedLogEntry {
            header,
            payload,
            checksum,
        };

        to_bytes(&serialized_entry).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize WAL entry: {e}"),
        })
    }

    /// Deserialize an entry after checking its magic number, version and
    /// checksum.
    ///
    /// # Errors
    ///
    /// Returns `DeserializationError` if the bytes do not parse,
    /// `InvalidStateData` for a wrong magic number or version, and
    /// `ChecksumMismatch` if the payload is corrupt.
    fn deserialize_entry(&self, data: &[u8]) -> Result<LogEntry, StateError> {
        use crate::core::util::from_bytes;

        let serialized_entry: SerializedLogEntry =
            from_bytes(data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize WAL entry: {e}"),
            })?;

        if serialized_entry.header.magic != WAL_MAGIC {
            return Err(StateError::InvalidStateData {
                message: "Invalid WAL entry magic number".to_string(),
            });
        }

        if serialized_entry.header.version != WAL_VERSION {
            return Err(StateError::InvalidStateData {
                message: format!(
                    "Unsupported WAL entry version: {}",
                    serialized_entry.header.version
                ),
            });
        }

        let calculated_checksum = self.calculate_checksum(&serialized_entry.payload);
        if calculated_checksum != serialized_entry.checksum {
            return Err(StateError::ChecksumMismatch);
        }

        from_bytes(&serialized_entry.payload).map_err(|e| StateError::DeserializationError {
            message: format!("Failed to deserialize log entry payload: {e}"),
        })
    }

    /// Returns the low 32 bits of a `DefaultHasher` hash of `data`.
    ///
    /// `DefaultHasher` output is not guaranteed to stay the same across Rust
    /// releases.
    fn calculate_checksum(&self, data: &[u8]) -> u32 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        data.hash(&mut hasher);
        hasher.finish() as u32
    }

    /// Hash a component id for the entry header.
    fn hash_component_id(&self, component_id: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        component_id.hash(&mut hasher);
        hasher.finish()
    }
}

impl WALSegment {
    /// Open or create a segment file for append. Starts counting from zero
    /// entries even if the file already has some.
    fn new(segment_id: u64, file_path: &Path, start_offset: LogOffset) -> Result<Self, StateError> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(file_path)
            .map_err(|e| StateError::InvalidStateData {
                message: format!("Failed to create WAL segment file: {e}"),
            })?;

        Ok(Self {
            segment_id,
            file: BufWriter::new(file),
            current_size: 0,
            entry_count: 0,
            start_offset,
            end_offset: start_offset,
            file_path: file_path.to_path_buf(),
            is_sealed: false,
        })
    }

    /// Open an existing segment file and count its entries.
    ///
    /// Counting stops at the first entry that is cut short or has a length of 0
    /// or 1 MB or more. Entries after that point are not counted, but
    /// `current_size` is still the full file size.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStateData` if the file cannot be opened or its size read.
    fn recover(
        segment_id: u64,
        file_path: &Path,
        start_offset: LogOffset,
    ) -> Result<Self, StateError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file_path)
            .map_err(|e| StateError::InvalidStateData {
                message: format!("Failed to open WAL segment file: {e}"),
            })?;

        let metadata = file.metadata().map_err(|e| StateError::InvalidStateData {
            message: format!("Failed to get file metadata: {e}"),
        })?;

        let current_size = metadata.len() as usize;

        let mut reader = BufReader::new(&file);
        let mut entry_count = 0;
        let mut read_size = 0;

        while read_size < current_size {
            let mut size_buf = [0u8; 4];
            match reader.read_exact(&mut size_buf) {
                Ok(_) => {
                    let entry_size = u32::from_le_bytes(size_buf) as usize;
                    if entry_size > 0 && entry_size < 1024 * 1024 {
                        // A length outside this range means corrupt data.
                        // Read past the entry body.
                        let mut entry_data = vec![0u8; entry_size];
                        if reader.read_exact(&mut entry_data).is_ok() {
                            entry_count += 1;
                            read_size += 4 + entry_size;
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                Err(_) => break,
            }
        }

        Ok(Self {
            segment_id,
            file: BufWriter::new(file),
            current_size,
            entry_count,
            start_offset,
            end_offset: start_offset + entry_count as LogOffset,
            file_path: file_path.to_path_buf(),
            is_sealed: false,
        })
    }

    /// Write one entry as a 4 byte little endian length and the data.
    ///
    /// The write goes to the buffer and may not reach the file until it is
    /// flushed.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStateData` if the segment is sealed or the write fails.
    fn write_entry(&mut self, data: &[u8]) -> Result<(), StateError> {
        if self.is_sealed {
            return Err(StateError::InvalidStateData {
                message: "Cannot write to sealed segment".to_string(),
            });
        }

        let size = data.len() as u32;
        self.file
            .write_all(&size.to_le_bytes())
            .map_err(|e| StateError::InvalidStateData {
                message: format!("Failed to write entry size: {e}"),
            })?;

        self.file
            .write_all(data)
            .map_err(|e| StateError::InvalidStateData {
                message: format!("Failed to write entry data: {e}"),
            })?;

        self.current_size += 4 + data.len();
        self.entry_count += 1;
        self.end_offset += 1;

        Ok(())
    }

    /// Read up to `limit` raw entries from the file, starting at `offset`.
    ///
    /// Opens the file fresh and scans from the start, so entries still in the
    /// write buffer are not seen. Returns an empty list when `offset` is outside
    /// this segment. Stops early at the end of the file.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStateData` if the file cannot be opened, or an entry
    /// length is 0 or over 10 MB.
    fn read_entries_from(
        &self,
        offset: LogOffset,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>, StateError> {
        if offset < self.start_offset || offset >= self.end_offset {
            return Ok(Vec::new());
        }

        let file = File::open(&self.file_path).map_err(|e| StateError::InvalidStateData {
            message: format!("Failed to open segment file for reading: {e}"),
        })?;

        let mut reader = BufReader::new(file);
        let mut results = Vec::new();

        // No index, so scan from the start of the file to reach `offset`.
        let mut current_offset = self.start_offset;

        while current_offset < offset {
            let mut size_buf = [0u8; 4];
            if reader.read_exact(&mut size_buf).is_err() {
                break; // end of file or a cut short entry
            }

            let entry_size = u32::from_le_bytes(size_buf) as usize;

            if entry_size == 0 || entry_size > 10 * 1024 * 1024 {
                // 10 MB max
                return Err(StateError::InvalidStateData {
                    message: format!("Invalid entry size: {entry_size}"),
                });
            }

            let mut skip_buf = vec![0u8; entry_size];
            if reader.read_exact(&mut skip_buf).is_err() {
                break; // end of file
            }

            current_offset += 1;
        }

        while results.len() < limit && current_offset < self.end_offset {
            let mut size_buf = [0u8; 4];
            if reader.read_exact(&mut size_buf).is_err() {
                break; // end of file
            }

            let entry_size = u32::from_le_bytes(size_buf) as usize;

            if entry_size == 0 || entry_size > 10 * 1024 * 1024 {
                return Err(StateError::InvalidStateData {
                    message: format!("Invalid entry size: {entry_size}"),
                });
            }

            let mut entry_data = vec![0u8; entry_size];
            if reader.read_exact(&mut entry_data).is_err() {
                break; // end of file
            }

            results.push(entry_data);
            current_offset += 1;
        }

        Ok(results)
    }

    /// Flush the buffer and mark the segment so it rejects writes. Does not
    /// fsync.
    fn seal(&mut self) -> Result<(), StateError> {
        self.file
            .flush()
            .map_err(|e| StateError::InvalidStateData {
                message: format!("Failed to flush segment: {e}"),
            })?;

        self.is_sealed = true;
        Ok(())
    }

    /// fsync the segment file.
    ///
    /// Does not flush the write buffer first, so entries still in the buffer are
    /// not made durable by this call.
    fn sync(&self) -> Result<(), StateError> {
        self.file
            .get_ref()
            .sync_all()
            .map_err(|e| StateError::InvalidStateData {
                message: format!("Failed to sync segment: {e}"),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::persistence::state_holder::ChangeLog;
    use tempfile::TempDir;

    fn create_test_entry(component_id: &str, sequence: u64) -> LogEntry {
        LogEntry {
            component_id: component_id.to_string(),
            sequence,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            change: ChangeLog::new(0, sequence),
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn test_wal_creation_and_append() {
        let temp_dir = TempDir::new().unwrap();
        let config = WALConfig::default();

        let wal = SegmentedWAL::new(temp_dir.path(), 1024 * 1024, config).unwrap();

        let entry = create_test_entry("test_component", 1);
        let offset = wal.append(entry).unwrap();

        assert_eq!(offset, 0);
        assert_eq!(wal.tail_offset().unwrap(), 1);
    }

    #[test]
    fn test_wal_batch_append() {
        let temp_dir = TempDir::new().unwrap();
        let config = WALConfig::default();

        let wal = SegmentedWAL::new(temp_dir.path(), 1024 * 1024, config).unwrap();

        let entries = vec![
            create_test_entry("component1", 1),
            create_test_entry("component2", 1),
            create_test_entry("component1", 2),
        ];

        let offsets = wal.append_batch(entries).unwrap();

        assert_eq!(offsets, vec![0, 1, 2]);
        assert_eq!(wal.tail_offset().unwrap(), 3);
    }

    #[test]
    fn test_wal_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let config = WALConfig::default();

        {
            let wal = SegmentedWAL::new(temp_dir.path(), 1024 * 1024, config.clone()).unwrap();

            let entries = vec![
                create_test_entry("component1", 1),
                create_test_entry("component2", 1),
            ];

            wal.append_batch(entries).unwrap();
            wal.sync().unwrap();
        }

        // The WAL above was dropped, which flushed its buffer to the file.
        let recovered_wal = SegmentedWAL::recover(temp_dir.path(), 1024 * 1024, config).unwrap();

        assert_eq!(recovered_wal.tail_offset().unwrap(), 2);
    }

    #[test]
    fn test_segment_rotation() {
        let temp_dir = TempDir::new().unwrap();
        let config = WALConfig::default();

        // A 100 byte segment size forces rotation.
        let wal = SegmentedWAL::new(temp_dir.path(), 100, config).unwrap();

        for i in 0..10 {
            let entry = create_test_entry("test_component", i);
            wal.append(entry).unwrap();
        }

        let completed_segments = wal.completed_segments.read().unwrap();
        assert!(!completed_segments.is_empty());
    }

    #[test]
    fn test_wal_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let config = WALConfig {
            retention_segments: 2,
            ..Default::default()
        };

        let wal = SegmentedWAL::new(temp_dir.path(), 50, config).unwrap();

        // A 50 byte segment size makes many segments.
        for i in 0..20 {
            let entry = create_test_entry("test_component", i);
            wal.append(entry).unwrap();
        }

        let removed_count = wal.cleanup_old_segments().unwrap();

        assert!(removed_count > 0);

        let completed_segments = wal.completed_segments.read().unwrap();
        assert!(completed_segments.len() <= 2);
    }
}
