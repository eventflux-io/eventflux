// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/persistence/incremental/write_ahead_log.rs

//! Write-Ahead Log Implementation
//!
//! High-performance, durable Write-Ahead Log for state changes with segment-based storage,
//! atomic batch operations, and efficient cleanup. Designed for enterprise-grade reliability
//! with crash recovery and data integrity guarantees.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use super::{LogEntry, LogOffset, WriteAheadLog};
use crate::core::persistence::state_holder::StateError;
use crate::core::util::to_bytes;

/// High-performance segmented Write-Ahead Log
pub struct SegmentedWAL {
    /// Base directory for WAL segments
    base_dir: PathBuf,

    /// Maximum size per segment
    segment_size: usize,

    /// Current active segment
    active_segment: Arc<Mutex<WALSegment>>,

    /// Completed segments
    completed_segments: Arc<RwLock<HashMap<u64, WALSegment>>>,

    /// Current global offset
    global_offset: Arc<Mutex<LogOffset>>,

    /// Segment sequence number
    segment_sequence: Arc<Mutex<u64>>,

    /// Configuration
    config: WALConfig,

    /// Performance metrics
    metrics: Arc<Mutex<WALMetrics>>,
}

/// Configuration for WAL
#[derive(Debug, Clone)]
pub struct WALConfig {
    /// Enable fsync for durability
    pub enable_sync: bool,

    /// Buffer size for writes
    pub buffer_size: usize,

    /// Maximum entries per batch
    pub max_batch_size: usize,

    /// Segment retention policy
    pub retention_segments: usize,

    /// Enable compression
    pub enable_compression: bool,
}

impl Default for WALConfig {
    fn default() -> Self {
        Self {
            enable_sync: true,
            buffer_size: 64 * 1024, // 64KB
            max_batch_size: 1000,
            retention_segments: 10,
            enable_compression: false,
        }
    }
}

/// Individual WAL segment
pub struct WALSegment {
    /// Segment identifier
    pub segment_id: u64,

    /// File handle
    file: BufWriter<File>,

    /// Current size in bytes
    current_size: usize,

    /// Number of entries in this segment
    entry_count: usize,

    /// Start offset for this segment
    start_offset: LogOffset,

    /// End offset for this segment
    end_offset: LogOffset,

    /// Segment file path
    file_path: PathBuf,

    /// Whether segment is read-only
    is_sealed: bool,
}

/// WAL performance metrics
#[derive(Debug, Default)]
pub struct WALMetrics {
    /// Total entries written
    pub total_entries: u64,

    /// Total bytes written
    pub total_bytes: u64,

    /// Average batch size
    pub avg_batch_size: f64,

    /// Write throughput (entries/sec)
    pub write_throughput: f64,

    /// Sync operations count
    pub sync_count: u64,

    /// Active segments count
    pub active_segments: usize,

    /// Last sync timestamp
    pub last_sync: u64,
}

/// WAL entry format for serialization
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SerializedLogEntry {
    /// Entry header
    pub header: EntryHeader,

    /// Entry payload
    pub payload: Vec<u8>,

    /// Entry checksum
    pub checksum: u32,
}

/// Entry header with metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct EntryHeader {
    /// Magic number for validation
    pub magic: u32,

    /// Entry version
    pub version: u8,

    /// Payload length
    pub payload_length: u32,

    /// Timestamp
    pub timestamp: u64,

    /// Component ID hash
    pub component_hash: u64,

    /// Sequence number
    pub sequence: u64,
}

const WAL_MAGIC: u32 = 0x57414C00; // "WAL\0"
const WAL_VERSION: u8 = 1;

impl SegmentedWAL {
    /// Create a new segmented WAL
    pub fn new<P: AsRef<Path>>(
        base_dir: P,
        segment_size: usize,
        config: WALConfig,
    ) -> Result<Self, StateError> {
        let base_dir = base_dir.as_ref().to_path_buf();

        // Create base directory if it doesn't exist
        std::fs::create_dir_all(&base_dir).map_err(|e| StateError::InvalidStateData {
            message: format!("Failed to create WAL directory: {e}"),
        })?;

        // Initialize with first segment
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

    /// Recover WAL from existing segments
    pub fn recover<P: AsRef<Path>>(
        base_dir: P,
        segment_size: usize,
        config: WALConfig,
    ) -> Result<Self, StateError> {
        let base_dir = base_dir.as_ref().to_path_buf();

        if !base_dir.exists() {
            return Self::new(base_dir, segment_size, config);
        }

        // Find existing segments
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

        // Sort segments by ID
        segments.sort_by_key(|(id, _)| *id);

        if segments.is_empty() {
            return Self::new(base_dir, segment_size, config);
        }

        // Recover segments
        let mut completed_segments = HashMap::new();
        let mut global_offset = 0;
        let mut next_segment_id = 0;

        for (segment_id, segment_path) in &segments[..segments.len().saturating_sub(1)] {
            let segment = WALSegment::recover(*segment_id, segment_path, global_offset)?;
            global_offset = segment.end_offset;
            completed_segments.insert(*segment_id, segment);
            next_segment_id = segment_id + 1;
        }

        // Last segment becomes active
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

    /// Rotate to a new segment
    fn rotate_segment(&self) -> Result<(), StateError> {
        let mut active_segment = self.active_segment.lock().unwrap();
        let mut completed_segments = self.completed_segments.write().unwrap();
        let mut segment_sequence = self.segment_sequence.lock().unwrap();

        // Seal current segment
        active_segment.seal()?;

        // Move to completed segments
        let old_segment_id = active_segment.segment_id;

        // Create new active segment
        let new_segment_id = *segment_sequence;
        *segment_sequence += 1;

        let segment_path = self.base_dir.join(format!("wal-{new_segment_id:010}.log"));
        let start_offset = *self.global_offset.lock().unwrap();
        let new_segment = WALSegment::new(new_segment_id, &segment_path, start_offset)?;

        let old_segment = std::mem::replace(&mut *active_segment, new_segment);
        completed_segments.insert(old_segment_id, old_segment);

        Ok(())
    }

    /// Clean up old segments
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
                // Delete segment file
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

    fn append_batch(&self, entries: Vec<LogEntry>) -> Result<Vec<LogOffset>, StateError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::with_capacity(entries.len());
        let mut active_segment = self.active_segment.lock().unwrap();
        let mut global_offset = self.global_offset.lock().unwrap();

        let entries_len = entries.len();
        for entry in entries {
            // Check if we need to rotate segment
            if active_segment.current_size >= self.segment_size {
                drop(active_segment);
                drop(global_offset);
                self.rotate_segment()?;
                active_segment = self.active_segment.lock().unwrap();
                global_offset = self.global_offset.lock().unwrap();
            }

            // Serialize entry
            let serialized = self.serialize_entry(&entry)?;

            // Write to segment
            let offset = *global_offset;
            active_segment.write_entry(&serialized)?;

            *global_offset += 1;
            results.push(offset);
        }

        // Sync if configured
        if self.config.enable_sync {
            active_segment.sync()?;
        }

        // Update metrics
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

    fn read_from(&self, offset: LogOffset, limit: usize) -> Result<Vec<LogEntry>, StateError> {
        let mut results = Vec::new();
        let mut current_offset = offset;

        // Read from completed segments first
        {
            let completed_segments = self.completed_segments.read().unwrap();

            for (&segment_id, segment) in completed_segments.iter() {
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

        // Read from active segment if needed
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
                // Delete segment file
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

        // Update metrics
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
    /// Serialize a log entry
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

    /// Deserialize a log entry
    fn deserialize_entry(&self, data: &[u8]) -> Result<LogEntry, StateError> {
        use crate::core::util::from_bytes;

        let serialized_entry: SerializedLogEntry =
            from_bytes(data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize WAL entry: {e}"),
            })?;

        // Validate header
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

        // Validate checksum
        let calculated_checksum = self.calculate_checksum(&serialized_entry.payload);
        if calculated_checksum != serialized_entry.checksum {
            return Err(StateError::ChecksumMismatch);
        }

        // Deserialize payload
        from_bytes(&serialized_entry.payload).map_err(|e| StateError::DeserializationError {
            message: format!("Failed to deserialize log entry payload: {e}"),
        })
    }

    /// Calculate checksum for data
    fn calculate_checksum(&self, data: &[u8]) -> u32 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        data.hash(&mut hasher);
        hasher.finish() as u32
    }

    /// Hash component ID for indexing
    fn hash_component_id(&self, component_id: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        component_id.hash(&mut hasher);
        hasher.finish()
    }
}

impl WALSegment {
    /// Create a new WAL segment
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

    /// Recover an existing WAL segment
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

        // Count entries by reading through file
        let mut reader = BufReader::new(&file);
        let mut entry_count = 0;
        let mut read_size = 0;

        while read_size < current_size {
            // Try to read entry size (simplified recovery)
            let mut size_buf = [0u8; 4];
            match reader.read_exact(&mut size_buf) {
                Ok(_) => {
                    let entry_size = u32::from_le_bytes(size_buf) as usize;
                    if entry_size > 0 && entry_size < 1024 * 1024 {
                        // Sanity check
                        // Skip the entry data
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

    /// Write an entry to the segment
    fn write_entry(&mut self, data: &[u8]) -> Result<(), StateError> {
        if self.is_sealed {
            return Err(StateError::InvalidStateData {
                message: "Cannot write to sealed segment".to_string(),
            });
        }

        // Write entry size first
        let size = data.len() as u32;
        self.file
            .write_all(&size.to_le_bytes())
            .map_err(|e| StateError::InvalidStateData {
                message: format!("Failed to write entry size: {e}"),
            })?;

        // Write entry data
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

    /// Read entries from a given offset
    fn read_entries_from(
        &self,
        offset: LogOffset,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>, StateError> {
        // Return early if offset is outside this segment's range
        if offset < self.start_offset || offset >= self.end_offset {
            return Ok(Vec::new());
        }

        // Open file for reading
        let file = File::open(&self.file_path).map_err(|e| StateError::InvalidStateData {
            message: format!("Failed to open segment file for reading: {e}"),
        })?;

        let mut reader = BufReader::new(file);
        let mut results = Vec::new();

        // Scan from the beginning to find our starting offset
        let mut current_offset = self.start_offset;

        // Skip entries until we reach the target offset
        while current_offset < offset {
            // Read entry size
            let mut size_buf = [0u8; 4];
            if reader.read_exact(&mut size_buf).is_err() {
                break; // End of file or corrupt entry
            }

            let entry_size = u32::from_le_bytes(size_buf) as usize;

            // Sanity check on entry size
            if entry_size == 0 || entry_size > 10 * 1024 * 1024 {
                // 10MB max
                return Err(StateError::InvalidStateData {
                    message: format!("Invalid entry size: {entry_size}"),
                });
            }

            // Skip the entry data
            let mut skip_buf = vec![0u8; entry_size];
            if reader.read_exact(&mut skip_buf).is_err() {
                break; // End of file
            }

            current_offset += 1;
        }

        // Now read `limit` entries starting from `offset`
        while results.len() < limit && current_offset < self.end_offset {
            // Read entry size
            let mut size_buf = [0u8; 4];
            if reader.read_exact(&mut size_buf).is_err() {
                break; // End of file
            }

            let entry_size = u32::from_le_bytes(size_buf) as usize;

            // Sanity check
            if entry_size == 0 || entry_size > 10 * 1024 * 1024 {
                return Err(StateError::InvalidStateData {
                    message: format!("Invalid entry size: {entry_size}"),
                });
            }

            // Read entry data
            let mut entry_data = vec![0u8; entry_size];
            if reader.read_exact(&mut entry_data).is_err() {
                break; // End of file
            }

            results.push(entry_data);
            current_offset += 1;
        }

        Ok(results)
    }

    /// Seal the segment (make it read-only)
    fn seal(&mut self) -> Result<(), StateError> {
        self.file
            .flush()
            .map_err(|e| StateError::InvalidStateData {
                message: format!("Failed to flush segment: {e}"),
            })?;

        self.is_sealed = true;
        Ok(())
    }

    /// Sync segment to disk
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

        // Create WAL and add some entries
        {
            let wal = SegmentedWAL::new(temp_dir.path(), 1024 * 1024, config.clone()).unwrap();

            let entries = vec![
                create_test_entry("component1", 1),
                create_test_entry("component2", 1),
            ];

            wal.append_batch(entries).unwrap();
            wal.sync().unwrap();
        }

        // Recover WAL
        let recovered_wal = SegmentedWAL::recover(temp_dir.path(), 1024 * 1024, config).unwrap();

        assert_eq!(recovered_wal.tail_offset().unwrap(), 2);
    }

    #[test]
    fn test_segment_rotation() {
        let temp_dir = TempDir::new().unwrap();
        let config = WALConfig::default();

        // Small segment size to force rotation
        let wal = SegmentedWAL::new(temp_dir.path(), 100, config).unwrap();

        // Add entries to force segment rotation
        for i in 0..10 {
            let entry = create_test_entry("test_component", i);
            wal.append(entry).unwrap();
        }

        // Should have rotated segments
        let completed_segments = wal.completed_segments.read().unwrap();
        assert!(completed_segments.len() > 0);
    }

    #[test]
    fn test_wal_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = WALConfig::default();
        config.retention_segments = 2;

        let wal = SegmentedWAL::new(temp_dir.path(), 50, config).unwrap();

        // Force creation of multiple segments
        for i in 0..20 {
            let entry = create_test_entry("test_component", i);
            wal.append(entry).unwrap();
        }

        // Cleanup old segments
        let removed_count = wal.cleanup_old_segments().unwrap();

        // Should have removed some segments
        assert!(removed_count > 0);

        let completed_segments = wal.completed_segments.read().unwrap();
        assert!(completed_segments.len() <= 2);
    }
}
