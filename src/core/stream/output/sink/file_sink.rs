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

//! # File Sink
//!
//! Appends each event's formatted payload as one line to a file. Can roll the
//! file over by size or by age, and compress rolled files with zstd.
//!
//! ## Architecture
//!
//! ```text
//! Events → per event: SinkMapper → bytes → publish() → BufWriter (+ \n)
//!   → flush policy → rotation check (size/time) → roll (+ optional zstd)
//! ```
//!
//! One event gives one line. `publish()` blocks for the write, and for the
//! flush under the default policy. This is on purpose, so a slow disk slows the
//! pipeline down, like the other sinks. The rotation check runs after the
//! write, so a line is never split across files.

use super::sink_trait::Sink;
use crate::core::exception::EventFluxError;
use crate::core::extension::SinkFactory;
use crate::core::stream::connector_util::parse_or;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Compression applied to rolled files
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotateCompression {
    None,
    /// The rolled file becomes `<name>.zst`. zstd is already a workspace
    /// dependency. gzip is put off for now, see feat/file/README.md section 9
    /// for why.
    Zstd,
}

/// File sink configuration
#[derive(Debug, Clone)]
pub struct FileSinkConfig {
    /// Output file path. The parent directory must exist.
    pub path: PathBuf,
    /// Append to an existing file (default true), or empty it at startup
    pub append: bool,
    /// 0 flushes after every event (the default). N above 0 flushes when a
    /// write comes at least N ms after the last flush.
    pub flush_interval_ms: u64,
    /// Roll when the file reaches this many bytes. 0 turns it off (default).
    pub rotate_size_bytes: u64,
    /// Roll when the file's age, counted from when it was opened, reaches
    /// this many ms. 0 turns it off (default). Only checked after a write.
    pub rotate_interval_ms: u64,
    /// Compression for rolled files. Default none.
    pub rotate_compression: RotateCompression,
}

impl FileSinkConfig {
    /// Reads the config from properties.
    ///
    /// # Errors
    ///
    /// Returns an error when `file.path` is missing or blank,
    /// `file.rotate.compression` is not `none` or `zstd`, a number or bool does
    /// not parse, or compression is set with no rotation trigger.
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        let path = crate::core::stream::connector_util::parse_required(properties, "file.path")?;

        let rotate_compression = match properties
            .get("file.rotate.compression")
            .map(String::as_str)
            .unwrap_or("none")
        {
            "none" => RotateCompression::None,
            "zstd" => RotateCompression::Zstd,
            other => {
                return Err(format!(
                    "Invalid file.rotate.compression '{other}': expected 'none' or 'zstd'"
                ))
            }
        };

        let config = Self {
            path: PathBuf::from(&path),
            append: parse_or(properties, "file.append", true)?,
            flush_interval_ms: parse_or(properties, "file.flush.interval.ms", 0)?,
            rotate_size_bytes: parse_or(properties, "file.rotate.size.bytes", 0)?,
            rotate_interval_ms: parse_or(properties, "file.rotate.interval.ms", 0)?,
            rotate_compression,
        };

        if config.rotate_compression != RotateCompression::None
            && config.rotate_size_bytes == 0
            && config.rotate_interval_ms == 0
        {
            return Err(
                "file.rotate.compression is set but no rotation trigger is configured — \
                 set file.rotate.size.bytes and/or file.rotate.interval.ms"
                    .to_string(),
            );
        }

        Ok(config)
    }
}

/// The open output file, with how much was written and when it was last flushed
struct SinkInner {
    writer: BufWriter<File>,
    /// Current size of the file, used for size rotation
    bytes_written: u64,
    /// When this file was opened, used for time rotation
    opened_at: Instant,
    /// Time of the last flush, used for the interval flush policy
    last_flush: Instant,
}

/// Sink state behind the mutex. The open file and the counters for the stop
/// log. Plain integers, not atomics, since every use already holds this lock.
/// The HTTP sink takes no lock, so there the atomics matter.
#[derive(Default)]
struct SinkState {
    /// Open only while started. publish opens it on demand as a fallback.
    file: Option<SinkInner>,
    /// Set when the emptying open at startup failed. The open in publish()
    /// must then still empty the file, as `file.append = false` asks.
    truncate_pending: bool,
    written: u64,
    failed: u64,
    rolled: u64,
}

/// File sink that appends one line per event. See the module docs.
pub struct FileSink {
    config: FileSinkConfig,
    state: Arc<Mutex<SinkState>>,
}

impl Debug for FileSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileSink")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl FileSink {
    /// Creates a file sink. `start()` opens the file.
    pub fn new(config: FileSinkConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(SinkState::default())),
        }
    }

    /// Creates a file sink from properties. Fails when the config is invalid.
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        Ok(Self::new(FileSinkConfig::from_properties(properties)?))
    }

    /// Opens the output file, creating it if needed.
    ///
    /// `truncate` must be true only for the startup open with
    /// `file.append = false`. Every later reopen (rotation, recovery after a
    /// failed roll, publish after stop) appends. Emptying the file again there
    /// would wipe live data.
    fn open_inner(config: &FileSinkConfig, truncate: bool) -> std::io::Result<SinkInner> {
        let file = OpenOptions::new()
            .create(true)
            .append(!truncate)
            .write(true)
            .truncate(truncate)
            .open(&config.path)?;
        let bytes_written = if truncate { 0 } else { file.metadata()?.len() };
        let now = Instant::now();
        Ok(SinkInner {
            writer: BufWriter::new(file),
            bytes_written,
            opened_at: now,
            last_flush: now,
        })
    }

    fn io_error(&self, action: &str, e: &std::io::Error) -> EventFluxError {
        EventFluxError::ConnectionUnavailable {
            message: format!(
                "File sink {action} '{}' failed: {e}",
                self.config.path.display()
            ),
            source: None,
        }
    }

    /// Returns true when the size or age trigger has fired for the current file
    fn should_rotate(&self, inner: &SinkInner) -> bool {
        (self.config.rotate_size_bytes > 0 && inner.bytes_written >= self.config.rotate_size_bytes)
            || (self.config.rotate_interval_ms > 0
                && inner.opened_at.elapsed()
                    >= Duration::from_millis(self.config.rotate_interval_ms))
    }

    /// Rolls the current file. Flushes and closes it, renames it to a name with
    /// a timestamp (plus a number if that name is taken), compresses it if
    /// set, and opens a new empty file.
    ///
    /// # Errors
    ///
    /// Returns the I/O error from the flush, rename, compression, or reopen.
    /// If the flush fails the file stays open. After any later failure the
    /// file is closed, and the next publish opens it again.
    fn rotate(&self, state: &mut SinkState) -> std::io::Result<()> {
        // Flush BEFORE closing. If it fails, the writer and the events in its
        // buffer, which were already accepted, must stay alive for a later
        // retry instead of being dropped with the error lost
        if let Some(current) = state.file.as_mut() {
            current.writer.flush()?;
        }
        state.file = None; // closes the file

        let rolled = rolled_path(&self.config.path);
        std::fs::rename(&self.config.path, &rolled)?;

        // The roll really happened at the rename. Count it now so a failed
        // compression does not hide it from the stop summary.
        state.rolled += 1;

        if self.config.rotate_compression == RotateCompression::Zstd {
            compress_zstd(&rolled)?;
        }

        // The rename freed the path, so the new file always appends
        state.file = Some(Self::open_inner(&self.config, false)?);
        log::info!(
            "[FileSink] Rolled '{}' → '{}'{}",
            self.config.path.display(),
            rolled.display(),
            if self.config.rotate_compression == RotateCompression::Zstd {
                " (zstd)"
            } else {
                ""
            }
        );
        Ok(())
    }
}

/// Returns `<path>.<UTC timestamp>`, in the form `YYYYMMDDTHHMMSSZ`, with a
/// `.N` suffix if that name is taken. A name counts as taken when it or its
/// compressed form exists. After a zstd roll only `<name>.zst` is left, and a
/// second roll in the same second must not reuse, and so overwrite, it.
fn rolled_path(path: &Path) -> PathBuf {
    let taken = |p: &Path| p.exists() || PathBuf::from(format!("{}.zst", p.display())).exists();

    let stamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
    let base = PathBuf::from(format!("{}.{stamp}", path.display()));
    if !taken(&base) {
        return base;
    }
    for n in 1.. {
        let candidate = PathBuf::from(format!("{}.{n}", base.display()));
        if !taken(&candidate) {
            return candidate;
        }
    }
    unreachable!("suffix loop always finds a free name");
}

/// Compresses `<rolled>` to `<rolled>.zst` at zstd's default level and removes
/// the original. If encoding fails, the partial `.zst` is removed (best effort)
/// so readers never see a cut off frame, and the uncompressed file stays.
fn compress_zstd(rolled: &Path) -> std::io::Result<()> {
    let target = PathBuf::from(format!("{}.zst", rolled.display()));
    let mut input = File::open(rolled)?;
    let output = File::create(&target)?;
    if let Err(e) = zstd::stream::copy_encode(&mut input, output, 0) {
        let _ = std::fs::remove_file(&target);
        return Err(e);
    }
    std::fs::remove_file(rolled)
}

impl Clone for FileSink {
    fn clone(&self) -> Self {
        // Clones share the state. A file has exactly one write position, and
        // shared counters keep the stop log totals right.
        Self {
            config: self.config.clone(),
            state: Arc::clone(&self.state),
        }
    }
}

impl Sink for FileSink {
    fn publish(&self, payload: &[u8]) -> Result<(), EventFluxError> {
        let mut state = self.state.lock().unwrap();

        // start() opens the file. Opening on demand here keeps direct calls to
        // publish() working, such as in tests or when embedded.
        if state.file.is_none() {
            // Late open or recovery. Appends, unless the emptying open at
            // startup failed and the file still has to be emptied.
            match Self::open_inner(&self.config, state.truncate_pending) {
                Ok(inner) => {
                    state.file = Some(inner);
                    state.truncate_pending = false;
                }
                Err(e) => {
                    state.failed += 1;
                    return Err(self.io_error("open of", &e));
                }
            }
        }

        let write_result = {
            let inner = state.file.as_mut().expect("opened above");
            (|| -> std::io::Result<()> {
                inner.writer.write_all(payload)?;
                inner.writer.write_all(b"\n")?;
                inner.bytes_written += payload.len() as u64 + 1;
                // Flush after every event (0), or when N ms have passed
                // since the last flush
                if self.config.flush_interval_ms == 0
                    || inner.last_flush.elapsed()
                        >= Duration::from_millis(self.config.flush_interval_ms)
                {
                    inner.writer.flush()?;
                    inner.last_flush = Instant::now();
                }
                Ok(())
            })()
        };
        if let Err(e) = write_result {
            state.failed += 1;
            return Err(self.io_error("write to", &e));
        }

        // The event is in the writer from here on (flushed to the OS only per
        // the flush policy). Count it before the rotation check so a failed
        // roll does not report it as failed.
        state.written += 1;

        // Checked after the write, so a line never splits across files
        let rotate_due = state.file.as_ref().is_some_and(|f| self.should_rotate(f));
        if rotate_due {
            if let Err(e) = self.rotate(&mut state) {
                return Err(self.io_error("rotation of", &e));
            }
        }

        Ok(())
    }

    fn start(&self) {
        let mut state = self.state.lock().unwrap();
        let truncate = !self.config.append;
        // Every start and restart applies the startup rule the same way. With
        // file.append = false the file is emptied, even if a stray publish
        // already opened it for append between stop() and this start().
        if truncate {
            state.file = None; // close before reopening to empty it
        }
        if state.file.is_none() {
            match Self::open_inner(&self.config, truncate) {
                Ok(inner) => {
                    state.file = Some(inner);
                    state.truncate_pending = false;
                }
                Err(e) => {
                    // publish() will try the open again, still emptying the
                    // file, and report errors through the pipeline's error
                    // handling
                    state.truncate_pending = truncate;
                    log::error!(
                        "[FileSink] Failed to open '{}': {e}",
                        self.config.path.display()
                    );
                }
            }
        }
    }

    fn stop(&self) {
        let mut state = self.state.lock().unwrap();
        if let Some(mut inner) = state.file.take() {
            if let Err(e) = inner.writer.flush() {
                log::error!(
                    "[FileSink] Final flush of '{}' failed: {e}",
                    self.config.path.display()
                );
            }
        }
        log::info!(
            "[FileSink] Stopped (written: {}, failed: {}, rolled: {})",
            state.written,
            state.failed,
            state.rolled,
        );
    }

    fn clone_box(&self) -> Box<dyn Sink> {
        Box::new(self.clone())
    }

    fn validate_connectivity(&self) -> Result<(), EventFluxError> {
        // Open for append and close. This creates the file if it is missing,
        // which append mode would do anyway, and catches a missing or read
        // only directory
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.config.path)
            .map(|_| ())
            .map_err(|e| self.io_error("validation open of", &e))
    }
}

// ============================================================================
// File Sink Factory
// ============================================================================

/// Factory for file sinks.
///
/// Registered with EventFluxContext and used to build file sinks from the SQL
/// WITH clause config.
#[derive(Debug, Clone)]
pub struct FileSinkFactory;

impl SinkFactory for FileSinkFactory {
    fn name(&self) -> &'static str {
        "file"
    }

    fn supported_formats(&self) -> &[&str] {
        &["json", "csv", "bytes"]
    }

    fn required_parameters(&self) -> &[&str] {
        &["file.path"]
    }

    fn optional_parameters(&self) -> &[&str] {
        &[
            "file.append",
            "file.flush.interval.ms",
            "file.rotate.size.bytes",
            "file.rotate.interval.ms",
            "file.rotate.compression",
        ]
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn Sink>, EventFluxError> {
        let parsed =
            FileSinkConfig::from_properties(config).map_err(EventFluxError::configuration)?;
        Ok(Box::new(FileSink::new(parsed)))
    }

    fn clone_box(&self) -> Box<dyn SinkFactory> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_props(path: &str) -> HashMap<String, String> {
        let mut props = HashMap::new();
        props.insert("file.path".to_string(), path.to_string());
        props
    }

    #[test]
    fn test_config_defaults() {
        let config = FileSinkConfig::from_properties(&base_props("/tmp/out.jsonl")).unwrap();
        assert_eq!(config.path, PathBuf::from("/tmp/out.jsonl"));
        assert!(config.append);
        assert_eq!(config.flush_interval_ms, 0);
        assert_eq!(config.rotate_size_bytes, 0);
        assert_eq!(config.rotate_interval_ms, 0);
        assert_eq!(config.rotate_compression, RotateCompression::None);
    }

    #[test]
    fn test_config_all_options() {
        let mut props = base_props("/tmp/out.jsonl");
        props.insert("file.append".to_string(), "false".to_string());
        props.insert("file.flush.interval.ms".to_string(), "250".to_string());
        props.insert("file.rotate.size.bytes".to_string(), "1024".to_string());
        props.insert("file.rotate.interval.ms".to_string(), "60000".to_string());
        props.insert("file.rotate.compression".to_string(), "zstd".to_string());

        let config = FileSinkConfig::from_properties(&props).unwrap();
        assert!(!config.append);
        assert_eq!(config.flush_interval_ms, 250);
        assert_eq!(config.rotate_size_bytes, 1024);
        assert_eq!(config.rotate_interval_ms, 60000);
        assert_eq!(config.rotate_compression, RotateCompression::Zstd);
    }

    #[test]
    fn test_config_requires_path() {
        assert!(FileSinkConfig::from_properties(&HashMap::new())
            .unwrap_err()
            .contains("file.path"));
    }

    #[test]
    fn test_config_rejects_invalid_compression() {
        let mut props = base_props("/tmp/out.jsonl");
        props.insert("file.rotate.compression".to_string(), "gzip".to_string());
        assert!(FileSinkConfig::from_properties(&props)
            .unwrap_err()
            .contains("file.rotate.compression"));
    }

    #[test]
    fn test_config_rejects_compression_without_rotation() {
        let mut props = base_props("/tmp/out.jsonl");
        props.insert("file.rotate.compression".to_string(), "zstd".to_string());
        assert!(FileSinkConfig::from_properties(&props)
            .unwrap_err()
            .contains("rotation trigger"));
    }

    #[test]
    fn test_validate_missing_parent_dir_fails() {
        let sink =
            FileSink::from_properties(&base_props("/nonexistent-dir-136/out.jsonl")).unwrap();
        assert!(sink.validate_connectivity().is_err());
    }

    // =========================================================================
    // Factory Tests
    // =========================================================================

    #[test]
    fn test_factory_metadata() {
        let factory = FileSinkFactory;
        assert_eq!(factory.name(), "file");
        assert!(factory.supported_formats().contains(&"json"));
        assert!(factory.supported_formats().contains(&"csv"));
        assert!(factory.supported_formats().contains(&"bytes"));
        assert!(factory.required_parameters().contains(&"file.path"));
        assert!(factory.optional_parameters().contains(&"file.append"));
    }

    #[test]
    fn test_factory_create_and_missing_required() {
        let factory = FileSinkFactory;
        assert!(factory
            .create_initialized(&base_props("/tmp/out.jsonl"))
            .is_ok());
        assert!(factory.create_initialized(&HashMap::new()).is_err());
    }

    #[test]
    fn test_factory_clone() {
        let factory = FileSinkFactory;
        assert_eq!(factory.clone_box().name(), "file");
    }
}
