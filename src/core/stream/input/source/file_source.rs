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

//! # File Source
//!
//! Observes one file and delivers each complete line as one payload. The
//! source never finishes: reaching EOF only means "nothing new yet" — it
//! keeps polling and delivers whatever other applications append later.
//!
//! ## Architecture
//!
//! ```text
//! SourceWorker thread → poll every interval:
//!   metadata check (missing / truncated / replaced → reopen)
//!   → read appended complete lines (partial final line held)
//!   → SourceCallback → mapper → junction
//! ```
//!
//! Rotation handling follows the `tail -F` contract: when the path points
//! at a different file (new device/inode, or creation time elsewhere), the
//! renamed-away file's remaining lines are first drained through the
//! still-open handle, then reading reopens from offset 0 and re-applies
//! `file.skip.lines`; a size shrink (truncation) reopens without salvage. A
//! missing file is awaited, not an error. Transient read errors resume at
//! the last consumed offset instead of replaying, and an unterminated line
//! beyond an 8 MiB safety cap is dropped with a resync at the next
//! terminator.

use super::{sleep_while_running, Source, SourceCallback, SourceWorker};
use crate::core::error::source_support::{
    deliver_with_error_handling, DeliveryVerdict, SourceErrorContext,
};
use crate::core::exception::EventFluxError;
use crate::core::extension::SourceFactory;
use crate::core::stream::connector_util::parse_or;
use crate::core::stream::input::input_handler::InputHandler;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::{File, Metadata};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Where to begin on the initially-present file
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartPosition {
    /// Deliver existing content first, then follow appends (default)
    Beginning,
    /// Only lines appended after startup
    End,
}

/// File source configuration
#[derive(Debug, Clone)]
pub struct FileSourceConfig {
    /// Path of the file to observe
    pub path: PathBuf,
    /// Where to begin on the initially-present file
    pub start_position: StartPosition,
    /// Poll cadence (interruptible; default 500)
    pub poll_interval_ms: u64,
    /// Lines to skip whenever reading starts from offset 0 (e.g. CSV header)
    pub skip_lines: u64,
    /// Fail fast at startup when the file is absent (default: await it)
    pub require_exists: bool,
}

impl FileSourceConfig {
    /// Parse configuration from properties HashMap
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        let path = crate::core::stream::connector_util::parse_required(properties, "file.path")?;

        let start_position = match properties
            .get("file.start.position")
            .map(String::as_str)
            .unwrap_or("beginning")
        {
            "beginning" => StartPosition::Beginning,
            "end" => StartPosition::End,
            other => {
                return Err(format!(
                    "Invalid file.start.position '{other}': expected 'beginning' or 'end'"
                ))
            }
        };

        Ok(Self {
            path: PathBuf::from(&path),
            start_position,
            poll_interval_ms: parse_or(properties, "file.poll.interval.ms", 500)?,
            skip_lines: parse_or(properties, "file.skip.lines", 0)?,
            require_exists: parse_or(properties, "file.require.exists", false)?,
        })
    }
}

/// Identity of the file behind a path, used to detect rename-over rotation.
/// Inode on Unix; creation time elsewhere (best effort).
#[derive(Debug, Clone, Copy, PartialEq)]
struct FileIdentity {
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
    #[cfg(not(unix))]
    created: Option<std::time::SystemTime>,
}

impl FileIdentity {
    fn of(meta: &Metadata) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Self {
                dev: meta.dev(),
                ino: meta.ino(),
            }
        }
        #[cfg(not(unix))]
        {
            Self {
                created: meta.created().ok(),
            }
        }
    }
}

/// Open file being followed, with read progress and the held partial line
struct TailReader {
    file: File,
    identity: FileIdentity,
    /// Bytes consumed from the file so far (including any held partial line).
    /// The OS cursor stays in lockstep: the only seek happens at open, and a
    /// read error discards the whole reader.
    offset: u64,
    /// Bytes after the last `\n` — held until the line is terminated
    partial: Vec<u8>,
    /// Set when `partial` blew past [`MAX_HELD_LINE_BYTES`]: the oversized
    /// line's remainder is discarded until the next terminator resyncs
    discarding: bool,
    /// Remaining `file.skip.lines` budget for this open-from-zero
    lines_to_skip: u64,
    /// Reusable read buffer (allocated once per open)
    buf: Box<[u8]>,
}

/// Safety cap on a held unterminated line (a delimiter-less file — e.g. a
/// binary pointed at by mistake — must not accrete into memory without
/// bound). The oversized line is dropped and reading resyncs at the next
/// terminator. A configurable `file.max.line.bytes` is a planned follow-up.
const MAX_HELD_LINE_BYTES: usize = 8 * 1024 * 1024;

/// Running totals for the shutdown summary log
#[derive(Default)]
struct DeliveryStats {
    delivered: u64,
    failed: u64,
}

/// File source observing a single file (see module docs)
#[derive(Debug)]
pub struct FileSource {
    config: FileSourceConfig,
    worker: SourceWorker,
    error_ctx: Option<SourceErrorContext>,
}

impl Clone for FileSource {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            worker: self.worker.clone(), // Clones as a fresh, unstarted worker
            error_ctx: None,             // Error context contains runtime state, not cloneable
        }
    }
}

impl FileSource {
    pub fn new(config: FileSourceConfig) -> Self {
        Self {
            config,
            worker: SourceWorker::new("FileSource"),
            error_ctx: None,
        }
    }

    /// Create a file source from properties
    ///
    /// # Required Properties
    /// - `file.path`
    ///
    /// # Optional Properties
    /// - `file.start.position`, `file.poll.interval.ms`, `file.skip.lines`,
    ///   `file.require.exists`
    /// - `error.*`: Error handling properties (see SourceErrorContext)
    pub fn from_properties(
        properties: &HashMap<String, String>,
        dlq_junction: Option<Arc<Mutex<InputHandler>>>,
        stream_name: &str,
    ) -> Result<Self, String> {
        let config = FileSourceConfig::from_properties(properties)?;
        let error_ctx = SourceErrorContext::from_properties(properties, dlq_junction, stream_name)?;

        Ok(Self {
            error_ctx,
            ..Self::new(config)
        })
    }
}

/// A continuation point stashed when a read error drops the reader: the
/// file's identity, the offset of the first unconsumed byte (held partial
/// excluded, so no line gets truncated), and the unspent skip budget.
type ResumeMarker = (FileIdentity, u64, u64);

/// Open the file and position per the rules: `at_end` seeks to the current
/// end (initial `start.position = 'end'` only); a `resume` marker matching
/// the opened file continues after a transient read error — at its offset,
/// with its remaining skip budget — instead of replaying from 0; otherwise
/// offset 0 with the full skip-lines budget.
///
/// Identity is taken from the OPENED handle's metadata, so it can never
/// diverge from the file actually being read (no path-vs-handle race).
fn open_reader(
    path: &std::path::Path,
    at_end: bool,
    skip_lines: u64,
    resume: Option<ResumeMarker>,
) -> std::io::Result<TailReader> {
    let mut file = File::open(path)?;
    let meta = file.metadata()?;
    let identity = FileIdentity::of(&meta);

    let (offset, lines_to_skip) = if at_end {
        (file.seek(SeekFrom::End(0))?, 0)
    } else {
        match resume {
            // Same file as before the error — continue where reading left
            // off with the skip budget that was still unspent there
            Some((resume_identity, resume_offset, resume_skip))
                if resume_identity == identity && resume_offset <= meta.len() =>
            {
                (file.seek(SeekFrom::Start(resume_offset))?, resume_skip)
            }
            _ => (0, skip_lines),
        }
    };

    Ok(TailReader {
        file,
        identity,
        offset,
        partial: Vec::new(),
        discarding: false,
        lines_to_skip,
        buf: vec![0u8; 64 * 1024].into_boxed_slice(),
    })
}

/// Deliver one complete line (skip-lines budget and empty lines are handled
/// here). `Break` means an unrecoverable delivery failure.
fn handle_line(
    line: &[u8],
    lines_to_skip: &mut u64,
    callback: &dyn SourceCallback,
    error_ctx: &mut Option<SourceErrorContext>,
    stats: &mut DeliveryStats,
) -> std::ops::ControlFlow<()> {
    if *lines_to_skip > 0 {
        *lines_to_skip -= 1;
        return std::ops::ControlFlow::Continue(());
    }
    if line.is_empty() {
        return std::ops::ControlFlow::Continue(()); // nothing to deliver
    }

    match deliver_with_error_handling(callback, line, error_ctx, "FileSource") {
        DeliveryVerdict::Delivered => stats.delivered += 1,
        DeliveryVerdict::Disposed => stats.failed += 1,
        DeliveryVerdict::Fail => {
            stats.failed += 1;
            log::error!("[FileSource] Unrecoverable error, stopping");
            return std::ops::ControlFlow::Break(());
        }
    }
    std::ops::ControlFlow::Continue(())
}

/// Read newly appended bytes and deliver each complete line zero-copy.
///
/// Complete lines inside a read chunk are delivered as slices of the reused
/// buffer; only a fragment spanning chunk boundaries touches the `partial`
/// buffer. `Break` propagates an unrecoverable delivery failure.
fn drain_new_lines(
    reader: &mut TailReader,
    running: &std::sync::atomic::AtomicBool,
    callback: &dyn SourceCallback,
    error_ctx: &mut Option<SourceErrorContext>,
    stats: &mut DeliveryStats,
) -> std::io::Result<std::ops::ControlFlow<()>> {
    // The chunk loop checks `running` so stop() stays bounded even while
    // draining a multi-GB backlog — the worker must never outlive its
    // grace period mid-drain
    while running.load(Ordering::SeqCst) {
        let read = reader.file.read(&mut reader.buf)?;
        if read == 0 {
            break;
        }
        reader.offset += read as u64;

        let mut start = 0;
        while let Some(pos) = reader.buf[start..read].iter().position(|b| *b == b'\n') {
            let end = start + pos;
            let flow = if reader.discarding {
                // Remainder of an oversized dropped line — this terminator
                // resyncs reading onto real line boundaries. The dropped
                // line still counts against the skip budget: skipping the
                // wrong (real) line later would be worse.
                reader.discarding = false;
                reader.partial.clear();
                reader.lines_to_skip = reader.lines_to_skip.saturating_sub(1);
                std::ops::ControlFlow::Continue(())
            } else if reader.partial.is_empty() {
                // Whole line inside the chunk — deliver without copying
                let mut line = &reader.buf[start..end];
                if line.last() == Some(&b'\r') {
                    line = &line[..line.len() - 1];
                }
                handle_line(line, &mut reader.lines_to_skip, callback, error_ctx, stats)
            } else {
                // This chunk completes the held fragment
                reader.partial.extend_from_slice(&reader.buf[start..end]);
                if reader.partial.last() == Some(&b'\r') {
                    reader.partial.pop();
                }
                let flow = handle_line(
                    &reader.partial,
                    &mut reader.lines_to_skip,
                    callback,
                    error_ctx,
                    stats,
                );
                reader.partial.clear();
                flow
            };
            if flow.is_break() {
                return Ok(std::ops::ControlFlow::Break(()));
            }
            start = end + 1;
        }

        // Unterminated tail (usually empty) — hold until completed, bounded
        // by the safety cap; beyond it the line is dropped and reading
        // resyncs at the next terminator
        if !reader.discarding {
            reader.partial.extend_from_slice(&reader.buf[start..read]);
            if reader.partial.len() > MAX_HELD_LINE_BYTES {
                log::warn!(
                    "[FileSource] Dropping unterminated line exceeding {MAX_HELD_LINE_BYTES} \
                     bytes — resyncing at the next line terminator"
                );
                reader.partial = Vec::new(); // also releases the capacity
                reader.discarding = true;
            }
        }
    }
    Ok(std::ops::ControlFlow::Continue(()))
}

/// Best-effort final drain of a file that went away (unlinked, rotated, or
/// replaced by a directory): the open fd still reads to EOF. `Break` means
/// an unrecoverable delivery failure; read errors forfeit only the
/// remaining tail and are logged.
fn salvage_tail(
    old: &mut TailReader,
    what: &str,
    path: &std::path::Path,
    running: &std::sync::atomic::AtomicBool,
    callback: &dyn SourceCallback,
    error_ctx: &mut Option<SourceErrorContext>,
    stats: &mut DeliveryStats,
) -> std::ops::ControlFlow<()> {
    match drain_new_lines(old, running, callback, error_ctx, stats) {
        Ok(flow) => flow,
        Err(e) => {
            log::warn!(
                "[FileSource] Salvage read of the {what} '{}' failed — its \
                 remaining tail is lost: {e}",
                path.display()
            );
            std::ops::ControlFlow::Continue(())
        }
    }
}

impl Source for FileSource {
    fn start(&mut self, callback: Arc<dyn SourceCallback>) {
        let config = self.config.clone();
        let mut error_ctx = self.error_ctx.take();

        self.worker.start(move |running| {
            let interval = Duration::from_millis(config.poll_interval_ms);
            let mut reader: Option<TailReader> = None;
            // start.position applies only to the file present at startup; a
            // file that appears later is new and reads from the beginning
            let mut initial = true;
            // Set on a transient read error so the reopen continues at the
            // last consumed offset instead of replaying the whole file
            let mut resume: Option<ResumeMarker> = None;
            let mut stats = DeliveryStats::default();

            log::info!(
                "[FileSource] Observing '{}' every {}ms",
                config.path.display(),
                config.poll_interval_ms
            );

            'poll: while running.load(Ordering::SeqCst) {
                let mut io_error: Option<std::io::Error> = None;

                match std::fs::metadata(&config.path) {
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        // Missing is awaited, not an error (tail -F). The
                        // unlinked file stays readable through the open fd —
                        // salvage its last lines before letting it go
                        if let Some(mut old) = reader.take() {
                            log::info!(
                                "[FileSource] '{}' disappeared — awaiting recreation",
                                config.path.display()
                            );
                            if salvage_tail(
                                &mut old,
                                "removed",
                                &config.path,
                                &running,
                                callback.as_ref(),
                                &mut error_ctx,
                                &mut stats,
                            )
                            .is_break()
                            {
                                break 'poll;
                            }
                        }
                        initial = false;
                    }
                    Err(e) => {
                        // A real stat failure (EACCES, EIO, …) — keep the
                        // reader (the open fd may be fine) and `initial`
                        // (nothing about the file was decided), and route
                        // the error through the strategy like open errors
                        io_error = Some(e);
                    }
                    Ok(meta) if meta.is_dir() => {
                        // A directory can never become the startup file —
                        // whatever real file appears later is new. The
                        // previously-followed file (if any) was replaced;
                        // its tail is still readable through the open fd.
                        if let Some(mut old) = reader.take() {
                            if salvage_tail(
                                &mut old,
                                "replaced",
                                &config.path,
                                &running,
                                callback.as_ref(),
                                &mut error_ctx,
                                &mut stats,
                            )
                            .is_break()
                            {
                                break 'poll;
                            }
                        }
                        initial = false;
                        io_error = Some(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "path is a directory, not a file",
                        ));
                    }
                    Ok(meta) => {
                        let identity = FileIdentity::of(&meta);
                        let rotated =
                            matches!(&reader, Some(r) if r.identity != identity);
                        let truncated =
                            matches!(&reader, Some(r) if r.identity == identity && meta.len() < r.offset);

                        if rotated {
                            log::info!(
                                "[FileSource] '{}' was rotated — reopening",
                                config.path.display()
                            );
                            // The renamed-away file is still readable through
                            // the old fd — deliver its tail before switching
                            // (the tail -F contract)
                            if let Some(mut old) = reader.take() {
                                if salvage_tail(
                                    &mut old,
                                    "rotated-away",
                                    &config.path,
                                    &running,
                                    callback.as_ref(),
                                    &mut error_ctx,
                                    &mut stats,
                                )
                                .is_break()
                                {
                                    break 'poll;
                                }
                            }
                        } else if truncated {
                            log::info!(
                                "[FileSource] '{}' was truncated — reopening",
                                config.path.display()
                            );
                            // Same file object — the overwritten content is
                            // gone; nothing to salvage
                            reader = None;
                        }

                        if reader.is_none() {
                            let at_end = initial && config.start_position == StartPosition::End;
                            // `resume` is only cleared on a successful open —
                            // a transient open failure must not burn the
                            // marker and force a replay later
                            match open_reader(&config.path, at_end, config.skip_lines, resume) {
                                Ok(r) => {
                                    reader = Some(r);
                                    resume = None;
                                    initial = false;
                                }
                                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                    // Lost a race with an rm/rename between
                                    // the metadata check and the open — back
                                    // to awaiting, not an error
                                    initial = false;
                                }
                                Err(e) => {
                                    // Transient (e.g. EACCES): keep `initial`
                                    // so start.position=end still holds once
                                    // the file finally opens
                                    io_error = Some(e);
                                }
                            }
                        } else {
                            initial = false;
                        }

                        // Only read when there are new bytes — idle polls
                        // stay syscall-free after the metadata check. (An
                        // identity mismatch means the handle is newer than
                        // the path metadata — drain it regardless.)
                        if let Some(r) = &mut reader {
                            if meta.len() > r.offset || r.identity != identity {
                                match drain_new_lines(
                                    r,
                                    &running,
                                    callback.as_ref(),
                                    &mut error_ctx,
                                    &mut stats,
                                ) {
                                    Ok(std::ops::ControlFlow::Continue(())) => {}
                                    Ok(std::ops::ControlFlow::Break(())) => break 'poll,
                                    Err(e) => {
                                        // Stash where reading stopped (held
                                        // partial excluded, so no line gets
                                        // truncated): if the same file is
                                        // still there at reopen, reading
                                        // resumes instead of replaying. A
                                        // mid-discard reader gets no marker —
                                        // the fresh replay re-hits the cap
                                        // and resyncs the same way.
                                        if let Some(r) = reader.take() {
                                            if !r.discarding {
                                                resume = Some((
                                                    r.identity,
                                                    r.offset - r.partial.len() as u64,
                                                    r.lines_to_skip,
                                                ));
                                            }
                                        }
                                        io_error = Some(e);
                                    }
                                }
                            }
                        }
                    }
                }

                if let Some(e) = io_error {
                    let err = EventFluxError::ConnectionUnavailable {
                        message: format!("File '{}' read failed: {e}", config.path.display()),
                        source: Some(Box::new(e)),
                    };
                    if let Some(ctx) = &mut error_ctx {
                        if !ctx.handle_error(None, &err) {
                            break 'poll;
                        }
                    } else {
                        log::error!("[FileSource] {err}");
                    }
                }

                // Interruptible: a long interval never delays stop()
                sleep_while_running(&running, interval);
            }

            log::info!(
                "[FileSource] Stopped (delivered: {}, failed: {})",
                stats.delivered,
                stats.failed
            );
        });
    }

    fn stop(&mut self) {
        log::info!("[FileSource] Stopping...");
        self.worker.stop();
    }

    fn clone_box(&self) -> Box<dyn Source> {
        Box::new(self.clone())
    }

    fn set_error_dlq_junction(&mut self, junction: Arc<Mutex<InputHandler>>) {
        if let Some(ref mut ctx) = self.error_ctx {
            ctx.set_dlq_junction(junction);
        }
    }

    fn validate_connectivity(&self) -> Result<(), EventFluxError> {
        if self.config.path.is_dir() {
            return Err(EventFluxError::ConnectionUnavailable {
                message: format!(
                    "file.path '{}' is a directory, not a file",
                    self.config.path.display()
                ),
                source: None,
            });
        }

        let parent = self
            .config
            .path
            .parent()
            .filter(|p| !p.as_os_str().is_empty());
        if let Some(parent) = parent {
            if !parent.is_dir() {
                return Err(EventFluxError::ConnectionUnavailable {
                    message: format!(
                        "Parent directory '{}' of file.path does not exist",
                        parent.display()
                    ),
                    source: None,
                });
            }
        }

        if self.config.require_exists {
            File::open(&self.config.path).map(|_| ()).map_err(|e| {
                EventFluxError::ConnectionUnavailable {
                    message: format!(
                        "file.require.exists is set but '{}' cannot be opened: {e}",
                        self.config.path.display()
                    ),
                    source: Some(Box::new(e)),
                }
            })?;
        }

        Ok(())
    }
}

// ============================================================================
// File Source Factory
// ============================================================================

/// Factory for creating file source instances.
///
/// This factory is registered with EventFluxContext and used to create
/// file sources from SQL WITH clause configuration.
#[derive(Debug, Clone)]
pub struct FileSourceFactory;

impl SourceFactory for FileSourceFactory {
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
        // Connector keys + the shared error-handling keys (owned by
        // SourceErrorContext) — concatenated once, on first use
        static PARAMS: std::sync::LazyLock<Vec<&'static str>> = std::sync::LazyLock::new(|| {
            [
                &[
                    "file.start.position",
                    "file.poll.interval.ms",
                    "file.skip.lines",
                    "file.require.exists",
                ][..],
                crate::core::error::source_support::SOURCE_ERROR_PARAMETERS,
            ]
            .concat()
        });
        &PARAMS
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn Source>, EventFluxError> {
        // DLQ junction is None initially — stream_initializer calls
        // set_error_dlq_junction() after creation. The path identifies the
        // stream in error-context log messages.
        let stream_tag = config.get("file.path").cloned().unwrap_or_default();
        FileSource::from_properties(config, None, &stream_tag)
            .map(|source| Box::new(source) as Box<dyn Source>)
            .map_err(EventFluxError::configuration)
    }

    fn clone_box(&self) -> Box<dyn SourceFactory> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_props() -> HashMap<String, String> {
        let mut props = HashMap::new();
        props.insert("file.path".to_string(), "/tmp/test-input.jsonl".to_string());
        props
    }

    #[test]
    fn test_config_defaults() {
        let config = FileSourceConfig::from_properties(&base_props()).unwrap();
        assert_eq!(config.path, PathBuf::from("/tmp/test-input.jsonl"));
        assert_eq!(config.start_position, StartPosition::Beginning);
        assert_eq!(config.poll_interval_ms, 500);
        assert_eq!(config.skip_lines, 0);
        assert!(!config.require_exists);
    }

    #[test]
    fn test_config_all_options() {
        let mut props = base_props();
        props.insert("file.start.position".to_string(), "end".to_string());
        props.insert("file.poll.interval.ms".to_string(), "100".to_string());
        props.insert("file.skip.lines".to_string(), "1".to_string());
        props.insert("file.require.exists".to_string(), "true".to_string());

        let config = FileSourceConfig::from_properties(&props).unwrap();
        assert_eq!(config.start_position, StartPosition::End);
        assert_eq!(config.poll_interval_ms, 100);
        assert_eq!(config.skip_lines, 1);
        assert!(config.require_exists);
    }

    #[test]
    fn test_config_requires_path() {
        assert!(FileSourceConfig::from_properties(&HashMap::new())
            .unwrap_err()
            .contains("file.path"));

        let mut props = HashMap::new();
        props.insert("file.path".to_string(), "  ".to_string());
        assert!(FileSourceConfig::from_properties(&props)
            .unwrap_err()
            .contains("file.path"));
    }

    #[test]
    fn test_config_rejects_invalid_start_position() {
        let mut props = base_props();
        props.insert("file.start.position".to_string(), "middle".to_string());
        assert!(FileSourceConfig::from_properties(&props)
            .unwrap_err()
            .contains("file.start.position"));
    }

    #[test]
    fn test_source_from_properties_with_error_handling() {
        let mut props = base_props();
        props.insert("error.strategy".to_string(), "drop".to_string());

        let source = FileSource::from_properties(&props, None, "TestStream").unwrap();
        assert!(source.error_ctx.is_some());
    }

    #[test]
    fn test_source_clone_resets_runtime_state() {
        let mut props = base_props();
        props.insert("error.strategy".to_string(), "drop".to_string());
        let source = FileSource::from_properties(&props, None, "TestStream").unwrap();
        let cloned = source.clone();
        assert!(cloned.error_ctx.is_none());
    }

    #[test]
    fn test_validate_missing_parent_dir_fails() {
        let mut props = HashMap::new();
        props.insert(
            "file.path".to_string(),
            "/nonexistent-dir-136/x.jsonl".to_string(),
        );
        let source = FileSource::from_properties(&props, None, "Test").unwrap();
        assert!(source.validate_connectivity().is_err());
    }

    #[test]
    fn test_validate_rejects_directory_path() {
        let dir = tempfile::tempdir().unwrap();
        let mut props = HashMap::new();
        props.insert("file.path".to_string(), dir.path().display().to_string());
        let source = FileSource::from_properties(&props, None, "Test").unwrap();
        assert!(source
            .validate_connectivity()
            .unwrap_err()
            .to_string()
            .contains("directory"));
    }

    #[test]
    fn test_validate_require_exists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("absent.jsonl");
        let mut props = HashMap::new();
        props.insert("file.path".to_string(), path.display().to_string());
        props.insert("file.require.exists".to_string(), "true".to_string());

        let source = FileSource::from_properties(&props, None, "Test").unwrap();
        assert!(source.validate_connectivity().is_err());

        std::fs::write(&path, "x\n").unwrap();
        assert!(source.validate_connectivity().is_ok());
    }

    // =========================================================================
    // Factory Tests
    // =========================================================================

    #[test]
    fn test_factory_metadata() {
        let factory = FileSourceFactory;
        assert_eq!(factory.name(), "file");
        assert!(factory.supported_formats().contains(&"json"));
        assert!(factory.supported_formats().contains(&"csv"));
        assert!(factory.supported_formats().contains(&"bytes"));
        assert!(factory.required_parameters().contains(&"file.path"));
        assert!(factory
            .optional_parameters()
            .contains(&"file.start.position"));
    }

    #[test]
    fn test_factory_create_and_missing_required() {
        let factory = FileSourceFactory;
        assert!(factory.create_initialized(&base_props()).is_ok());
        assert!(factory.create_initialized(&HashMap::new()).is_err());
    }

    #[test]
    fn test_factory_clone() {
        let factory = FileSourceFactory;
        assert_eq!(factory.clone_box().name(), "file");
    }
}
