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
//! Watches one file and delivers each complete line as one payload. The source
//! never finishes. Reaching EOF only means nothing new has arrived yet. It keeps
//! polling and delivers whatever other programs append later.
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
//! Rotation works like `tail -F`. When the path points at a different file
//! (a new device and inode on Unix, or a new creation time elsewhere), the
//! lines left in the renamed file are first read through the handle that is
//! still open. Then reading reopens the path from offset 0 and applies
//! `file.skip.lines` again. If the file shrinks (truncation), it is reopened
//! without reading the old handle. A missing file is waited for, not treated as
//! an error. After a short lived read error, reading picks up at the last
//! consumed offset instead of starting over. A line with no terminator that
//! grows past an 8 MiB safety cap is dropped, and reading picks up again after
//! the next newline.

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

/// Where to start reading the file that exists at startup. A file that
/// appears later is always read from the beginning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartPosition {
    /// Deliver the existing content first, then follow appends. The default.
    Beginning,
    /// Deliver only lines appended after startup
    End,
}

/// File source configuration
#[derive(Debug, Clone)]
pub struct FileSourceConfig {
    /// Path of the file to watch (`file.path`)
    pub path: PathBuf,
    /// Where to start on the file present at startup (`file.start.position`)
    pub start_position: StartPosition,
    /// Time between polls in ms. Stop does not wait for it. Default 500.
    pub poll_interval_ms: u64,
    /// Lines to skip each time reading starts from offset 0, such as a CSV
    /// header. Default 0.
    pub skip_lines: u64,
    /// Fail at startup when the file is missing. Default false, which waits
    /// for the file to appear.
    pub require_exists: bool,
}

impl FileSourceConfig {
    /// Reads the config from properties.
    ///
    /// # Errors
    ///
    /// Returns an error when `file.path` is missing or blank, when
    /// `file.start.position` is not `beginning` or `end`, or when a number or
    /// bool does not parse.
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

/// Identity of the file behind a path, used to spot rotation, where a new file
/// is renamed over the path. Device and inode on Unix. Creation time elsewhere,
/// which is best effort.
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

/// The open file being followed, with read progress and the held partial line
struct TailReader {
    file: File,
    identity: FileIdentity,
    /// Bytes read from the file so far, including any held partial line.
    /// The OS cursor always matches it. The only seek is at open, and a read
    /// error throws away the whole reader.
    offset: u64,
    /// Bytes after the last `\n`, held until the line gets its terminator
    partial: Vec<u8>,
    /// Set when `partial` grew past [`MAX_HELD_LINE_BYTES`]. The rest of that
    /// line is thrown away until the next newline.
    discarding: bool,
    /// How many `file.skip.lines` are still left to skip since this reader
    /// started at offset 0
    lines_to_skip: u64,
    /// Read buffer, allocated once per open and reused
    buf: Box<[u8]>,
}

/// Safety cap on a held line with no terminator. A file with no newlines, such
/// as a binary picked by mistake, must not grow in memory without limit. The
/// long line is dropped and reading picks up after the next newline. A
/// `file.max.line.bytes` setting is planned but not added yet.
const MAX_HELD_LINE_BYTES: usize = 8 * 1024 * 1024;

/// Running totals for the log line written at shutdown
#[derive(Default)]
struct DeliveryStats {
    delivered: u64,
    failed: u64,
}

/// Source that watches a single file. See the module docs.
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
            worker: self.worker.clone(), // the clone is a new worker that has not started
            // New context with the same strategy and DLQ, and runtime counters reset
            error_ctx: self.error_ctx.as_ref().map(SourceErrorContext::fresh),
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

    /// Creates a file source from properties.
    ///
    /// # Required Properties
    /// - `file.path`
    ///
    /// # Optional Properties
    /// - `file.start.position`, `file.poll.interval.ms`, `file.skip.lines`,
    ///   `file.require.exists`
    /// - `error.*`: error handling properties (see SourceErrorContext)
    ///
    /// # Errors
    ///
    /// Returns an error when the file config or the error config is invalid.
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

/// Where to pick up after a read error drops the reader. Holds the file
/// identity, the offset of the first byte not yet used, and the number of lines
/// still to skip. The offset leaves out the held partial line, so that line is
/// read again in full and not cut short.
type ResumeMarker = (FileIdentity, u64, u64);

/// Opens the file and picks where to start reading.
///
/// - `at_end` seeks to the current end. Only used for the startup file when
///   `file.start.position` is `end`.
/// - A `resume` marker that matches the opened file, and whose offset is not
///   past its end, continues at that offset with its remaining skip count,
///   instead of starting over from 0.
/// - Otherwise it starts at offset 0 and skips `skip_lines` lines.
///
/// The identity comes from the metadata of the opened handle, so it always
/// matches the file being read, even if the path changes in between.
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
            // Same file as before the error. Continue where reading stopped,
            // with the lines still left to skip at that point.
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

/// Delivers one complete line. Lines still to be skipped and empty lines are
/// not delivered. Returns `Break` when delivery failed and the source must stop.
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
        return std::ops::ControlFlow::Continue(());
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

/// Reads newly appended bytes and delivers each complete line without copying
/// it. A trailing `\r` is removed from each line.
///
/// A line that sits fully inside one read chunk is delivered as a slice of the
/// reused buffer. Only a line split across chunks is copied into `partial`.
/// Returns `Break` when delivery failed and the source must stop.
///
/// # Errors
///
/// Returns the I/O error from a failed read.
fn drain_new_lines(
    reader: &mut TailReader,
    running: &std::sync::atomic::AtomicBool,
    callback: &dyn SourceCallback,
    error_ctx: &mut Option<SourceErrorContext>,
    stats: &mut DeliveryStats,
) -> std::io::Result<std::ops::ControlFlow<()>> {
    // Check `running` on every chunk so stop() returns quickly even while a
    // backlog of many GB is being read. The worker must not run past its
    // grace period in the middle of a read.
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
                // End of a dropped long line. This newline puts reading back on
                // real line boundaries. The dropped line still counts as a
                // skipped line, because skipping a real line later would be
                // worse.
                reader.discarding = false;
                reader.partial.clear();
                reader.lines_to_skip = reader.lines_to_skip.saturating_sub(1);
                std::ops::ControlFlow::Continue(())
            } else if reader.partial.is_empty() {
                // Whole line inside the chunk, so deliver it without copying
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

        // Hold the unfinished end of the chunk (usually empty) until its line
        // completes. Past the safety cap the line is dropped and reading picks
        // up after the next newline.
        if !reader.discarding {
            reader.partial.extend_from_slice(&reader.buf[start..read]);
            if reader.partial.len() > MAX_HELD_LINE_BYTES {
                log::warn!(
                    "[FileSource] Dropping unterminated line exceeding {MAX_HELD_LINE_BYTES} \
                     bytes — resyncing at the next line terminator"
                );
                reader.partial = Vec::new(); // a new Vec also frees the memory
                reader.discarding = true;
            }
        }
    }
    Ok(std::ops::ControlFlow::Continue(()))
}

/// Best effort final read of a file that went away because it was deleted,
/// rotated, or replaced by a directory. The open fd can still read it to EOF.
/// Returns `Break` when delivery failed and the source must stop. A read error
/// is logged and loses only the rest of that file.
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
        let mut error_ctx = self.error_ctx.as_ref().map(SourceErrorContext::fresh);

        self.worker.start(move |running| {
            if let Some(ctx) = &mut error_ctx {
                ctx.bind_cancellation(Arc::clone(&running));
            }
            let interval = Duration::from_millis(config.poll_interval_ms);
            let mut reader: Option<TailReader> = None;
            // start.position applies only to the file present at startup. A
            // file that appears later is new and is read from the beginning.
            let mut initial = true;
            // Set on a read error so the reopen continues at the last consumed
            // offset instead of reading the whole file again
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
                        // A missing file is waited for, not an error, like
                        // tail -F. The deleted file can still be read through
                        // the open fd, so deliver its last lines first.
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
                        // A real stat failure such as EACCES or EIO. Keep the
                        // reader, since the open fd may still be fine. Keep
                        // `initial`, since nothing about the file was decided.
                        // Send the error to the error strategy like open errors.
                        io_error = Some(e);
                    }
                    Ok(meta) if meta.is_dir() => {
                        // A directory can never be the startup file, so any
                        // real file that appears later is new. The file being
                        // followed before, if any, was replaced. Its last lines
                        // can still be read through the open fd.
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
                            // The renamed file can still be read through the
                            // old fd. Deliver its last lines before switching,
                            // like tail -F does.
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
                            // Same file, so the cut content is gone and there
                            // is nothing left to read from the old handle
                            reader = None;
                        }

                        if reader.is_none() {
                            let at_end = initial && config.start_position == StartPosition::End;
                            // Clear `resume` only after a successful open. A
                            // failed open must not lose the marker and force
                            // the file to be read again from the start.
                            match open_reader(&config.path, at_end, config.skip_lines, resume) {
                                Ok(r) => {
                                    reader = Some(r);
                                    resume = None;
                                    initial = false;
                                }
                                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                    // The file was removed or renamed between
                                    // the metadata check and the open. Go back
                                    // to waiting. This is not an error.
                                    initial = false;
                                }
                                Err(e) => {
                                    // Possibly short lived, such as EACCES.
                                    // Keep `initial` so start.position=end
                                    // still applies once the file opens.
                                    io_error = Some(e);
                                }
                            }
                        } else {
                            initial = false;
                        }

                        // Read only when there are new bytes, so an idle poll
                        // makes no system call after the metadata check. If
                        // the identity does not match, the handle is newer
                        // than the path metadata, so read it anyway.
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
                                        // Save where reading stopped, leaving
                                        // out the held partial line so it is
                                        // not cut short. If the same file is
                                        // still there at reopen, reading picks
                                        // up there instead of starting over.
                                        // A reader that is dropping a long
                                        // line saves no marker. The new read
                                        // hits the cap again and recovers the
                                        // same way.
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

                // Wakes early on stop, so a long interval never delays stop()
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

    fn set_error_dlq_junction(
        &mut self,
        stream_name: &str,
        junction: Arc<Mutex<InputHandler>>,
    ) -> Result<(), EventFluxError> {
        // With no error.strategy the source drops failures, so there is no DLQ
        // to connect
        if let Some(ref mut ctx) = self.error_ctx {
            ctx.set_dlq_junction(stream_name, junction);
        }
        Ok(())
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

/// Factory for file sources.
///
/// Registered with EventFluxContext and used to build file sources from the
/// SQL WITH clause config.
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
        // File keys plus the shared error handling keys owned by
        // SourceErrorContext. Joined once, on first use.
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
        // The DLQ junction starts as None. The runtime calls
        // set_error_dlq_junction() when it attaches the source, which also sets
        // the stream name. Until then the path names the stream in error log
        // messages.
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
        // The clone keeps the strategy but starts with reset counters
        let ctx = cloned.error_ctx.expect("clone keeps the error strategy");
        assert_eq!(ctx.error_count(), 0);
    }

    #[test]
    fn test_restart_preserves_error_strategy() {
        // Regression (#142). start() used to take() the context, so a stop and
        // start cycle silently lost the configured strategy.
        #[derive(Debug)]
        struct NoopCallback;
        impl crate::core::stream::input::source::SourceCallback for NoopCallback {
            fn on_data(&self, _data: &[u8]) -> Result<(), EventFluxError> {
                Ok(())
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("in.jsonl");
        std::fs::write(&path, "").unwrap();

        let mut props = HashMap::new();
        props.insert("file.path".to_string(), path.display().to_string());
        props.insert("error.strategy".to_string(), "fail".to_string());

        let mut source = FileSource::from_properties(&props, None, "TestStream").unwrap();
        for _ in 0..2 {
            source.start(Arc::new(NoopCallback));
            source.stop();
            assert!(
                source.error_ctx.is_some(),
                "the configured error strategy must survive stop() for the next start()"
            );
        }
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
