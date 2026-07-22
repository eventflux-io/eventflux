# File Connector — Specification

**Status**: IMPLEMENTED (2026-07) — spec reviewed, single always-following source
**Priority**: 3 (per ROADMAP)
**Feature flag**: `file` (no external dependencies)
**Extension name**: `file` (source and sink)

---

## 1. Overview

The File connector ingests events from a local file and writes processed
events to local files. It targets the classic operational uses:

- **Replay then follow**: read an existing JSONL or CSV file through a query,
  then keep delivering lines as other processes append
  (`file.start.position = 'beginning'`, the default)
- **Log following**: start at the end of a growing file and follow appends,
  with rotation/truncation handling (`file.start.position = 'end'`)
- **Durable output**: append query results to a file with size/time-based
  rotation and optional compression of rolled files

The source **never finishes**: reaching EOF just means "nothing new yet" —
it keeps observing the file and delivers whatever any other application
appends. Everything is long-running and realtime; there is no bounded
"read once and stop" mode.

Everything is line-delimited in v1: **one line = one event payload** (JSONL
line, CSV row, or arbitrary bytes per line). This matches the one event = one
message contract every other connector follows, and both the JSON and CSV
mappers already consume exactly one record per payload.

## 2. Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Dependencies | **None** — `std::fs` + polling | No `notify`/inotify crate in v1. Polling with an interruptible interval (`sleep_while_running`) is deterministic on every platform, needs no watcher lifecycle, and the config surface stays compatible if a native watcher backend is added later (the poll interval simply becomes its fallback). Known costs, both documented: detection latency ≤ poll interval, and (since the rotated-away file is drained through the still-open fd before switching) only lines a writer appends to the old file after that final drain are missed. |
| One mode, one knob | No `file.mode`; only `file.start.position` | Since the source always keeps following after EOF, "read" vs "tail" collapses to where to begin. Less surface, no semantics fork. |
| Framing | Line-delimited (`\n`, `\r\n` tolerated) | One line = one event. Custom delimiters and multi-line records are future additive config. |
| Rotation detection | Size shrink → reopen from 0; path replacement (new identity: inode on Unix, creation time elsewhere) → reopen from 0 | The `tail -F` contract, detected per poll via `metadata()` — no OS watcher needed. |
| Offsets across restarts | **Not persisted in v1** | A file has no broker-side offsets; after a restart only `file.start.position` governs: `end` skips lines appended while down, `beginning` re-reads (duplicates downstream). Checkpointed `(file identity, offset)` via StateHolder is the designed follow-up (§9) — no config break. |
| Rolled-file compression | `zstd` (already a workspace dependency) | Zero new deps. See §9 for when gzip becomes worth adding. |
| Directory spool mode | **Deferred** | Observing one given file keeps v1 simple (reviewer decision). §9 sketches the forward path; `file.mode` stays reserved for it. |
| Blocking in `publish()` | Yes (write + flush policy) | Same backpressure-by-design contract as the HTTP/Kafka sinks. |

## 3. Architecture

```text
source:  SourceWorker thread → poll every interval:
           metadata check (missing / truncated / replaced → reopen)
           → read appended complete lines (partial line held)
           → SourceCallback → mapper → junction
sink:    Events → per event: SinkMapper → bytes → publish() → BufWriter (+ \n)
           → flush policy → rotation check (size/time) → roll (+ optional zstd)
```

Both sides are `std`-only and synchronous. The source embeds a `SourceWorker`
(interruptible polling, bounded stop, Drop-RAII) exactly like the other
connectors.

## 4. File Source

### 4.1 Configuration

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `file.path` | Yes | – | Path of the file to observe |
| `file.start.position` | No | `beginning` | `beginning` = deliver existing content first, then follow appends; `end` = only lines appended after startup |
| `file.poll.interval.ms` | No | `500` | How often to check for new data / rotation (interruptible) |
| `file.skip.lines` | No | `0` | Skip the first N lines (e.g. a CSV header row) — applies whenever reading starts from offset 0 (startup from `beginning`, and every rotation reopen) |
| `file.require.exists` | No | `false` | `false` = a missing file is awaited (`tail -F` semantics); `true` = fail fast at startup if absent |
| `error.*` | No | – | Standard error strategies (`drop`/`retry`/`dlq`/`fail`) via `SourceErrorContext` |

### 4.2 Semantics

- Startup: open, seek per `file.start.position` (`beginning` applies
  `file.skip.lines` first), then poll forever.
- Each poll delivers any appended **complete** lines. A partial final line
  (no terminator yet) is held until completed — writers that flush mid-line
  do not produce split events.
- **EOF is not completion**: after existing content is consumed, the source
  idles on the poll interval and delivers whatever other applications append
  later.
- **Rotation** (path points to a different file): the renamed-away file's
  remaining lines are drained through the still-open handle first, then
  reading switches to the new file from offset 0, re-applying
  `file.skip.lines`. **Truncation** (same file, size shrank): reopen from
  offset 0 — the overwritten content is gone by definition.
- **Missing file**: awaited (unless `file.require.exists = true` failed the
  start); a file appearing later is not an error, and a file disappearing
  mid-run returns to the awaiting state.
- **I/O errors** (permission loss, read failures): routed through the
  standard `error.*` strategy as connection errors — `retry` gives
  reconnect-with-backoff behavior. After a transient read error, reading
  resumes at the last consumed offset (skip budget preserved) rather than
  replaying the file.
- Payload bytes are the raw line without the terminator. Empty lines are
  skipped (consistent with "empty body → nothing to deliver" elsewhere).

### 4.3 Validation (`validate_connectivity`)

Parent directory exists and `file.path` is not a directory; the file
itself must exist only when `file.require.exists = true` (in which case it
must also open for reading).

## 5. File Sink

### 5.1 Configuration

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `file.path` | Yes | – | Output file path (parent directory must exist) |
| `file.append` | No | `true` | `true` = append to an existing file, `false` = truncate at startup |
| `file.flush.interval.ms` | No | `0` | `0` = flush after every event (durable, default); N > 0 = flush at most once every N ms, checked per publish — crash-loss bounded in bytes (write buffer), not wall time |
| `file.rotate.size.bytes` | No | `0` (off) | Roll the file when it reaches this size |
| `file.rotate.interval.ms` | No | `0` (off) | Roll the file at this age, counted from file open |
| `file.rotate.compression` | No | `none` | `none` or `zstd` — compress the rolled file (`<name>.zst`), applied in the publishing thread (blocking = backpressure) |

Both rotation triggers may be set; whichever fires first rolls. Rolled files
are renamed to `<path>.<UTC timestamp>` (e.g. `out.jsonl.20260722T101530Z`,
with a numeric suffix on collision), then a fresh file is opened at
`file.path`. Setting `file.rotate.compression` without any rotation trigger
is rejected at parse time (two sources of truth for "never rolls").

### 5.2 Semantics

- One event = one line: payload bytes + `\n`. The mapper produces the payload
  (JSON object, CSV row, raw bytes); the sink adds only the terminator.
- `publish()` returns after the write (and flush, under the default policy)
  succeeds — a returned `Ok` means the bytes reached the OS. Write errors are
  returned to the pipeline as connection errors.
- Rotation check runs inside `publish()` after the write, so a line is never
  split across files.
- `stop()` flushes and closes; the summary log reports written/failed/rolled counts
  (same shape as the other sinks).

### 5.3 Validation

Parent directory exists and is writable (probe: open the file in append mode
and close — creates it if absent, which matches append semantics).

## 6. Delivery Guarantees

- **Source**: at-least-once *within a run*. No offsets are persisted; a
  restart obeys `file.start.position` (`end` may skip lines written while
  down, `beginning` re-reads — both documented). Checkpointed offsets are
  the designed follow-up (§9).
- **Sink**: an event is durably in the file (through the OS) before
  `publish()` returns under the default flush policy; `file.flush.interval.ms > 0`
  trades a bounded loss window for throughput.

## 7. Feature Gating

Standard checklist (docs/writing_extensions.md):

- `[features] file = []` — no optional deps (zstd is already a root
  dependency); added to `connectors-all`
- `#[cfg(feature = "file")]` module declarations + registrations
- `GATED_OUT_EXTENSIONS` entry
- `#![cfg(feature = "file")]` on `tests/file_integration.rs`
- `cargo check --all-targets --features file` in CI, justfile, CONTRIBUTING

## 8. Testing Plan

Fully self-hosting via `tempfile` (already a dev-dependency) — nothing
`#[ignore]`d, no external services:

- **Unit (config)**: defaults, invalid `file.start.position`,
  compression-without-rotation rejected, skip-lines / flush / rotation
  parsing, validation failures (missing dir; `require.exists` with missing
  file)
- **Source**: existing JSONL and CSV content delivered from `beginning`
  (with `file.skip.lines = 1`); **EOF-then-append: content consumed, then an
  external append is delivered on the next poll** (the review scenario);
  `end` delivers only appends; partial line held until terminated;
  truncation reopen; rename-over rotation reopen; missing file appears
  later; prompt stop during a long poll interval
- **Sink**: append vs truncate; one line per event; flush-per-event
  durability; size rotation (rolled name format, fresh file continues); time
  rotation; zstd-compressed roll (decode and assert content)
- **Round trip**: file sink → file source through the direct connector API
  (the self-hosting analogue of the HTTP round trip)
- **E2E**: SQL app reading JSONL, filtering, writing JSONL out (CSV framing
  is covered by the skip-header source test)
- **Regression pins** from the audit rounds: rotated-file tail salvage,
  oversized-line drop/resync, truncate-mode reopen never wipes, same-second
  zstd rolls never collide

Plus `examples/file.eventflux`: replay a JSONL file of trades → filter →
JSONL output, verifiable with `printf`, `echo >>`, and `tail -f` alone.

## 9. Deferred / Forward-Compatibility Notes

| Feature | Forward path |
|---------|--------------|
| Directory spool mode (process files appearing in a dir; move/delete after processing) | Reserved `file.mode = 'dir'` (unknown keys today produce no silent behavior); config keys would be additive (`file.dir.uri`, `file.action.after.process`) |
| Offset checkpointing across restarts | `StateHolder` integration: persist `(file identity, offset)`; activates via the existing checkpoint machinery, no config change needed (`file.start.position` becomes the no-checkpoint fallback) |
| Native FS watching (inotify/FSEvents via `notify`) | Backend swap behind the same config; `file.poll.interval.ms` becomes the fallback for platforms/filesystems without native events |
| gzip rolled files | Additive `file.rotate.compression = 'gzip'` value (`flat2` dep at that point). Needed when rolled files must interop with the gzip ecosystem: stock `zcat`/`gzip` on machines without zstd, logrotate-style tooling, log shippers and S3/data-lake ingestion pipelines that auto-detect only `.gz`. Until a consumer like that exists, zstd is strictly better (faster, smaller). |
| Custom delimiters / multi-line records | Additive `file.delimiter` / framing config |
| Max line length guard | v1 ships a fixed 8 MiB safety cap (oversized lines dropped, reading resyncs at the next terminator); a configurable `file.max.line.bytes` is additive |
| Writing to a file being tailed by the same app | Works (sink flushes complete lines; source holds partial lines) — exercised by the round-trip test |
