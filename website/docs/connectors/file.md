---
sidebar_position: 2
title: File Connector
description: Replay and follow files, write line-delimited output with rotation
---

# File Connector

The File connector ingests events from a local file and writes processed events to local files. The source **never finishes**: after consuming existing content it keeps observing the file and delivers whatever any other application appends — everything is long-running and realtime. It supports JSON, CSV, and bytes formats.

Everything is line-delimited: **one line = one event** (a JSONL line, a CSV row, or arbitrary bytes per line).

## Prerequisites

The File connector is feature-gated but adds **no new dependencies** (rolled-file compression and timestamps use zstd and chrono, which are already core dependencies):

```bash
cargo build --release --features file
# or: cargo build --release --features connectors-all
```

## File Source

Observes one file, delivering each complete line as one payload.

```sql
CREATE STREAM Trades (symbol STRING, price DOUBLE, volume INT) WITH (
    type = 'source',
    extension = 'file',
    format = 'json',
    "file.path" = '/var/data/trades.jsonl',
    "file.poll.interval.ms" = '200'
);
```

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `file.path` | Yes | - | Path of the file to observe |
| `file.start.position` | No | `beginning` | `beginning` = deliver existing content first, then follow appends; `end` = only lines appended after startup |
| `file.poll.interval.ms` | No | `500` | How often to check for new data / rotation (interruptible — long intervals never delay shutdown) |
| `file.skip.lines` | No | `0` | Skip the first N lines (e.g. a CSV header row); re-applied whenever reading restarts from the top (rotation) |
| `file.require.exists` | No | `false` | `false` = a missing file is awaited (`tail -F` semantics); `true` = fail fast at startup if absent |
| `error.*` | No | - | Error strategies (`drop`/`retry`/`dlq`/`fail`) — same options as [Kafka](/docs/connectors/kafka#error-handling) |

Behavior:

- **Rotation and truncation** follow the `tail -F` contract: if the path points at a new file (rename-over / recreate), the renamed-away file's remaining lines are first drained through the still-open handle, then reading switches to the new file from the top. Only lines a writer appends to the old file *after* that final drain are missed — keep poll intervals short if writers keep old files open long after rotation. A truncated file (same file object, size shrank) reopens from the top; its overwritten content is gone by definition.
- A **partial final line** (no terminator yet) is held until completed — writers that flush mid-line never produce split events.
- Empty lines are skipped; payload bytes are the raw line without the terminator (`\n` and `\r\n` both accepted).
- I/O errors are routed through the `error.*` strategy as connection errors; after a transient read error, reading resumes where it stopped instead of replaying the file. Unterminated lines beyond an 8 MiB safety cap are dropped, with reading resyncing at the next terminator.

**Restarts:** no read offset is persisted. After an app restart, `file.start.position` governs: `end` skips lines written while down; `beginning` re-reads the file (duplicates downstream). Checkpointed offsets are planned as a follow-up.

## File Sink

Appends each event as one line. Rotation and compressed rolls are built in.

```sql
CREATE STREAM HighVolume (symbol STRING, volume INT) WITH (
    type = 'sink',
    extension = 'file',
    format = 'json',
    "file.path" = '/var/data/high_volume.jsonl',
    "file.rotate.size.bytes" = '104857600',
    "file.rotate.compression" = 'zstd'
);
```

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `file.path` | Yes | - | Output file path (parent directory must exist) |
| `file.append` | No | `true` | `true` = append to an existing file, `false` = truncate at startup |
| `file.flush.interval.ms` | No | `0` | `0` = flush after every event (durable, default); N > 0 = flush at most once every N ms, checked per publish. The crash-loss window is bounded in bytes (the write buffer), not in wall time — a trickle stream can hold buffered events until its next publish or `stop()` |
| `file.rotate.size.bytes` | No | `0` (off) | Roll the file when it reaches this size |
| `file.rotate.interval.ms` | No | `0` (off) | Roll the file at this age, counted from file open |
| `file.rotate.compression` | No | `none` | `none` or `zstd` — compress the rolled file to `<name>.zst` |

Behavior:

- One event = one line: the mapper's payload plus `\n`. Rotation checks run after the write, so a line never splits across files.
- Rolled files are renamed to `<path>.<UTC timestamp>` (e.g. `out.jsonl.20260722T101530Z`), then a fresh file continues at `file.path`. Both rotation triggers may be set; whichever fires first rolls.
- Under the default flush policy an event is durably in the file (through the OS) before the publish returns; compression runs in the publishing thread — backpressure by design.
- Setting `file.rotate.compression` without a rotation trigger is rejected at startup.

## Connection Validation

At startup (fail-fast):

- **Source**: the parent directory must exist and `file.path` must not be a directory; the file itself must exist only when `file.require.exists = true`.
- **Sink**: the file is opened in append mode and closed (creating it if absent, matching append semantics) — missing or unwritable directories fail immediately.

## Complete End-to-End Example

See [`examples/file.eventflux`](https://github.com/eventflux-io/eventflux/blob/main/examples/file.eventflux) — replay a JSONL file, filter, append matches to an output file; testable with `printf`, `echo >>`, and `tail -f` alone.

## Not Yet Supported

- Directory spool mode (processing files appearing in a directory, with move/delete after processing) — v1 deliberately observes one given file
- Offset checkpointing across restarts — see **Restarts** above
- Native filesystem watching (inotify/FSEvents) — polling is the v1 backend; the config is forward-compatible
- gzip-compressed rolls — `zstd` only until interop with gzip-only tooling (stock `zcat`, logrotate ecosystems, `.gz`-detecting shippers) is needed
- Custom delimiters / multi-line records — line-delimited only
