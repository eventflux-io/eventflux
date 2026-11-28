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

//! # File Connector Integration Tests
//!
//! Requires building with the `file` feature:
//! `cargo test --features file --test file_integration`
//!
//! The tests host themselves. Every test uses temp files, so nothing is
//! `#[ignore]`d and no outside service is needed.

#![cfg(feature = "file")]

#[path = "common/mod.rs"]
mod common;
use common::{CollectingCallback, DLQ_COLUMNS};

use eventflux::core::stream::input::source::file_source::FileSource;
use eventflux::core::stream::input::source::Source;
use eventflux::core::stream::output::sink::file_sink::FileSink;
use eventflux::core::stream::output::sink::Sink;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn source_props(path: &Path) -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert("file.path".to_string(), path.display().to_string());
    // Poll every 20 ms so the tests stay fast
    props.insert("file.poll.interval.ms".to_string(), "20".to_string());
    props
}

fn sink_props(path: &Path) -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert("file.path".to_string(), path.display().to_string());
    props
}

fn append(path: &Path, content: &str) {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    file.write_all(content.as_bytes()).unwrap();
    file.flush().unwrap();
}

fn started_source(props: &HashMap<String, String>) -> (FileSource, CollectingCallback) {
    let callback = CollectingCallback::new();
    let mut source = FileSource::from_properties(props, None, "Test").unwrap();
    source.start(Arc::new(callback.clone()));
    (source, callback)
}

// ============================================================================
// Source. Replay the file, then follow it (the default behaviour)
// ============================================================================

#[test]
fn test_source_delivers_existing_content_from_beginning() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.jsonl");
    fs::write(&path, "{\"n\":1}\n{\"n\":2}\n").unwrap();

    let (mut source, callback) = started_source(&source_props(&path));
    let payloads = callback.wait_for(2, Duration::from_secs(10));
    source.stop();

    assert_eq!(payloads, vec![b"{\"n\":1}".to_vec(), b"{\"n\":2}".to_vec()]);
}

#[test]
fn test_source_eof_then_external_append_is_delivered() {
    // The case raised in review. Existing content is read, and reaching EOF does
    // not mean done. When another application appends, the new lines must arrive.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.jsonl");
    fs::write(&path, "{\"n\":1}\n").unwrap();

    let (mut source, callback) = started_source(&source_props(&path));
    assert_eq!(callback.wait_for(1, Duration::from_secs(10)).len(), 1);

    // Stand in for another application appending after EOF was reached
    append(&path, "{\"n\":2}\n{\"n\":3}\n");

    let payloads = callback.wait_for(3, Duration::from_secs(10));
    source.stop();

    assert_eq!(
        payloads,
        vec![
            b"{\"n\":1}".to_vec(),
            b"{\"n\":2}".to_vec(),
            b"{\"n\":3}".to_vec(),
        ]
    );
}

#[test]
fn test_source_skip_lines_for_csv_header() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.csv");
    fs::write(&path, "symbol,price\nAAPL,185.5\nGOOGL,142.3\n").unwrap();

    let mut props = source_props(&path);
    props.insert("file.skip.lines".to_string(), "1".to_string());

    let (mut source, callback) = started_source(&props);
    let payloads = callback.wait_for(2, Duration::from_secs(10));
    source.stop();

    assert_eq!(
        payloads,
        vec![b"AAPL,185.5".to_vec(), b"GOOGL,142.3".to_vec()],
        "the header row must be skipped"
    );
}

#[test]
fn test_source_start_position_end_only_delivers_appends() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.jsonl");
    fs::write(&path, "{\"old\":true}\n").unwrap();

    let mut props = source_props(&path);
    props.insert("file.start.position".to_string(), "end".to_string());

    let (mut source, callback) = started_source(&props);
    // Give the source time to open at the end before appending
    std::thread::sleep(Duration::from_millis(100));
    append(&path, "{\"new\":true}\n");

    let payloads = callback.wait_for(1, Duration::from_secs(10));
    source.stop();

    assert_eq!(
        payloads,
        vec![b"{\"new\":true}".to_vec()],
        "existing content must not be delivered with start.position=end"
    );
}

#[test]
fn test_source_holds_partial_line_until_terminated() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.jsonl");
    fs::write(&path, "").unwrap();

    let (mut source, callback) = started_source(&source_props(&path));

    append(&path, "{\"half\":");
    // A partial line must not be delivered
    std::thread::sleep(Duration::from_millis(150));
    assert!(callback.wait_for(1, Duration::from_millis(1)).is_empty());

    append(&path, "1}\n");
    let payloads = callback.wait_for(1, Duration::from_secs(10));
    source.stop();

    assert_eq!(
        payloads,
        vec![b"{\"half\":1}".to_vec()],
        "the two writes must arrive as one un-split event"
    );
}

// ============================================================================
// Source. Rotation, truncation and missing files (the tail -F behaviour)
// ============================================================================

#[test]
fn test_source_reopens_after_truncation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.log");
    fs::write(&path, "before\n").unwrap();

    let (mut source, callback) = started_source(&source_props(&path));
    assert_eq!(callback.wait_for(1, Duration::from_secs(10)).len(), 1);

    // Truncate and rewrite, like logrotate copytruncate
    fs::write(&path, "after\n").unwrap();

    let payloads = callback.wait_for(2, Duration::from_secs(10));
    source.stop();

    assert_eq!(payloads, vec![b"before".to_vec(), b"after".to_vec()]);
}

#[test]
fn test_source_reopens_after_rename_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.log");
    fs::write(&path, "in-old-file\n").unwrap();

    let (mut source, callback) = started_source(&source_props(&path));
    assert_eq!(callback.wait_for(1, Duration::from_secs(10)).len(), 1);

    // Classic rotation. Rename the file away, then create a new one at the same path.
    fs::rename(&path, dir.path().join("in.log.1")).unwrap();
    fs::write(&path, "in-new-file\n").unwrap();

    let payloads = callback.wait_for(2, Duration::from_secs(10));
    source.stop();

    assert_eq!(
        payloads,
        vec![b"in-old-file".to_vec(), b"in-new-file".to_vec()]
    );
}

#[test]
fn test_source_delivers_rotated_file_tail() {
    // Lines appended to the old file just before rotation must still be read
    // through the handle that is still open, as tail -F does, not lost
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.log");
    fs::write(&path, "one\n").unwrap();

    let mut props = source_props(&path);
    // A wide 300 ms poll gap so the append and rename below land between polls
    props.insert("file.poll.interval.ms".to_string(), "300".to_string());

    let (mut source, callback) = started_source(&props);
    assert_eq!(callback.wait_for(1, Duration::from_secs(10)).len(), 1);

    // Within one poll window the tail grows, then the file is rotated away
    append(&path, "two\n");
    fs::rename(&path, dir.path().join("in.log.1")).unwrap();
    fs::write(&path, "three\n").unwrap();

    let payloads = callback.wait_for(3, Duration::from_secs(10));
    source.stop();

    assert_eq!(
        payloads,
        vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()],
        "the rotated-away file's tail must be delivered before switching"
    );
}

#[test]
fn test_source_drops_oversized_line_and_resyncs() {
    // A blob with no line break that is over the safety limit must be dropped, so
    // memory does not run out, and reading must pick up again at the next real line
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.log");
    let mut blob = vec![b'x'; 9 * 1024 * 1024]; // over the 8 MiB limit, no newline
    blob.push(b'\n');
    blob.extend_from_slice(b"ok\n");
    fs::write(&path, &blob).unwrap();

    let (mut source, callback) = started_source(&source_props(&path));
    let payloads = callback.wait_for(1, Duration::from_secs(10));
    source.stop();

    assert_eq!(
        payloads,
        vec![b"ok".to_vec()],
        "the oversized line is dropped, the following line delivered"
    );
}

#[test]
fn test_source_awaits_missing_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("late.jsonl");

    // The file does not exist yet. The source must wait, not fail.
    let (mut source, callback) = started_source(&source_props(&path));
    std::thread::sleep(Duration::from_millis(100));
    fs::write(&path, "{\"late\":true}\n").unwrap();

    let payloads = callback.wait_for(1, Duration::from_secs(10));
    source.stop();

    assert_eq!(payloads, vec![b"{\"late\":true}".to_vec()]);
}

#[test]
fn test_source_long_interval_stops_promptly() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.jsonl");
    fs::write(&path, "{\"n\":1}\n").unwrap();

    let mut props = source_props(&path);
    // An interval far beyond the stop grace period
    props.insert("file.poll.interval.ms".to_string(), "600000".to_string());

    let (mut source, callback) = started_source(&props);
    callback.wait_for(1, Duration::from_secs(10));

    // sleep_while_running makes the interval interruptible
    let start = Instant::now();
    source.stop();
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "stop() must interrupt a long poll interval"
    );
}

// ============================================================================
// Sink
// ============================================================================

#[test]
fn test_sink_one_line_per_event_append_and_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.jsonl");
    fs::write(&path, "{\"pre\":true}\n").unwrap();

    // Append (default) keeps existing content
    let sink = FileSink::from_properties(&sink_props(&path)).unwrap();
    sink.start();
    sink.publish(b"{\"n\":1}").unwrap();
    sink.publish(b"{\"n\":2}").unwrap();
    sink.stop();
    assert_eq!(
        fs::read_to_string(&path).unwrap(),
        "{\"pre\":true}\n{\"n\":1}\n{\"n\":2}\n"
    );

    // Truncate discards it
    let mut props = sink_props(&path);
    props.insert("file.append".to_string(), "false".to_string());
    let sink = FileSink::from_properties(&props).unwrap();
    sink.start();
    sink.publish(b"{\"n\":3}").unwrap();
    sink.stop();
    assert_eq!(fs::read_to_string(&path).unwrap(), "{\"n\":3}\n");
}

#[test]
fn test_sink_flush_per_event_is_immediately_readable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.jsonl");

    let sink = FileSink::from_properties(&sink_props(&path)).unwrap();
    sink.start();
    sink.publish(b"{\"n\":1}").unwrap();

    // The default policy flushes every event, so it is visible before stop()
    assert_eq!(fs::read_to_string(&path).unwrap(), "{\"n\":1}\n");
    sink.stop();
}

#[test]
fn test_sink_size_rotation_rolls_and_continues() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.log");

    let mut props = sink_props(&path);
    props.insert("file.rotate.size.bytes".to_string(), "10".to_string());
    let sink = FileSink::from_properties(&props).unwrap();
    sink.start();

    sink.publish(b"0123456789").unwrap(); // 11 bytes with \n → rolls
    sink.publish(b"next").unwrap(); // lands in the fresh file
    sink.stop();

    let rolled: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p != &path)
        .collect();
    assert_eq!(rolled.len(), 1, "exactly one rolled file: {rolled:?}");
    assert!(
        rolled[0]
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("out.log."),
        "rolled name keeps the original as prefix: {rolled:?}"
    );
    assert_eq!(fs::read_to_string(&rolled[0]).unwrap(), "0123456789\n");
    assert_eq!(fs::read_to_string(&path).unwrap(), "next\n");
}

#[test]
fn test_sink_time_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.log");

    let mut props = sink_props(&path);
    props.insert("file.rotate.interval.ms".to_string(), "50".to_string());
    let sink = FileSink::from_properties(&props).unwrap();
    sink.start();

    sink.publish(b"first").unwrap();
    std::thread::sleep(Duration::from_millis(80));
    sink.publish(b"second").unwrap(); // age trigger fires after this write
    sink.publish(b"third").unwrap();
    sink.stop();

    let rolled_count = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p != &path)
        .count();
    assert!(rolled_count >= 1, "age-based rotation must have rolled");
    assert!(fs::read_to_string(&path).unwrap().contains("third"));
}

#[test]
fn test_sink_same_second_zstd_rolls_do_not_overwrite() {
    // Two rolls within one second get the same timestamp. The name clash check
    // must see the compressed name of the first roll, or the second roll quietly
    // overwrites it.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.log");

    let mut props = sink_props(&path);
    props.insert("file.rotate.size.bytes".to_string(), "5".to_string());
    props.insert("file.rotate.compression".to_string(), "zstd".to_string());
    let sink = FileSink::from_properties(&props).unwrap();
    sink.start();

    sink.publish(b"first-roll").unwrap(); // rolls and compresses
    sink.publish(b"second-roll").unwrap(); // rolls again, same second
    sink.stop();

    let mut rolled: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p != &path)
        .collect();
    rolled.sort();
    assert_eq!(rolled.len(), 2, "both rolls must survive: {rolled:?}");
    let decoded: Vec<Vec<u8>> = rolled
        .iter()
        .map(|p| zstd::stream::decode_all(fs::File::open(p).unwrap()).unwrap())
        .collect();
    assert!(decoded.contains(&b"first-roll\n".to_vec()), "{decoded:?}");
    assert!(decoded.contains(&b"second-roll\n".to_vec()), "{decoded:?}");
}

#[test]
fn test_sink_truncate_mode_reopen_does_not_wipe() {
    // file.append=false truncates ONLY in start(). A later reopen, for recovery or
    // a publish after stop, must append and not wipe the data already written.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.jsonl");

    let mut props = sink_props(&path);
    props.insert("file.append".to_string(), "false".to_string());
    let sink = FileSink::from_properties(&props).unwrap();
    sink.start();
    sink.publish(b"before-stop").unwrap();
    sink.stop();

    sink.publish(b"after-stop").unwrap();
    assert_eq!(
        fs::read_to_string(&path).unwrap(),
        "before-stop\nafter-stop\n",
        "the reopen must not reapply startup truncation"
    );

    // A real start or restart DOES truncate, every time, even though the stray
    // publish above already reopened the file
    sink.start();
    sink.publish(b"after-restart").unwrap();
    sink.stop();
    assert_eq!(
        fs::read_to_string(&path).unwrap(),
        "after-restart\n",
        "start() must truncate deterministically with file.append = false"
    );
}

#[test]
fn test_sink_zstd_compressed_roll_decodes_to_content() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.log");

    let mut props = sink_props(&path);
    props.insert("file.rotate.size.bytes".to_string(), "10".to_string());
    props.insert("file.rotate.compression".to_string(), "zstd".to_string());
    let sink = FileSink::from_properties(&props).unwrap();
    sink.start();

    sink.publish(b"0123456789").unwrap(); // rolls and compresses
    sink.publish(b"next").unwrap();
    sink.stop();

    let rolled: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p != &path)
        .collect();
    assert_eq!(rolled.len(), 1, "one compressed rolled file: {rolled:?}");
    assert!(
        rolled[0].extension().unwrap() == "zst",
        "rolled file is .zst: {rolled:?}"
    );
    let decoded = zstd::stream::decode_all(fs::File::open(&rolled[0]).unwrap()).unwrap();
    assert_eq!(decoded, b"0123456789\n");
    assert_eq!(fs::read_to_string(&path).unwrap(), "next\n");
}

// ============================================================================
// Round trip, file sink → file source, the file version of the HTTP round trip
// ============================================================================

#[test]
fn test_sink_to_source_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pipe.jsonl");

    let (mut source, callback) = started_source(&source_props(&path));

    let sink = FileSink::from_properties(&sink_props(&path)).unwrap();
    sink.start();
    sink.publish(b"{\"n\":1}").unwrap();
    sink.publish(b"{\"n\":2}").unwrap();
    sink.publish(b"{\"n\":3}").unwrap();
    sink.stop();

    let payloads = callback.wait_for(3, Duration::from_secs(10));
    source.stop();

    assert_eq!(
        payloads,
        vec![
            b"{\"n\":1}".to_vec(),
            b"{\"n\":2}".to_vec(),
            b"{\"n\":3}".to_vec(),
        ]
    );
}

// ============================================================================
// End to end SQL app. Replay a JSONL file, filter, write JSONL out
// ============================================================================

/// Polls until `path` holds at least `min_lines` lines or 10 s have passed.
fn await_jsonl_lines(path: &Path, min_lines: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let content = fs::read_to_string(path).unwrap_or_default();
        if content.lines().count() >= min_lines || Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Read a JSONL file into parsed JSON values, one per line
fn read_jsonl(path: &Path) -> Vec<serde_json::Value> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[tokio::test]
async fn test_e2e_sql_file_to_file() {
    use eventflux::core::eventflux_manager::EventFluxManager;

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("trades.jsonl");
    let output = dir.path().join("alerts.jsonl");
    fs::write(
        &input,
        concat!(
            "{\"symbol\":\"AAPL\",\"volume\":5000}\n",
            "{\"symbol\":\"GOOGL\",\"volume\":800}\n",
            "{\"symbol\":\"MSFT\",\"volume\":1500}\n",
        ),
    )
    .unwrap();

    let sql = format!(
        r#"
        CREATE STREAM Trades (symbol STRING, volume INT) WITH (
            type = 'source', extension = 'file', format = 'json',
            "file.path" = '{input}',
            "file.poll.interval.ms" = '20'
        );
        CREATE STREAM HighVolume (symbol STRING, volume INT) WITH (
            type = 'sink', extension = 'file', format = 'json',
            "file.path" = '{output}'
        );
        INSERT INTO HighVolume SELECT symbol, volume FROM Trades WHERE volume > 1000;
    "#,
        input = input.display(),
        output = output.display(),
    );

    let manager = EventFluxManager::new();
    let runtime = manager
        .create_eventflux_app_runtime_from_string(&sql)
        .await
        .expect("runtime");
    runtime.start().expect("start");

    // Wait until both events that pass the filter are written to the output file
    await_jsonl_lines(&output, 2);
    runtime.shutdown();

    let lines = read_jsonl(&output);
    assert_eq!(lines.len(), 2, "GOOGL (volume 800) must be filtered out");
    assert_eq!(lines[0]["symbol"], "AAPL");
    assert_eq!(lines[0]["volume"], 5000);
    assert_eq!(lines[1]["symbol"], "MSFT");
    assert_eq!(lines[1]["volume"], 1500);
}

// ============================================================================
// End to end SQL app. JSON fields map by name, not position (#201)
// ============================================================================

/// Regression test for https://github.com/eventflux-io/eventflux/issues/201.
/// The stream schema declares (price, symbol, volume) while the JSON lines
/// carry the keys in a different order (symbol, price, volume). The mapper must
/// match fields by name. It used to map them by position, which gave
/// {"price":"AAPL","symbol":185.5,...}.
#[tokio::test]
async fn test_e2e_sql_json_fields_map_by_name_not_position() {
    use eventflux::core::eventflux_manager::EventFluxManager;

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("trades.jsonl");
    let output = dir.path().join("out.jsonl");
    fs::write(
        &input,
        concat!(
            "{\"symbol\":\"AAPL\",\"price\":185.5,\"volume\":5000}\n",
            "{\"symbol\":\"GOOGL\",\"price\":142.3,\"volume\":800}\n",
        ),
    )
    .unwrap();

    // Schema order deliberately differs from the JSON key order
    let sql = format!(
        r#"
        CREATE STREAM Trades (price DOUBLE, symbol STRING, volume INT) WITH (
            type = 'source', extension = 'file', format = 'json',
            "file.path" = '{input}',
            "file.start.position" = 'beginning',
            "file.poll.interval.ms" = '20'
        );
        CREATE STREAM EventOutput (price DOUBLE, symbol STRING, volume INT) WITH (
            type = 'sink', extension = 'file', format = 'json',
            "file.path" = '{output}'
        );
        INSERT INTO EventOutput SELECT * FROM Trades;
    "#,
        input = input.display(),
        output = output.display(),
    );

    let manager = EventFluxManager::new();
    let runtime = manager
        .create_eventflux_app_runtime_from_string(&sql)
        .await
        .expect("runtime");
    runtime.start().expect("start");

    await_jsonl_lines(&output, 2);
    runtime.shutdown();

    let lines = read_jsonl(&output);
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["price"], 185.5);
    assert_eq!(lines[0]["symbol"], "AAPL");
    assert_eq!(lines[0]["volume"], 5000);
    assert_eq!(lines[1]["price"], 142.3);
    assert_eq!(lines[1]["symbol"], "GOOGL");
    assert_eq!(lines[1]["volume"], 800);
}

/// Name matching ignores case, as documented. JSON keys in a different case
/// still map to the right stream fields.
#[tokio::test]
async fn test_e2e_sql_json_fields_map_case_insensitive() {
    use eventflux::core::eventflux_manager::EventFluxManager;

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("trades.jsonl");
    let output = dir.path().join("out.jsonl");
    fs::write(
        &input,
        "{\"Symbol\":\"AAPL\",\"PRICE\":185.5,\"Volume\":5000}\n",
    )
    .unwrap();

    let sql = format!(
        r#"
        CREATE STREAM Trades (price DOUBLE, symbol STRING, volume INT) WITH (
            type = 'source', extension = 'file', format = 'json',
            "file.path" = '{input}',
            "file.poll.interval.ms" = '20'
        );
        CREATE STREAM EventOutput (price DOUBLE, symbol STRING, volume INT) WITH (
            type = 'sink', extension = 'file', format = 'json',
            "file.path" = '{output}'
        );
        INSERT INTO EventOutput SELECT * FROM Trades;
    "#,
        input = input.display(),
        output = output.display(),
    );

    let manager = EventFluxManager::new();
    let runtime = manager
        .create_eventflux_app_runtime_from_string(&sql)
        .await
        .expect("runtime");
    runtime.start().expect("start");

    await_jsonl_lines(&output, 1);
    runtime.shutdown();

    let lines = read_jsonl(&output);
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["price"], 185.5);
    assert_eq!(lines[0]["symbol"], "AAPL");
    assert_eq!(lines[0]["volume"], 5000);
}

/// A JSON source whose schema declares the same field twice cannot be
/// mapped by name ({"id": 1} would fill both positions), so the app must
/// fail at creation instead of starting and corrupting events.
#[tokio::test]
async fn test_e2e_sql_json_source_duplicate_field_names_fail_at_creation() {
    use eventflux::core::eventflux_manager::EventFluxManager;

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.jsonl");
    let output = dir.path().join("out.jsonl");
    fs::write(&input, "{\"id\":1}\n").unwrap();

    let sql = format!(
        r#"
        CREATE STREAM Src (id INT, id INT) WITH (
            type = 'source', extension = 'file', format = 'json',
            "file.path" = '{input}'
        );
        CREATE STREAM Out (a INT, b INT) WITH (
            type = 'sink', extension = 'file', format = 'json',
            "file.path" = '{output}'
        );
        INSERT INTO Out SELECT * FROM Src;
    "#,
        input = input.display(),
        output = output.display(),
    );

    let manager = EventFluxManager::new();
    let result = match manager.create_eventflux_app_runtime_from_string(&sql).await {
        Ok(runtime) => runtime.start().map(|_| runtime).map_err(|e| e.to_string()),
        Err(e) => Err(e.to_string()),
    };
    let err = match result {
        Ok(runtime) => {
            runtime.shutdown();
            panic!("app with duplicate source field names must not start");
        }
        Err(e) => e,
    };
    assert!(err.contains("more than once"), "got: {err}");
}

// ============================================================================
// End to end SQL app. DLQ error strategy (#207)
// ============================================================================

/// An `Orders` JSON file source with `error.strategy = strategy` that names
/// `dlq_stream` as its DLQ
fn orders_source(input: &Path, strategy: &str, dlq_stream: &str) -> String {
    format!(
        r#"
        CREATE STREAM Orders (id STRING, amount DOUBLE) WITH (
            type = 'source', extension = 'file', format = 'json',
            "file.path" = '{input}',
            "file.poll.interval.ms" = '20',
            "error.strategy" = '{strategy}',
            "error.dlq.stream" = '{dlq_stream}'
        );
    "#,
        input = input.display(),
    )
}

/// An `Orders` JSON file source that routes failures to `dlq_stream`
fn orders_source_with_dlq(input: &Path, dlq_stream: &str) -> String {
    orders_source(input, "dlq", dlq_stream)
}

/// One malformed record between two valid ones
fn write_orders_with_malformed_record(input: &Path) {
    fs::write(
        input,
        concat!(
            "{\"id\":\"o1\",\"amount\":10.5}\n",
            "this is not json\n",
            "{\"id\":\"o2\",\"amount\":20.0}\n",
        ),
    )
    .unwrap();
}

/// Asserts that `dlq_rows` holds exactly the one malformed record sent to the DLQ.
fn assert_single_dlq_row_for_malformed_record(dlq_rows: &[serde_json::Value]) {
    assert_eq!(
        dlq_rows.len(),
        1,
        "exactly the malformed record is dead-lettered"
    );
    let row = &dlq_rows[0];
    assert_eq!(row["streamName"], "Orders");
    assert_eq!(row["attemptCount"], 1);
    assert!(
        !row["errorType"].as_str().unwrap_or_default().is_empty(),
        "errorType must be set: {row}"
    );
    assert!(
        !row["errorMessage"].as_str().unwrap_or_default().is_empty(),
        "errorMessage must be set: {row}"
    );
    assert!(row["timestamp"].as_i64().unwrap_or_default() > 0, "{row}");

    // originalEvent is the serialized fallback event carrying the raw bytes
    use eventflux::core::event::{event::Event, value::AttributeValue};
    let original: Event = serde_json::from_str(row["originalEvent"].as_str().unwrap())
        .expect("originalEvent is a serialized Event");
    assert_eq!(
        original.data,
        vec![AttributeValue::Bytes(b"this is not json".to_vec())],
        "originalEvent must carry the failed payload"
    );
}

/// Runs `sql` on `manager` against [`write_orders_with_malformed_record`] input.
/// Asserts that both valid records reach `output` and only the malformed one
/// reaches the DLQ file `dlq`.
async fn assert_malformed_record_dead_lettered(
    manager: eventflux::core::eventflux_manager::EventFluxManager,
    sql: &str,
    output: &Path,
    dlq: &Path,
) {
    let runtime = manager
        .create_eventflux_app_runtime_from_string(sql)
        .await
        .expect("runtime");
    runtime.start().expect("start");

    await_jsonl_lines(output, 2);
    await_jsonl_lines(dlq, 1);
    runtime.shutdown();

    let out_rows = read_jsonl(output);
    assert_eq!(out_rows.len(), 2, "valid records still flow");
    assert_eq!(out_rows[0]["id"], "o1");
    assert_eq!(out_rows[1]["id"], "o2");

    assert_single_dlq_row_for_malformed_record(&read_jsonl(dlq));
}

/// Regression test for https://github.com/eventflux-io/eventflux/issues/207.
/// A record the mapper rejects must land in the DLQ stream, here a file sink,
/// instead of being logged and dropped, while valid records carry on.
#[tokio::test]
async fn test_e2e_sql_dlq_receives_malformed_record() {
    use eventflux::core::eventflux_manager::EventFluxManager;

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("orders.jsonl");
    let output = dir.path().join("out.jsonl");
    let dlq = dir.path().join("errors.jsonl");
    write_orders_with_malformed_record(&input);

    let sql = format!(
        r#"
        {source}
        CREATE STREAM OrderErrors ({DLQ_COLUMNS}) WITH (
            type = 'sink', extension = 'file', format = 'json',
            "file.path" = '{dlq}'
        );
        CREATE STREAM Out (id STRING, amount DOUBLE) WITH (
            type = 'sink', extension = 'file', format = 'json',
            "file.path" = '{output}'
        );
        INSERT INTO Out SELECT * FROM Orders;
    "#,
        source = orders_source_with_dlq(&input, "OrderErrors"),
        dlq = dlq.display(),
        output = output.display(),
    );

    assert_malformed_record_dead_lettered(EventFluxManager::new(), &sql, &output, &dlq).await;
}

/// The DLQ stream may be a plain internal stream that queries read from. This is
/// the documented "internal DLQ" pattern. Events sent to the DLQ flow through it.
#[tokio::test]
async fn test_e2e_sql_dlq_internal_stream_feeds_queries() {
    use eventflux::core::eventflux_manager::EventFluxManager;

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("orders.jsonl");
    let dlq_out = dir.path().join("errors.jsonl");
    write_orders_with_malformed_record(&input);

    let sql = format!(
        r#"
        {source}
        CREATE STREAM OrderErrors ({DLQ_COLUMNS});
        CREATE STREAM ErrorSink ({DLQ_COLUMNS}) WITH (
            type = 'sink', extension = 'file', format = 'json',
            "file.path" = '{dlq_out}'
        );
        INSERT INTO ErrorSink SELECT * FROM OrderErrors;
    "#,
        source = orders_source_with_dlq(&input, "OrderErrors"),
        dlq_out = dlq_out.display(),
    );

    let manager = EventFluxManager::new();
    let runtime = manager
        .create_eventflux_app_runtime_from_string(&sql)
        .await
        .expect("runtime");
    runtime.start().expect("start");

    await_jsonl_lines(&dlq_out, 1);
    runtime.shutdown();

    assert_single_dlq_row_for_malformed_record(&read_jsonl(&dlq_out));
}

/// strategy=retry with a DLQ stream. Once retries run out the record goes to the
/// DLQ instead of being dropped. Here that happens at once, because a mapping
/// error cannot be retried. This is the documented "retry, then DLQ" pattern.
#[tokio::test]
async fn test_e2e_sql_retry_then_dlq_receives_malformed_record() {
    use eventflux::core::eventflux_manager::EventFluxManager;

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("orders.jsonl");
    let output = dir.path().join("out.jsonl");
    let dlq = dir.path().join("errors.jsonl");
    write_orders_with_malformed_record(&input);

    let sql = format!(
        r#"
        {source}
        CREATE STREAM OrderErrors ({DLQ_COLUMNS}) WITH (
            type = 'sink', extension = 'file', format = 'json',
            "file.path" = '{dlq}'
        );
        CREATE STREAM Out (id STRING, amount DOUBLE) WITH (
            type = 'sink', extension = 'file', format = 'json',
            "file.path" = '{output}'
        );
        INSERT INTO Out SELECT * FROM Orders;
    "#,
        source = orders_source(&input, "retry", "OrderErrors"),
        dlq = dlq.display(),
        output = output.display(),
    );

    let manager = EventFluxManager::new();
    let runtime = manager
        .create_eventflux_app_runtime_from_string(&sql)
        .await
        .expect("runtime");
    runtime.start().expect("start");

    await_jsonl_lines(&output, 2);
    await_jsonl_lines(&dlq, 1);
    runtime.shutdown();

    assert_eq!(read_jsonl(&output).len(), 2, "valid records still flow");
    assert_single_dlq_row_for_malformed_record(&read_jsonl(&dlq));
}

/// Creates an app that must be rejected at creation, not at start, and
/// returns the creation error.
async fn creation_error(sql: &str) -> String {
    use eventflux::core::eventflux_manager::EventFluxManager;

    match EventFluxManager::new()
        .create_eventflux_app_runtime_from_string(sql)
        .await
    {
        Ok(runtime) => {
            runtime.shutdown();
            panic!("app with an invalid DLQ configuration must fail at creation");
        }
        Err(e) => e.to_string(),
    }
}

#[tokio::test]
async fn test_e2e_sql_dlq_missing_stream_fails_at_creation() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("orders.jsonl");

    // The reference names a stream that is not defined (the DLQ stream is
    // OrderErrors)
    let sql = format!(
        "{source}\nCREATE STREAM OrderErrors ({DLQ_COLUMNS});",
        source = orders_source_with_dlq(&input, "OrderDeadLetters"),
    );

    let err = creation_error(&sql).await;
    assert!(err.contains("'OrderDeadLetters'"), "got: {err}");
    assert!(err.contains("does not exist"), "got: {err}");
}

#[tokio::test]
async fn test_e2e_sql_dlq_wrong_schema_fails_at_creation() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("orders.jsonl");

    // attemptCount is missing
    let sql = format!(
        r#"{source}
        CREATE STREAM OrderErrors (originalEvent STRING, errorMessage STRING,
            errorType STRING, timestamp LONG, streamName STRING);"#,
        source = orders_source_with_dlq(&input, "OrderErrors"),
    );

    let err = creation_error(&sql).await;
    assert!(err.contains("'OrderErrors'"), "got: {err}");
    assert!(err.contains("exactly 6 required"), "got: {err}");
}

#[tokio::test]
async fn test_e2e_sql_dlq_duplicate_field_fails_at_creation() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("orders.jsonl");

    // Every required field plus a second originalEvent
    let sql = format!(
        "{source}\nCREATE STREAM OrderErrors ({DLQ_COLUMNS}, originalEvent STRING);",
        source = orders_source_with_dlq(&input, "OrderErrors"),
    );

    let err = creation_error(&sql).await;
    assert!(err.contains("has 7 fields"), "got: {err}");
}

#[tokio::test]
async fn test_e2e_sql_dlq_reordered_fields_fail_at_creation() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("orders.jsonl");

    // The right fields in the wrong order. DLQ values go by position, so they
    // would land under the wrong attributes.
    let sql = format!(
        r#"{source}
        CREATE STREAM OrderErrors (streamName STRING, originalEvent STRING,
            errorMessage STRING, errorType STRING, timestamp LONG, attemptCount INT);"#,
        source = orders_source_with_dlq(&input, "OrderErrors"),
    );

    let err = creation_error(&sql).await;
    assert!(
        err.contains("field 1 is 'streamName', expected 'originalEvent'"),
        "got: {err}"
    );
}

#[tokio::test]
async fn test_e2e_sql_dlq_stream_with_own_dlq_fails_at_creation() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("orders.jsonl");
    let errors_input = dir.path().join("errors.jsonl");

    // OrderErrors is a source that itself routes failures to a DLQ
    let sql = format!(
        r#"{source}
        CREATE STREAM OrderErrors ({DLQ_COLUMNS}) WITH (
            type = 'source', extension = 'file', format = 'json',
            "file.path" = '{errors_input}',
            "error.strategy" = 'dlq',
            "error.dlq.stream" = 'MoreErrors'
        );
        CREATE STREAM MoreErrors ({DLQ_COLUMNS});"#,
        source = orders_source_with_dlq(&input, "OrderErrors"),
        errors_input = errors_input.display(),
    );

    let err = creation_error(&sql).await;
    assert!(
        err.contains("'OrderErrors' cannot have its own DLQ"),
        "got: {err}"
    );
}

// ============================================================================
// YAML only file connectors. Null and empty paths (#210)
// ============================================================================

/// Builds a runtime for [`YAML_ONLY_SQL`] with `app_config_yaml` as its whole YAML
/// app config, starts it, and returns the start error.
///
/// The old bug wrote to a file named `null` in the working directory, which is
/// the crate root under cargo test. If that file appears, it is removed and the
/// test fails, so a regression never leaves it behind to be committed.
///
/// Panics if creation fails, the runtime starts, or `./null` was created.
async fn yaml_only_start_error(app_config_yaml: &str) -> String {
    let stray = Path::new("null");
    let stray_existed = stray.exists();

    let manager = common::manager_with_app_config("YamlNullPathApp", app_config_yaml);
    let runtime = manager
        .create_eventflux_app_runtime_from_string(YAML_ONLY_SQL)
        .await
        .expect("runtime");
    let result = runtime.start();
    runtime.shutdown();

    if !stray_existed && stray.exists() {
        let _ = fs::remove_file(stray);
        panic!("a null file.path was used as the file name `null`");
    }
    match result {
        Ok(()) => panic!("runtime must not start with a missing or empty file.path"),
        Err(e) => e.to_string(),
    }
}

/// App with a YAML only source on `In` and a YAML only sink on `Out`. Neither
/// stream has a SQL WITH clause.
const YAML_ONLY_SQL: &str = r#"
    CREATE STREAM In (msg STRING);
    CREATE STREAM Out (msg STRING);
    INSERT INTO Out SELECT msg FROM In;
"#;

/// Regression test for https://github.com/eventflux-io/eventflux/issues/210.
/// A null `file.path` on a YAML only sink used to reach the sink as the text
/// "null", so every event was written to `./null`. It must fail start instead.
#[tokio::test]
async fn test_yaml_only_file_sink_null_path_fails_start() {
    let err = yaml_only_start_error(
        r#"
streams:
  Out:
    sink:
      type: file
      format: json
      file.path:
"#,
    )
    .await;
    assert!(
        err.contains("Missing required property: file.path"),
        "got: {err}"
    );
}

#[tokio::test]
async fn test_yaml_only_file_sink_empty_path_fails_start() {
    let err = yaml_only_start_error(
        r#"
streams:
  Out:
    sink:
      type: file
      format: json
      file.path: ""
"#,
    )
    .await;
    assert!(err.contains("file.path cannot be empty"), "got: {err}");
}

#[tokio::test]
async fn test_yaml_only_file_source_null_path_fails_start() {
    let err = yaml_only_start_error(
        r#"
streams:
  In:
    source:
      type: file
      format: json
      file.path:
"#,
    )
    .await;
    assert!(
        err.contains("Missing required property: file.path"),
        "got: {err}"
    );
}

/// SQL NULL is the same bug on the SQL side: `"file.path" = NULL` used to reach
/// the sink as the text "null". It now counts as not set.
#[tokio::test]
async fn test_sql_with_null_path_fails_start() {
    use eventflux::core::eventflux_manager::EventFluxManager;

    let sql = r#"
        CREATE STREAM In (msg STRING);
        CREATE STREAM Out (msg STRING) WITH (
            type = 'sink', extension = 'file', format = 'json', "file.path" = NULL
        );
        INSERT INTO Out SELECT msg FROM In;
    "#;
    let runtime = EventFluxManager::new()
        .create_eventflux_app_runtime_from_string(sql)
        .await
        .expect("runtime");
    let err = runtime.start().expect_err("NULL file.path must not start");
    runtime.shutdown();

    assert!(
        err.to_string()
            .contains("Missing required property: file.path"),
        "got: {err}"
    );
}

/// A SQL NULL leaves the option unset, so the YAML value underneath still applies
#[tokio::test]
async fn test_sql_with_null_keeps_yaml_value() {
    use eventflux::core::event::value::AttributeValue;

    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("out.jsonl");
    let app_config = format!(
        "streams:\n  Out:\n    sink:\n      type: file\n      file.path: '{}'\n",
        output.display()
    );
    let sql = r#"
        CREATE STREAM In (msg STRING);
        CREATE STREAM Out (msg STRING) WITH (
            type = 'sink', extension = 'file', format = 'json', "file.path" = NULL
        );
        INSERT INTO Out SELECT msg FROM In;
    "#;

    let manager = common::manager_with_app_config("SqlNullYamlApp", &app_config);
    let runtime = manager
        .create_eventflux_app_runtime_from_string(sql)
        .await
        .expect("runtime");
    runtime.start().expect("start");
    runtime
        .get_input_handler("In")
        .expect("input handler")
        .lock()
        .unwrap()
        .send_data(vec![AttributeValue::String("a".to_string())])
        .expect("send");

    await_jsonl_lines(&output, 1);
    runtime.shutdown();
    assert_eq!(read_jsonl(&output)[0]["msg"], "a");
}

// ============================================================================
// YAML source error_handling (#211)
// ============================================================================

/// YAML app config with a JSON file source on `Orders` reading `input`, and the
/// given `error_handling` block, indented to sit under `source:`.
fn yaml_orders_source(input: &Path, error_handling: &str) -> String {
    format!(
        "streams:\n  Orders:\n    source:\n      type: file\n      format: json\n      file.path: '{}'\n      file.poll.interval.ms: 20\n{}",
        input.display(),
        error_handling
    )
}

/// Regression test for https://github.com/eventflux-io/eventflux/issues/211.
/// A YAML `error_handling` block used to be ignored, so the malformed record was
/// logged and dropped. With strategy `dlq` it must land in the DLQ stream, while
/// valid records carry on.
#[tokio::test]
async fn test_e2e_yaml_error_handling_dlq_receives_malformed_record() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("orders.jsonl");
    let output = dir.path().join("out.jsonl");
    let dlq = dir.path().join("errors.jsonl");
    write_orders_with_malformed_record(&input);

    let app_config = yaml_orders_source(
        &input,
        "      error_handling:\n        strategy: dlq\n        dlq:\n          stream: OrderErrors\n",
    );
    let sql = format!(
        r#"
        CREATE STREAM Orders (id STRING, amount DOUBLE);
        CREATE STREAM OrderErrors ({DLQ_COLUMNS}) WITH (
            type = 'sink', extension = 'file', format = 'json',
            "file.path" = '{dlq}'
        );
        CREATE STREAM Out (id STRING, amount DOUBLE) WITH (
            type = 'sink', extension = 'file', format = 'json',
            "file.path" = '{output}'
        );
        INSERT INTO Out SELECT * FROM Orders;
    "#,
        dlq = dlq.display(),
        output = output.display(),
    );

    let manager = common::manager_with_app_config("YamlErrorHandlingApp", &app_config);
    assert_malformed_record_dead_lettered(manager, &sql, &output, &dlq).await;
}

/// An invalid YAML `error_handling` block must stop the app from being created,
/// naming the stream, instead of being ignored. The file is never read, so it
/// need not exist.
#[tokio::test]
async fn test_e2e_yaml_invalid_error_handling_fails_at_creation() {
    let input = Path::new("/nonexistent/orders.jsonl");
    let app_config = yaml_orders_source(input, "      error_handling:\n        strategy: dlq\n");
    let manager = common::manager_with_app_config("YamlBadErrorHandlingApp", &app_config);
    let err = match manager
        .create_eventflux_app_runtime_from_string(
            "CREATE STREAM Orders (id STRING, amount DOUBLE);",
        )
        .await
    {
        Ok(_) => panic!("an invalid error_handling block must fail creation"),
        Err(e) => e.to_string(),
    };
    assert!(
        err.contains("source stream 'Orders'") && err.contains("requires 'error.dlq.stream'"),
        "got: {err}"
    );
}
