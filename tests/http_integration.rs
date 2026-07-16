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

//! # HTTP Integration Tests
//!
//! Requires building with the `http` feature:
//! `cargo test --features http --test http_integration`
//!
//! Unlike the broker-backed connectors, these tests are self-hosting — the
//! webhook source receives the sink's output, and poll tests use in-process
//! `tiny_http` responders — so nothing is `#[ignore]`d and no external
//! service is needed.

#![cfg(feature = "http")]

#[path = "common/mod.rs"]
mod common;
use common::{free_port, wait_listening, CollectingCallback};

use eventflux::core::stream::input::source::http_source::HttpSource;
use eventflux::core::stream::input::source::Source;
use eventflux::core::stream::output::sink::http_sink::HttpSink;
use eventflux::core::stream::output::sink::Sink;
use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn webhook_props(port: u16, path: &str) -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert("http.mode".to_string(), "webhook".to_string());
    props.insert("http.host".to_string(), "127.0.0.1".to_string());
    props.insert("http.port".to_string(), port.to_string());
    props.insert("http.path".to_string(), path.to_string());
    props
}

fn sink_props(url: &str) -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert("http.url".to_string(), url.to_string());
    // Fast tests: minimal backoff
    props.insert("http.retry.initial-delay-ms".to_string(), "20".to_string());
    props.insert("http.retry.max-delay-ms".to_string(), "50".to_string());
    props
}

/// In-process responder: serves the given (status, body) sequence, repeating
/// the last entry, and counts requests. Returns (url, hit counter, stopper).
fn spawn_responder(
    responses: Vec<(u16, &'static str)>,
) -> (String, Arc<AtomicUsize>, impl FnOnce()) {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let url = format!("http://{}/", server.server_addr());
    let hits = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let thread_hits = Arc::clone(&hits);
    let thread_stop = Arc::clone(&stop);
    let handle = std::thread::spawn(move || {
        while !thread_stop.load(Ordering::SeqCst) {
            match server.recv_timeout(Duration::from_millis(50)) {
                Ok(Some(request)) => {
                    let n = thread_hits.fetch_add(1, Ordering::SeqCst);
                    let (status, body) = responses[n.min(responses.len() - 1)];
                    let response = tiny_http::Response::from_string(body)
                        .with_status_code(tiny_http::StatusCode(status));
                    let _ = request.respond(response);
                }
                _ => continue,
            }
        }
    });

    let stopper = move || {
        stop.store(true, Ordering::SeqCst);
        let _ = handle.join();
    };
    (url, hits, stopper)
}

// ============================================================================
// Round trip: HTTP sink → webhook source (public API end to end)
// ============================================================================

#[test]
fn test_sink_to_webhook_source_round_trip() {
    let port = free_port();
    let callback = CollectingCallback::new();
    let mut source =
        HttpSource::from_properties(&webhook_props(port, "/ingest"), None, "Test").unwrap();
    source.start(Arc::new(callback.clone()));
    wait_listening(port);

    let sink =
        HttpSink::from_properties(&sink_props(&format!("http://127.0.0.1:{port}/ingest"))).unwrap();
    sink.publish(b"one").unwrap();
    sink.publish(b"two").unwrap();
    sink.publish(b"three").unwrap();
    sink.stop();

    let payloads = callback.wait_for(3, Duration::from_secs(10));
    source.stop();

    assert_eq!(
        payloads,
        vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]
    );
}

#[test]
fn test_webhook_concurrent_requests_all_delivered() {
    let port = free_port();
    let callback = CollectingCallback::new();
    let mut source = HttpSource::from_properties(&webhook_props(port, "/"), None, "Test").unwrap();
    source.start(Arc::new(callback.clone()));
    wait_listening(port);

    let url = format!("http://127.0.0.1:{port}/");
    let handles: Vec<_> = (0..8)
        .map(|i| {
            let props = sink_props(&url);
            std::thread::spawn(move || {
                let sink = HttpSink::from_properties(&props).unwrap();
                sink.publish(format!("payload-{i}").as_bytes()).unwrap();
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }

    let mut payloads = callback.wait_for(8, Duration::from_secs(10));
    source.stop();

    payloads.sort();
    let expected: Vec<Vec<u8>> = (0..8)
        .map(|i| format!("payload-{i}").into_bytes())
        .collect();
    assert_eq!(payloads, expected, "all concurrent requests must arrive");
}

// ============================================================================
// Webhook protocol behavior (asserted with a direct ureq client so the
// expectations are numeric response statuses)
// ============================================================================

#[test]
fn test_webhook_rejections() {
    let port = free_port();
    let mut props = webhook_props(port, "/hook");
    props.insert("http.auth.header".to_string(), "X-Token".to_string());
    props.insert("http.auth.token".to_string(), "secret".to_string());
    props.insert("http.max.body.bytes".to_string(), "64".to_string());

    let callback = CollectingCallback::new();
    let mut source = HttpSource::from_properties(&props, None, "Test").unwrap();
    source.start(Arc::new(callback.clone()));
    wait_listening(port);

    // Direct client so protocol assertions are numeric response statuses,
    // decoupled from the HTTP sink's error-message wording
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into();
    let base = format!("http://127.0.0.1:{port}");

    // Missing auth → 401
    let status = agent.post(format!("{base}/hook")).send(&b"x"[..]).unwrap();
    assert_eq!(status.status().as_u16(), 401);

    // Wrong path → 404
    let status = agent
        .post(format!("{base}/other"))
        .header("X-Token", "secret")
        .send(&b"x"[..])
        .unwrap();
    assert_eq!(status.status().as_u16(), 404);

    // Wrong method → 405
    let status = agent
        .put(format!("{base}/hook"))
        .header("X-Token", "secret")
        .send(&b"x"[..])
        .unwrap();
    assert_eq!(status.status().as_u16(), 405);

    // Oversized body → 413
    let status = agent
        .post(format!("{base}/hook"))
        .header("X-Token", "secret")
        .send(&[0u8; 128][..])
        .unwrap();
    assert_eq!(status.status().as_u16(), 413);

    // Oversized AND unauthenticated → 401: auth is checked before the body
    // is buffered, so unauthenticated floods cannot consume memory
    let status = agent
        .post(format!("{base}/hook"))
        .send(&[0u8; 128][..])
        .unwrap();
    assert_eq!(status.status().as_u16(), 401);

    // Correct auth, path, method, size → 200 and delivered
    let status = agent
        .post(format!("{base}/hook"))
        .header("X-Token", "secret")
        .send(&b"valid"[..])
        .unwrap();
    assert_eq!(status.status().as_u16(), 200);

    let payloads = callback.wait_for(1, Duration::from_secs(10));
    source.stop();
    assert_eq!(payloads, vec![b"valid".to_vec()]);
}

// ============================================================================
// Poll mode (in-process canned responders)
// ============================================================================

#[test]
fn test_poll_source_delivers_bodies() {
    let (url, _hits, stop) = spawn_responder(vec![(200, "{\"n\":1}")]);

    let mut props = HashMap::new();
    props.insert("http.mode".to_string(), "poll".to_string());
    props.insert("http.url".to_string(), url);
    props.insert("http.poll.interval.ms".to_string(), "50".to_string());

    let callback = CollectingCallback::new();
    let mut source = HttpSource::from_properties(&props, None, "Test").unwrap();
    source.start(Arc::new(callback.clone()));

    let payloads = callback.wait_for(2, Duration::from_secs(10));
    source.stop();
    stop();

    assert!(payloads.len() >= 2, "expected repeated polls to deliver");
    assert_eq!(payloads[0], b"{\"n\":1}".to_vec());
}

#[test]
fn test_poll_source_skips_empty_and_not_modified() {
    // 204 empty, then 304, then a real body
    let (url, _hits, stop) = spawn_responder(vec![(204, ""), (304, ""), (200, "data")]);

    let mut props = HashMap::new();
    props.insert("http.mode".to_string(), "poll".to_string());
    props.insert("http.url".to_string(), url);
    props.insert("http.poll.interval.ms".to_string(), "30".to_string());

    let callback = CollectingCallback::new();
    let mut source = HttpSource::from_properties(&props, None, "Test").unwrap();
    source.start(Arc::new(callback.clone()));

    let payloads = callback.wait_for(1, Duration::from_secs(10));
    source.stop();
    stop();

    assert_eq!(payloads[0], b"data".to_vec(), "empty/304 must be skipped");
}

#[test]
fn test_poll_empty_body_respects_interval() {
    // Regression: an empty 2xx body must NOT skip the inter-poll sleep —
    // that would busy-loop against the endpoint at full speed
    let (url, hits, stop) = spawn_responder(vec![(204, "")]);

    let mut props = HashMap::new();
    props.insert("http.mode".to_string(), "poll".to_string());
    props.insert("http.url".to_string(), url);
    props.insert("http.poll.interval.ms".to_string(), "100".to_string());

    let callback = CollectingCallback::new();
    let mut source = HttpSource::from_properties(&props, None, "Test").unwrap();
    source.start(Arc::new(callback.clone()));

    std::thread::sleep(Duration::from_millis(600));
    source.stop();
    stop();

    // ~6 polls fit in the window; a busy loop would rack up thousands
    let polls = hits.load(Ordering::SeqCst);
    assert!(
        polls <= 10,
        "empty bodies must not bypass the interval (got {polls} polls)"
    );
}

#[test]
fn test_poll_long_interval_stops_promptly() {
    let (url, _hits, stop) = spawn_responder(vec![(200, "x")]);

    let mut props = HashMap::new();
    props.insert("http.mode".to_string(), "poll".to_string());
    props.insert("http.url".to_string(), url);
    // An interval far beyond the stop grace period
    props.insert("http.poll.interval.ms".to_string(), "600000".to_string());

    let callback = CollectingCallback::new();
    let mut source = HttpSource::from_properties(&props, None, "Test").unwrap();
    source.start(Arc::new(callback.clone()));
    callback.wait_for(1, Duration::from_secs(10));

    // sleep_while_running makes the interval interruptible
    let start = Instant::now();
    source.stop();
    stop();
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "stop() must interrupt a long poll interval"
    );
}

// ============================================================================
// Sink retry semantics
// ============================================================================

#[test]
fn test_sink_retries_5xx_then_succeeds() {
    let (url, hits, stop) = spawn_responder(vec![(500, ""), (503, ""), (200, "ok")]);

    let mut props = sink_props(&url);
    props.insert("http.retry.max-attempts".to_string(), "3".to_string());
    let sink = HttpSink::from_properties(&props).unwrap();

    sink.publish(b"payload").unwrap();
    stop();
    assert_eq!(hits.load(Ordering::SeqCst), 3, "two failures + one success");
}

#[test]
fn test_sink_does_not_retry_4xx() {
    let (url, hits, stop) = spawn_responder(vec![(400, "bad")]);

    let mut props = sink_props(&url);
    props.insert("http.retry.max-attempts".to_string(), "5".to_string());
    let sink = HttpSink::from_properties(&props).unwrap();

    let err = sink.publish(b"payload").unwrap_err().to_string();
    stop();
    assert!(err.contains("400"), "got: {err}");
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "4xx must not be retried — the request itself is wrong"
    );
}

#[test]
fn test_sink_does_not_follow_redirects() {
    // Following a 302 would rewrite the POST to a body-less GET and the
    // final 200 would count a dropped payload as delivered — redirects
    // must surface as permanent errors instead
    let (url, hits, stop) = spawn_responder(vec![(302, "")]);

    let sink = HttpSink::from_properties(&sink_props(&url)).unwrap();
    let err = sink.publish(b"payload").unwrap_err().to_string();
    stop();

    assert!(err.contains("302"), "got: {err}");
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "redirect must not be followed or retried"
    );
}

#[test]
fn test_sink_retries_exhausted() {
    let (url, hits, stop) = spawn_responder(vec![(500, "")]);

    let mut props = sink_props(&url);
    props.insert("http.retry.max-attempts".to_string(), "2".to_string());
    let sink = HttpSink::from_properties(&props).unwrap();

    let err = sink.publish(b"payload").unwrap_err().to_string();
    stop();
    assert!(err.contains("failed after 3 attempt"), "got: {err}");
    assert_eq!(hits.load(Ordering::SeqCst), 3, "initial + 2 retries");
}

// ============================================================================
// Connectivity validation
// ============================================================================

#[test]
fn test_validate_connectivity() {
    // Any HTTP response — even 404 — proves reachability
    let (url, _hits, stop) = spawn_responder(vec![(404, "")]);
    let sink = HttpSink::from_properties(&sink_props(&url)).unwrap();
    sink.validate_connectivity().unwrap();
    stop();

    // Nothing listening → transport failure → validation error
    let dead = format!("http://127.0.0.1:{}/", free_port());
    let sink = HttpSink::from_properties(&sink_props(&dead)).unwrap();
    assert!(sink.validate_connectivity().is_err());

    // Webhook validation fails fast on an occupied port
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let source = HttpSource::from_properties(&webhook_props(port, "/"), None, "Test").unwrap();
    assert!(source.validate_connectivity().is_err());
    drop(listener);
}

// ============================================================================
// Shutdown
// ============================================================================

#[test]
fn test_webhook_source_stops_promptly() {
    let port = free_port();
    let callback = CollectingCallback::new();
    let mut source = HttpSource::from_properties(&webhook_props(port, "/"), None, "Test").unwrap();
    source.start(Arc::new(callback.clone()));
    wait_listening(port);

    let start = Instant::now();
    source.stop();
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "graceful shutdown must complete promptly"
    );

    // The port is released after shutdown (retry briefly for OS cleanup)
    let deadline = Instant::now() + Duration::from_secs(5);
    while TcpListener::bind(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "port not released after stop()");
        std::thread::sleep(Duration::from_millis(10));
    }
}
