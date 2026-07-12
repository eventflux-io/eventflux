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

//! # Kafka Integration Tests
//!
//! Requires building with the `kafka` feature:
//! `cargo test --features kafka --test kafka_integration -- --ignored`
//!
//! Tests marked `#[ignore]` require a running Kafka broker. Each test
//! creates its own unique topic by producing to it first.
//!
//! ## Setup
//!
//! Start a single-node KRaft broker with Docker:
//! ```bash
//! docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.1
//! ```
//!
//! Override the broker address with the `KAFKA_BROKERS` env var.

#![cfg(feature = "kafka")]

use eventflux::core::exception::EventFluxError;
use eventflux::core::stream::input::source::kafka_source::KafkaSource;
use eventflux::core::stream::input::source::{Source, SourceCallback};
use eventflux::core::stream::output::sink::kafka_sink::KafkaSink;
use eventflux::core::stream::output::sink::Sink;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn broker() -> String {
    std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string())
}

/// Unique per-run topic so tests never see each other's records
fn unique_topic(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

fn source_props(topic: &str, group: &str) -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert("kafka.bootstrap.servers".to_string(), broker());
    props.insert("kafka.topic".to_string(), topic.to_string());
    props.insert("kafka.group.id".to_string(), group.to_string());
    props.insert(
        "kafka.auto.offset.reset".to_string(),
        "earliest".to_string(),
    );
    props
}

fn sink_props(topic: &str) -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert("kafka.bootstrap.servers".to_string(), broker());
    props.insert("kafka.topic".to_string(), topic.to_string());
    // Deterministic delivery for test assertions
    props.insert("kafka.delivery.sync".to_string(), "true".to_string());
    // Exercise the static record-key path
    props.insert("kafka.key".to_string(), "test-key".to_string());
    props
}

/// Callback that records every payload it receives
#[derive(Debug, Clone)]
struct CollectingCallback {
    payloads: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl CollectingCallback {
    fn new() -> Self {
        Self {
            payloads: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn wait_for(&self, count: usize, timeout: Duration) -> Vec<Vec<u8>> {
        let deadline = Instant::now() + timeout;
        loop {
            let payloads = self.payloads.lock().unwrap();
            if payloads.len() >= count || Instant::now() >= deadline {
                return payloads.clone();
            }
            drop(payloads);
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl SourceCallback for CollectingCallback {
    fn on_data(&self, data: &[u8]) -> Result<(), EventFluxError> {
        self.payloads.lock().unwrap().push(data.to_vec());
        Ok(())
    }
}

// Factory metadata and config-validation coverage lives in the in-module
// unit tests (kafka_source.rs / kafka_sink.rs); this file holds only the
// broker-backed tests.

// ============================================================================
// Broker Tests (run with --ignored against a live broker)
// ============================================================================

#[test]
#[ignore = "Requires Kafka broker - run with --ignored"]
fn test_sink_to_source_round_trip() {
    let topic = unique_topic("roundtrip");

    // Produce three records (sync delivery; broker auto-creates the topic)
    let sink = KafkaSink::from_properties(&sink_props(&topic)).unwrap();
    sink.start();
    sink.publish(b"one").unwrap();
    sink.publish(b"two").unwrap();
    sink.publish(b"three").unwrap();
    sink.stop();

    // Consume them from the beginning
    let callback = CollectingCallback::new();
    let mut source =
        KafkaSource::from_properties(&source_props(&topic, &unique_topic("grp")), None, "Test")
            .unwrap();
    source.start(Arc::new(callback.clone()));

    let payloads = callback.wait_for(3, Duration::from_secs(30));
    source.stop();

    assert_eq!(
        payloads,
        vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()],
        "all produced records must arrive in order"
    );
}

#[test]
#[ignore = "Requires Kafka broker - run with --ignored"]
fn test_manual_commit_no_redelivery_across_restarts() {
    let topic = unique_topic("commit");
    let group = unique_topic("grp");

    let sink = KafkaSink::from_properties(&sink_props(&topic)).unwrap();
    sink.start();
    sink.publish(b"first").unwrap();

    // At-least-once mode: offset committed only after delivery
    let mut props = source_props(&topic, &group);
    props.insert("kafka.enable.auto.commit".to_string(), "false".to_string());

    let callback = CollectingCallback::new();
    let mut source = KafkaSource::from_properties(&props, None, "Test").unwrap();
    source.start(Arc::new(callback.clone()));
    let got = callback.wait_for(1, Duration::from_secs(30));
    assert_eq!(got, vec![b"first".to_vec()]);
    source.stop(); // final sync commit happens here

    // Produce a second record, then resume with the SAME group:
    // only the uncommitted record may arrive
    sink.publish(b"second").unwrap();
    sink.stop();

    let callback2 = CollectingCallback::new();
    let mut source2 = KafkaSource::from_properties(&props, None, "Test").unwrap();
    source2.start(Arc::new(callback2.clone()));
    let got2 = callback2.wait_for(1, Duration::from_secs(30));
    source2.stop();

    assert_eq!(
        got2,
        vec![b"second".to_vec()],
        "committed offsets must survive restarts — 'first' must not be redelivered"
    );
}

#[test]
#[ignore = "Requires Kafka broker - run with --ignored"]
fn test_validate_connectivity_existing_topic() {
    let topic = unique_topic("validate");

    // Create the topic by producing to it
    let sink = KafkaSink::from_properties(&sink_props(&topic)).unwrap();
    sink.start();
    sink.publish(b"seed").unwrap();
    sink.stop();

    let source =
        KafkaSource::from_properties(&source_props(&topic, &unique_topic("grp")), None, "Test")
            .unwrap();
    source.validate_connectivity().unwrap();

    let sink = KafkaSink::from_properties(&sink_props(&topic)).unwrap();
    sink.validate_connectivity().unwrap();
}

/// Callback that rejects every payload — used to exercise the fail strategy
#[derive(Debug)]
struct AlwaysFailCallback;

impl SourceCallback for AlwaysFailCallback {
    fn on_data(&self, _data: &[u8]) -> Result<(), EventFluxError> {
        Err(EventFluxError::app_runtime("simulated failure".to_string()))
    }
}

#[test]
#[ignore = "Requires Kafka broker - run with --ignored"]
fn test_fail_strategy_leaves_record_uncommitted() {
    // The at-least-once contract for the `fail` strategy: the poisoned
    // record's offset must NOT be committed by ANY path (including the
    // shutdown/revoke commits), so it is redelivered after restart.
    let topic = unique_topic("poison");
    let group = unique_topic("grp");

    let sink = KafkaSink::from_properties(&sink_props(&topic)).unwrap();
    sink.start();
    sink.publish(b"poison").unwrap();
    sink.stop();

    let mut props = source_props(&topic, &group);
    props.insert("kafka.enable.auto.commit".to_string(), "false".to_string());
    props.insert("error.strategy".to_string(), "fail".to_string());

    // First source hits the poison record and stops without committing
    let mut source = KafkaSource::from_properties(&props, None, "Test").unwrap();
    source.start(Arc::new(AlwaysFailCallback));
    std::thread::sleep(Duration::from_secs(10)); // let it consume + fail
    source.stop();

    // Restart with the SAME group: the record must be redelivered
    let callback = CollectingCallback::new();
    let mut source2 = KafkaSource::from_properties(&props, None, "Test").unwrap();
    source2.start(Arc::new(callback.clone()));
    let redelivered = callback.wait_for(1, Duration::from_secs(30));
    source2.stop();

    assert_eq!(
        redelivered,
        vec![b"poison".to_vec()],
        "a failed record must be redelivered after restart — its offset must never be committed"
    );
}

#[test]
#[ignore = "Requires Kafka broker - run with --ignored"]
fn test_validate_connectivity_missing_topic_fails() {
    // Validation must fail on a topic that does not exist — and must not
    // create it as a side effect (fail-fast, not create-on-probe)
    let topic = unique_topic("missing");

    let source =
        KafkaSource::from_properties(&source_props(&topic, &unique_topic("grp")), None, "Test")
            .unwrap();
    assert!(
        source.validate_connectivity().is_err(),
        "source validation must fail for a missing topic"
    );

    let sink = KafkaSink::from_properties(&sink_props(&topic)).unwrap();
    assert!(
        sink.validate_connectivity().is_err(),
        "sink validation must fail for a missing topic"
    );

    // Probing must not have created the topic: a second validation still fails
    let sink = KafkaSink::from_properties(&sink_props(&topic)).unwrap();
    assert!(
        sink.validate_connectivity().is_err(),
        "validation must not auto-create the topic it probes"
    );
}

#[test]
#[ignore = "Slow (waits out the 10s metadata timeout) - run with --ignored"]
fn test_validate_connectivity_unreachable_broker_fails_fast() {
    let mut props = source_props("any-topic", "any-group");
    props.insert(
        "kafka.bootstrap.servers".to_string(),
        "127.0.0.1:1".to_string(),
    );

    let source = KafkaSource::from_properties(&props, None, "Test").unwrap();
    let start = Instant::now();
    let result = source.validate_connectivity();
    assert!(result.is_err(), "unreachable broker must fail validation");
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "validation must fail fast (bounded by the 10s metadata timeout)"
    );
}

#[test]
#[ignore = "Requires Kafka broker - run with --ignored"]
fn test_source_stop_joins_promptly() {
    let topic = unique_topic("shutdown");

    // Ensure the topic exists so the consumer settles into its poll loop
    let sink = KafkaSink::from_properties(&sink_props(&topic)).unwrap();
    sink.start();
    sink.publish(b"seed").unwrap();
    sink.stop();

    let callback = CollectingCallback::new();
    let mut source =
        KafkaSource::from_properties(&source_props(&topic, &unique_topic("grp")), None, "Test")
            .unwrap();
    source.start(Arc::new(callback.clone()));
    callback.wait_for(1, Duration::from_secs(30));

    // The poll loop checks the running flag every ≤100ms — stop() must
    // return well within the grace period
    let start = Instant::now();
    source.stop();
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "stop() must join the consumer thread promptly"
    );
}
