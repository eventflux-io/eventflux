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

//! # Kafka Sink
//!
//! Sends formatted event payloads to a Kafka topic.
//!
//! ## Architecture
//!
//! ```text
//! Events → per event: SinkMapper → bytes → KafkaSink::publish() → ThreadedProducer → Kafka
//! ```
//!
//! No tokio runtime is needed. `ThreadedProducer` runs librdkafka's own
//! background poll thread, which groups records into batches (`linger.ms`,
//! `batch.size`) and calls the delivery callback for each record.
//!
//! ## Delivery semantics
//!
//! - Default (async): `publish()` puts the record in librdkafka's local queue
//!   and returns. Delivery reports arrive on the producer poll thread, and
//!   failures are counted and logged. librdkafka retries on its own until
//!   `kafka.message.timeout.ms`.
//! - `kafka.delivery.sync = true`: `publish()` flushes and returns an error if
//!   the record's delivery report came back failed. Each message is confirmed,
//!   at the cost of throughput.
//!
//! `stop()` flushes records still in flight, waiting up to
//! `kafka.flush.timeout.ms`, and logs the totals.

use super::sink_trait::Sink;
use crate::core::exception::EventFluxError;
use crate::core::extension::SinkFactory;
use crate::core::stream::connector_util::parse_or;
use crate::core::stream::kafka_common::{validate_topics_exist, KafkaConnectionConfig};
use rdkafka::client::ClientContext;
use rdkafka::error::KafkaError;
use rdkafka::producer::{BaseRecord, Producer, ProducerContext, ThreadedProducer};
use rdkafka::types::RDKafkaErrorCode;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// librdkafka settings that have their own config field. They are rejected
/// when passed through the raw `kafka.rdkafka.*` keys.
const RESERVED_RDKAFKA_KEYS: &[&str] = &[
    "bootstrap.servers",
    "acks",
    "compression.type",
    "linger.ms",
    "batch.size",
    "enable.idempotence",
    "message.timeout.ms",
];

/// What `publish()` does when librdkafka's local queue is full
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueFullStrategy {
    /// Keep trying to queue the record until it fits or the flush timeout
    /// passes. This slows the pipeline down. The default.
    Block,
    /// Fail at once and pass the error to the pipeline's error handling
    Error,
}

/// Configuration for Kafka sink
#[derive(Debug, Clone)]
pub struct KafkaSinkConfig {
    /// Broker connection and security settings, shared with the source
    pub connection: KafkaConnectionConfig,
    /// Topic to write to
    pub topic: String,
    /// Optional fixed key for every record. Records with no key use the
    /// default partitioner.
    pub key: Option<String>,
    /// Trade safety for speed with `0`, `1`, or `all`. Default `all`.
    pub acks: String,
    /// `none`, `gzip`, `snappy`, `lz4`, or `zstd`. Default `none`.
    pub compression: String,
    /// How long to wait to fill a batch, in ms. Default 5.
    pub linger_ms: u64,
    /// Largest batch in bytes. librdkafka's default when not set.
    pub batch_size: Option<u64>,
    /// Idempotent producer, so retries do not write duplicates. Needs
    /// `acks = all`. Default false.
    pub enable_idempotence: bool,
    /// Total time in ms to deliver a record before it is reported failed.
    /// Default 30000.
    pub message_timeout_ms: u64,
    /// true: `publish()` flushes and returns delivery failures. Strict but
    /// slow. Default false.
    pub delivery_sync: bool,
    /// Limit in ms for every wait in the sink: the `stop()` flush, sync mode
    /// flushes, and waiting for room in a full queue. Default 30000.
    pub flush_timeout_ms: u64,
    /// What to do when librdkafka's local queue is full
    pub queue_full_strategy: QueueFullStrategy,
}

impl KafkaSinkConfig {
    /// Reads the config from properties.
    ///
    /// # Errors
    ///
    /// Returns an error when the connection config is invalid or uses a
    /// reserved `kafka.rdkafka.*` key, `kafka.topic` is missing or blank,
    /// `kafka.acks`, `kafka.compression`, or `kafka.queue.full.strategy` has an
    /// unknown value, idempotence is on without `acks = all`, or a number or
    /// bool does not parse.
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        let connection = KafkaConnectionConfig::from_properties(properties, RESERVED_RDKAFKA_KEYS)?;

        let topic = properties
            .get("kafka.topic")
            .ok_or("Missing required property: kafka.topic")?
            .trim()
            .to_string();
        if topic.is_empty() {
            return Err("kafka.topic cannot be empty".to_string());
        }

        let key = properties.get("kafka.key").cloned();

        let acks = properties
            .get("kafka.acks")
            .cloned()
            .unwrap_or_else(|| "all".to_string());
        if !["0", "1", "all"].contains(&acks.as_str()) {
            return Err(format!(
                "Invalid kafka.acks '{acks}': expected '0', '1', or 'all'"
            ));
        }

        let compression = properties
            .get("kafka.compression")
            .cloned()
            .unwrap_or_else(|| "none".to_string());
        if !["none", "gzip", "snappy", "lz4", "zstd"].contains(&compression.as_str()) {
            return Err(format!(
                "Invalid kafka.compression '{compression}': expected none, gzip, snappy, lz4, or zstd"
            ));
        }

        let enable_idempotence = parse_or(properties, "kafka.enable.idempotence", false)?;
        // Kafka rejects this pair at the protocol level. Fail with a clear
        // message while parsing instead of a librdkafka create error.
        if enable_idempotence && acks != "all" {
            return Err(format!(
                "kafka.enable.idempotence requires kafka.acks = 'all' (got '{acks}')"
            ));
        }

        let queue_full_strategy = match properties
            .get("kafka.queue.full.strategy")
            .map(String::as_str)
        {
            None | Some("block") => QueueFullStrategy::Block,
            Some("error") => QueueFullStrategy::Error,
            Some(other) => {
                return Err(format!(
                    "Invalid kafka.queue.full.strategy '{other}': expected 'block' or 'error'"
                ))
            }
        };

        Ok(Self {
            connection,
            topic,
            key,
            acks,
            compression,
            linger_ms: parse_or(properties, "kafka.linger.ms", 5)?,
            batch_size: match properties.get("kafka.batch.size") {
                Some(v) => Some(
                    v.parse()
                        .map_err(|e| format!("Invalid kafka.batch.size: {e}"))?,
                ),
                None => None,
            },
            enable_idempotence,
            message_timeout_ms: parse_or(properties, "kafka.message.timeout.ms", 30_000)?,
            delivery_sync: parse_or(properties, "kafka.delivery.sync", false)?,
            flush_timeout_ms: parse_or(properties, "kafka.flush.timeout.ms", 30_000)?,
            queue_full_strategy,
        })
    }

    /// Builds the librdkafka client config. Connection settings and raw
    /// `kafka.rdkafka.*` settings are applied last.
    pub fn to_client_config(&self) -> rdkafka::config::ClientConfig {
        let mut config = rdkafka::config::ClientConfig::new();
        config
            .set("acks", &self.acks)
            .set("compression.type", &self.compression)
            .set("linger.ms", self.linger_ms.to_string())
            .set("enable.idempotence", self.enable_idempotence.to_string())
            .set("message.timeout.ms", self.message_timeout_ms.to_string());

        if let Some(v) = self.batch_size {
            config.set("batch.size", v.to_string());
        }
        self.connection.apply_to(&mut config);
        config
    }
}

/// Counters updated by the delivery callback on the producer poll thread
#[derive(Debug, Default)]
struct SinkMetrics {
    records_produced: AtomicU64,
    records_failed: AtomicU64,
}

/// Producer context that gets the delivery report for each record
struct DeliveryTracker {
    metrics: Arc<SinkMetrics>,
}

impl ClientContext for DeliveryTracker {}

impl ProducerContext for DeliveryTracker {
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        delivery_result: &rdkafka::message::DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
        match delivery_result {
            Ok(_) => {
                self.metrics
                    .records_produced
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err((e, msg)) => {
                self.metrics.records_failed.fetch_add(1, Ordering::Relaxed);
                use rdkafka::message::Message;
                log::error!(
                    "[KafkaSink] Delivery failed for record to '{}' ({} bytes): {e}",
                    msg.topic(),
                    msg.payload().map_or(0, <[u8]>::len),
                );
            }
        }
    }
}

type KafkaProducer = ThreadedProducer<DeliveryTracker>;

/// Kafka sink that writes records to a topic.
///
/// - Uses `ThreadedProducer`. librdkafka batches and delivers in the
///   background, and by default `publish()` only queues the record and does
///   not wait, unless the queue is full
/// - Counts delivery failures in the callback and logs them on `stop()`
/// - Can confirm each record with `kafka.delivery.sync`
pub struct KafkaSink {
    config: KafkaSinkConfig,
    /// Producer, created in start(). The Arc lets publish() take a handle and
    /// do all I/O outside the lock. The mutex only guards start() and stop()
    /// setting or clearing the Option.
    producer: Mutex<Option<Arc<KafkaProducer>>>,
    /// Produced and failed counters, shared with the delivery callback
    metrics: Arc<SinkMetrics>,
}

// Written by hand because ThreadedProducer does not implement Debug
impl Debug for KafkaSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSink")
            .field("config", &self.config)
            .field(
                "connected",
                &self.producer.lock().map(|g| g.is_some()).unwrap_or(false),
            )
            .finish()
    }
}

impl KafkaSink {
    /// Creates a Kafka sink. `start()` creates the producer.
    pub fn new(config: KafkaSinkConfig) -> Self {
        Self {
            config,
            producer: Mutex::new(None),
            metrics: Arc::new(SinkMetrics::default()),
        }
    }

    /// Creates a Kafka sink from properties. Fails when the config is invalid.
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        Ok(Self::new(KafkaSinkConfig::from_properties(properties)?))
    }

    /// Builds a record for the topic, with the fixed key when one is set
    fn make_record<'a>(&'a self, payload: &'a [u8]) -> BaseRecord<'a, str, [u8]> {
        let record = BaseRecord::to(&self.config.topic).payload(payload);
        match &self.config.key {
            Some(key) => record.key(key.as_str()),
            None => record,
        }
    }
}

impl Clone for KafkaSink {
    fn clone(&self) -> Self {
        // A clone starts with no producer and its own counters. start()
        // creates the producer.
        Self {
            config: self.config.clone(),
            producer: Mutex::new(None),
            metrics: Arc::new(SinkMetrics::default()),
        }
    }
}

impl Sink for KafkaSink {
    fn start(&self) {
        let context = DeliveryTracker {
            metrics: Arc::clone(&self.metrics),
        };
        match self.config.to_client_config().create_with_context(context) {
            Ok(producer) => {
                *self.producer.lock().unwrap() = Some(Arc::new(producer));
                log::info!(
                    "[KafkaSink] Producing to topic '{}' (brokers: '{}')",
                    self.config.topic,
                    self.config.connection.bootstrap_servers
                );
            }
            Err(e) => {
                log::error!("[KafkaSink] Failed to create producer: {e}");
            }
        }
    }

    fn publish(&self, payload: &[u8]) -> Result<(), EventFluxError> {
        // Clone the Arc and drop the guard at once. All I/O below (retries on a
        // full queue, the sync flush) happens outside the lock, so stop() and
        // other callers never wait behind a slow publish.
        let producer =
            {
                let guard = self.producer.lock().map_err(|_| {
                    EventFluxError::app_runtime("Producer lock poisoned".to_string())
                })?;
                guard.as_ref().map(Arc::clone).ok_or_else(|| {
                    EventFluxError::ConnectionUnavailable {
                        message: "Kafka sink not started - call start() first".to_string(),
                        source: None,
                    }
                })?
            };

        // Sync mode spots a delivery failure by a rise in the failed counter.
        // SinkCallbackAdapter runs publishes one at a time, so any rise belongs
        // to this record.
        let failed_before = self
            .config
            .delivery_sync
            .then(|| self.metrics.records_failed.load(Ordering::Relaxed));

        // Queue the record, retrying for a limited time while the local queue
        // is full. The block strategy slows the pipeline down, and the error
        // strategy fails at once. The deadline starts at the first QueueFull.
        let mut deadline: Option<Instant> = None;
        let mut record = self.make_record(payload);
        loop {
            match producer.send(record) {
                Ok(()) => break,
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), returned))
                    if self.config.queue_full_strategy == QueueFullStrategy::Block =>
                {
                    let deadline = *deadline.get_or_insert_with(|| {
                        Instant::now() + Duration::from_millis(self.config.flush_timeout_ms)
                    });
                    if Instant::now() >= deadline {
                        return Err(EventFluxError::app_runtime(
                            "Kafka enqueue failed: local queue full past the flush timeout"
                                .to_string(),
                        ));
                    }
                    // The local queue is full, so give the poll thread time to empty it
                    std::thread::sleep(Duration::from_millis(10));
                    record = returned;
                }
                Err((e, _)) => {
                    return Err(EventFluxError::app_runtime(format!(
                        "Kafka enqueue failed: {e}"
                    )));
                }
            }
        }

        if let Some(failed_before) = failed_before {
            // Strict mode. Wait for the delivery report, then check it.
            producer
                .flush(Duration::from_millis(self.config.flush_timeout_ms))
                .map_err(|e| EventFluxError::app_runtime(format!("Kafka flush failed: {e}")))?;

            if self.metrics.records_failed.load(Ordering::Relaxed) > failed_before {
                return Err(EventFluxError::app_runtime(
                    "Kafka delivery failed (see delivery report log for details)".to_string(),
                ));
            }
        }

        Ok(())
    }

    fn stop(&self) {
        log::info!("[KafkaSink] Stopping...");
        let producer = self.producer.lock().unwrap().take();
        if let Some(producer) = producer {
            let timeout = Duration::from_millis(self.config.flush_timeout_ms);
            if let Err(e) = producer.flush(timeout) {
                log::warn!(
                    "[KafkaSink] Flush did not complete within {timeout:?}: {e} — \
                     some records may be undelivered"
                );
            }
        }
        log::info!(
            "[KafkaSink] Stopped (produced: {}, failed: {})",
            self.metrics.records_produced.load(Ordering::Relaxed),
            self.metrics.records_failed.load(Ordering::Relaxed)
        );
    }

    fn clone_box(&self) -> Box<dyn Sink> {
        Box::new(self.clone())
    }

    fn validate_connectivity(&self) -> Result<(), EventFluxError> {
        use rdkafka::consumer::{BaseConsumer, Consumer};

        // Check with a consumer client built from the connection settings
        // only. On lenient brokers a producer metadata request CREATES missing
        // topics (checked against apache/kafka defaults). That would make this
        // check pass wrongly and change the broker. Consumer clients never
        // create topics. The made up group id below never shows up on the
        // broker either, since group state only exists after a subscribe or a
        // commit, and neither happens here.
        let mut probe = rdkafka::config::ClientConfig::new();
        self.config.connection.apply_to(&mut probe);
        probe.set("group.id", "eventflux-validate");

        let consumer: BaseConsumer =
            probe
                .create()
                .map_err(|e| EventFluxError::ConnectionUnavailable {
                    message: format!("Failed to create Kafka client: {e}"),
                    source: Some(Box::new(e)),
                })?;

        validate_topics_exist(
            consumer.client(),
            &self.config.connection.bootstrap_servers,
            &[&self.config.topic],
        )
    }
}

// ============================================================================
// Kafka Sink Factory
// ============================================================================

/// Factory for Kafka sinks.
///
/// Registered with EventFluxContext and used to build Kafka sinks from the SQL
/// WITH clause config.
#[derive(Debug, Clone)]
pub struct KafkaSinkFactory;

impl SinkFactory for KafkaSinkFactory {
    fn name(&self) -> &'static str {
        "kafka"
    }

    fn supported_formats(&self) -> &[&str] {
        &["json", "csv", "bytes"]
    }

    fn required_parameters(&self) -> &[&str] {
        &["kafka.bootstrap.servers", "kafka.topic"]
    }

    fn optional_parameters(&self) -> &[&str] {
        &[
            "kafka.key",
            "kafka.acks",
            "kafka.compression",
            "kafka.linger.ms",
            "kafka.batch.size",
            "kafka.enable.idempotence",
            "kafka.message.timeout.ms",
            "kafka.delivery.sync",
            "kafka.flush.timeout.ms",
            "kafka.queue.full.strategy",
            "kafka.security.protocol",
            "kafka.sasl.mechanism",
            "kafka.sasl.username",
            "kafka.sasl.password",
            "kafka.ssl.ca.location",
            // kafka.rdkafka.<prop> keys are also accepted
        ]
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn Sink>, EventFluxError> {
        let parsed =
            KafkaSinkConfig::from_properties(config).map_err(EventFluxError::configuration)?;
        Ok(Box::new(KafkaSink::new(parsed)))
    }

    fn clone_box(&self) -> Box<dyn SinkFactory> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_props() -> HashMap<String, String> {
        let mut props = HashMap::new();
        props.insert(
            "kafka.bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        );
        props.insert("kafka.topic".to_string(), "out-events".to_string());
        props
    }

    #[test]
    fn test_config_from_properties_required_only() {
        let config = KafkaSinkConfig::from_properties(&base_props()).unwrap();
        assert_eq!(config.connection.bootstrap_servers, "localhost:9092");
        assert_eq!(config.topic, "out-events");
        assert!(config.key.is_none());
        assert_eq!(config.acks, "all"); // safest default
        assert_eq!(config.compression, "none");
        assert_eq!(config.linger_ms, 5);
        assert!(config.batch_size.is_none());
        assert!(!config.enable_idempotence);
        assert_eq!(config.message_timeout_ms, 30_000);
        assert!(!config.delivery_sync);
        assert_eq!(config.flush_timeout_ms, 30_000);
        assert_eq!(config.queue_full_strategy, QueueFullStrategy::Block);
    }

    #[test]
    fn test_config_from_properties_all_options() {
        let mut props = base_props();
        props.insert("kafka.key".to_string(), "my-key".to_string());
        // acks=all is set on purpose, since idempotence below needs it
        props.insert("kafka.acks".to_string(), "all".to_string());
        props.insert("kafka.compression".to_string(), "lz4".to_string());
        props.insert("kafka.linger.ms".to_string(), "20".to_string());
        props.insert("kafka.batch.size".to_string(), "65536".to_string());
        props.insert("kafka.enable.idempotence".to_string(), "true".to_string());
        props.insert("kafka.message.timeout.ms".to_string(), "5000".to_string());
        props.insert("kafka.delivery.sync".to_string(), "true".to_string());
        props.insert("kafka.flush.timeout.ms".to_string(), "10000".to_string());
        props.insert("kafka.queue.full.strategy".to_string(), "error".to_string());

        let config = KafkaSinkConfig::from_properties(&props).unwrap();
        assert_eq!(config.key, Some("my-key".to_string()));
        assert_eq!(config.acks, "all");
        assert_eq!(config.compression, "lz4");
        assert_eq!(config.linger_ms, 20);
        assert_eq!(config.batch_size, Some(65536));
        assert!(config.enable_idempotence);
        assert_eq!(config.message_timeout_ms, 5000);
        assert!(config.delivery_sync);
        assert_eq!(config.flush_timeout_ms, 10000);
        assert_eq!(config.queue_full_strategy, QueueFullStrategy::Error);
    }

    #[test]
    fn test_config_acks_one_accepted_without_idempotence() {
        let mut props = base_props();
        props.insert("kafka.acks".to_string(), "1".to_string());
        let config = KafkaSinkConfig::from_properties(&props).unwrap();
        assert_eq!(config.acks, "1");
    }

    #[test]
    fn test_config_missing_topic() {
        let mut props = base_props();
        props.remove("kafka.topic");
        assert!(KafkaSinkConfig::from_properties(&props)
            .unwrap_err()
            .contains("kafka.topic"));
    }

    #[test]
    fn test_config_invalid_acks() {
        let mut props = base_props();
        props.insert("kafka.acks".to_string(), "2".to_string());
        assert!(KafkaSinkConfig::from_properties(&props)
            .unwrap_err()
            .contains("kafka.acks"));
    }

    #[test]
    fn test_config_invalid_compression() {
        let mut props = base_props();
        props.insert("kafka.compression".to_string(), "brotli".to_string());
        assert!(KafkaSinkConfig::from_properties(&props)
            .unwrap_err()
            .contains("kafka.compression"));
    }

    #[test]
    fn test_config_idempotence_requires_acks_all() {
        let mut props = base_props();
        props.insert("kafka.enable.idempotence".to_string(), "true".to_string());
        props.insert("kafka.acks".to_string(), "1".to_string());
        assert!(KafkaSinkConfig::from_properties(&props)
            .unwrap_err()
            .contains("kafka.enable.idempotence"));
    }

    #[test]
    fn test_all_compression_codecs_compiled_in() {
        // Creating a producer checks compression.type against the codecs
        // librdkafka was BUILT with. This guards the Cargo feature set (libz
        // for gzip, the zstd feature for zstd, and snappy and lz4 are built
        // in). No broker is needed, since creation is local.
        for codec in ["none", "gzip", "snappy", "lz4", "zstd"] {
            let mut props = base_props();
            props.insert("kafka.compression".to_string(), codec.to_string());
            let sink = KafkaSink::from_properties(&props).unwrap();

            let producer: Result<rdkafka::producer::BaseProducer, _> =
                sink.config.to_client_config().create();
            assert!(
                producer.is_ok(),
                "librdkafka must support compression codec '{codec}': {:?}",
                producer.err()
            );
        }
    }

    #[test]
    fn test_config_invalid_queue_full_strategy() {
        let mut props = base_props();
        props.insert("kafka.queue.full.strategy".to_string(), "drop".to_string());
        assert!(KafkaSinkConfig::from_properties(&props)
            .unwrap_err()
            .contains("kafka.queue.full.strategy"));
    }

    #[test]
    fn test_config_rdkafka_reserved_keys_rejected() {
        // Settings the producer owns must be set through their own options
        for reserved in RESERVED_RDKAFKA_KEYS {
            let mut props = base_props();
            props.insert(format!("kafka.rdkafka.{reserved}"), "x".to_string());
            assert!(
                KafkaSinkConfig::from_properties(&props)
                    .unwrap_err()
                    .contains("reserved"),
                "kafka.rdkafka.{reserved} must be rejected"
            );
        }
    }

    #[test]
    fn test_client_config_translation() {
        let mut props = base_props();
        props.insert("kafka.compression".to_string(), "zstd".to_string());
        props.insert("kafka.rdkafka.max.in.flight".to_string(), "1".to_string());

        let config = KafkaSinkConfig::from_properties(&props).unwrap();
        let client_config = config.to_client_config();
        assert_eq!(
            client_config.get("bootstrap.servers"),
            Some("localhost:9092")
        );
        assert_eq!(client_config.get("acks"), Some("all"));
        assert_eq!(client_config.get("compression.type"), Some("zstd"));
        assert_eq!(client_config.get("linger.ms"), Some("5"));
        assert_eq!(client_config.get("max.in.flight"), Some("1"));
    }

    #[test]
    fn test_publish_before_start_errors() {
        let sink = KafkaSink::from_properties(&base_props()).unwrap();
        let result = sink.publish(b"payload");
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("call start() first"));
    }

    #[test]
    fn test_stop_without_start_is_noop() {
        let sink = KafkaSink::from_properties(&base_props()).unwrap();
        sink.stop(); // must not panic, since no producer was created
        sink.stop(); // safe to repeat
    }

    #[test]
    fn test_sink_clone_resets_runtime_state() {
        let sink = KafkaSink::from_properties(&base_props()).unwrap();
        let cloned = sink.clone();
        assert_eq!(cloned.config.topic, "out-events");
        assert!(cloned.producer.lock().unwrap().is_none());
    }

    // =========================================================================
    // Factory Tests
    // =========================================================================

    #[test]
    fn test_factory_metadata() {
        let factory = KafkaSinkFactory;
        assert_eq!(factory.name(), "kafka");
        assert!(factory.supported_formats().contains(&"json"));
        assert!(factory.supported_formats().contains(&"csv"));
        assert!(factory.supported_formats().contains(&"bytes"));
        assert!(factory
            .required_parameters()
            .contains(&"kafka.bootstrap.servers"));
        assert!(factory.required_parameters().contains(&"kafka.topic"));
        assert!(factory.optional_parameters().contains(&"kafka.acks"));
    }

    #[test]
    fn test_factory_create() {
        let factory = KafkaSinkFactory;
        assert!(factory.create_initialized(&base_props()).is_ok());
    }

    #[test]
    fn test_factory_missing_required() {
        let factory = KafkaSinkFactory;
        let mut config = base_props();
        config.remove("kafka.topic");
        assert!(factory.create_initialized(&config).is_err());
    }

    #[test]
    fn test_factory_clone() {
        let factory = KafkaSinkFactory;
        let cloned = factory.clone_box();
        assert_eq!(cloned.name(), "kafka");
    }
}
