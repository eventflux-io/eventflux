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

//! # Kafka Source
//!
//! Reads records from Kafka topics and delivers their payloads to the
//! EventFlux pipeline.
//!
//! ## Architecture
//!
//! Unlike the RabbitMQ and WebSocket sources, this needs no tokio runtime.
//! librdkafka runs its own background threads for network I/O and reconnects,
//! and the blocking `BaseConsumer` poll loop runs straight on the
//! [`SourceWorker`] thread.
//!
//! ```text
//! Kafka topic → BaseConsumer::poll(100ms) → bytes → SourceCallback → SourceMapper → Events
//! ```
//!
//! ## Delivery semantics
//!
//! - `kafka.enable.auto.commit = true` (default): librdkafka stores and
//!   commits offsets on its own. A commit can happen before the record is
//!   processed, so this is roughly at-most-once.
//! - `kafka.enable.auto.commit = false`: an offset can be committed only after
//!   the pipeline accepted the record, or the error strategy dropped it or sent
//!   it to the DLQ. This is at-least-once. Commits go out every
//!   `auto.commit.interval.ms` (librdkafka default 5s, set it with
//!   `kafka.rdkafka.auto.commit.interval.ms`), and also as a blocking commit
//!   when partitions are revoked in a rebalance and at shutdown.
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use std::collections::HashMap;
//!
//! let mut properties = HashMap::new();
//! properties.insert("kafka.bootstrap.servers".to_string(), "localhost:9092".to_string());
//! properties.insert("kafka.topic".to_string(), "events".to_string());
//!
//! let source = KafkaSource::from_properties(&properties, None, "KafkaStream")?;
//! ```

use super::{Source, SourceCallback, SourceWorker};
use crate::core::error::source_support::{
    deliver_with_error_handling, DeliveryVerdict, SourceErrorContext,
};
use crate::core::exception::EventFluxError;
use crate::core::extension::SourceFactory;
use crate::core::stream::connector_util::parse_or;
use crate::core::stream::input::input_handler::InputHandler;
use crate::core::stream::kafka_common::{validate_topics_exist, KafkaConnectionConfig};
use rdkafka::client::ClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance};
use rdkafka::message::Message;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// librdkafka settings that have their own config field. They are rejected
/// when passed through the raw `kafka.rdkafka.*` keys, so one setting is never
/// set in two places.
const RESERVED_RDKAFKA_KEYS: &[&str] = &[
    "bootstrap.servers",
    "group.id",
    "enable.auto.commit",
    "enable.auto.offset.store",
    "auto.offset.reset",
];

/// Configuration for Kafka source
#[derive(Debug, Clone)]
pub struct KafkaSourceConfig {
    /// Broker connection and security settings, shared with the sink
    pub connection: KafkaConnectionConfig,
    /// Topics to read, from the comma separated `kafka.topic`
    pub topics: Vec<String>,
    /// Consumer group id. Default `eventflux-<first-topic>`, which stays the
    /// same across restarts so committed offsets are kept.
    pub group_id: String,
    /// Where to start when there is no committed offset, `earliest` or
    /// `latest`. Default `latest`.
    pub auto_offset_reset: String,
    /// true: librdkafka commits on a timer, roughly at-most-once. The default.
    /// false: commit only after the pipeline accepted the record (at-least-once).
    pub enable_auto_commit: bool,
    /// Poll timeout in ms. Also the longest stop has to wait. Default 100.
    pub poll_timeout_ms: u64,
}

impl KafkaSourceConfig {
    /// Reads the config from properties.
    ///
    /// # Errors
    ///
    /// Returns an error when the connection config is invalid or uses a
    /// reserved `kafka.rdkafka.*` key, `kafka.topic` is missing or empty,
    /// `kafka.auto.offset.reset` is not `earliest` or `latest`, or a value
    /// does not parse.
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        let connection = KafkaConnectionConfig::from_properties(properties, RESERVED_RDKAFKA_KEYS)?;

        let topics: Vec<String> = properties
            .get("kafka.topic")
            .ok_or("Missing required property: kafka.topic")?
            .split(',')
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .collect();
        if topics.is_empty() {
            return Err("kafka.topic cannot be empty".to_string());
        }

        // A default group id that never changes keeps committed offsets across
        // restarts
        let group_id = properties
            .get("kafka.group.id")
            .cloned()
            .unwrap_or_else(|| format!("eventflux-{}", topics[0]));

        let auto_offset_reset = properties
            .get("kafka.auto.offset.reset")
            .cloned()
            .unwrap_or_else(|| "latest".to_string());
        if auto_offset_reset != "earliest" && auto_offset_reset != "latest" {
            return Err(format!(
                "Invalid kafka.auto.offset.reset '{auto_offset_reset}': expected 'earliest' or 'latest'"
            ));
        }

        let enable_auto_commit = parse_or(properties, "kafka.enable.auto.commit", true)?;
        let poll_timeout_ms = parse_or(properties, "kafka.poll.timeout.ms", 100)?;

        Ok(Self {
            connection,
            topics,
            group_id,
            auto_offset_reset,
            enable_auto_commit,
            poll_timeout_ms,
        })
    }

    /// Builds the librdkafka client config.
    ///
    /// Offsets follow the standard librdkafka at-least-once pattern. The
    /// built in committer is ALWAYS on. It sends the local offset store to the
    /// broker on a timer, on rebalance, and on close. What
    /// `kafka.enable.auto.commit` really controls is what goes INTO that store.
    ///
    /// - `true` (default): librdkafka stores every polled offset itself. A
    ///   commit can happen before processing, so roughly at-most-once.
    /// - `false`: automatic storing is off, and the consume loop stores an
    ///   offset only AFTER the pipeline accepted the record. Every commit, from
    ///   any path, covers only processed records (at-least-once).
    pub fn to_client_config(&self) -> rdkafka::config::ClientConfig {
        let mut config = rdkafka::config::ClientConfig::new();
        config
            .set("group.id", &self.group_id)
            .set("enable.auto.commit", "true")
            .set(
                "enable.auto.offset.store",
                self.enable_auto_commit.to_string(),
            )
            .set("auto.offset.reset", &self.auto_offset_reset);
        self.connection.apply_to(&mut config);
        config
    }
}

/// Consumer context that logs rebalance events. In manual commit mode it also
/// commits stored offsets, blocking, before partitions are revoked.
struct KafkaSourceContext {
    /// true when `kafka.enable.auto.commit = false`
    manual_commit: bool,
}

impl ClientContext for KafkaSourceContext {}

impl ConsumerContext for KafkaSourceContext {
    fn pre_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        log::debug!("[KafkaSource] Pre-rebalance: {rebalance:?}");

        // In manual commit mode, commit stored offsets before the partitions
        // go away. Otherwise a commit that lands after the revoke is dropped,
        // and the records are delivered again (duplicate outputs).
        if self.manual_commit && matches!(rebalance, Rebalance::Revoke(_)) {
            if let Err(e) = consumer.commit_consumer_state(rdkafka::consumer::CommitMode::Sync) {
                // "No offset stored" just means nothing was waiting
                log::debug!("[KafkaSource] Commit on revoke: {e}");
            }
        }
    }

    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        log::info!("[KafkaSource] Post-rebalance: {rebalance:?}");
    }
}

/// Marks a record as processed in manual commit mode.
///
/// The offset goes into librdkafka's local store. The built in committer, and
/// the blocking commits on revoke and shutdown, send it to the broker. Only
/// offsets stored here are ever committed, which keeps the at-least-once
/// promise on every commit path. In auto commit mode librdkafka stores offsets
/// itself, so this does nothing.
fn store_processed_offset(
    consumer: &BaseConsumer<KafkaSourceContext>,
    msg: &rdkafka::message::BorrowedMessage<'_>,
    auto_commit: bool,
) {
    if auto_commit {
        return;
    }
    if let Err(e) = consumer.store_offset_from_message(msg) {
        // Usually the partition was just revoked. The record will be delivered
        // again to its new owner (at-least-once).
        log::warn!("[KafkaSource] Failed to store offset: {e}");
    }
}

/// Kafka source that reads records from one or more topics.
///
/// - Runs the blocking `BaseConsumer` poll loop on a [`SourceWorker`] thread.
///   No tokio runtime is needed, since librdkafka handles I/O and reconnects.
/// - Gives at-least-once delivery by committing only after success, when
///   `kafka.enable.auto.commit` is false
/// - Uses the `error.*` handling strategies
#[derive(Debug)]
pub struct KafkaSource {
    config: KafkaSourceConfig,
    /// Runs the worker thread. Starts it, signals it, and joins it on stop.
    worker: SourceWorker,
    /// Optional error handling context
    error_ctx: Option<SourceErrorContext>,
}

impl KafkaSource {
    /// Creates a Kafka source with no error handling
    pub fn new(config: KafkaSourceConfig) -> Self {
        Self {
            config,
            worker: SourceWorker::new("KafkaSource"),
            error_ctx: None,
        }
    }

    /// Creates a Kafka source from properties.
    ///
    /// # Required Properties
    /// - `kafka.bootstrap.servers`: comma separated broker list
    /// - `kafka.topic`: topics to read, comma separated
    ///
    /// # Optional Properties
    /// - `kafka.group.id`: consumer group (default `eventflux-<first-topic>`)
    /// - `kafka.auto.offset.reset`: `earliest` or `latest` (default `latest`)
    /// - `kafka.enable.auto.commit`: default `true`. `false` gives at-least-once
    /// - `kafka.poll.timeout.ms`: poll timeout in ms (default 100)
    /// - `kafka.security.protocol`, `kafka.sasl.*`, `kafka.ssl.ca.location`
    /// - `kafka.rdkafka.<prop>`: raw librdkafka settings passed as is
    /// - `error.*`: error handling properties (see SourceErrorContext)
    ///
    /// # Errors
    ///
    /// Returns an error when the Kafka config or the error config is invalid.
    pub fn from_properties(
        properties: &HashMap<String, String>,
        dlq_junction: Option<Arc<Mutex<InputHandler>>>,
        stream_name: &str,
    ) -> Result<Self, String> {
        let config = KafkaSourceConfig::from_properties(properties)?;
        let error_ctx = SourceErrorContext::from_properties(properties, dlq_junction, stream_name)?;

        Ok(Self {
            error_ctx,
            ..Self::new(config)
        })
    }
}

impl Clone for KafkaSource {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            worker: self.worker.clone(), // the clone is a new worker that has not started
            // New context with the same strategy and DLQ, and runtime counters reset
            error_ctx: self.error_ctx.as_ref().map(SourceErrorContext::fresh),
        }
    }
}

impl Source for KafkaSource {
    fn start(&mut self, callback: Arc<dyn SourceCallback>) {
        let config = self.config.clone();
        let mut error_ctx = self.error_ctx.as_ref().map(SourceErrorContext::fresh);

        self.worker.start(move |running| {
            if let Some(ctx) = &mut error_ctx {
                ctx.bind_cancellation(Arc::clone(&running));
            }
            // Setup failures in create and subscribe are reported the same way
            // and end the worker
            let report_fatal = |error_ctx: &mut Option<SourceErrorContext>, err: EventFluxError| {
                if let Some(ctx) = error_ctx {
                    ctx.handle_error(None, &err);
                } else {
                    log::error!("[KafkaSource] {err}");
                }
            };

            let context = KafkaSourceContext {
                manual_commit: !config.enable_auto_commit,
            };
            let consumer: BaseConsumer<KafkaSourceContext> =
                match config.to_client_config().create_with_context(context) {
                    Ok(c) => c,
                    Err(e) => {
                        report_fatal(
                            &mut error_ctx,
                            EventFluxError::ConnectionUnavailable {
                                message: format!("Failed to create Kafka consumer: {e}"),
                                source: Some(Box::new(e)),
                            },
                        );
                        return;
                    }
                };

            let topic_refs: Vec<&str> = config.topics.iter().map(String::as_str).collect();
            if let Err(e) = consumer.subscribe(&topic_refs) {
                report_fatal(
                    &mut error_ctx,
                    EventFluxError::ConnectionUnavailable {
                        message: format!("Failed to subscribe to {:?}: {e}", config.topics),
                        source: Some(Box::new(e)),
                    },
                );
                return;
            }

            log::info!(
                "[KafkaSource] Consuming topics {:?} (group '{}')",
                config.topics,
                config.group_id
            );

            let poll_timeout = Duration::from_millis(config.poll_timeout_ms);
            // Only the worker thread touches these, so plain integers are enough
            let mut records_received: u64 = 0;
            let mut records_failed: u64 = 0;

            'poll: while running.load(Ordering::SeqCst) {
                match consumer.poll(poll_timeout) {
                    None => continue, // timed out, so check the running flag again
                    Some(Err(e)) => {
                        // librdkafka retries short lived broker errors itself.
                        // Errors that reach here go through the error strategy.
                        let err = EventFluxError::ConnectionUnavailable {
                            message: format!("Kafka consumer error: {e}"),
                            source: Some(Box::new(e)),
                        };
                        if let Some(ctx) = &mut error_ctx {
                            if !ctx.handle_error(None, &err) {
                                break;
                            }
                        } else {
                            log::error!("[KafkaSource] {err}");
                        }
                    }
                    Some(Ok(msg)) => {
                        // Tombstones and empty payloads carry no event data,
                        // but still count as read. Store their offset so
                        // commits move past them.
                        let Some(payload) = msg.payload().filter(|p| !p.is_empty()) else {
                            log::debug!(
                                "[KafkaSource] Skipping empty payload at {}:{}@{}",
                                msg.topic(),
                                msg.partition(),
                                msg.offset()
                            );
                            store_processed_offset(&consumer, &msg, config.enable_auto_commit);
                            continue;
                        };

                        records_received += 1;

                        let verdict = deliver_with_error_handling(
                            callback.as_ref(),
                            payload,
                            &mut error_ctx,
                            "KafkaSource",
                        );

                        match verdict {
                            DeliveryVerdict::Delivered | DeliveryVerdict::Disposed => {
                                if verdict == DeliveryVerdict::Disposed {
                                    records_failed += 1;
                                }
                                // Delivered, dropped, or sent to the DLQ. The
                                // record is done, so store its offset and let
                                // the committer move past it.
                                store_processed_offset(&consumer, &msg, config.enable_auto_commit);
                            }
                            DeliveryVerdict::Fail => {
                                // Do NOT store it. No commit can move past
                                // this record, so it is delivered again after
                                // a restart (manual commit mode only).
                                records_failed += 1;
                                log::error!("[KafkaSource] Unrecoverable error, stopping");
                                break 'poll;
                            }
                        }
                    }
                }
            }

            // One last blocking commit of the stored offsets, which are the
            // processed ones, so a clean restart picks up exactly where
            // processing stopped
            if !config.enable_auto_commit {
                if let Err(e) = consumer.commit_consumer_state(rdkafka::consumer::CommitMode::Sync)
                {
                    // "No offset stored" just means there was nothing to commit
                    log::debug!("[KafkaSource] Final commit: {e}");
                }
            }
            consumer.unsubscribe();

            log::info!(
                "[KafkaSource] Stopped (received: {records_received}, failed: {records_failed})"
            );
        });
    }

    fn stop(&mut self) {
        log::info!("[KafkaSource] Stopping...");
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
        // Build the client from the exact config start() will use, so this
        // checks that config too
        let consumer: BaseConsumer = self.config.to_client_config().create().map_err(|e| {
            EventFluxError::ConnectionUnavailable {
                message: format!("Failed to create Kafka consumer: {e}"),
                source: Some(Box::new(e)),
            }
        })?;

        let topic_refs: Vec<&str> = self.config.topics.iter().map(String::as_str).collect();
        validate_topics_exist(
            consumer.client(),
            &self.config.connection.bootstrap_servers,
            &topic_refs,
        )
    }
}

// ============================================================================
// Kafka Source Factory
// ============================================================================

/// Factory for Kafka sources.
///
/// Registered with EventFluxContext and used to build Kafka sources from the
/// SQL WITH clause config.
#[derive(Debug, Clone)]
pub struct KafkaSourceFactory;

impl SourceFactory for KafkaSourceFactory {
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
        // Kafka keys plus the shared error handling keys owned by
        // SourceErrorContext. Joined once, on first use.
        static PARAMS: std::sync::LazyLock<Vec<&'static str>> = std::sync::LazyLock::new(|| {
            [
                &[
                    "kafka.group.id",
                    "kafka.auto.offset.reset",
                    "kafka.enable.auto.commit",
                    "kafka.poll.timeout.ms",
                    "kafka.security.protocol",
                    "kafka.sasl.mechanism",
                    "kafka.sasl.username",
                    "kafka.sasl.password",
                    "kafka.ssl.ca.location",
                    // kafka.rdkafka.<prop> keys are also accepted
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
        // the stream name. Until then the first topic names the stream in error
        // log messages.
        let stream_name = config
            .get("kafka.topic")
            .and_then(|t| t.split(',').next())
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .unwrap_or("kafka-source")
            .to_string();

        let source = KafkaSource::from_properties(config, None, &stream_name)
            .map_err(EventFluxError::configuration)?;

        Ok(Box::new(source))
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
        props.insert(
            "kafka.bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        );
        props.insert("kafka.topic".to_string(), "events".to_string());
        props
    }

    #[test]
    fn test_config_from_properties_required_only() {
        let config = KafkaSourceConfig::from_properties(&base_props()).unwrap();
        assert_eq!(config.connection.bootstrap_servers, "localhost:9092");
        assert_eq!(config.topics, vec!["events"]);
        assert_eq!(config.group_id, "eventflux-events"); // fixed default
        assert_eq!(config.auto_offset_reset, "latest");
        assert!(config.enable_auto_commit);
        assert_eq!(config.poll_timeout_ms, 100);
        assert!(config.connection.security_protocol.is_none());
        assert!(config.connection.rdkafka_overrides.is_empty());
    }

    #[test]
    fn test_config_from_properties_all_options() {
        let mut props = base_props();
        props.insert(
            "kafka.bootstrap.servers".to_string(),
            "b1:9092,b2:9092".to_string(),
        );
        props.insert("kafka.group.id".to_string(), "my-group".to_string());
        props.insert(
            "kafka.auto.offset.reset".to_string(),
            "earliest".to_string(),
        );
        props.insert("kafka.enable.auto.commit".to_string(), "false".to_string());
        props.insert("kafka.poll.timeout.ms".to_string(), "250".to_string());
        props.insert(
            "kafka.security.protocol".to_string(),
            "sasl_ssl".to_string(),
        );
        props.insert("kafka.sasl.mechanism".to_string(), "PLAIN".to_string());
        props.insert("kafka.sasl.username".to_string(), "user".to_string());
        props.insert("kafka.sasl.password".to_string(), "secret".to_string());

        let config = KafkaSourceConfig::from_properties(&props).unwrap();
        assert_eq!(config.connection.bootstrap_servers, "b1:9092,b2:9092");
        assert_eq!(config.group_id, "my-group");
        assert_eq!(config.auto_offset_reset, "earliest");
        assert!(!config.enable_auto_commit);
        assert_eq!(config.poll_timeout_ms, 250);
        assert_eq!(
            config.connection.security_protocol,
            Some("sasl_ssl".to_string())
        );
        assert_eq!(config.connection.sasl_username, Some("user".to_string()));
    }

    #[test]
    fn test_config_multiple_topics() {
        let mut props = base_props();
        props.insert("kafka.topic".to_string(), "a, b ,c".to_string());

        let config = KafkaSourceConfig::from_properties(&props).unwrap();
        assert_eq!(config.topics, vec!["a", "b", "c"]);
        assert_eq!(config.group_id, "eventflux-a"); // first topic
    }

    #[test]
    fn test_config_missing_topic() {
        let mut props = base_props();
        props.remove("kafka.topic");

        let result = KafkaSourceConfig::from_properties(&props);
        assert!(result.unwrap_err().contains("kafka.topic"));
    }

    #[test]
    fn test_config_invalid_offset_reset() {
        let mut props = base_props();
        props.insert(
            "kafka.auto.offset.reset".to_string(),
            "somewhere".to_string(),
        );

        let result = KafkaSourceConfig::from_properties(&props);
        assert!(result.unwrap_err().contains("kafka.auto.offset.reset"));
    }

    #[test]
    fn test_config_invalid_auto_commit() {
        let mut props = base_props();
        props.insert("kafka.enable.auto.commit".to_string(), "maybe".to_string());

        let result = KafkaSourceConfig::from_properties(&props);
        assert!(result.unwrap_err().contains("kafka.enable.auto.commit"));
    }

    #[test]
    fn test_config_rdkafka_reserved_keys_rejected() {
        // Settings the consumer owns must be set through their own options
        for reserved in RESERVED_RDKAFKA_KEYS {
            let mut props = base_props();
            props.insert(format!("kafka.rdkafka.{reserved}"), "x".to_string());

            let result = KafkaSourceConfig::from_properties(&props);
            assert!(
                result.unwrap_err().contains("reserved"),
                "kafka.rdkafka.{reserved} must be rejected"
            );
        }
    }

    #[test]
    fn test_client_config_translation_manual_commit() {
        let mut props = base_props();
        props.insert("kafka.enable.auto.commit".to_string(), "false".to_string());
        props.insert(
            "kafka.rdkafka.fetch.min.bytes".to_string(),
            "1024".to_string(),
        );

        let config = KafkaSourceConfig::from_properties(&props).unwrap();
        let client_config = config.to_client_config();
        assert_eq!(
            client_config.get("bootstrap.servers"),
            Some("localhost:9092")
        );
        assert_eq!(client_config.get("group.id"), Some("eventflux-events"));
        // Standard at-least-once. The built in committer stays on, but only
        // offsets stored by hand, which are the processed ones, get committed
        assert_eq!(client_config.get("enable.auto.commit"), Some("true"));
        assert_eq!(client_config.get("enable.auto.offset.store"), Some("false"));
        assert_eq!(client_config.get("fetch.min.bytes"), Some("1024"));
    }

    #[test]
    fn test_client_config_translation_auto_commit() {
        // Default mode: librdkafka stores and commits on its own
        let config = KafkaSourceConfig::from_properties(&base_props()).unwrap();
        let client_config = config.to_client_config();
        assert_eq!(client_config.get("enable.auto.commit"), Some("true"));
        assert_eq!(client_config.get("enable.auto.offset.store"), Some("true"));
    }

    #[test]
    fn test_source_from_properties() {
        let source = KafkaSource::from_properties(&base_props(), None, "TestStream").unwrap();
        assert_eq!(source.config.topics, vec!["events"]);
        assert!(source.error_ctx.is_none());
    }

    #[test]
    fn test_source_from_properties_with_error_handling() {
        let mut props = base_props();
        props.insert("error.strategy".to_string(), "drop".to_string());

        let source = KafkaSource::from_properties(&props, None, "TestStream").unwrap();
        assert!(source.error_ctx.is_some());
    }

    #[test]
    fn test_source_clone_resets_runtime_state() {
        let mut props = base_props();
        props.insert("error.strategy".to_string(), "drop".to_string());

        let source = KafkaSource::from_properties(&props, None, "TestStream").unwrap();
        let cloned = source.clone();
        assert_eq!(cloned.config.topics, vec!["events"]);
        // The clone keeps the strategy but starts with reset counters
        let ctx = cloned.error_ctx.expect("clone keeps the error strategy");
        assert_eq!(ctx.error_count(), 0); // runtime state not cloned
    }

    // =========================================================================
    // Factory Tests
    // =========================================================================

    #[test]
    fn test_factory_metadata() {
        let factory = KafkaSourceFactory;
        assert_eq!(factory.name(), "kafka");
        assert!(factory.supported_formats().contains(&"json"));
        assert!(factory.supported_formats().contains(&"csv"));
        assert!(factory.supported_formats().contains(&"bytes"));
        assert!(!factory.supported_formats().contains(&"avro")); // no avro mapper exists
        assert!(factory
            .required_parameters()
            .contains(&"kafka.bootstrap.servers"));
        assert!(factory.required_parameters().contains(&"kafka.topic"));
        assert!(factory.optional_parameters().contains(&"kafka.group.id"));
    }

    #[test]
    fn test_factory_create() {
        let factory = KafkaSourceFactory;
        assert!(factory.create_initialized(&base_props()).is_ok());
    }

    #[test]
    fn test_factory_missing_required() {
        let factory = KafkaSourceFactory;
        let mut config = base_props();
        config.remove("kafka.bootstrap.servers");
        assert!(factory.create_initialized(&config).is_err());

        let mut config = base_props();
        config.remove("kafka.topic");
        assert!(factory.create_initialized(&config).is_err());
    }

    #[test]
    fn test_factory_accepts_error_handling_config() {
        let factory = KafkaSourceFactory;
        let mut config = base_props();
        config.insert("error.strategy".to_string(), "retry".to_string());
        config.insert("error.retry.max-attempts".to_string(), "3".to_string());

        assert!(factory.create_initialized(&config).is_ok());
    }

    #[test]
    fn test_factory_clone() {
        let factory = KafkaSourceFactory;
        let cloned = factory.clone_box();
        assert_eq!(cloned.name(), "kafka");
    }
}
