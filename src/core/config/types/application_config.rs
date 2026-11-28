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

//! Application Configuration Types
//!
//! Configuration for one application, down to each stream, definition and query.
//! Durations use `duration_serde`, so they read and write as text such as "30s".
//! Source error handling delays are the exception. They use the `error.*` duration
//! parser, which also takes milliseconds such as "100ms".

use super::{duration_serde, optional_duration_serde};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Configuration of one application
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ApplicationConfig {
    /// Config for each stream, keyed by stream name, with its source or sink
    #[serde(default)]
    pub streams: HashMap<String, StreamConfig>,

    /// Config for each definition, such as tables and windows, keyed by name
    #[serde(default)]
    pub definitions: HashMap<String, DefinitionConfig>,

    /// Config for each query, keyed by query name
    #[serde(default)]
    pub queries: HashMap<String, QueryConfig>,

    /// Persistence settings for the whole application
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistence: Option<PersistenceConfig>,

    /// Monitoring settings for the whole application
    #[serde(skip_serializing_if = "Option::is_none")]
    pub monitoring: Option<MonitoringConfig>,

    /// Error handling settings for the whole application
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_handling: Option<ErrorHandlingConfig>,
}

/// Config of one definition. The `type` field picks the variant, in lower case.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
#[allow(clippy::large_enum_variant)]
pub enum DefinitionConfig {
    /// Stream definition configuration
    Stream(StreamConfig),

    /// Table definition configuration
    Table(TableConfig),

    /// Window definition configuration
    Window(WindowConfig),

    /// Aggregation definition configuration
    Aggregation(AggregationConfig),

    /// Function definition configuration
    Function(FunctionConfig),

    /// Trigger definition configuration
    Trigger(TriggerConfig),
}

/// Stream configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamConfig {
    /// Source configuration, for input streams
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceConfig>,

    /// Sink configuration, for output streams
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink: Option<SinkConfig>,

    /// Schema validation configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaConfig>,

    /// Processing settings for this stream
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processing: Option<StreamProcessingConfig>,
}

/// Source configuration for input streams
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceConfig {
    /// Source type, such as kafka, http, tcp or file. Written as `type` in YAML.
    #[serde(rename = "type")]
    pub source_type: String,

    /// Data format, such as json, avro, protobuf or csv
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,

    /// Connection settings. Holds every other key at the same YAML level.
    #[serde(flatten)]
    pub connection: serde_yaml::Value,

    /// Security configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<SourceSecurityConfig>,

    /// Error handling for source
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_handling: Option<SourceErrorHandling>,

    /// Rate limiting
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<RateLimitConfig>,
}

/// Sink configuration for output streams
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SinkConfig {
    /// Sink type, such as kafka, http, tcp or file. Written as `type` in YAML.
    #[serde(rename = "type")]
    pub sink_type: String,

    /// Data format, such as json, avro, protobuf or csv
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,

    /// Connection settings. Holds every other key at the same YAML level.
    #[serde(flatten)]
    pub connection: serde_yaml::Value,

    /// Security configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<SinkSecurityConfig>,

    /// Delivery guarantee. See [`DeliveryGuarantee`] for the default.
    #[serde(default)]
    pub delivery_guarantee: DeliveryGuarantee,

    /// Retry configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryConfig>,

    /// Batching configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batching: Option<BatchingConfig>,
}

impl SourceConfig {
    /// Flattens this source config into the string map that source factories take.
    ///
    /// Connection keys go in at the top level, written as described in
    /// [`connection_properties`]. `error_handling` goes in as the `error.*` keys
    /// that sources read (see [`SourceErrorHandling`]). `security` and `rate_limit`
    /// go in under their own name, such as `rate_limit.max_rate`. `type` and
    /// `format` are left out because callers pass them separately.
    ///
    /// Errors in the cases listed on [`connection_properties`] and
    /// [`flatten_yaml_into`], if `error_handling` sets a block its strategy never
    /// uses, or if a field cannot be serialized.
    pub fn to_properties(&self) -> Result<HashMap<String, String>, String> {
        // Destructured so a new field is a compile error here until it is passed on.
        let Self {
            source_type: _,
            format: _,
            connection,
            security,
            error_handling,
            rate_limit,
        } = self;

        let mut properties = connection_properties(connection)?;
        flatten_field(security, "security", &mut properties)?;
        if let Some(error_handling) = error_handling {
            error_handling.write_properties(&mut properties)?;
        }
        flatten_field(rate_limit, "rate_limit", &mut properties)?;
        Ok(properties)
    }
}

impl SinkConfig {
    /// Flattens this sink config into the string map that sink factories take.
    ///
    /// Connection keys go in at the top level, written as described in
    /// [`connection_properties`]. `security`, `delivery_guarantee`, `retry` and
    /// `batching` go in under their own name, such as `retry.max_attempts`.
    /// `delivery_guarantee` is always set because it has a default. `type` and
    /// `format` are left out because callers pass them separately.
    ///
    /// Errors in the cases listed on [`connection_properties`] and
    /// [`flatten_yaml_into`], or if a field cannot be serialized.
    pub fn to_properties(&self) -> Result<HashMap<String, String>, String> {
        // Destructured so a new field is a compile error here until it is passed on.
        let Self {
            sink_type: _,
            format: _,
            connection,
            security,
            delivery_guarantee,
            retry,
            batching,
        } = self;

        let mut properties = connection_properties(connection)?;
        flatten_field(security, "security", &mut properties)?;
        flatten_field(delivery_guarantee, "delivery_guarantee", &mut properties)?;
        flatten_field(retry, "retry", &mut properties)?;
        flatten_field(batching, "batching", &mut properties)?;
        Ok(properties)
    }
}

/// Flattens the connection keys of a source or sink.
///
/// Each key is written as described in [`flatten_yaml_into`]. A key whose value
/// is a mapping is also written whole as JSON text, so `json.template: {a: 1}`
/// gives `json.template = {"a":1}` as well as `json.template.a = 1`. Connectors
/// read some properties, such as `json.template`, as one JSON value. Only these
/// top level keys get the JSON copy, because connectors also read nested keys by
/// prefix, such as `json.mapping.<field>`, and a JSON copy there would look like
/// one more field.
///
/// A null connection means no keys were given. Errors if the connection is any
/// other non mapping value, or for the cases listed on [`flatten_yaml_into`].
fn connection_properties(
    connection: &serde_yaml::Value,
) -> Result<HashMap<String, String>, String> {
    let mut properties = HashMap::new();
    match connection {
        serde_yaml::Value::Mapping(map) => {
            for (key, value) in map {
                let key = yaml_key(key, "")?;
                if value.is_mapping() {
                    insert_unique(&mut properties, &key, json_text(value, &key)?)?;
                }
                flatten_yaml_into(value, &key, &mut properties)?;
            }
        }
        serde_yaml::Value::Null => {}
        _ => {
            return Err(format!(
                "Connection configuration must be a mapping, got: {:?}",
                connection
            ))
        }
    }
    Ok(properties)
}

/// Serializes one config field and flattens it under `name`. A `None` field adds nothing.
fn flatten_field<T: Serialize>(
    field: &T,
    name: &str,
    properties: &mut HashMap<String, String>,
) -> Result<(), String> {
    let value = serde_yaml::to_value(field)
        .map_err(|e| format!("Failed to serialize '{}': {}", name, e))?;
    flatten_yaml_into(&value, name, properties)
}

/// Flattens a YAML value into `out` under `prefix`, joining nested keys with dots.
///
/// For example `{mysql: {host: localhost}}` becomes `mysql.host = localhost`.
/// Strings, numbers and bools become their text form. A non empty list of only
/// strings, numbers and bools becomes its items joined by commas, because
/// connectors such as Kafka split these values on commas. Any other list, such
/// as an empty one or one holding mappings, becomes JSON text so no item is lost.
///
/// Null is left out, not written as text. YAML `key:` with no value means "not
/// set", so a connector sees the key as missing instead of reading the string
/// "null" as a real value, such as a file path or topic name.
///
/// Errors if a mapping key is not a string, two keys flatten to the same name
/// (such as `kafka.topic` next to `kafka: {topic: ...}`), or the value holds a
/// custom YAML tag such as `!env`. Nothing here resolves tags, so passing the
/// tagged text on as if it were the real value would be wrong.
pub(crate) fn flatten_yaml_into(
    value: &serde_yaml::Value,
    prefix: &str,
    out: &mut HashMap<String, String>,
) -> Result<(), String> {
    use serde_yaml::Value;

    match value {
        Value::Mapping(map) => {
            for (key, val) in map {
                flatten_yaml_into(val, &yaml_key(key, prefix)?, out)?;
            }
            Ok(())
        }
        Value::Null => Ok(()),
        Value::Sequence(seq) => {
            let items: Option<Vec<String>> = seq.iter().map(scalar_text).collect();
            let text = match items {
                Some(items) if !items.is_empty() => items.join(","),
                _ => json_text(value, prefix)?,
            };
            insert_unique(out, prefix, text)
        }
        Value::Tagged(tagged) => Err(tag_error(&tagged.tag, prefix)),
        Value::String(_) | Value::Number(_) | Value::Bool(_) => {
            let text = scalar_text(value).unwrap_or_default();
            insert_unique(out, prefix, text)
        }
    }
}

/// Returns the text of a string, number or bool, or `None` for any other value.
fn scalar_text(value: &serde_yaml::Value) -> Option<String> {
    match value {
        serde_yaml::Value::String(s) => Some(s.clone()),
        serde_yaml::Value::Number(n) => Some(n.to_string()),
        serde_yaml::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Joins a mapping key onto `prefix` with a dot. Errors if the key is not a string.
fn yaml_key(key: &serde_yaml::Value, prefix: &str) -> Result<String, String> {
    let key = key.as_str().ok_or_else(|| {
        let location = if prefix.is_empty() {
            "the top level"
        } else {
            prefix
        };
        format!("Non-string key {:?} in YAML under {}", key, location)
    })?;
    Ok(if prefix.is_empty() {
        key.to_string()
    } else {
        format!("{}.{}", prefix, key)
    })
}

/// Writes `value` as JSON text. Errors if it holds a custom YAML tag.
fn json_text(value: &serde_yaml::Value, key: &str) -> Result<String, String> {
    if let Some(tag) = find_tag(value) {
        return Err(tag_error(tag, key));
    }
    serde_json::to_string(value).map_err(|e| format!("Failed to write '{}' as JSON: {}", key, e))
}

/// Returns the first custom YAML tag found anywhere in `value`.
fn find_tag(value: &serde_yaml::Value) -> Option<&serde_yaml::value::Tag> {
    match value {
        serde_yaml::Value::Tagged(tagged) => Some(&tagged.tag),
        serde_yaml::Value::Sequence(seq) => seq.iter().find_map(find_tag),
        serde_yaml::Value::Mapping(map) => map.values().find_map(find_tag),
        _ => None,
    }
}

fn tag_error(tag: &serde_yaml::value::Tag, key: &str) -> String {
    format!("YAML tag {} on '{}' is not supported", tag, key)
}

/// Inserts `key`, failing if it is already set so one value never silently replaces another.
fn insert_unique(
    out: &mut HashMap<String, String>,
    key: &str,
    value: String,
) -> Result<(), String> {
    if out.contains_key(key) {
        return Err(format!("Property '{}' is set more than once", key));
    }
    out.insert(key.to_string(), value);
    Ok(())
}

/// Source security configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceSecurityConfig {
    /// Authentication configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authentication: Option<serde_yaml::Value>,

    /// TLS configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfig>,

    /// Authorization configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorization: Option<serde_yaml::Value>,
}

/// Sink security configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SinkSecurityConfig {
    /// Authentication configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authentication: Option<serde_yaml::Value>,

    /// TLS configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfig>,

    /// Authorization configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorization: Option<serde_yaml::Value>,
}

/// TLS configuration for sources and sinks
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TlsConfig {
    /// Turn TLS on. Defaults to false.
    #[serde(default)]
    pub enabled: bool,

    /// Certificate, as a file path or the PEM text itself
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert: Option<String>,

    /// Private key, as a file path or the PEM text itself
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,

    /// CA certificate, as a file path or the PEM text itself
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca: Option<String>,

    /// Check the peer certificate. Defaults to true.
    #[serde(default = "default_true")]
    pub verify_peer: bool,

    /// Server name for SNI
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_name: Option<String>,
}

/// Delivery guarantee options. Written in kebab case, such as `at-least-once`.
/// Defaults to `AtLeastOnce`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeliveryGuarantee {
    /// At most once delivery
    AtMostOnce,

    /// At least once delivery
    AtLeastOnce,

    /// Exactly once delivery
    ExactlyOnce,
}

impl Default for DeliveryGuarantee {
    fn default() -> Self {
        Self::AtLeastOnce
    }
}

/// What a source does with an event that fails, such as a record the mapper
/// cannot parse.
///
/// The fields map to the `error.*` properties that a SQL `WITH` clause takes, and
/// take the same values.
///
/// ```yaml
/// error_handling:
///   strategy: retry          # error.strategy: drop, retry, dlq or fail
///   log_level: warn          # error.log-level
///   retry:                   # error.retry.*
///     max_attempts: 5
///     backoff: exponential   # exponential, linear or fixed
///     initial_delay: 100ms
///     max_delay: 30s
///   dlq:                     # error.dlq.*
///     stream: OrderErrors
///     fallback_strategy: retry # log, fail or retry
///     fallback_retry:        # error.dlq.fallback-retry.*, same keys as retry
///       max_attempts: 3
/// ```
///
/// A delay is a whole number with `ms`, `s` or `m`, or a bare whole number of
/// milliseconds, as in the `error.*` properties. Unknown keys are rejected. A field left out is not written, so
/// the runtime default applies. `error.*` keys in a SQL `WITH` clause fill in or
/// override these values. The merged keys are then checked when the app is
/// created (see `ElementConfigResolver`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SourceErrorHandling {
    /// What to do with a failed event.
    pub strategy: crate::core::error::ErrorStrategy,

    /// Lowest level of error handling messages to log.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_level: Option<crate::core::error::LogLevel>,

    /// Retry settings. Only allowed with strategy `retry`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<SourceRetryConfig>,

    /// Dead letter queue settings. Strategy `dlq` needs a stream, from here or
    /// from `error.dlq.stream` in SQL `WITH`. With strategy `retry`, events whose
    /// retries run out go to the stream instead of being dropped. Not allowed
    /// with any other strategy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dlq: Option<SourceDlqConfig>,
}

/// Retry settings of a source, written as `error.retry.*` or
/// `error.dlq.fallback-retry.*`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SourceRetryConfig {
    /// Attempts in total, counting the first one. Must be at least 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<usize>,

    /// How the delay grows between attempts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff: Option<crate::core::error::BackoffStrategy>,

    /// Delay before the first retry, such as `100ms`. Must not be longer than
    /// `max_delay`.
    #[serde(
        default,
        with = "error_duration_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub initial_delay: Option<Duration>,

    /// Longest delay between retries, such as `30s`.
    #[serde(
        default,
        with = "error_duration_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_delay: Option<Duration>,
}

/// Dead letter queue settings of a source, written as `error.dlq.*`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SourceDlqConfig {
    /// Stream that receives failed events.
    #[serde(deserialize_with = "deserialize_dlq_stream")]
    pub stream: String,

    /// What to do when sending to the DLQ stream fails.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_strategy: Option<crate::core::error::DlqFallbackStrategy>,

    /// Retry settings for the DLQ send. Only allowed with fallback strategy
    /// `retry`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_retry: Option<SourceRetryConfig>,
}

impl SourceErrorHandling {
    /// Writes these settings into `properties` as the `error.*` keys that sources
    /// read.
    ///
    /// Values are not parsed here. The resolver checks them with the runtime
    /// parser after SQL `WITH` keys are merged on top, so a `WITH` key can fill in
    /// or override a YAML field.
    ///
    /// Errors if a block is set that the strategy never uses, which the runtime
    /// would otherwise ignore without a word, or if a key is already in
    /// `properties`, such as a connection key `error.strategy`.
    fn write_properties(&self, properties: &mut HashMap<String, String>) -> Result<(), String> {
        use crate::core::error::{
            DlqFallbackStrategy, DLQ_FALLBACK_RETRY_KEY_PREFIX, DLQ_STREAM_KEY, RETRY_KEY_PREFIX,
        };

        // Destructured so a new field is a compile error here until it is written.
        let Self {
            strategy,
            log_level,
            retry,
            dlq,
        } = self;

        if retry.is_some() && !strategy.requires_retry_config() {
            return Err(format!(
                "error_handling.retry is only used with strategy 'retry', not '{}'",
                strategy.as_str()
            ));
        }
        if dlq.is_some() && !strategy.requires_dlq_config() && !strategy.requires_retry_config() {
            return Err(format!(
                "error_handling.dlq is only used with strategy 'dlq' or 'retry', not '{}'",
                strategy.as_str()
            ));
        }
        if let Some(dlq) = dlq {
            if dlq.fallback_retry.is_some()
                && dlq.fallback_strategy != Some(DlqFallbackStrategy::Retry)
            {
                return Err(
                    "error_handling.dlq.fallback_retry is only used with fallback_strategy 'retry'"
                        .into(),
                );
            }
        }

        insert_unique(properties, "error.strategy", strategy.as_str().to_string())?;
        if let Some(level) = log_level {
            insert_unique(properties, "error.log-level", level.as_str().to_string())?;
        }
        if let Some(retry) = retry {
            retry.write_properties(RETRY_KEY_PREFIX, properties)?;
        }
        if let Some(SourceDlqConfig {
            stream,
            fallback_strategy,
            fallback_retry,
        }) = dlq
        {
            insert_unique(properties, DLQ_STREAM_KEY, stream.clone())?;
            if let Some(fallback) = fallback_strategy {
                insert_unique(
                    properties,
                    "error.dlq.fallback-strategy",
                    fallback.as_str().to_string(),
                )?;
            }
            if let Some(retry) = fallback_retry {
                retry.write_properties(DLQ_FALLBACK_RETRY_KEY_PREFIX, properties)?;
            }
        }
        Ok(())
    }
}

impl SourceRetryConfig {
    /// Writes the fields that are set as `<prefix>.<key>` properties. Delays are
    /// written in milliseconds, so `100ms` is kept exactly.
    ///
    /// Errors if a key is already in `properties`.
    fn write_properties(
        &self,
        prefix: &str,
        properties: &mut HashMap<String, String>,
    ) -> Result<(), String> {
        let Self {
            max_attempts,
            backoff,
            initial_delay,
            max_delay,
        } = self;
        let mut write =
            |key: &str, value: String| insert_unique(properties, &format!("{prefix}.{key}"), value);
        if let Some(n) = max_attempts {
            write("max-attempts", n.to_string())?;
        }
        if let Some(backoff) = backoff {
            write("backoff", backoff.as_str().to_string())?;
        }
        if let Some(delay) = initial_delay {
            write("initial-delay", format!("{}ms", delay.as_millis()))?;
        }
        if let Some(delay) = max_delay {
            write("max-delay", format!("{}ms", delay.as_millis()))?;
        }
        Ok(())
    }
}

/// Reads and writes an optional delay for [`SourceRetryConfig`] with the same
/// parser as the `error.retry.*` properties, so `100ms` works. (`duration_serde`
/// has no unit below a second.) A bare whole number is milliseconds, as in the
/// properties. Writes the delay as `"<n>ms"`.
mod error_duration_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use serde_yaml::Value;
    use std::time::Duration;

    pub fn serialize<S: Serializer>(
        delay: &Option<Duration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match delay {
            Some(delay) => serializer.serialize_str(&format!("{}ms", delay.as_millis())),
            None => serializer.serialize_none(),
        }
    }

    /// Reads the value as a YAML value first, so a bare number works on every load
    /// path. Text parsed straight from a file and a `serde_yaml::Value` (used for
    /// merge keys and `--set` overrides) hand numbers over in different ways.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Duration>, D::Error> {
        let text = match Value::deserialize(deserializer)? {
            Value::Null => return Ok(None),
            Value::String(text) => text,
            Value::Number(n) if n.is_u64() => n.to_string(),
            other => {
                return Err(serde::de::Error::custom(format!(
                    "expected a delay such as '100ms', '30s' or a whole number of milliseconds, got {:?}",
                    other
                )))
            }
        };
        crate::core::error::parse_duration(&text)
            .map(Some)
            .map_err(serde::de::Error::custom)
    }
}

/// Reads the required DLQ stream name. A plain `String` would take YAML null as
/// the text "null" and drop a tag such as `!env`, so the source would look for a
/// stream with the wrong name.
fn deserialize_dlq_stream<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<String, D::Error> {
    match serde_yaml::Value::deserialize(deserializer)? {
        serde_yaml::Value::String(stream) => Ok(stream),
        serde_yaml::Value::Tagged(tagged) => Err(serde::de::Error::custom(tag_error(
            &tagged.tag,
            "error_handling.dlq.stream",
        ))),
        other => Err(serde::de::Error::custom(format!(
            "error_handling.dlq.stream must be a stream name, got {:?}",
            other
        ))),
    }
}

/// Error handling strategies. Written in kebab case, such as `log-and-continue`.
/// Defaults to `LogAndContinue`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ErrorStrategy {
    /// Log error and continue processing
    LogAndContinue,

    /// Log error and stop processing
    LogAndStop,

    /// Ignore errors silently
    Ignore,

    /// Send to dead letter queue
    DeadLetterQueue,

    /// Retry with backoff
    RetryWithBackoff,

    /// Fail at the first error
    FailFast,
}

impl Default for ErrorStrategy {
    fn default() -> Self {
        Self::LogAndContinue
    }
}

/// Retry configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetryConfig {
    /// Maximum retry attempts. Defaults to 3.
    #[serde(default = "default_max_retries")]
    pub max_attempts: usize,

    /// Delay before the first retry. Defaults to 100 ms.
    #[serde(with = "duration_serde", default = "default_initial_delay")]
    pub initial_delay: Duration,

    /// Longest delay between retries. Defaults to 30 seconds.
    #[serde(with = "duration_serde", default = "default_max_delay")]
    pub max_delay: Duration,

    /// Factor the delay grows by after each retry. Defaults to 2.0.
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,

    /// Add random jitter to the delay. False when missing from YAML, but
    /// `RetryConfig::default()` sets it to true.
    #[serde(default)]
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_max_retries(),
            initial_delay: default_initial_delay(),
            max_delay: default_max_delay(),
            backoff_multiplier: default_backoff_multiplier(),
            jitter: true,
        }
    }
}

/// Rate limiting configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RateLimitConfig {
    /// Maximum requests per second
    pub max_rate: f64,

    /// Requests allowed at once above the steady rate. Defaults to 100.
    #[serde(default = "default_burst_capacity")]
    pub burst_capacity: usize,

    /// Rate limiting strategy. Defaults to `TokenBucket`.
    #[serde(default)]
    pub strategy: RateLimitStrategy,
}

/// Rate limiting strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RateLimitStrategy {
    /// Token bucket algorithm
    TokenBucket,

    /// Leaky bucket algorithm
    LeakyBucket,

    /// Fixed window algorithm
    FixedWindow,

    /// Sliding window algorithm
    SlidingWindow,
}

impl Default for RateLimitStrategy {
    fn default() -> Self {
        Self::TokenBucket
    }
}

/// Batching configuration for sinks
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BatchingConfig {
    /// Most events in one batch. Defaults to 100.
    #[serde(default = "default_batch_size")]
    pub max_batch_size: usize,

    /// Longest wait before a partial batch is sent. Defaults to 100 ms.
    #[serde(with = "duration_serde", default = "default_batch_timeout")]
    pub max_batch_timeout: Duration,

    /// Memory limit for one batch
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_batch_memory: Option<usize>,
}

impl Default for BatchingConfig {
    fn default() -> Self {
        Self {
            max_batch_size: default_batch_size(),
            max_batch_timeout: default_batch_timeout(),
            max_batch_memory: None,
        }
    }
}

/// Schema validation configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaConfig {
    /// Schema definition
    pub schema: serde_yaml::Value,

    /// Validation mode. Defaults to `Strict`.
    #[serde(default)]
    pub validation_mode: SchemaValidationMode,

    /// Schema evolution configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evolution: Option<SchemaEvolutionConfig>,
}

/// Schema validation modes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaValidationMode {
    /// Reject invalid data
    Strict,

    /// Log invalid data and carry on
    Lenient,

    /// No validation
    None,
}

impl Default for SchemaValidationMode {
    fn default() -> Self {
        Self::Strict
    }
}

/// Schema evolution configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaEvolutionConfig {
    /// Allow the schema to change over time. Defaults to false.
    #[serde(default)]
    pub enabled: bool,

    /// Move existing data to the new schema automatically. Defaults to false.
    #[serde(default)]
    pub auto_migrate: bool,
}

/// Stream processing configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamProcessingConfig {
    /// Process the stream asynchronously. Defaults to false.
    #[serde(default)]
    pub async_mode: bool,

    /// Buffer size for the stream
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buffer_size: Option<usize>,

    /// Number of parallel workers. Defaults to 1.
    #[serde(default = "default_parallelism")]
    pub parallelism: usize,

    /// Ordering guarantee. Defaults to `None`.
    #[serde(default)]
    pub ordering: OrderingGuarantee,
}

/// Ordering guarantee options. Written in kebab case, such as `per-key`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OrderingGuarantee {
    /// No ordering guarantee
    None,

    /// Events with the same key stay in order
    PerKey,

    /// Global ordering
    Global,
}

impl Default for OrderingGuarantee {
    fn default() -> Self {
        Self::None
    }
}

/// Table configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableConfig {
    /// Store configuration
    pub store: StoreConfig,

    /// Schema configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaConfig>,

    /// Caching configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caching: Option<CachingConfig>,

    /// Indexing configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexing: Option<IndexingConfig>,
}

/// Store configuration for tables
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoreConfig {
    /// Store type, such as rdbms, mongodb or redis. Written as `type` in YAML.
    #[serde(rename = "type")]
    pub store_type: String,

    /// Connection settings. Holds every other key at the same YAML level.
    #[serde(flatten)]
    pub connection: serde_yaml::Value,

    /// Connection pooling
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool: Option<ConnectionPoolConfig>,

    /// Security settings, in whatever shape the store expects
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<serde_yaml::Value>,
}

/// Connection pool configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectionPoolConfig {
    /// Maximum pool size. Defaults to 10.
    #[serde(default = "default_max_pool_size")]
    pub max_size: usize,

    /// Minimum pool size. Defaults to 1.
    #[serde(default = "default_min_pool_size")]
    pub min_size: usize,

    /// Time allowed to open a connection. Defaults to 30 seconds.
    #[serde(with = "duration_serde", default = "default_connection_timeout")]
    pub connection_timeout: Duration,

    /// How long an unused connection is kept
    #[serde(
        with = "optional_duration_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub idle_timeout: Option<Duration>,

    /// Maximum connection lifetime
    #[serde(
        with = "optional_duration_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_lifetime: Option<Duration>,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_size: default_max_pool_size(),
            min_size: default_min_pool_size(),
            connection_timeout: default_connection_timeout(),
            idle_timeout: None,
            max_lifetime: None,
        }
    }
}

/// Caching configuration for tables
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CachingConfig {
    /// Turn caching on. Defaults to false.
    #[serde(default)]
    pub enabled: bool,

    /// Cache type. Defaults to `Memory`.
    #[serde(default)]
    pub cache_type: CacheType,

    /// Maximum cache size
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_size: Option<usize>,

    /// How long an entry stays in the cache
    #[serde(
        with = "optional_duration_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub ttl: Option<Duration>,

    /// Cache eviction policy. Defaults to `Lru`.
    #[serde(default)]
    pub eviction_policy: CacheEvictionPolicy,
}

/// Cache types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheType {
    /// Cache in memory
    Memory,

    /// Cache on disk
    Disk,

    /// Distributed cache
    Distributed,
}

impl Default for CacheType {
    fn default() -> Self {
        Self::Memory
    }
}

/// Cache eviction policies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CacheEvictionPolicy {
    /// Least Recently Used
    Lru,

    /// Least Frequently Used
    Lfu,

    /// First In, First Out
    Fifo,

    /// Expire entries after a set time
    TimeToLive,
}

impl Default for CacheEvictionPolicy {
    fn default() -> Self {
        Self::Lru
    }
}

/// Indexing configuration for tables
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexingConfig {
    /// Primary key columns
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<Vec<String>>,

    /// Secondary indexes
    #[serde(default)]
    pub indexes: Vec<IndexConfig>,

    /// Automatic index creation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_indexing: Option<AutoIndexingConfig>,
}

/// Individual index configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexConfig {
    /// Index name
    pub name: String,

    /// Indexed columns
    pub columns: Vec<String>,

    /// Index type. Defaults to `BTree`.
    #[serde(default)]
    pub index_type: IndexType,

    /// Whether the indexed values must be unique. Defaults to false.
    #[serde(default)]
    pub unique: bool,
}

/// Index types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IndexType {
    /// B-tree index
    BTree,

    /// Hash index
    Hash,

    /// Full text search index
    FullText,

    /// Spatial index
    Spatial,
}

impl Default for IndexType {
    fn default() -> Self {
        Self::BTree
    }
}

/// Automatic index creation settings
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AutoIndexingConfig {
    /// Turn automatic indexing on. Defaults to false.
    #[serde(default)]
    pub enabled: bool,

    /// Threshold for creating an index. Defaults to 1000.
    #[serde(default = "default_auto_index_threshold")]
    pub threshold: usize,

    /// Most indexes that may be created automatically. Defaults to 10.
    #[serde(default = "default_max_auto_indexes")]
    pub max_indexes: usize,
}

/// Window configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WindowConfig {
    /// Window type, such as time, length or session
    pub window_type: String,

    /// Window parameters
    #[serde(default)]
    pub parameters: serde_yaml::Value,

    /// Persistence configuration for window
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistence: Option<WindowPersistenceConfig>,

    /// Memory management for window
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<WindowMemoryConfig>,
}

/// Window persistence configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WindowPersistenceConfig {
    /// Turn persistence on for this window. Defaults to false.
    #[serde(default)]
    pub enabled: bool,

    /// Time between checkpoints of the window state. Defaults to 60 seconds.
    #[serde(
        with = "duration_serde",
        default = "default_window_checkpoint_interval"
    )]
    pub checkpoint_interval: Duration,

    /// Compression for the window state. Defaults to `Lz4`.
    #[serde(default)]
    pub compression: CompressionType,

    /// Persistence backend to use instead of the global one
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend: Option<String>,
}

/// Compression types for window persistence. Defaults to `Lz4`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionType {
    None,
    Lz4,
    Snappy,
    Zstd,
}

impl Default for CompressionType {
    fn default() -> Self {
        Self::Lz4
    }
}

/// Window memory management configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WindowMemoryConfig {
    /// Maximum memory for the window, as text such as "512MB"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_memory: Option<String>,

    /// Memory level at which window data spills to disk
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spill_threshold: Option<f32>,

    /// Track memory use. Defaults to true.
    #[serde(default = "default_true")]
    pub monitoring_enabled: bool,
}

/// Aggregation configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AggregationConfig {
    /// Store configuration for aggregation
    pub store: StoreConfig,

    /// Retention policy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention: Option<RetentionConfig>,

    /// Partitioning strategy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitioning: Option<PartitioningConfig>,
}

/// Retention configuration for aggregations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetentionConfig {
    /// Keep data for this long
    #[serde(
        with = "optional_duration_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub by_time: Option<Duration>,

    /// Keep data up to this much memory, as text
    #[serde(skip_serializing_if = "Option::is_none")]
    pub by_memory: Option<String>,

    /// Keep up to this many records
    #[serde(skip_serializing_if = "Option::is_none")]
    pub by_count: Option<usize>,
}

/// Partitioning configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartitioningConfig {
    /// Partitioning strategy
    pub strategy: PartitioningStrategy,

    /// Number of partitions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_count: Option<usize>,

    /// Partition key fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_keys: Option<Vec<String>>,
}

/// Partitioning strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PartitioningStrategy {
    /// Partition by hash of the key
    Hash,

    /// Partition by key ranges
    Range,

    /// Partition by time
    Time,

    /// Custom partitioning
    Custom,
}

/// Function configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FunctionConfig {
    /// Implementation settings. Holds every other key at the same YAML level.
    #[serde(flatten)]
    pub implementation: serde_yaml::Value,

    /// Security configuration for function
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<FunctionSecurityConfig>,

    /// Resource limits for function
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<FunctionResourceConfig>,
}

/// Function security configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FunctionSecurityConfig {
    /// Run the function in a sandbox. Defaults to true.
    #[serde(default = "default_true")]
    pub sandboxing_enabled: bool,

    /// Allowed operations
    #[serde(default)]
    pub allowed_operations: Vec<String>,

    /// Denied operations
    #[serde(default)]
    pub denied_operations: Vec<String>,
}

/// Function resource configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FunctionResourceConfig {
    /// Maximum memory usage
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_memory: Option<String>,

    /// Maximum execution time
    #[serde(
        with = "optional_duration_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_execution_time: Option<Duration>,

    /// CPU limits
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_cpu: Option<String>,
}

/// Trigger configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TriggerConfig {
    /// Trigger settings. Holds every other key at the same YAML level.
    #[serde(flatten)]
    pub config: serde_yaml::Value,

    /// Error handling for trigger
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_handling: Option<TriggerErrorHandling>,
}

/// Trigger error handling
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TriggerErrorHandling {
    /// Error strategy. Defaults to `LogAndContinue`.
    #[serde(default)]
    pub strategy: ErrorStrategy,

    /// Failures allowed before the trigger stops. Defaults to 5.
    #[serde(default = "default_max_failures")]
    pub max_failures: usize,

    /// Recovery strategy. Defaults to `Restart`.
    #[serde(default)]
    pub recovery: TriggerRecovery,
}

/// Trigger recovery strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TriggerRecovery {
    /// Restart trigger
    Restart,

    /// Skip failed execution
    Skip,

    /// Manual intervention required
    Manual,
}

impl Default for TriggerRecovery {
    fn default() -> Self {
        Self::Restart
    }
}

/// Configuration of one query
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryConfig {
    /// Number of parallel workers for the query. Defaults to 1.
    #[serde(default = "default_parallelism")]
    pub parallelism: usize,

    /// Enable async mode for query
    #[serde(default)]
    pub async_mode: bool,

    /// Error handling for query
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_handling: Option<QueryErrorHandling>,

    /// Checkpointing configuration for query
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpointing: Option<QueryCheckpointingConfig>,

    /// Resource limits for query
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<QueryResourceConfig>,
}

/// Query error handling configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryErrorHandling {
    /// Error strategy. Defaults to `LogAndContinue`.
    #[serde(default)]
    pub strategy: ErrorStrategy,

    /// Dead letter queue for failed events
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dead_letter_queue: Option<String>,

    /// Maximum retry attempts. Defaults to 3.
    #[serde(default = "default_max_retries")]
    pub max_retry_attempts: usize,
}

/// Query checkpointing configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryCheckpointingConfig {
    /// Time between checkpoints. Defaults to 60 seconds.
    #[serde(with = "duration_serde", default = "default_checkpoint_interval")]
    pub interval: Duration,

    /// Checkpointing strategy. Defaults to `Async`.
    #[serde(default)]
    pub strategy: CheckpointingStrategy,

    /// Save only what changed since the last checkpoint. Defaults to true.
    #[serde(default = "default_true")]
    pub incremental: bool,
}

/// Checkpointing strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckpointingStrategy {
    /// Synchronous checkpointing
    Sync,

    /// Asynchronous checkpointing
    Async,

    /// Adaptive checkpointing
    Adaptive,
}

impl Default for CheckpointingStrategy {
    fn default() -> Self {
        Self::Async
    }
}

/// Query resource configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryResourceConfig {
    /// Maximum memory usage
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_memory: Option<String>,

    /// CPU allocation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,

    /// Maximum execution time per event
    #[serde(
        with = "optional_duration_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_execution_time: Option<Duration>,
}

/// Persistence settings for the whole application
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistenceConfig {
    /// Turn persistence on. Defaults to false.
    #[serde(default)]
    pub enabled: bool,

    /// Store type to use instead of the global one
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store_type: Option<String>,

    /// Time between checkpoints. Defaults to 60 seconds.
    #[serde(with = "duration_serde", default = "default_checkpoint_interval")]
    pub checkpoint_interval: Duration,

    /// Save only what changed since the last checkpoint. Defaults to true.
    #[serde(default = "default_true")]
    pub incremental: bool,

    /// Extra settings for the persistence store
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<serde_yaml::Value>,
}

/// Monitoring settings for the whole application
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MonitoringConfig {
    /// Collect metrics. Defaults to true.
    #[serde(default = "default_true")]
    pub metrics_enabled: bool,

    /// Prometheus metrics port. Defaults to 9090.
    #[serde(default = "default_prometheus_port")]
    pub prometheus_port: u16,

    /// Health check port. Defaults to 8080.
    #[serde(default = "default_health_port")]
    pub health_check_port: u16,

    /// Log level for the application. Defaults to `Info`.
    #[serde(default)]
    pub log_level: LogLevel,

    /// Custom monitoring configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom: Option<serde_yaml::Value>,
}

/// Log levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl Default for LogLevel {
    fn default() -> Self {
        Self::Info
    }
}

/// Error handling settings for the whole application
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ErrorHandlingConfig {
    /// Error strategy used when nothing more specific is set. Defaults to `LogAndContinue`.
    #[serde(default)]
    pub default_strategy: ErrorStrategy,

    /// Maximum retry attempts. Defaults to 3.
    #[serde(default = "default_max_retries")]
    pub max_retry_attempts: usize,

    /// Default dead letter queue
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dead_letter_queue: Option<String>,

    /// Global error handlers
    #[serde(default)]
    pub handlers: Vec<ErrorHandlerConfig>,
}

/// Error handler configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ErrorHandlerConfig {
    /// Handler name
    pub name: String,

    /// Error types to handle
    pub error_types: Vec<String>,

    /// Handler settings. Holds every other key at the same YAML level.
    #[serde(flatten)]
    pub config: serde_yaml::Value,
}

// Default values used by the serde attributes above
fn default_true() -> bool {
    true
}

fn default_max_retries() -> usize {
    3
}

fn default_initial_delay() -> Duration {
    Duration::from_millis(100)
}

fn default_max_delay() -> Duration {
    Duration::from_secs(30)
}

fn default_backoff_multiplier() -> f64 {
    2.0
}

fn default_burst_capacity() -> usize {
    100
}

fn default_batch_size() -> usize {
    100
}

fn default_batch_timeout() -> Duration {
    Duration::from_millis(100)
}

fn default_parallelism() -> usize {
    1
}

fn default_max_pool_size() -> usize {
    10
}

fn default_min_pool_size() -> usize {
    1
}

fn default_connection_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_auto_index_threshold() -> usize {
    1000
}

fn default_max_auto_indexes() -> usize {
    10
}

fn default_window_checkpoint_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_checkpoint_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_max_failures() -> usize {
    5
}

fn default_prometheus_port() -> u16 {
    9090
}

fn default_health_port() -> u16 {
    8080
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_application_config_default() {
        let config = ApplicationConfig::default();
        assert!(config.definitions.is_empty());
        assert!(config.queries.is_empty());
        assert!(config.persistence.is_none());
    }

    #[test]
    fn test_stream_config_serialization() {
        let stream_config = StreamConfig {
            source: Some(SourceConfig {
                source_type: "kafka".to_string(),
                format: Some("json".to_string()),
                connection: serde_yaml::Value::Mapping(serde_yaml::Mapping::new()),
                security: None,
                error_handling: None,
                rate_limit: None,
            }),
            sink: None,
            schema: None,
            processing: None,
        };

        let definition = DefinitionConfig::Stream(stream_config);
        let yaml = serde_yaml::to_string(&definition).unwrap();
        let deserialized: DefinitionConfig = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(definition, deserialized);
    }

    #[test]
    fn test_error_strategy_serde() {
        let strategy = ErrorStrategy::RetryWithBackoff;
        let yaml = serde_yaml::to_string(&strategy).unwrap();
        assert_eq!(yaml.trim(), "retry-with-backoff");

        let deserialized: ErrorStrategy = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(deserialized, strategy);
    }

    #[test]
    fn test_delivery_guarantee_serde() {
        let guarantee = DeliveryGuarantee::ExactlyOnce;
        let yaml = serde_yaml::to_string(&guarantee).unwrap();
        assert_eq!(yaml.trim(), "exactly-once");
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.backoff_multiplier, 2.0);
        assert!(config.jitter);
    }

    #[test]
    fn test_window_config() {
        let window_config = WindowConfig {
            window_type: "time".to_string(),
            parameters: serde_yaml::Value::String("5min".to_string()),
            persistence: Some(WindowPersistenceConfig {
                enabled: true,
                checkpoint_interval: Duration::from_secs(30),
                compression: CompressionType::Zstd,
                backend: None,
            }),
            memory: None,
        };

        let yaml = serde_yaml::to_string(&window_config).unwrap();
        let deserialized: WindowConfig = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(window_config, deserialized);
    }

    #[test]
    fn test_query_config_defaults() {
        let config = QueryConfig {
            parallelism: default_parallelism(),
            async_mode: false,
            error_handling: None,
            checkpointing: None,
            resources: None,
        };

        assert_eq!(config.parallelism, 1);
        assert!(!config.async_mode);
    }

    fn sink_from_yaml(yaml: &str) -> SinkConfig {
        serde_yaml::from_str(yaml).unwrap()
    }

    fn source_from_yaml(yaml: &str) -> SourceConfig {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn test_flatten_yaml_into_simple_mapping() {
        let value: serde_yaml::Value = serde_yaml::from_str("host: localhost\nport: 3306").unwrap();
        let mut result = HashMap::new();
        flatten_yaml_into(&value, "", &mut result).unwrap();

        assert_eq!(result.get("host"), Some(&"localhost".to_string()));
        assert_eq!(result.get("port"), Some(&"3306".to_string()));
    }

    #[test]
    fn test_flatten_yaml_into_nested_mapping() {
        let value: serde_yaml::Value = serde_yaml::from_str("mysql:\n  host: localhost").unwrap();
        let mut result = HashMap::new();
        flatten_yaml_into(&value, "", &mut result).unwrap();

        assert_eq!(result.get("mysql.host"), Some(&"localhost".to_string()));
    }

    #[test]
    fn test_flatten_yaml_into_sequence() {
        let value: serde_yaml::Value = serde_yaml::from_str("[val1, val2, val3]").unwrap();
        let mut result = HashMap::new();
        flatten_yaml_into(&value, "list", &mut result).unwrap();

        assert_eq!(result.get("list"), Some(&"val1,val2,val3".to_string()));
    }

    #[test]
    fn test_flatten_yaml_into_skips_null() {
        let value: serde_yaml::Value =
            serde_yaml::from_str("a:\nb: ~\nc: null\nnested:\n  d:\n  e: x").unwrap();
        let mut result = HashMap::new();
        flatten_yaml_into(&value, "", &mut result).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result.get("nested.e"), Some(&"x".to_string()));
    }

    #[test]
    fn test_flatten_yaml_into_rejects_tags() {
        for yaml in [
            "k: !env SECRET",
            "k: [!env SECRET, b]",
            "k: [{a: !env SECRET}]",
        ] {
            let value: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
            let mut result = HashMap::new();
            let err = flatten_yaml_into(&value, "", &mut result).unwrap_err();
            assert!(err.contains("!env") && err.contains("'k'"), "{yaml}: {err}");
        }
    }

    #[test]
    fn test_flatten_yaml_into_accepts_core_tags() {
        let value: serde_yaml::Value = serde_yaml::from_str("port: !!str 9092").unwrap();
        let mut result = HashMap::new();
        flatten_yaml_into(&value, "", &mut result).unwrap();
        assert_eq!(result.get("port"), Some(&"9092".to_string()));
    }

    #[test]
    fn test_flatten_yaml_into_non_scalar_lists_become_json() {
        let value: serde_yaml::Value =
            serde_yaml::from_str("empty: []\nobjects: [{id: 1}]\nwith_null: [a, ~]").unwrap();
        let mut result = HashMap::new();
        flatten_yaml_into(&value, "", &mut result).unwrap();

        assert_eq!(result.get("empty"), Some(&"[]".to_string()));
        assert_eq!(result.get("objects"), Some(&r#"[{"id":1}]"#.to_string()));
        assert_eq!(result.get("with_null"), Some(&r#"["a",null]"#.to_string()));
    }

    #[test]
    fn test_flatten_yaml_into_rejects_duplicate_names() {
        let value: serde_yaml::Value =
            serde_yaml::from_str("kafka:\n  topic: a\nkafka.topic: b").unwrap();
        let mut result = HashMap::new();
        let err = flatten_yaml_into(&value, "", &mut result).unwrap_err();
        assert!(err.contains("'kafka.topic' is set more than once"), "{err}");
    }

    #[test]
    fn test_connection_mapping_value_is_also_json_text() {
        let sink = sink_from_yaml(
            "type: file\njson.template:\n  kind: ALERT\n  text: \"{{msg}}\"\n  extra:\n",
        );
        let props = sink.to_properties().unwrap();

        let template: serde_json::Value =
            serde_json::from_str(props.get("json.template").unwrap()).unwrap();
        assert_eq!(
            template,
            serde_json::json!({"kind": "ALERT", "text": "{{msg}}", "extra": null})
        );
        assert_eq!(props.get("json.template.kind"), Some(&"ALERT".to_string()));
        assert!(!props.contains_key("json.template.extra"));
    }

    #[test]
    fn test_only_top_level_connection_mappings_get_json_text() {
        let source = source_from_yaml("type: http\njson:\n  mapping:\n    name: $.n\n");
        let props = source.to_properties().unwrap();

        assert!(props.contains_key("json"));
        // A JSON copy here would be read as one more `json.mapping.<field>`
        assert!(!props.contains_key("json.mapping"));
        assert_eq!(props.get("json.mapping.name"), Some(&"$.n".to_string()));
    }

    #[test]
    fn test_to_properties_rejects_connection_key_that_clashes_with_field() {
        let sink = sink_from_yaml("type: kafka\nretry.max_attempts: 9\nretry:\n  max_attempts: 5\n  initial_delay: 1s\n  max_delay: 30s\n  backoff_multiplier: 2.0\n  jitter: true\n");
        let err = sink.to_properties().unwrap_err();
        assert!(
            err.contains("'retry.max_attempts' is set more than once"),
            "{err}"
        );
    }

    #[test]
    fn test_flatten_yaml_into_rejects_non_string_key() {
        let value: serde_yaml::Value = serde_yaml::from_str("1: one").unwrap();
        let mut result = HashMap::new();
        let err = flatten_yaml_into(&value, "", &mut result).unwrap_err();
        assert!(err.contains("Non-string key"), "{err}");
    }

    #[test]
    fn test_sink_to_properties_null_value_is_absent() {
        let sink = sink_from_yaml("type: file\nformat: json\nfile.path:\n");
        let props = sink.to_properties().unwrap();

        assert!(!props.contains_key("file.path"), "{props:?}");
        assert!(props.values().all(|v| v != "null"), "{props:?}");
    }

    #[test]
    fn test_sink_to_properties_empty_string_is_kept() {
        let sink = sink_from_yaml("type: file\nformat: json\nfile.path: \"\"\n");
        let props = sink.to_properties().unwrap();

        assert_eq!(props.get("file.path"), Some(&String::new()));
    }

    #[test]
    fn test_source_to_properties_null_value_is_absent() {
        let source = source_from_yaml("type: kafka\nkafka.topic:\nkafka.brokers: b1:9092\n");
        let props = source.to_properties().unwrap();

        assert!(!props.contains_key("kafka.topic"), "{props:?}");
        assert_eq!(props.get("kafka.brokers"), Some(&"b1:9092".to_string()));
        assert!(props.values().all(|v| v != "null"), "{props:?}");
    }

    #[test]
    fn test_to_properties_leaves_out_type_and_format() {
        let sink = sink_from_yaml("type: file\nformat: json\nfile.path: out.json\n");
        let props = sink.to_properties().unwrap();

        assert!(!props.contains_key("type"));
        assert!(!props.contains_key("format"));
        assert_eq!(props.get("file.path"), Some(&"out.json".to_string()));
    }

    #[test]
    fn test_sink_to_properties_connection_values_and_structured_fields() {
        let sink = sink_from_yaml(
            r#"
type: kafka
format: json
kafka:
  topics: [events, logs]
  port: 9092
  ack:
security:
  authentication:
    user: admin
    password:
delivery_guarantee: exactly-once
retry:
  max_attempts: 5
  initial_delay: 1s
  max_delay: 30s
  backoff_multiplier: 2.0
  jitter: true
"#,
        );
        let props = sink.to_properties().unwrap();

        assert_eq!(props.get("kafka.topics"), Some(&"events,logs".to_string()));
        assert_eq!(props.get("kafka.port"), Some(&"9092".to_string()));
        assert!(!props.contains_key("kafka.ack"));
        assert_eq!(
            props.get("security.authentication.user"),
            Some(&"admin".to_string())
        );
        assert!(!props.contains_key("security.authentication.password"));
        assert_eq!(
            props.get("delivery_guarantee"),
            Some(&"exactly-once".to_string())
        );
        assert_eq!(props.get("retry.max_attempts"), Some(&"5".to_string()));
        assert_eq!(props.get("retry.jitter"), Some(&"true".to_string()));
        assert!(props.values().all(|v| v != "null"), "{props:?}");
    }

    #[test]
    fn test_sink_to_properties_sets_default_delivery_guarantee() {
        let sink = sink_from_yaml("type: log\n");
        let props = sink.to_properties().unwrap();

        assert_eq!(
            props.get("delivery_guarantee"),
            Some(&"at-least-once".to_string())
        );
    }

    #[test]
    fn test_to_properties_rejects_non_mapping_connection() {
        let sink = SinkConfig {
            sink_type: "log".to_string(),
            format: None,
            connection: serde_yaml::Value::String("bad".to_string()),
            security: None,
            delivery_guarantee: DeliveryGuarantee::default(),
            retry: None,
            batching: None,
        };
        let err = sink.to_properties().unwrap_err();
        assert!(err.contains("must be a mapping"), "{err}");

        let source = SourceConfig {
            source_type: "timer".to_string(),
            format: None,
            connection: serde_yaml::Value::Null,
            security: None,
            error_handling: None,
            rate_limit: None,
        };
        assert!(source.to_properties().unwrap().is_empty());
    }

    /// Flattens a source whose YAML is `type: file` plus `error_yaml`.
    fn source_error_properties(error_yaml: &str) -> Result<HashMap<String, String>, String> {
        let yaml = format!("type: file\nfile.path: /in.jsonl\n{error_yaml}");
        serde_yaml::from_str::<SourceConfig>(&yaml)
            .map_err(|e| e.to_string())?
            .to_properties()
    }

    fn props(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Regression test for https://github.com/eventflux-io/eventflux/issues/211.
    /// The block used to be written as `error_handling.*` keys, which no source
    /// reads, so failed events were dropped whatever the YAML said.
    #[test]
    fn test_source_error_handling_writes_error_keys() {
        let props_out = source_error_properties(
            "error_handling:\n  strategy: dlq\n  log_level: error\n  dlq:\n    stream: OrderErrors\n    fallback_strategy: retry\n    fallback_retry:\n      max_attempts: 4\n      backoff: fixed\n      initial_delay: 250ms\n      max_delay: 2s\n",
        )
        .unwrap();

        assert_eq!(
            props_out,
            props(&[
                ("file.path", "/in.jsonl"),
                ("error.strategy", "dlq"),
                ("error.log-level", "error"),
                ("error.dlq.stream", "OrderErrors"),
                ("error.dlq.fallback-strategy", "retry"),
                ("error.dlq.fallback-retry.max-attempts", "4"),
                ("error.dlq.fallback-retry.backoff", "fixed"),
                ("error.dlq.fallback-retry.initial-delay", "250ms"),
                ("error.dlq.fallback-retry.max-delay", "2000ms"),
            ])
        );
        assert!(!props_out.keys().any(|k| k.starts_with("error_handling")));
    }

    #[test]
    fn test_source_error_handling_retry_with_dlq() {
        let props_out = source_error_properties(
            "error_handling:\n  strategy: retry\n  retry:\n    max_attempts: 5\n    backoff: linear\n    initial_delay: 100ms\n    max_delay: 30s\n  dlq:\n    stream: OrderErrors\n",
        )
        .unwrap();

        assert_eq!(
            props_out,
            props(&[
                ("file.path", "/in.jsonl"),
                ("error.strategy", "retry"),
                ("error.retry.max-attempts", "5"),
                ("error.retry.backoff", "linear"),
                ("error.retry.initial-delay", "100ms"),
                ("error.retry.max-delay", "30000ms"),
                ("error.dlq.stream", "OrderErrors"),
            ])
        );
    }

    #[test]
    fn test_source_error_handling_writes_only_set_fields() {
        let props_out = source_error_properties("error_handling:\n  strategy: drop\n").unwrap();
        assert_eq!(
            props_out,
            props(&[("file.path", "/in.jsonl"), ("error.strategy", "drop")])
        );
    }

    #[test]
    fn test_source_error_handling_rejects_invalid_blocks() {
        let cases = [
            // Strategy and field names from the old schema, which never worked
            ("error_handling:\n  strategy: dead-letter-queue\n", "Invalid error strategy"),
            ("error_handling:\n  strategy: dlq\n  dead_letter_queue: E\n", "unknown field"),
            ("error_handling:\n  strategy: retry\n  max_retry_attempts: 3\n", "unknown field"),
            ("error_handling:\n  log_level: warn\n", "missing field `strategy`"),
            ("error_handling:\n  strategy: drop\n  log_level: loud\n", "Invalid log level"),
            ("error_handling:\n  strategy: retry\n  retry:\n    backoff: random\n", "backoff"),
            ("error_handling:\n  strategy: retry\n  retry:\n    max_delay: soon\n", "Invalid duration 'soon'"),
            ("error_handling:\n  strategy: retry\n  retry:\n    max_delay: 1.5\n", "expected a delay"),
            ("error_handling:\n  strategy: dlq\n  dlq:\n    stream: null\n", "must be a stream name"),
            ("error_handling:\n  strategy: dlq\n  dlq:\n    stream: ~\n", "must be a stream name"),
            ("error_handling:\n  strategy: dlq\n  dlq:\n    stream: !env DLQ\n", "YAML tag !env"),
            ("error_handling:\n  strategy: dlq\n  dlq: {}\n", "missing field `stream`"),
            // Blocks the chosen strategy would never use
            ("error_handling:\n  strategy: drop\n  retry:\n    max_attempts: 2\n", "only used with strategy 'retry'"),
            ("error_handling:\n  strategy: dlq\n  retry:\n    max_attempts: 2\n", "only used with strategy 'retry'"),
            ("error_handling:\n  strategy: fail\n  dlq:\n    stream: E\n", "only used with strategy 'dlq' or 'retry'"),
            ("error_handling:\n  strategy: drop\n  dlq:\n    stream: E\n", "only used with strategy 'dlq' or 'retry'"),
            ("error_handling:\n  strategy: dlq\n  dlq:\n    stream: E\n    fallback_retry:\n      max_attempts: 2\n", "fallback_strategy 'retry'"),
            // A connection key that clashes with a key the block writes
            ("error.strategy: fail\nerror_handling:\n  strategy: drop\n", "set more than once"),
        ];
        for (yaml, expected) in cases {
            let err = source_error_properties(yaml).expect_err(yaml);
            assert!(
                err.contains(expected),
                "{yaml}: expected '{expected}', got: {err}"
            );
        }
    }

    #[test]
    fn test_source_error_handling_serde_round_trip() {
        let yaml = "strategy: retry\nlog_level: debug\nretry:\n  max_attempts: 2\n  backoff: exponential\n  initial_delay: 100ms\n  max_delay: 1s\ndlq:\n  stream: E\n  fallback_strategy: fail\n";
        let parsed: SourceErrorHandling = serde_yaml::from_str(yaml).unwrap();
        let reparsed: SourceErrorHandling =
            serde_yaml::from_str(&serde_yaml::to_string(&parsed).unwrap()).unwrap();
        assert_eq!(parsed, reparsed);
        assert_eq!(
            reparsed.retry.unwrap().initial_delay,
            Some(Duration::from_millis(100))
        );
    }

    /// A bare whole number delay is milliseconds, as in the `error.*` properties.
    /// Reading from a `serde_yaml::Value` (merge keys, `--set`) must accept it too.
    #[test]
    fn test_source_error_handling_bare_number_delay_is_milliseconds() {
        let yaml = "strategy: retry\nretry:\n  initial_delay: 500\n  max_delay: 750ms\n";
        let from_text: SourceErrorHandling = serde_yaml::from_str(yaml).unwrap();
        let value: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
        let from_value: SourceErrorHandling = serde_yaml::from_value(value).unwrap();
        assert_eq!(from_text, from_value);

        let retry = from_text.retry.unwrap();
        assert_eq!(retry.initial_delay, Some(Duration::from_millis(500)));
        assert_eq!(retry.max_delay, Some(Duration::from_millis(750)));
    }

    /// Ties the keys written here to the runtime readers. Every written key is one
    /// sources accept, and parsing them gives back each YAML value. A renamed key
    /// on either side would otherwise leave the YAML setting quietly unused.
    #[test]
    fn test_source_error_handling_keys_round_trip_through_runtime_parser() {
        use crate::core::error::source_support::SOURCE_ERROR_PARAMETERS;
        use crate::core::error::{
            BackoffStrategy, DlqFallbackStrategy, ErrorConfigBuilder, ErrorStrategy, LogLevel,
            DLQ_FALLBACK_RETRY_KEY_PREFIX,
        };

        let retry_props = source_error_properties(
            "error_handling:\n  strategy: retry\n  log_level: debug\n  retry:\n    max_attempts: 7\n    backoff: linear\n    initial_delay: 150ms\n    max_delay: 9s\n",
        )
        .unwrap();
        let dlq_props = source_error_properties(
            "error_handling:\n  strategy: dlq\n  dlq:\n    stream: OrderErrors\n    fallback_strategy: retry\n    fallback_retry:\n      max_attempts: 4\n      backoff: fixed\n      initial_delay: 250ms\n      max_delay: 2s\n",
        )
        .unwrap();

        for key in retry_props.keys().chain(dlq_props.keys()) {
            assert!(
                key == "file.path"
                    || SOURCE_ERROR_PARAMETERS.contains(&key.as_str())
                    || key.starts_with(&format!("{DLQ_FALLBACK_RETRY_KEY_PREFIX}.")),
                "sources do not accept the written key '{key}'"
            );
        }

        let config = ErrorConfigBuilder::from_properties(&retry_props)
            .build()
            .unwrap()
            .unwrap();
        assert_eq!(config.strategy, ErrorStrategy::Retry);
        assert_eq!(config.log_level, LogLevel::Debug);
        let retry = config.retry.unwrap();
        assert_eq!(retry.max_attempts, 7);
        assert_eq!(retry.backoff, BackoffStrategy::Linear);
        assert_eq!(retry.initial_delay, Duration::from_millis(150));
        assert_eq!(retry.max_delay, Duration::from_secs(9));

        let config = ErrorConfigBuilder::from_properties(&dlq_props)
            .build()
            .unwrap()
            .unwrap();
        assert_eq!(config.strategy, ErrorStrategy::Dlq);
        let dlq = config.dlq.unwrap();
        assert_eq!(dlq.stream, "OrderErrors");
        assert_eq!(dlq.fallback_strategy, DlqFallbackStrategy::Retry);
        let fallback = dlq.fallback_retry.unwrap();
        assert_eq!(fallback.max_attempts, 4);
        assert_eq!(fallback.backoff, BackoffStrategy::Fixed);
        assert_eq!(fallback.initial_delay, Duration::from_millis(250));
        assert_eq!(fallback.max_delay, Duration::from_secs(2));
    }
}
