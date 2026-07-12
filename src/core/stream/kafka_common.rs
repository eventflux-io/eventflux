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

//! Configuration and validation shared by the Kafka source and sink.
//!
//! Broker connection, security (SASL/SSL), the `kafka.rdkafka.*` passthrough,
//! and topic-existence validation are identical on both sides — this module
//! keeps them in one place so they cannot drift.

use crate::core::exception::EventFluxError;
use rdkafka::client::{Client, ClientContext};
use rdkafka::config::ClientConfig;
use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;

/// Parse an optional property, falling back to `default` when absent.
pub fn parse_or<T: FromStr>(
    properties: &HashMap<String, String>,
    key: &str,
    default: T,
) -> Result<T, String>
where
    T::Err: Display,
{
    match properties.get(key) {
        Some(v) => v.parse().map_err(|e| format!("Invalid {key}: {e}")),
        None => Ok(default),
    }
}

/// Broker connection + security settings shared by source and sink configs
#[derive(Debug, Clone)]
pub struct KafkaConnectionConfig {
    /// Comma-separated broker list (`host1:9092,host2:9092`)
    pub bootstrap_servers: String,
    /// `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl`
    pub security_protocol: Option<String>,
    /// `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    /// CA certificate path
    pub ssl_ca_location: Option<String>,
    /// Verbatim librdkafka overrides from `kafka.rdkafka.<prop>`
    pub rdkafka_overrides: Vec<(String, String)>,
}

impl KafkaConnectionConfig {
    /// Parse the connection/security properties.
    ///
    /// `reserved` lists librdkafka keys owned by the caller's first-class
    /// options — passing them through `kafka.rdkafka.*` is rejected to avoid
    /// two sources of truth for the same knob.
    pub fn from_properties(
        properties: &HashMap<String, String>,
        reserved: &[&str],
    ) -> Result<Self, String> {
        let bootstrap_servers = properties
            .get("kafka.bootstrap.servers")
            .ok_or("Missing required property: kafka.bootstrap.servers")?
            .clone();
        if bootstrap_servers.trim().is_empty() {
            return Err("kafka.bootstrap.servers cannot be empty".to_string());
        }

        let security_protocol = properties.get("kafka.security.protocol").cloned();
        let sasl_mechanism = properties.get("kafka.sasl.mechanism").cloned();
        let sasl_username = properties.get("kafka.sasl.username").cloned();
        let sasl_password = properties.get("kafka.sasl.password").cloned();
        let ssl_ca_location = properties.get("kafka.ssl.ca.location").cloned();

        // SASL credentials only make sense with a SASL security protocol.
        // librdkafka accepts protocol names case-insensitively (Kafka
        // convention is uppercase SASL_SSL), so compare accordingly.
        if (sasl_username.is_some() || sasl_password.is_some() || sasl_mechanism.is_some())
            && !security_protocol
                .as_deref()
                .is_some_and(|p| p.to_ascii_lowercase().starts_with("sasl_"))
        {
            return Err(
                "kafka.sasl.* settings require kafka.security.protocol to be \
                 'sasl_plaintext' or 'sasl_ssl'"
                    .to_string(),
            );
        }

        let mut rdkafka_overrides = Vec::new();
        for (key, value) in properties {
            if let Some(prop) = key.strip_prefix("kafka.rdkafka.") {
                if reserved.contains(&prop) {
                    return Err(format!(
                        "kafka.rdkafka.{prop} is reserved — use the first-class kafka.* option instead"
                    ));
                }
                rdkafka_overrides.push((prop.to_string(), value.clone()));
            }
        }

        Ok(Self {
            bootstrap_servers,
            security_protocol,
            sasl_mechanism,
            sasl_username,
            sasl_password,
            ssl_ca_location,
            rdkafka_overrides,
        })
    }

    /// Apply broker, security, and override settings to a client config
    pub fn apply_to(&self, config: &mut ClientConfig) {
        config.set("bootstrap.servers", &self.bootstrap_servers);

        if let Some(v) = &self.security_protocol {
            config.set("security.protocol", v);
        }
        if let Some(v) = &self.sasl_mechanism {
            config.set("sasl.mechanism", v);
        }
        if let Some(v) = &self.sasl_username {
            config.set("sasl.username", v);
        }
        if let Some(v) = &self.sasl_password {
            config.set("sasl.password", v);
        }
        if let Some(v) = &self.ssl_ca_location {
            config.set("ssl.ca.location", v);
        }
        for (key, value) in &self.rdkafka_overrides {
            config.set(key, value);
        }
    }
}

/// Fail-fast topic validation used by both connectors at startup.
///
/// One metadata round-trip regardless of topic count: a single topic is
/// fetched directly; multiple topics reuse one full-metadata response.
pub fn validate_topics_exist<C: ClientContext>(
    client: &Client<C>,
    brokers: &str,
    topics: &[&str],
) -> Result<(), EventFluxError> {
    let fetch_topic = if topics.len() == 1 {
        Some(topics[0])
    } else {
        None
    };
    let metadata = client
        .fetch_metadata(fetch_topic, Duration::from_secs(10))
        .map_err(|e| EventFluxError::ConnectionUnavailable {
            message: format!("Failed to reach Kafka brokers at '{brokers}': {e}"),
            source: Some(Box::new(e)),
        })?;

    for topic in topics {
        let exists = metadata
            .topics()
            .iter()
            .any(|t| t.name() == *topic && t.error().is_none() && !t.partitions().is_empty());
        if !exists {
            return Err(EventFluxError::configuration(format!(
                "Kafka topic '{topic}' does not exist (brokers: '{brokers}')"
            )));
        }
    }

    Ok(())
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
        props
    }

    #[test]
    fn test_parse_or_default_and_value() {
        let mut props = HashMap::new();
        assert_eq!(parse_or(&props, "k", 5u64).unwrap(), 5);

        props.insert("k".to_string(), "9".to_string());
        assert_eq!(parse_or(&props, "k", 5u64).unwrap(), 9);

        props.insert("k".to_string(), "no".to_string());
        assert!(parse_or(&props, "k", 5u64)
            .unwrap_err()
            .contains("Invalid k"));
    }

    #[test]
    fn test_connection_missing_bootstrap_servers() {
        let result = KafkaConnectionConfig::from_properties(&HashMap::new(), &[]);
        assert!(result.unwrap_err().contains("kafka.bootstrap.servers"));
    }

    #[test]
    fn test_connection_empty_bootstrap_servers() {
        let mut props = HashMap::new();
        props.insert("kafka.bootstrap.servers".to_string(), "  ".to_string());
        let result = KafkaConnectionConfig::from_properties(&props, &[]);
        assert!(result.unwrap_err().contains("cannot be empty"));
    }

    #[test]
    fn test_connection_sasl_requires_sasl_protocol() {
        let mut props = base_props();
        props.insert("kafka.sasl.username".to_string(), "user".to_string());

        let result = KafkaConnectionConfig::from_properties(&props, &[]);
        assert!(result.unwrap_err().contains("kafka.security.protocol"));
    }

    #[test]
    fn test_connection_sasl_accepted_with_sasl_protocol() {
        let mut props = base_props();
        props.insert(
            "kafka.security.protocol".to_string(),
            "sasl_ssl".to_string(),
        );
        props.insert("kafka.sasl.mechanism".to_string(), "PLAIN".to_string());
        props.insert("kafka.sasl.username".to_string(), "user".to_string());
        props.insert("kafka.sasl.password".to_string(), "secret".to_string());

        let config = KafkaConnectionConfig::from_properties(&props, &[]).unwrap();
        assert_eq!(config.security_protocol, Some("sasl_ssl".to_string()));
        assert_eq!(config.sasl_mechanism, Some("PLAIN".to_string()));
    }

    #[test]
    fn test_connection_sasl_protocol_case_insensitive() {
        // Kafka convention is uppercase; librdkafka accepts both
        let mut props = base_props();
        props.insert(
            "kafka.security.protocol".to_string(),
            "SASL_SSL".to_string(),
        );
        props.insert("kafka.sasl.username".to_string(), "user".to_string());

        assert!(KafkaConnectionConfig::from_properties(&props, &[]).is_ok());
    }

    #[test]
    fn test_connection_rdkafka_passthrough_and_reserved() {
        let mut props = base_props();
        props.insert(
            "kafka.rdkafka.fetch.min.bytes".to_string(),
            "1024".to_string(),
        );
        let config = KafkaConnectionConfig::from_properties(&props, &["acks"]).unwrap();
        assert_eq!(
            config.rdkafka_overrides,
            vec![("fetch.min.bytes".to_string(), "1024".to_string())]
        );

        props.insert("kafka.rdkafka.acks".to_string(), "0".to_string());
        let result = KafkaConnectionConfig::from_properties(&props, &["acks"]);
        assert!(result.unwrap_err().contains("reserved"));
    }

    #[test]
    fn test_apply_to_sets_connection_keys() {
        let mut props = base_props();
        props.insert(
            "kafka.rdkafka.fetch.min.bytes".to_string(),
            "1024".to_string(),
        );
        let connection = KafkaConnectionConfig::from_properties(&props, &[]).unwrap();

        let mut client_config = ClientConfig::new();
        connection.apply_to(&mut client_config);
        assert_eq!(
            client_config.get("bootstrap.servers"),
            Some("localhost:9092")
        );
        assert_eq!(client_config.get("fetch.min.bytes"), Some("1024"));
    }
}
