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

//! Placeholder Extension Factory Implementations
//!
//! Placeholder factories that show how to write a source or sink factory. They do no
//! real work.
//!
//! ## Placeholder Factories (NOT for production)
//! - `ExampleSourceFactory`. Made up source that shows the factory pattern.
//! - `ExampleSinkFactory`. Made up sink that shows the factory pattern.
//!
//! ## Real Factories, in their own modules
//! - `KafkaSourceFactory` and `KafkaSinkFactory` in `kafka_source.rs` and `kafka_sink.rs`
//! - `RabbitMQSourceFactory` in `src/core/stream/input/source/rabbitmq_source.rs`
//! - `RabbitMQSinkFactory` in `src/core/stream/output/sink/rabbitmq_sink.rs`
//! - `TimerSourceFactory` in `src/core/extension/mod.rs`
//! - `LogSinkFactory` in `src/core/extension/mod.rs`
//!
//! ## Mapper Factories
//! All mapper factories are in `src/core/stream/mapper/factory.rs`:
//! - `JsonSourceMapperFactory`, `JsonSinkMapperFactory`
//! - `CsvSourceMapperFactory`, `CsvSinkMapperFactory`
//! - `BytesSourceMapperFactory`, `BytesSinkMapperFactory`

use crate::core::exception::EventFluxError;
use crate::core::extension::{SinkFactory, SourceFactory};
use crate::core::stream::input::source::Source;
use crate::core::stream::output::sink::Sink;
use std::collections::HashMap;

// ============================================================================
// Example Source Factory (Placeholder)
// ============================================================================

/// Checked, typed config of ExampleSourceFactory. Private to this module.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ExampleSourceConfig {
    bootstrap_servers: Vec<String>,
    topic: String,
    consumer_group: String,
    timeout_ms: u64,
}

impl ExampleSourceConfig {
    /// Check the raw config and turn it into typed values.
    ///
    /// `example.servers` and `example.topic` are required. `example.timeout` is in
    /// milliseconds and defaults to 30000. `example.consumer.group` defaults to
    /// `eventflux-<topic>`.
    fn parse(raw_config: &HashMap<String, String>) -> Result<Self, EventFluxError> {
        // 1. Check the required parameters are present
        let brokers_str = raw_config
            .get("example.servers")
            .ok_or_else(|| EventFluxError::missing_parameter("example.servers"))?;

        let topic = raw_config
            .get("example.topic")
            .ok_or_else(|| EventFluxError::missing_parameter("example.topic"))?;

        // 2. Split the comma separated broker list, dropping empty entries
        let bootstrap_servers: Vec<String> = brokers_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if bootstrap_servers.is_empty() {
            return Err(EventFluxError::configuration_with_key(
                "example.servers cannot be empty",
                "example.servers",
            ));
        }

        // 3. Parse the optional timeout
        let timeout_ms = raw_config
            .get("example.timeout")
            .map(|s| s.parse::<u64>())
            .transpose()
            .map_err(|_| {
                EventFluxError::invalid_parameter_with_details(
                    "example.timeout must be a valid integer",
                    "example.timeout",
                    "positive integer (milliseconds)",
                )
            })?
            .unwrap_or(30000);

        // 4. Read the consumer group, or build the default
        let consumer_group = raw_config
            .get("example.consumer.group")
            .cloned()
            .unwrap_or_else(|| format!("eventflux-{}", topic));

        // 5. Return the typed config
        Ok(ExampleSourceConfig {
            bootstrap_servers,
            topic: topic.clone(),
            consumer_group,
            timeout_ms,
        })
    }
}

/// Placeholder source that shows which trait methods a source has. Does nothing.
#[derive(Debug)]
struct ExampleSource {
    _topic: String,
    _bootstrap_servers: Vec<String>,
}

impl Source for ExampleSource {
    fn start(
        &mut self,
        _callback: std::sync::Arc<dyn crate::core::stream::input::source::SourceCallback>,
    ) {
        // Placeholder. A real source would:
        // 1. Read bytes from the external system
        // 2. Call callback.on_data(bytes)
        // 3. Leave parsing to the callback, which uses the SourceMapper
    }

    fn stop(&mut self) {
        // Placeholder. A real source would stop its worker here. It can embed a
        // SourceWorker, see docs/writing_extensions.md.
    }

    fn clone_box(&self) -> Box<dyn Source> {
        Box::new(ExampleSource {
            _topic: self._topic.clone(),
            _bootstrap_servers: self._bootstrap_servers.clone(),
        })
    }
}

/// Placeholder source factory that shows the factory metadata and how to check the
/// config before creating the source.
///
/// This is NOT for production. For real examples, see `KafkaSourceFactory` in
/// `kafka_source.rs` or `RabbitMQSourceFactory` in `rabbitmq_source.rs`.
#[derive(Debug, Clone)]
pub struct ExampleSourceFactory;

impl SourceFactory for ExampleSourceFactory {
    fn name(&self) -> &'static str {
        "example"
    }

    fn supported_formats(&self) -> &[&str] {
        &["json", "avro", "bytes"]
    }

    fn required_parameters(&self) -> &[&str] {
        &["example.servers", "example.topic"]
    }

    fn optional_parameters(&self) -> &[&str] {
        &["example.consumer.group", "example.timeout"]
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn Source>, EventFluxError> {
        // 1. Check and parse the configuration
        let parsed = ExampleSourceConfig::parse(config)?;

        // 2. Create the source. A real factory would:
        // - Create the client from the parsed config
        // - Test the connection and fail at once if it is down
        // - Return a source that is ready to start

        // 3. Return the source
        Ok(Box::new(ExampleSource {
            _topic: parsed.topic,
            _bootstrap_servers: parsed.bootstrap_servers,
        }))
    }

    fn clone_box(&self) -> Box<dyn SourceFactory> {
        Box::new(self.clone())
    }
}

// ============================================================================
// Example Sink Factory (Placeholder)
// ============================================================================

/// Checked, typed config of ExampleSinkFactory
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ExampleSinkConfig {
    url: String,
    method: String,
    headers: HashMap<String, String>,
    timeout_secs: u64,
}

impl ExampleSinkConfig {
    // `example.url` is required. `example.method` defaults to POST and is upper cased.
    // `example.timeout` is in seconds and defaults to 30. `example.headers` is not
    // parsed yet.
    fn parse(raw_config: &HashMap<String, String>) -> Result<Self, EventFluxError> {
        let url = raw_config
            .get("example.url")
            .ok_or_else(|| EventFluxError::missing_parameter("example.url"))?;

        let method = raw_config
            .get("example.method")
            .cloned()
            .unwrap_or_else(|| "POST".to_string());

        // Only these methods are allowed, in any case
        if !["GET", "POST", "PUT", "DELETE", "PATCH"].contains(&method.to_uppercase().as_str()) {
            return Err(EventFluxError::invalid_parameter_with_details(
                format!("Invalid example.method: {}", method),
                "example.method",
                "one of: GET, POST, PUT, DELETE, PATCH",
            ));
        }

        let timeout_secs = raw_config
            .get("example.timeout")
            .map(|s| s.parse::<u64>())
            .transpose()
            .map_err(|_| {
                EventFluxError::invalid_parameter_with_details(
                    "example.timeout must be a valid integer",
                    "example.timeout",
                    "positive integer (seconds)",
                )
            })?
            .unwrap_or(30);

        // Headers are not parsed yet
        let headers = HashMap::new(); // Always empty for now

        Ok(ExampleSinkConfig {
            url: url.clone(),
            method: method.to_uppercase(),
            headers,
            timeout_secs,
        })
    }
}

/// Placeholder sink that shows which trait methods a sink has. Drops every payload.
#[derive(Debug)]
struct ExampleSink {
    _url: String,
    _method: String,
}

impl Sink for ExampleSink {
    fn publish(&self, _payload: &[u8]) -> Result<(), EventFluxError> {
        // Placeholder. A real sink would send the payload to the external system.
        // See `HttpSinkFactory` in `http_sink.rs` for a real example.
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn Sink> {
        Box::new(ExampleSink {
            _url: self._url.clone(),
            _method: self._method.clone(),
        })
    }
}

/// Placeholder sink factory that shows the factory metadata and how to check the
/// config before creating the sink.
///
/// This is NOT for production. For real examples, see `HttpSinkFactory` in
/// `http_sink.rs` or `RabbitMQSinkFactory` in `rabbitmq_sink.rs`.
#[derive(Debug, Clone)]
pub struct ExampleSinkFactory;

impl SinkFactory for ExampleSinkFactory {
    fn name(&self) -> &'static str {
        "example"
    }

    fn supported_formats(&self) -> &[&str] {
        &["json", "xml", "text"]
    }

    fn required_parameters(&self) -> &[&str] {
        &["example.url"]
    }

    fn optional_parameters(&self) -> &[&str] {
        &["example.method", "example.headers", "example.timeout"]
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn Sink>, EventFluxError> {
        let parsed = ExampleSinkConfig::parse(config)?;

        Ok(Box::new(ExampleSink {
            _url: parsed.url,
            _method: parsed.method,
        }))
    }

    fn clone_box(&self) -> Box<dyn SinkFactory> {
        Box::new(self.clone())
    }
}

// Note: the JSON, CSV and bytes mapper factories live in src/core/stream/mapper/factory.rs
// and are re-exported through crate::core::extension, such as JsonSourceMapperFactory

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_example_source_config_parse() {
        let mut config = HashMap::new();
        config.insert("example.servers".to_string(), "localhost:9092".to_string());
        config.insert("example.topic".to_string(), "test-topic".to_string());

        let parsed = ExampleSourceConfig::parse(&config).unwrap();
        assert_eq!(parsed.bootstrap_servers, vec!["localhost:9092"]);
        assert_eq!(parsed.topic, "test-topic");
        assert_eq!(parsed.consumer_group, "eventflux-test-topic");
        assert_eq!(parsed.timeout_ms, 30000);
    }

    #[test]
    fn test_example_source_config_missing_required() {
        let config = HashMap::new();
        let result = ExampleSourceConfig::parse(&config);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            EventFluxError::InvalidParameter { .. }
        ));
    }

    #[test]
    fn test_example_source_config_empty_brokers() {
        let mut config = HashMap::new();
        config.insert("example.servers".to_string(), "".to_string());
        config.insert("example.topic".to_string(), "test".to_string());

        let result = ExampleSourceConfig::parse(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_example_factory_supported_formats() {
        let factory = ExampleSourceFactory;
        assert!(factory.supported_formats().contains(&"json"));
        assert!(factory.supported_formats().contains(&"avro"));
        assert!(!factory.supported_formats().contains(&"xml"));
    }

    #[test]
    fn test_example_sink_config_parse() {
        let mut config = HashMap::new();
        config.insert(
            "example.url".to_string(),
            "http://localhost:8080/api".to_string(),
        );
        config.insert("example.method".to_string(), "POST".to_string());

        let parsed = ExampleSinkConfig::parse(&config).unwrap();
        assert_eq!(parsed.url, "http://localhost:8080/api");
        assert_eq!(parsed.method, "POST");
        assert_eq!(parsed.timeout_secs, 30);
    }

    #[test]
    fn test_example_sink_config_invalid_method() {
        let mut config = HashMap::new();
        config.insert(
            "example.url".to_string(),
            "http://localhost:8080".to_string(),
        );
        config.insert("example.method".to_string(), "INVALID".to_string());

        let result = ExampleSinkConfig::parse(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_factory_create_initialized() {
        let factory = ExampleSourceFactory;
        let mut config = HashMap::new();
        config.insert("example.servers".to_string(), "localhost:9092".to_string());
        config.insert("example.topic".to_string(), "test".to_string());

        let result = factory.create_initialized(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_factory_create_initialized_missing_params() {
        let factory = ExampleSourceFactory;
        let config = HashMap::new();

        let result = factory.create_initialized(&config);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Missing required parameter"));
    }
}
