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

//! Configuration Type Definitions
//!
//! This module defines all configuration structures for EventFlux Rust,
//! following industry-standard practices for configuration management.

pub mod application_config;
pub mod distributed_config;
pub mod eventflux_config;
pub mod global_config;
pub mod metadata;

// Re-export all configuration types
pub use eventflux_config::EventFluxConfig;
pub use metadata::ConfigMetadata;

// Application config re-exports (avoid conflicts)
pub use application_config::{
    AggregationConfig, ApplicationConfig, CompressionType as AppCompressionType, DefinitionConfig,
    DeliveryGuarantee, ErrorHandlingConfig, FunctionConfig,
    MonitoringConfig as AppMonitoringConfig, PersistenceConfig, QueryConfig, SchemaConfig,
    SinkConfig, SourceConfig, StreamConfig, TableConfig, TriggerConfig, WindowConfig,
};

// Global config re-exports (avoid conflicts)
pub use global_config::{
    AuditConfig, AuditDestination, AuditLevel, AuthenticationConfig, AuthenticationMethod,
    AuthorizationConfig, AuthorizationPolicy, AuthorizationProvider, BackpressureStrategy,
    CpuConfig, EventFluxGlobalConfig, ExtensionConfig, ExtensionSecurityConfig, ExtensionsConfig,
    GcStrategy, GlobalPersistenceConfig, LogFormat, LogLevel as GlobalLogLevel, LogOutput,
    LoggingConfig, MemoryConfig, MetricsConfig, MetricsProvider, NetworkConfig,
    ObservabilityConfig, PerformanceConfig, PersistenceBackendType, PersistenceCompression,
    ResourceConfig, RuntimeConfig, RuntimeMode, SecurityConfig, StorageConfig,
    TlsConfig as GlobalTlsConfig, TracingConfig, TracingProvider,
};

// Distributed config re-exports
pub use distributed_config::{
    AcknowledgmentLevel, ClusterConfig, CompressionType as DistCompressionType, ConsensusLevel,
    ConsumerConfig, CoordinationConfig, CoordinationImplementation, DistributedConfig,
    MessageBrokerConfig, MessageBrokerImplementation, NodeCapabilities, NodeConfig,
    PartitionHandling, ProducerConfig, ResourceLimits, ServiceDiscoveryConfig,
    ServiceDiscoveryMethod, StateBackendConfig, StateBackendImplementation, TransportConfig,
    TransportImplementation,
};

use serde::Deserialize;
use std::time::Duration;

// Utility function for duration parsing from strings
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    // Support formats like "30s", "5m", "1h", "2d"
    let s = s.trim();
    if s.is_empty() {
        return Err("Empty duration string".to_string());
    }

    let (number_part, unit_part) = if let Some(pos) = s.find(|c: char| c.is_alphabetic()) {
        (&s[..pos], &s[pos..])
    } else {
        return Err("Duration must include a unit (s, m, h, d)".to_string());
    };

    let number: f64 = number_part
        .parse()
        .map_err(|_| format!("Invalid number in duration: {}", number_part))?;

    let multiplier = match unit_part.to_lowercase().as_str() {
        "s" | "sec" | "second" | "seconds" => 1.0,
        "m" | "min" | "minute" | "minutes" => 60.0,
        "h" | "hr" | "hour" | "hours" => 3600.0,
        "d" | "day" | "days" => 86400.0,
        _ => return Err(format!("Unknown duration unit: {}", unit_part)),
    };

    let seconds = number * multiplier;
    Ok(Duration::from_secs_f64(seconds))
}

// Custom serde module for Duration parsing from strings
pub mod duration_serde {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let seconds = duration.as_secs();
        let formatted = if seconds % 86400 == 0 {
            format!("{}d", seconds / 86400)
        } else if seconds % 3600 == 0 {
            format!("{}h", seconds / 3600)
        } else if seconds % 60 == 0 {
            format!("{}m", seconds / 60)
        } else {
            format!("{}s", seconds)
        };
        serializer.serialize_str(&formatted)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_duration(&s).map_err(serde::de::Error::custom)
    }
}

// Optional Duration serde module
pub mod optional_duration_serde {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(duration: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match duration {
            Some(d) => duration_serde::serialize(d, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt = Option::<String>::deserialize(deserializer)?;
        match opt {
            Some(s) => parse_duration(&s)
                .map(Some)
                .map_err(serde::de::Error::custom),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("2d").unwrap(), Duration::from_secs(172800));
        assert_eq!(parse_duration("1.5h").unwrap(), Duration::from_secs(5400));

        assert!(parse_duration("").is_err());
        assert!(parse_duration("30").is_err());
        assert!(parse_duration("30x").is_err());
        assert!(parse_duration("abc").is_err());
    }

    #[test]
    fn test_duration_serde() {
        use serde_json;

        let duration = Duration::from_secs(3600);
        let serialized = serde_json::to_string(&duration).unwrap();
        // Note: This test would work with a wrapper struct that uses the serde module
    }
}
