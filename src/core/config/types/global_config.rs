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

//! Global EventFlux Configuration Types
//!
//! Defines global runtime configuration for EventFlux applications.

use super::duration_serde;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Global EventFlux runtime configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventFluxGlobalConfig {
    /// Application configuration (name, async mode, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub application: Option<ApplicationGlobalConfig>,

    /// Runtime configuration
    pub runtime: RuntimeConfig,

    /// Global persistence backend configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistence: Option<GlobalPersistenceConfig>,

    /// Distributed processing configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distributed: Option<DistributedConfig>,

    /// Security configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<SecurityConfig>,

    /// Observability configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observability: Option<ObservabilityConfig>,

    /// Monitoring configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub monitoring: Option<crate::core::config::monitoring::MonitoringConfig>,

    /// Extensions configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Option<ExtensionsConfig>,
}

impl Default for EventFluxGlobalConfig {
    fn default() -> Self {
        Self {
            application: None,
            runtime: RuntimeConfig::default(),
            persistence: None,
            distributed: None,
            security: None,
            observability: None,
            monitoring: None,
            extensions: None,
        }
    }
}

impl EventFluxGlobalConfig {
    /// Merge another global configuration into this one
    ///
    /// The other configuration takes precedence for conflicting values.
    pub fn merge(&mut self, other: EventFluxGlobalConfig) {
        // Merge application configuration
        if other.application.is_some() {
            match (&mut self.application, other.application) {
                (Some(ref mut self_app), Some(other_app)) => {
                    self_app.merge(other_app);
                }
                (self_app, Some(other_app)) => {
                    *self_app = Some(other_app);
                }
                _ => {}
            }
        }

        // Deep merge runtime configuration
        self.runtime.merge(other.runtime);

        // Merge persistence configuration
        if other.persistence.is_some() {
            match (&mut self.persistence, other.persistence) {
                (Some(ref mut self_pers), Some(other_pers)) => {
                    self_pers.merge(other_pers);
                }
                (self_pers, Some(other_pers)) => {
                    *self_pers = Some(other_pers);
                }
                _ => {}
            }
        }

        // For optional configurations, other takes precedence if present
        if other.distributed.is_some() {
            match (&mut self.distributed, other.distributed) {
                (Some(ref mut self_dist), Some(other_dist)) => {
                    self_dist.merge(other_dist);
                }
                (self_dist, Some(other_dist)) => {
                    *self_dist = Some(other_dist);
                }
                _ => {}
            }
        }

        if other.security.is_some() {
            match (&mut self.security, other.security) {
                (Some(ref mut self_sec), Some(other_sec)) => {
                    self_sec.merge(other_sec);
                }
                (self_sec, Some(other_sec)) => {
                    *self_sec = Some(other_sec);
                }
                _ => {}
            }
        }

        if other.observability.is_some() {
            match (&mut self.observability, other.observability) {
                (Some(ref mut self_obs), Some(other_obs)) => {
                    self_obs.merge(other_obs);
                }
                (self_obs, Some(other_obs)) => {
                    *self_obs = Some(other_obs);
                }
                _ => {}
            }
        }

        if other.monitoring.is_some() {
            match (&mut self.monitoring, other.monitoring) {
                (Some(ref mut self_mon), Some(other_mon)) => {
                    self_mon.merge(other_mon);
                }
                (self_mon, Some(other_mon)) => {
                    *self_mon = Some(other_mon);
                }
                _ => {}
            }
        }

        if other.extensions.is_some() {
            match (&mut self.extensions, other.extensions) {
                (Some(ref mut self_ext), Some(other_ext)) => {
                    self_ext.merge(other_ext);
                }
                (self_ext, Some(other_ext)) => {
                    *self_ext = Some(other_ext);
                }
                _ => {}
            }
        }
    }
}

/// Application-level global configuration
///
/// Configures application-wide settings like application name and default async mode.
/// Used to replace `@app` annotations in the old EventFluxQL syntax.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ApplicationGlobalConfig {
    /// Application name used for runtime identification and persistence
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Default async mode for all streams (unless overridden by stream-level config)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub async_default: Option<bool>,
}

impl Default for ApplicationGlobalConfig {
    fn default() -> Self {
        Self {
            name: None,
            async_default: None,
        }
    }
}

impl ApplicationGlobalConfig {
    /// Merge another application configuration into this one
    ///
    /// The other configuration takes precedence for conflicting values.
    pub fn merge(&mut self, other: ApplicationGlobalConfig) {
        // Other's values take precedence if present
        if other.name.is_some() {
            self.name = other.name;
        }
        if other.async_default.is_some() {
            self.async_default = other.async_default;
        }
    }
}

/// Runtime mode selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeMode {
    /// Single-node mode (default, zero overhead)
    SingleNode,

    /// Distributed mode with cluster coordination
    Distributed,

    /// Hybrid mode (local processing with distributed state)
    Hybrid,
}

impl Default for RuntimeMode {
    fn default() -> Self {
        Self::SingleNode
    }
}

/// Runtime performance configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeConfig {
    /// Runtime mode selection
    pub mode: RuntimeMode,

    /// Performance configuration
    pub performance: PerformanceConfig,

    /// Resource limits
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceConfig>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            mode: RuntimeMode::SingleNode,
            performance: PerformanceConfig::default(),
            resources: None,
        }
    }
}

impl RuntimeConfig {
    /// Merge another runtime configuration into this one
    pub fn merge(&mut self, other: RuntimeConfig) {
        // Other's mode takes precedence
        self.mode = other.mode;

        // Deep merge performance configuration
        self.performance.merge(other.performance);

        // Other's resources take precedence if present
        if other.resources.is_some() {
            match (&mut self.resources, other.resources) {
                (Some(ref mut self_res), Some(other_res)) => {
                    self_res.merge(other_res);
                }
                (self_res, Some(other_res)) => {
                    *self_res = Some(other_res);
                }
                _ => {}
            }
        }
    }
}

/// Performance tuning configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PerformanceConfig {
    /// Number of threads in the thread pool
    pub thread_pool_size: usize,

    /// Size of event buffer for each stream
    pub event_buffer_size: usize,

    /// Enable batch processing for improved throughput
    #[serde(default = "default_true")]
    pub batch_processing: bool,

    /// Batch size for batch processing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<usize>,

    /// Enable async processing mode
    #[serde(default)]
    pub async_processing: bool,

    /// Backpressure strategy
    #[serde(default)]
    pub backpressure_strategy: BackpressureStrategy,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            thread_pool_size: num_cpus::get(),
            event_buffer_size: 1000000,
            batch_processing: true,
            batch_size: None,
            async_processing: false,
            backpressure_strategy: BackpressureStrategy::Block,
        }
    }
}

impl PerformanceConfig {
    /// Merge another performance configuration into this one
    pub fn merge(&mut self, other: PerformanceConfig) {
        // Other values take precedence for all fields
        self.thread_pool_size = other.thread_pool_size;
        self.event_buffer_size = other.event_buffer_size;
        self.batch_processing = other.batch_processing;
        self.async_processing = other.async_processing;
        self.backpressure_strategy = other.backpressure_strategy;

        // Other's batch size takes precedence if present
        if other.batch_size.is_some() {
            self.batch_size = other.batch_size;
        }
    }
}

/// Backpressure handling strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BackpressureStrategy {
    /// Drop new events when buffer is full
    Drop,

    /// Block producers when buffer is full
    Block,

    /// Use exponential backoff
    ExponentialBackoff,
}

impl Default for BackpressureStrategy {
    fn default() -> Self {
        Self::Block
    }
}

/// Resource limits for the runtime engine
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResourceConfig {
    /// Memory limits
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<MemoryConfig>,

    /// CPU limits
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu: Option<CpuConfig>,

    /// Network limits
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<NetworkConfig>,

    /// Storage limits
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<StorageConfig>,
}

impl ResourceConfig {
    /// Merge another resource configuration into this one
    pub fn merge(&mut self, other: ResourceConfig) {
        // Merge memory configuration
        if other.memory.is_some() {
            match (&mut self.memory, other.memory) {
                (Some(ref mut self_mem), Some(other_mem)) => {
                    self_mem.merge(other_mem);
                }
                (self_mem, Some(other_mem)) => {
                    *self_mem = Some(other_mem);
                }
                _ => {}
            }
        }

        // Merge CPU configuration
        if other.cpu.is_some() {
            match (&mut self.cpu, other.cpu) {
                (Some(ref mut self_cpu), Some(other_cpu)) => {
                    self_cpu.merge(other_cpu);
                }
                (self_cpu, Some(other_cpu)) => {
                    *self_cpu = Some(other_cpu);
                }
                _ => {}
            }
        }

        // Merge network configuration
        if other.network.is_some() {
            match (&mut self.network, other.network) {
                (Some(ref mut self_net), Some(other_net)) => {
                    self_net.merge(other_net);
                }
                (self_net, Some(other_net)) => {
                    *self_net = Some(other_net);
                }
                _ => {}
            }
        }

        // Merge storage configuration
        if other.storage.is_some() {
            match (&mut self.storage, other.storage) {
                (Some(ref mut self_stor), Some(other_stor)) => {
                    self_stor.merge(other_stor);
                }
                (self_stor, Some(other_stor)) => {
                    *self_stor = Some(other_stor);
                }
                _ => {}
            }
        }
    }
}

/// Memory resource configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryConfig {
    /// Maximum heap size (e.g., "2Gi", "512Mi")
    pub max_heap: String,

    /// Initial heap size (e.g., "1Gi", "256Mi")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_heap: Option<String>,

    /// Memory management strategy
    #[serde(default)]
    pub gc_strategy: GcStrategy,
}

impl MemoryConfig {
    /// Merge another memory configuration into this one
    pub fn merge(&mut self, other: MemoryConfig) {
        // Other values take precedence
        self.max_heap = other.max_heap;
        self.gc_strategy = other.gc_strategy;

        // Other's initial heap takes precedence if present
        if other.initial_heap.is_some() {
            self.initial_heap = other.initial_heap;
        }
    }
}

/// Garbage collection strategy (Rust memory management equivalent)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GcStrategy {
    /// Adaptive memory management
    Adaptive,

    /// Conservative memory management
    Conservative,

    /// Aggressive memory cleanup
    Aggressive,
}

impl Default for GcStrategy {
    fn default() -> Self {
        Self::Adaptive
    }
}

/// CPU resource configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CpuConfig {
    /// Maximum CPU cores (e.g., "2", "500m")
    pub max_cpu: String,

    /// CPU requests (e.g., "1", "100m")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requests: Option<String>,
}

impl CpuConfig {
    /// Merge another CPU configuration into this one
    pub fn merge(&mut self, other: CpuConfig) {
        // Other values take precedence
        self.max_cpu = other.max_cpu;

        // Other's requests take precedence if present
        if other.requests.is_some() {
            self.requests = other.requests;
        }
    }
}

/// Network resource configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NetworkConfig {
    /// Maximum network bandwidth (bytes/sec)
    pub max_bandwidth: u64,

    /// Connection limits
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<usize>,
}

impl NetworkConfig {
    /// Merge another network configuration into this one
    pub fn merge(&mut self, other: NetworkConfig) {
        // Other values take precedence
        self.max_bandwidth = other.max_bandwidth;

        // Other's max connections take precedence if present
        if other.max_connections.is_some() {
            self.max_connections = other.max_connections;
        }
    }
}

/// Storage resource configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageConfig {
    /// Maximum storage capacity
    pub max_capacity: String,

    /// Storage class
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
}

impl StorageConfig {
    /// Merge another storage configuration into this one
    pub fn merge(&mut self, other: StorageConfig) {
        // Other values take precedence
        self.max_capacity = other.max_capacity;

        // Other's storage class takes precedence if present
        if other.storage_class.is_some() {
            self.storage_class = other.storage_class;
        }
    }
}

/// Security configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SecurityConfig {
    /// Authentication configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authentication: Option<AuthenticationConfig>,

    /// Authorization configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorization: Option<AuthorizationConfig>,

    /// TLS configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfig>,

    /// Audit configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audit: Option<AuditConfig>,
}

impl SecurityConfig {
    /// Merge another security configuration into this one
    pub fn merge(&mut self, other: SecurityConfig) {
        // For simplicity, other takes precedence for all optional configurations
        if other.authentication.is_some() {
            self.authentication = other.authentication;
        }

        if other.authorization.is_some() {
            self.authorization = other.authorization;
        }

        if other.tls.is_some() {
            self.tls = other.tls;
        }

        if other.audit.is_some() {
            self.audit = other.audit;
        }
    }
}

/// Authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuthenticationConfig {
    /// Authentication method
    pub method: AuthenticationMethod,

    /// Provider-specific configuration
    #[serde(flatten)]
    pub provider_config: serde_yaml::Value,
}

/// Authentication methods
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuthenticationMethod {
    /// Mutual TLS authentication
    MutualTls,

    /// OAuth2/OIDC
    OAuth2,

    /// JWT tokens
    Jwt,

    /// API keys
    ApiKey,

    /// Basic authentication
    Basic,
}

/// Authorization configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuthorizationConfig {
    /// Enable authorization
    #[serde(default)]
    pub enabled: bool,

    /// Authorization provider
    pub provider: AuthorizationProvider,

    /// Default policy
    #[serde(default)]
    pub default_policy: AuthorizationPolicy,
}

/// Authorization providers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuthorizationProvider {
    /// Role-based access control
    Rbac,

    /// Attribute-based access control
    Abac,

    /// Open Policy Agent
    Opa,
}

/// Authorization policies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuthorizationPolicy {
    Allow,
    Deny,
}

impl Default for AuthorizationPolicy {
    fn default() -> Self {
        Self::Deny
    }
}

/// TLS configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TlsConfig {
    /// Enable TLS
    #[serde(default)]
    pub enabled: bool,

    /// Certificate file path
    pub cert_file: String,

    /// Private key file path
    pub key_file: String,

    /// CA certificate file path
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<String>,

    /// Verify peer certificates
    #[serde(default = "default_true")]
    pub verify_peer: bool,

    /// Minimum TLS version
    #[serde(default)]
    pub min_version: TlsVersion,
}

/// TLS versions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TlsVersion {
    #[serde(rename = "1.2")]
    Tls12,
    #[serde(rename = "1.3")]
    Tls13,
}

impl Default for TlsVersion {
    fn default() -> Self {
        Self::Tls12
    }
}

/// Audit configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuditConfig {
    /// Enable audit logging
    #[serde(default)]
    pub enabled: bool,

    /// Audit log level
    #[serde(default)]
    pub level: AuditLevel,

    /// Audit log destination
    pub destination: AuditDestination,
}

/// Audit log levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuditLevel {
    Minimal,
    Standard,
    Detailed,
}

impl Default for AuditLevel {
    fn default() -> Self {
        Self::Standard
    }
}

/// Audit log destinations
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuditDestination {
    File { path: String },
    Syslog,
    Http { url: String },
}

/// Observability configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ObservabilityConfig {
    /// Metrics configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<MetricsConfig>,

    /// Tracing configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracing: Option<TracingConfig>,

    /// Logging configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logging: Option<LoggingConfig>,
}

impl ObservabilityConfig {
    /// Merge another observability configuration into this one
    pub fn merge(&mut self, other: ObservabilityConfig) {
        // For simplicity, other takes precedence for all optional configurations
        if other.metrics.is_some() {
            self.metrics = other.metrics;
        }

        if other.tracing.is_some() {
            self.tracing = other.tracing;
        }

        if other.logging.is_some() {
            self.logging = other.logging;
        }
    }
}

/// Metrics configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricsConfig {
    /// Enable metrics collection
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Metrics provider
    #[serde(default)]
    pub provider: MetricsProvider,

    /// Metrics server port
    #[serde(default = "default_metrics_port")]
    pub port: u16,

    /// Metrics endpoint path
    #[serde(default = "default_metrics_path")]
    pub path: String,

    /// Collection interval
    #[serde(with = "duration_serde", default = "default_metrics_interval")]
    pub interval: Duration,

    /// Custom labels for all metrics
    #[serde(default)]
    pub labels: std::collections::HashMap<String, String>,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            provider: MetricsProvider::Prometheus,
            port: default_metrics_port(),
            path: default_metrics_path(),
            interval: default_metrics_interval(),
            labels: std::collections::HashMap::new(),
        }
    }
}

/// Metrics providers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricsProvider {
    Prometheus,
    StatsD,
    InfluxDB,
}

impl Default for MetricsProvider {
    fn default() -> Self {
        Self::Prometheus
    }
}

/// Tracing configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TracingConfig {
    /// Enable distributed tracing
    #[serde(default)]
    pub enabled: bool,

    /// Tracing provider
    pub provider: TracingProvider,

    /// Tracing endpoint
    pub endpoint: String,

    /// Sample rate (0.0 to 1.0)
    #[serde(default = "default_sample_rate")]
    pub sample_rate: f64,
}

/// Tracing providers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TracingProvider {
    Jaeger,
    Zipkin,
    OpenTelemetry,
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LoggingConfig {
    /// Log level
    #[serde(default)]
    pub level: LogLevel,

    /// Log format
    #[serde(default)]
    pub format: LogFormat,

    /// Log output destination
    #[serde(default)]
    pub output: LogOutput,

    /// Include correlation ID in logs
    #[serde(default = "default_true")]
    pub correlation_id: bool,

    /// Structured logging fields
    #[serde(default)]
    pub fields: std::collections::HashMap<String, String>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            format: LogFormat::Json,
            output: LogOutput::Stdout,
            correlation_id: true,
            fields: std::collections::HashMap::new(),
        }
    }
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

/// Log formats
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Json,
    Text,
    Pretty,
}

impl Default for LogFormat {
    fn default() -> Self {
        Self::Json
    }
}

/// Log output destinations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogOutput {
    Stdout,
    Stderr,
    File,
}

impl Default for LogOutput {
    fn default() -> Self {
        Self::Stdout
    }
}

/// Extensions configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExtensionsConfig {
    /// Directories to search for extensions
    #[serde(default)]
    pub directories: Vec<String>,

    /// Explicitly loaded extensions
    #[serde(default)]
    pub extensions: std::collections::HashMap<String, ExtensionConfig>,

    /// Extension security settings
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<ExtensionSecurityConfig>,
}

impl ExtensionsConfig {
    /// Merge another extensions configuration into this one
    pub fn merge(&mut self, other: ExtensionsConfig) {
        // Merge directories (other's directories are appended)
        self.directories.extend(other.directories);

        // Merge extensions (other takes precedence for conflicts)
        self.extensions.extend(other.extensions);

        // Other's security takes precedence if present
        if other.security.is_some() {
            self.security = other.security;
        }
    }
}

/// Individual extension configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExtensionConfig {
    /// Path to extension library
    pub path: String,

    /// Extension-specific configuration
    #[serde(default)]
    pub config: serde_yaml::Value,

    /// Enable/disable extension
    #[serde(default = "default_true")]
    pub enabled: bool,
}

/// Extension security configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExtensionSecurityConfig {
    /// Allow loading unsigned extensions
    #[serde(default)]
    pub allow_unsigned: bool,

    /// Trusted extension publishers
    #[serde(default)]
    pub trusted_publishers: Vec<String>,

    /// Extension sandboxing
    #[serde(default = "default_true")]
    pub sandboxing: bool,
}

// Helper functions for defaults
fn default_true() -> bool {
    true
}

fn default_metrics_port() -> u16 {
    9090
}

fn default_metrics_path() -> String {
    "/metrics".to_string()
}

fn default_metrics_interval() -> Duration {
    Duration::from_secs(10)
}

fn default_sample_rate() -> f64 {
    0.1
}

fn default_checkpoint_interval() -> Duration {
    Duration::from_secs(60)
}

// ============================================================================
// Global Persistence Configuration
// ============================================================================

/// Global persistence backend configuration
///
/// Configures the persistence backend for EventFlux engine state, including
/// checkpoints, window state, and aggregation state.
///
/// # Example YAML
///
/// ```yaml
/// eventflux:
///   persistence:
///     type: sqlite
///     path: ./eventflux.db
/// ```
///
/// Or for file-based:
///
/// ```yaml
/// eventflux:
///   persistence:
///     type: file
///     path: ./snapshots
/// ```
///
/// Or for Redis:
///
/// ```yaml
/// eventflux:
///   persistence:
///     type: redis
///     url: redis://localhost:6379
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GlobalPersistenceConfig {
    /// Persistence backend type
    #[serde(rename = "type")]
    pub backend_type: PersistenceBackendType,

    /// Path for file-based or SQLite persistence
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    /// URL for network-based persistence (Redis, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,

    /// Enable persistence (defaults to true when config is present)
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Checkpoint interval
    #[serde(with = "duration_serde", default = "default_checkpoint_interval")]
    pub checkpoint_interval: Duration,

    /// Enable compression for persisted state
    #[serde(default)]
    pub compression: PersistenceCompression,

    /// Additional backend-specific options
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub options: std::collections::HashMap<String, String>,
}

impl Default for GlobalPersistenceConfig {
    fn default() -> Self {
        Self {
            backend_type: PersistenceBackendType::File,
            path: None,
            url: None,
            enabled: true,
            checkpoint_interval: default_checkpoint_interval(),
            compression: PersistenceCompression::default(),
            options: std::collections::HashMap::new(),
        }
    }
}

impl GlobalPersistenceConfig {
    /// Create a file-based persistence configuration
    pub fn file(path: impl Into<String>) -> Self {
        Self {
            backend_type: PersistenceBackendType::File,
            path: Some(path.into()),
            ..Default::default()
        }
    }

    /// Create a SQLite-based persistence configuration
    pub fn sqlite(path: impl Into<String>) -> Self {
        Self {
            backend_type: PersistenceBackendType::Sqlite,
            path: Some(path.into()),
            ..Default::default()
        }
    }

    /// Create a Redis-based persistence configuration
    pub fn redis(url: impl Into<String>) -> Self {
        Self {
            backend_type: PersistenceBackendType::Redis,
            url: Some(url.into()),
            ..Default::default()
        }
    }

    /// Create an in-memory persistence configuration (no persistence)
    pub fn memory() -> Self {
        Self {
            backend_type: PersistenceBackendType::Memory,
            enabled: true,
            ..Default::default()
        }
    }

    /// Merge another persistence configuration into this one
    pub fn merge(&mut self, other: GlobalPersistenceConfig) {
        self.backend_type = other.backend_type;
        self.enabled = other.enabled;
        self.checkpoint_interval = other.checkpoint_interval;
        self.compression = other.compression;

        if other.path.is_some() {
            self.path = other.path;
        }
        if other.url.is_some() {
            self.url = other.url;
        }

        // Merge options
        self.options.extend(other.options);
    }
}

/// Persistence backend types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PersistenceBackendType {
    /// In-memory only (no persistence across restarts)
    Memory,

    /// File-based persistence (directory with snapshot files)
    File,

    /// SQLite database persistence
    Sqlite,

    /// Redis persistence
    Redis,
}

impl Default for PersistenceBackendType {
    fn default() -> Self {
        Self::File
    }
}

/// Compression options for persistence
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PersistenceCompression {
    /// No compression
    None,

    /// LZ4 compression (fast)
    Lz4,

    /// Zstd compression (better ratio)
    Zstd,
}

impl Default for PersistenceCompression {
    fn default() -> Self {
        Self::None
    }
}

// Import distributed config from separate module
use super::distributed_config::DistributedConfig;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_global_config_default() {
        let config = EventFluxGlobalConfig::default();
        assert_eq!(config.runtime.mode, RuntimeMode::SingleNode);
        assert_eq!(config.runtime.performance.thread_pool_size, num_cpus::get());
        assert!(config.distributed.is_none());
    }

    #[test]
    fn test_runtime_mode_serde() {
        let mode = RuntimeMode::Distributed;
        let yaml = serde_yaml::to_string(&mode).unwrap();
        assert_eq!(yaml.trim(), "distributed");

        let deserialized: RuntimeMode = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(deserialized, mode);
    }

    #[test]
    fn test_backpressure_strategy_serde() {
        let strategy = BackpressureStrategy::ExponentialBackoff;
        let yaml = serde_yaml::to_string(&strategy).unwrap();
        assert_eq!(yaml.trim(), "exponential-backoff");
    }

    #[test]
    fn test_performance_config() {
        let config = PerformanceConfig::default();
        assert!(config.batch_processing);
        assert_eq!(config.backpressure_strategy, BackpressureStrategy::Block);
    }

    #[test]
    fn test_metrics_config_default() {
        let config = MetricsConfig::default();
        assert!(config.enabled);
        assert_eq!(config.port, 9090);
        assert_eq!(config.path, "/metrics");
        assert_eq!(config.provider, MetricsProvider::Prometheus);
    }
}
