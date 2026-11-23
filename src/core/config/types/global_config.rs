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
//! Global runtime configuration shared by all EventFlux applications in a process.

use super::duration_serde;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Global EventFlux runtime configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct EventFluxGlobalConfig {
    /// Application settings, such as the name and the default async mode
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

impl EventFluxGlobalConfig {
    /// Merge `other` into this config. Values from `other` win.
    ///
    /// Each optional section is merged when both sides have it, taken from `other`
    /// when only `other` has it, and left alone otherwise. `runtime` is always merged,
    /// and [`RuntimeConfig::merge`] copies every scalar from `other`, so a default
    /// `other.runtime` resets values set earlier.
    pub fn merge(&mut self, other: EventFluxGlobalConfig) {
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

        self.runtime.merge(other.runtime);

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

/// Settings for the whole application, the `eventflux.application` section
///
/// Holds the application name and the default async mode. Takes the place of the
/// `@app` annotations of the old EventFluxQL syntax.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ApplicationGlobalConfig {
    /// Application name, used to identify the runtime and its persisted state
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Whether streams run asynchronously by default. A stream can override it.
    /// Treated as false when unset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub async_default: Option<bool>,
}

impl ApplicationGlobalConfig {
    /// Merge `other` into this config. Each field is replaced only when set in `other`.
    pub fn merge(&mut self, other: ApplicationGlobalConfig) {
        if other.name.is_some() {
            self.name = other.name;
        }
        if other.async_default.is_some() {
            self.async_default = other.async_default;
        }
    }
}

/// Runtime mode. Written in kebab case, such as `single-node`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeMode {
    /// One node with no cluster overhead. The default.
    SingleNode,

    /// Several nodes that coordinate as a cluster
    Distributed,

    /// Processing on the local node with state shared across nodes
    Hybrid,
}

impl Default for RuntimeMode {
    fn default() -> Self {
        Self::SingleNode
    }
}

/// Runtime mode, performance and resource settings
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeConfig {
    /// Runtime mode
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
    /// Merge `other` into this config.
    ///
    /// `mode` always comes from `other`. Performance is merged with
    /// [`PerformanceConfig::merge`]. Resources are merged when both sides have them and
    /// taken from `other` when only it has them.
    pub fn merge(&mut self, other: RuntimeConfig) {
        self.mode = other.mode;

        self.performance.merge(other.performance);

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
    /// Number of threads in the thread pool. `Default` uses the CPU count.
    pub thread_pool_size: usize,

    /// Event buffer size for each stream. `Default` uses 1,000,000.
    pub event_buffer_size: usize,

    /// Process events in batches for more throughput. Defaults to true.
    #[serde(default = "default_true")]
    pub batch_processing: bool,

    /// Batch size for batch processing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<usize>,

    /// Process events asynchronously. Defaults to false.
    #[serde(default)]
    pub async_processing: bool,

    /// Backpressure strategy. Defaults to `Block`.
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
    /// Merge `other` into this config.
    ///
    /// Every field except `batch_size` always comes from `other`, even when it only
    /// holds a default. `batch_size` is replaced only when set in `other`.
    pub fn merge(&mut self, other: PerformanceConfig) {
        self.thread_pool_size = other.thread_pool_size;
        self.event_buffer_size = other.event_buffer_size;
        self.batch_processing = other.batch_processing;
        self.async_processing = other.async_processing;
        self.backpressure_strategy = other.backpressure_strategy;

        if other.batch_size.is_some() {
            self.batch_size = other.batch_size;
        }
    }
}

/// What to do when a buffer is full. Defaults to `Block`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BackpressureStrategy {
    /// Drop new events while the buffer is full
    Drop,

    /// Make producers wait while the buffer is full
    Block,

    /// Retry with a delay that doubles each time
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
    /// Merge `other` into this config. Each section is merged when both sides have it,
    /// taken from `other` when only it has it, and left alone otherwise.
    pub fn merge(&mut self, other: ResourceConfig) {
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
    /// Maximum heap size, such as "2Gi" or "512Mi"
    pub max_heap: String,

    /// Initial heap size, such as "1Gi" or "256Mi"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_heap: Option<String>,

    /// Memory management strategy. Defaults to `Adaptive`.
    #[serde(default)]
    pub gc_strategy: GcStrategy,
}

impl MemoryConfig {
    /// Merge `other` into this config. `initial_heap` is replaced only when set in
    /// `other`. The other fields always come from `other`.
    pub fn merge(&mut self, other: MemoryConfig) {
        self.max_heap = other.max_heap;
        self.gc_strategy = other.gc_strategy;

        if other.initial_heap.is_some() {
            self.initial_heap = other.initial_heap;
        }
    }
}

/// How eagerly memory is cleaned up. Named after garbage collection settings, though
/// Rust has no garbage collector.
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
    /// Maximum CPU, such as "2" cores or "500m" millicores
    pub max_cpu: String,

    /// CPU requested, such as "1" or "100m"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requests: Option<String>,
}

impl CpuConfig {
    /// Merge `other` into this config. `requests` is replaced only when set in `other`.
    pub fn merge(&mut self, other: CpuConfig) {
        self.max_cpu = other.max_cpu;

        if other.requests.is_some() {
            self.requests = other.requests;
        }
    }
}

/// Network resource configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NetworkConfig {
    /// Maximum network bandwidth in bytes per second
    pub max_bandwidth: u64,

    /// Maximum number of connections
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<usize>,
}

impl NetworkConfig {
    /// Merge `other` into this config. `max_connections` is replaced only when set in
    /// `other`.
    pub fn merge(&mut self, other: NetworkConfig) {
        self.max_bandwidth = other.max_bandwidth;

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
    /// Merge `other` into this config. `storage_class` is replaced only when set in
    /// `other`.
    pub fn merge(&mut self, other: StorageConfig) {
        self.max_capacity = other.max_capacity;

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
    /// Merge `other` into this config. Each section is replaced whole when `other`
    /// has it. Sections are not merged field by field.
    pub fn merge(&mut self, other: SecurityConfig) {
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

    /// Settings for the chosen method. Holds every other key at the same YAML level.
    #[serde(flatten)]
    pub provider_config: serde_yaml::Value,
}

/// Authentication methods
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuthenticationMethod {
    /// Mutual TLS authentication
    MutualTls,

    /// OAuth2 or OIDC
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
    /// Turn authorization on. Defaults to false.
    #[serde(default)]
    pub enabled: bool,

    /// Authorization provider
    pub provider: AuthorizationProvider,

    /// Policy when no rule matches. Defaults to `Deny`.
    #[serde(default)]
    pub default_policy: AuthorizationPolicy,
}

/// Authorization providers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuthorizationProvider {
    /// Access control by role (RBAC)
    Rbac,

    /// Access control by attributes (ABAC)
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
    /// Turn TLS on. Defaults to false.
    #[serde(default)]
    pub enabled: bool,

    /// Certificate file path
    pub cert_file: String,

    /// Private key file path
    pub key_file: String,

    /// CA certificate file path
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<String>,

    /// Check the peer certificate. Defaults to true.
    #[serde(default = "default_true")]
    pub verify_peer: bool,

    /// Lowest TLS version allowed. Defaults to 1.2.
    #[serde(default)]
    pub min_version: TlsVersion,
}

/// TLS versions, written as "1.2" and "1.3"
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
    /// Turn audit logging on. Defaults to false.
    #[serde(default)]
    pub enabled: bool,

    /// Audit log level. Defaults to `Standard`.
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
    /// Merge `other` into this config. Each section is replaced whole when `other`
    /// has it. Sections are not merged field by field.
    pub fn merge(&mut self, other: ObservabilityConfig) {
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
    /// Collect metrics. Defaults to true.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Metrics provider. Defaults to `Prometheus`.
    #[serde(default)]
    pub provider: MetricsProvider,

    /// Metrics server port. Defaults to 9090.
    #[serde(default = "default_metrics_port")]
    pub port: u16,

    /// Metrics endpoint path. Defaults to "/metrics".
    #[serde(default = "default_metrics_path")]
    pub path: String,

    /// Time between collections. Defaults to 10 seconds.
    #[serde(with = "duration_serde", default = "default_metrics_interval")]
    pub interval: Duration,

    /// Labels added to every metric
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
    /// Turn tracing across services on. Defaults to false.
    #[serde(default)]
    pub enabled: bool,

    /// Tracing provider
    pub provider: TracingProvider,

    /// Tracing endpoint
    pub endpoint: String,

    /// Share of requests to trace, from 0.0 to 1.0. Defaults to 0.1.
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
    /// Log level. Defaults to `Info`.
    #[serde(default)]
    pub level: LogLevel,

    /// Log format. Defaults to `Json`.
    #[serde(default)]
    pub format: LogFormat,

    /// Log output destination. Defaults to `Stdout`.
    #[serde(default)]
    pub output: LogOutput,

    /// Include a correlation id in each log line. Defaults to true.
    #[serde(default = "default_true")]
    pub correlation_id: bool,

    /// Fields added to each structured log entry
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

    /// Extensions to load by name
    #[serde(default)]
    pub extensions: std::collections::HashMap<String, ExtensionConfig>,

    /// Extension security settings
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<ExtensionSecurityConfig>,
}

impl ExtensionsConfig {
    /// Merge `other` into this config.
    ///
    /// Directories from `other` are appended, so duplicates are kept. Extensions are
    /// added by name, and one with the same name is replaced. Security is replaced
    /// only when `other` has it.
    pub fn merge(&mut self, other: ExtensionsConfig) {
        self.directories.extend(other.directories);

        self.extensions.extend(other.extensions);

        if other.security.is_some() {
            self.security = other.security;
        }
    }
}

/// Individual extension configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExtensionConfig {
    /// Path to the extension library
    pub path: String,

    /// Settings passed to the extension
    #[serde(default)]
    pub config: serde_yaml::Value,

    /// Whether the extension is loaded. Defaults to true.
    #[serde(default = "default_true")]
    pub enabled: bool,
}

/// Extension security configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExtensionSecurityConfig {
    /// Allow loading unsigned extensions. Defaults to false.
    #[serde(default)]
    pub allow_unsigned: bool,

    /// Trusted extension publishers
    #[serde(default)]
    pub trusted_publishers: Vec<String>,

    /// Run extensions in a sandbox. Defaults to true.
    #[serde(default = "default_true")]
    pub sandboxing: bool,
}

// Default values used by the serde attributes above
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
/// Chooses where the EventFlux engine saves its state, such as checkpoints, window
/// state and aggregation state.
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
/// Or for files on disk:
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
    /// Persistence backend type. Written as `type` in YAML.
    #[serde(rename = "type")]
    pub backend_type: PersistenceBackendType,

    /// Directory for file persistence, or database file for SQLite
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    /// URL for persistence over the network, such as Redis
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,

    /// Turn persistence on. Defaults to true when the section is present.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Time between checkpoints. Defaults to 60 seconds.
    #[serde(with = "duration_serde", default = "default_checkpoint_interval")]
    pub checkpoint_interval: Duration,

    /// Compression for persisted state. Defaults to `None`.
    #[serde(default)]
    pub compression: PersistenceCompression,

    /// Extra options for the chosen backend
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
    /// Create a config that saves snapshot files in the `path` directory
    pub fn file(path: impl Into<String>) -> Self {
        Self {
            backend_type: PersistenceBackendType::File,
            path: Some(path.into()),
            ..Default::default()
        }
    }

    /// Create a config that saves state in the SQLite database at `path`
    pub fn sqlite(path: impl Into<String>) -> Self {
        Self {
            backend_type: PersistenceBackendType::Sqlite,
            path: Some(path.into()),
            ..Default::default()
        }
    }

    /// Create a config that saves state in Redis at `url`
    pub fn redis(url: impl Into<String>) -> Self {
        Self {
            backend_type: PersistenceBackendType::Redis,
            url: Some(url.into()),
            ..Default::default()
        }
    }

    /// Create a config that keeps state in memory only. It is enabled, but nothing
    /// survives a restart.
    pub fn memory() -> Self {
        Self {
            backend_type: PersistenceBackendType::Memory,
            enabled: true,
            ..Default::default()
        }
    }

    /// Merge `other` into this config.
    ///
    /// Backend type, enabled, checkpoint interval and compression always come from
    /// `other`. `path` and `url` are replaced only when set in `other`. Options are
    /// added by key, and `other` wins on a clash.
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

        self.options.extend(other.options);
    }
}

/// Persistence backend types. Defaults to `File`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PersistenceBackendType {
    /// Memory only. Nothing survives a restart.
    Memory,

    /// Snapshot files in a directory
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

/// Compression options for persistence. Defaults to `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PersistenceCompression {
    /// No compression
    None,

    /// LZ4 compression, which is fast
    Lz4,

    /// Zstd compression, which makes smaller output
    Zstd,
}

impl Default for PersistenceCompression {
    fn default() -> Self {
        Self::None
    }
}

// DistributedConfig lives in its own module
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
