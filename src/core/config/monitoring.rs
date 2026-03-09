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

//! Configuration Monitoring & Health Checks
//!
//! Provides comprehensive monitoring configuration for EventFlux applications,
//! including health checks, metrics collection, alerting, and observability features.

use crate::core::config::types::duration_serde;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Main monitoring configuration structure
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MonitoringConfig {
    /// Enable or disable monitoring globally
    pub enabled: bool,

    /// Health check configuration
    pub health: HealthCheckConfig,

    /// Metrics collection configuration
    pub metrics: MetricsConfig,

    /// Alerting configuration
    pub alerting: Option<AlertingConfig>,

    /// Observability settings (tracing, logging)
    pub observability: Option<ObservabilityConfig>,

    /// Endpoints configuration for health and metrics
    pub endpoints: EndpointConfig,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            health: HealthCheckConfig::default(),
            metrics: MetricsConfig::default(),
            alerting: None,
            observability: None,
            endpoints: EndpointConfig::default(),
        }
    }
}

impl MonitoringConfig {
    /// Create a new monitoring configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable monitoring with default settings
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            ..Self::default()
        }
    }

    /// Disable monitoring
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }

    /// Configure for development environment
    pub fn development() -> Self {
        Self {
            enabled: true,
            health: HealthCheckConfig::development(),
            metrics: MetricsConfig::development(),
            alerting: None, // No alerting in dev
            observability: Some(ObservabilityConfig::development()),
            endpoints: EndpointConfig::development(),
        }
    }

    /// Configure for production environment
    pub fn production() -> Self {
        Self {
            enabled: true,
            health: HealthCheckConfig::production(),
            metrics: MetricsConfig::production(),
            alerting: Some(AlertingConfig::production()),
            observability: Some(ObservabilityConfig::production()),
            endpoints: EndpointConfig::production(),
        }
    }

    /// Configure for Kubernetes deployment
    pub fn kubernetes() -> Self {
        Self {
            enabled: true,
            health: HealthCheckConfig::kubernetes(),
            metrics: MetricsConfig::kubernetes(),
            alerting: Some(AlertingConfig::kubernetes()),
            observability: Some(ObservabilityConfig::kubernetes()),
            endpoints: EndpointConfig::kubernetes(),
        }
    }

    /// Merge another monitoring configuration into this one
    pub fn merge(&mut self, other: MonitoringConfig) {
        self.enabled = other.enabled;
        self.health.merge(other.health);
        self.metrics.merge(other.metrics);

        if other.alerting.is_some() {
            self.alerting = other.alerting;
        }

        if other.observability.is_some() {
            self.observability = other.observability;
        }

        self.endpoints.merge(other.endpoints);
    }
}

/// Health check configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HealthCheckConfig {
    /// Enable health checks
    pub enabled: bool,

    /// Health check interval
    #[serde(with = "duration_serde")]
    pub interval: Duration,

    /// Health check timeout
    #[serde(with = "duration_serde")]
    pub timeout: Duration,

    /// Failure threshold before marking unhealthy
    pub failure_threshold: u32,

    /// Success threshold before marking healthy again
    pub success_threshold: u32,

    /// Individual health checks to perform
    pub checks: Vec<HealthCheck>,

    /// Startup probe configuration
    pub startup: Option<ProbeConfig>,

    /// Readiness probe configuration
    pub readiness: Option<ProbeConfig>,

    /// Liveness probe configuration
    pub liveness: Option<ProbeConfig>,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(30),
            timeout: Duration::from_secs(5),
            failure_threshold: 3,
            success_threshold: 1,
            checks: vec![
                HealthCheck::system_resource(),
                HealthCheck::event_processing(),
                HealthCheck::state_persistence(),
            ],
            startup: Some(ProbeConfig::startup_default()),
            readiness: Some(ProbeConfig::readiness_default()),
            liveness: Some(ProbeConfig::liveness_default()),
        }
    }
}

impl HealthCheckConfig {
    /// Development configuration
    pub fn development() -> Self {
        Self {
            interval: Duration::from_secs(10), // More frequent in dev
            timeout: Duration::from_secs(2),   // Shorter timeout
            failure_threshold: 1,              // Fail fast
            ..Self::default()
        }
    }

    /// Production configuration
    pub fn production() -> Self {
        Self {
            interval: Duration::from_secs(60), // Less frequent
            timeout: Duration::from_secs(10),  // Longer timeout
            failure_threshold: 5,              // More tolerant
            success_threshold: 2,              // Require sustained health
            ..Self::default()
        }
    }

    /// Kubernetes configuration
    pub fn kubernetes() -> Self {
        Self {
            interval: Duration::from_secs(30),
            timeout: Duration::from_secs(5),
            failure_threshold: 3,
            success_threshold: 1,
            startup: Some(ProbeConfig::kubernetes_startup()),
            readiness: Some(ProbeConfig::kubernetes_readiness()),
            liveness: Some(ProbeConfig::kubernetes_liveness()),
            ..Self::default()
        }
    }

    /// Merge another health check configuration
    pub fn merge(&mut self, other: HealthCheckConfig) {
        self.enabled = other.enabled;
        self.interval = other.interval;
        self.timeout = other.timeout;
        self.failure_threshold = other.failure_threshold;
        self.success_threshold = other.success_threshold;

        if !other.checks.is_empty() {
            self.checks = other.checks;
        }

        if other.startup.is_some() {
            self.startup = other.startup;
        }

        if other.readiness.is_some() {
            self.readiness = other.readiness;
        }

        if other.liveness.is_some() {
            self.liveness = other.liveness;
        }
    }
}

/// Individual health check definition
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HealthCheck {
    /// Health check name
    pub name: String,

    /// Health check type
    pub check_type: HealthCheckType,

    /// Check-specific configuration
    pub config: HashMap<String, String>,

    /// Weight of this check in overall health (0.0 to 1.0)
    pub weight: f32,

    /// Whether this check is critical for overall health
    pub critical: bool,
}

impl HealthCheck {
    /// System resource health check
    pub fn system_resource() -> Self {
        let mut config = HashMap::new();
        config.insert("max_cpu_percent".to_string(), "80".to_string());
        config.insert("max_memory_percent".to_string(), "85".to_string());
        config.insert("max_disk_percent".to_string(), "90".to_string());

        Self {
            name: "system_resources".to_string(),
            check_type: HealthCheckType::SystemResource,
            config,
            weight: 0.3,
            critical: true,
        }
    }

    /// Event processing health check
    pub fn event_processing() -> Self {
        let mut config = HashMap::new();
        config.insert("max_processing_latency_ms".to_string(), "1000".to_string());
        config.insert("min_throughput_eps".to_string(), "100".to_string());
        config.insert("max_error_rate_percent".to_string(), "1".to_string());

        Self {
            name: "event_processing".to_string(),
            check_type: HealthCheckType::EventProcessing,
            config,
            weight: 0.4,
            critical: true,
        }
    }

    /// State persistence health check
    pub fn state_persistence() -> Self {
        let mut config = HashMap::new();
        config.insert("max_checkpoint_age_minutes".to_string(), "10".to_string());
        config.insert(
            "min_checkpoint_success_rate".to_string(),
            "0.95".to_string(),
        );

        Self {
            name: "state_persistence".to_string(),
            check_type: HealthCheckType::StatePersistence,
            config,
            weight: 0.3,
            critical: true,
        }
    }
}

/// Types of health checks
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum HealthCheckType {
    /// Check system resource usage
    SystemResource,

    /// Check event processing performance
    EventProcessing,

    /// Check state persistence health
    StatePersistence,

    /// Check external dependency connectivity
    ExternalDependency,

    /// Custom health check
    Custom,

    /// HTTP endpoint check
    HttpEndpoint,
}

/// Probe configuration for Kubernetes-style health checks
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProbeConfig {
    /// Initial delay before first probe
    pub initial_delay: Duration,

    /// Interval between probes
    pub period: Duration,

    /// Probe timeout
    pub timeout: Duration,

    /// Failure threshold
    pub failure_threshold: u32,

    /// Success threshold
    pub success_threshold: u32,
}

impl ProbeConfig {
    /// Startup probe defaults
    pub fn startup_default() -> Self {
        Self {
            initial_delay: Duration::from_secs(10),
            period: Duration::from_secs(5),
            timeout: Duration::from_secs(3),
            failure_threshold: 30, // Allow longer startup
            success_threshold: 1,
        }
    }

    /// Readiness probe defaults
    pub fn readiness_default() -> Self {
        Self {
            initial_delay: Duration::from_secs(0),
            period: Duration::from_secs(10),
            timeout: Duration::from_secs(3),
            failure_threshold: 3,
            success_threshold: 1,
        }
    }

    /// Liveness probe defaults
    pub fn liveness_default() -> Self {
        Self {
            initial_delay: Duration::from_secs(30),
            period: Duration::from_secs(10),
            timeout: Duration::from_secs(5),
            failure_threshold: 3,
            success_threshold: 1,
        }
    }

    /// Kubernetes startup probe
    pub fn kubernetes_startup() -> Self {
        Self {
            initial_delay: Duration::from_secs(5),
            period: Duration::from_secs(10),
            timeout: Duration::from_secs(5),
            failure_threshold: 30,
            success_threshold: 1,
        }
    }

    /// Kubernetes readiness probe
    pub fn kubernetes_readiness() -> Self {
        Self {
            initial_delay: Duration::from_secs(5),
            period: Duration::from_secs(10),
            timeout: Duration::from_secs(5),
            failure_threshold: 3,
            success_threshold: 1,
        }
    }

    /// Kubernetes liveness probe
    pub fn kubernetes_liveness() -> Self {
        Self {
            initial_delay: Duration::from_secs(30),
            period: Duration::from_secs(10),
            timeout: Duration::from_secs(5),
            failure_threshold: 3,
            success_threshold: 1,
        }
    }
}

/// Metrics collection configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricsConfig {
    /// Enable metrics collection
    pub enabled: bool,

    /// Metrics collection interval
    #[serde(with = "duration_serde")]
    pub collection_interval: Duration,

    /// Metrics retention period
    #[serde(with = "duration_serde")]
    pub retention_period: Duration,

    /// Metrics exporters configuration
    pub exporters: Vec<MetricsExporter>,

    /// Custom metrics to collect
    pub custom_metrics: Vec<CustomMetric>,

    /// Histogram buckets for latency metrics
    pub histogram_buckets: Vec<f64>,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            collection_interval: Duration::from_secs(15),
            retention_period: Duration::from_secs(3600), // 1 hour
            exporters: vec![MetricsExporter::prometheus_default()],
            custom_metrics: Vec::new(),
            histogram_buckets: vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
        }
    }
}

impl MetricsConfig {
    /// Development configuration
    pub fn development() -> Self {
        Self {
            collection_interval: Duration::from_secs(5), // More frequent
            retention_period: Duration::from_secs(1800), // 30 minutes
            ..Self::default()
        }
    }

    /// Production configuration
    pub fn production() -> Self {
        Self {
            collection_interval: Duration::from_secs(30),
            retention_period: Duration::from_secs(86400), // 24 hours
            exporters: vec![
                MetricsExporter::prometheus_default(),
                MetricsExporter::opentelemetry_default(),
            ],
            ..Self::default()
        }
    }

    /// Kubernetes configuration
    pub fn kubernetes() -> Self {
        Self {
            collection_interval: Duration::from_secs(15),
            retention_period: Duration::from_secs(7200), // 2 hours
            exporters: vec![
                MetricsExporter::prometheus_kubernetes(),
                MetricsExporter::opentelemetry_kubernetes(),
            ],
            ..Self::default()
        }
    }

    /// Merge another metrics configuration
    pub fn merge(&mut self, other: MetricsConfig) {
        self.enabled = other.enabled;
        self.collection_interval = other.collection_interval;
        self.retention_period = other.retention_period;

        if !other.exporters.is_empty() {
            self.exporters = other.exporters;
        }

        if !other.custom_metrics.is_empty() {
            self.custom_metrics = other.custom_metrics;
        }

        if !other.histogram_buckets.is_empty() {
            self.histogram_buckets = other.histogram_buckets;
        }
    }
}

/// Metrics exporter configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricsExporter {
    /// Exporter name
    pub name: String,

    /// Exporter type
    pub exporter_type: MetricsExporterType,

    /// Exporter-specific configuration
    pub config: HashMap<String, String>,

    /// Whether this exporter is enabled
    pub enabled: bool,
}

impl MetricsExporter {
    /// Default Prometheus exporter
    pub fn prometheus_default() -> Self {
        let mut config = HashMap::new();
        config.insert("endpoint".to_string(), "/metrics".to_string());
        config.insert("port".to_string(), "9090".to_string());

        Self {
            name: "prometheus".to_string(),
            exporter_type: MetricsExporterType::Prometheus,
            config,
            enabled: true,
        }
    }

    /// Prometheus exporter for Kubernetes
    pub fn prometheus_kubernetes() -> Self {
        let mut config = HashMap::new();
        config.insert("endpoint".to_string(), "/metrics".to_string());
        config.insert("port".to_string(), "8080".to_string());
        config.insert("namespace".to_string(), "eventflux".to_string());

        Self {
            name: "prometheus-k8s".to_string(),
            exporter_type: MetricsExporterType::Prometheus,
            config,
            enabled: true,
        }
    }

    /// Default OpenTelemetry exporter
    pub fn opentelemetry_default() -> Self {
        let mut config = HashMap::new();
        config.insert("endpoint".to_string(), "http://localhost:4317".to_string());
        config.insert("protocol".to_string(), "grpc".to_string());

        Self {
            name: "opentelemetry".to_string(),
            exporter_type: MetricsExporterType::OpenTelemetry,
            config,
            enabled: true,
        }
    }

    /// OpenTelemetry exporter for Kubernetes
    pub fn opentelemetry_kubernetes() -> Self {
        let mut config = HashMap::new();
        config.insert(
            "endpoint".to_string(),
            "http://otel-collector.monitoring.svc.cluster.local:4317".to_string(),
        );
        config.insert("protocol".to_string(), "grpc".to_string());
        config.insert(
            "headers".to_string(),
            "x-honeycomb-team=YOUR_API_KEY".to_string(),
        );

        Self {
            name: "opentelemetry-k8s".to_string(),
            exporter_type: MetricsExporterType::OpenTelemetry,
            config,
            enabled: true,
        }
    }
}

/// Types of metrics exporters
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MetricsExporterType {
    /// Prometheus metrics format
    Prometheus,

    /// OpenTelemetry metrics
    OpenTelemetry,

    /// InfluxDB time series
    InfluxDB,

    /// Custom metrics exporter
    Custom,

    /// Log-based metrics
    Log,
}

/// Custom metric definition
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CustomMetric {
    /// Metric name
    pub name: String,

    /// Metric type
    pub metric_type: MetricType,

    /// Metric description
    pub description: String,

    /// Metric labels
    pub labels: Vec<String>,

    /// Collection query/expression
    pub query: String,
}

/// Types of metrics
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MetricType {
    /// Counter metric (always increasing)
    Counter,

    /// Gauge metric (can go up and down)
    Gauge,

    /// Histogram metric (distribution of values)
    Histogram,

    /// Summary metric (quantiles)
    Summary,
}

/// Alerting configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AlertingConfig {
    /// Enable alerting
    pub enabled: bool,

    /// Alert evaluation interval
    #[serde(with = "duration_serde")]
    pub evaluation_interval: Duration,

    /// Alert rules
    pub rules: Vec<AlertRule>,

    /// Notification channels
    pub notification_channels: Vec<NotificationChannel>,
}

impl Default for AlertingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            evaluation_interval: Duration::from_secs(60),
            rules: Vec::new(),
            notification_channels: Vec::new(),
        }
    }
}

impl AlertingConfig {
    /// Production alerting configuration
    pub fn production() -> Self {
        Self {
            enabled: true,
            evaluation_interval: Duration::from_secs(60),
            rules: vec![
                AlertRule::high_cpu_usage(),
                AlertRule::high_memory_usage(),
                AlertRule::high_processing_latency(),
                AlertRule::checkpoint_failure(),
            ],
            notification_channels: vec![NotificationChannel::email_default()],
        }
    }

    /// Kubernetes alerting configuration
    pub fn kubernetes() -> Self {
        Self {
            enabled: true,
            evaluation_interval: Duration::from_secs(30),
            rules: vec![
                AlertRule::high_cpu_usage(),
                AlertRule::high_memory_usage(),
                AlertRule::high_processing_latency(),
                AlertRule::checkpoint_failure(),
            ],
            notification_channels: vec![NotificationChannel::slack_default()],
        }
    }
}

/// Alert rule definition
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AlertRule {
    /// Alert name
    pub name: String,

    /// Alert expression (PromQL-like)
    pub expression: String,

    /// Duration before firing
    pub for_duration: Duration,

    /// Alert labels
    pub labels: HashMap<String, String>,

    /// Alert annotations
    pub annotations: HashMap<String, String>,
}

impl AlertRule {
    /// High CPU usage alert
    pub fn high_cpu_usage() -> Self {
        let mut labels = HashMap::new();
        labels.insert("severity".to_string(), "warning".to_string());

        let mut annotations = HashMap::new();
        annotations.insert("summary".to_string(), "High CPU usage detected".to_string());
        annotations.insert(
            "description".to_string(),
            "CPU usage is above 80%".to_string(),
        );

        Self {
            name: "HighCpuUsage".to_string(),
            expression: "eventflux_system_cpu_usage > 0.8".to_string(),
            for_duration: Duration::from_secs(300),
            labels,
            annotations,
        }
    }

    /// High memory usage alert
    pub fn high_memory_usage() -> Self {
        let mut labels = HashMap::new();
        labels.insert("severity".to_string(), "warning".to_string());

        let mut annotations = HashMap::new();
        annotations.insert(
            "summary".to_string(),
            "High memory usage detected".to_string(),
        );

        Self {
            name: "HighMemoryUsage".to_string(),
            expression: "eventflux_system_memory_usage > 0.85".to_string(),
            for_duration: Duration::from_secs(300),
            labels,
            annotations,
        }
    }

    /// High processing latency alert
    pub fn high_processing_latency() -> Self {
        let mut labels = HashMap::new();
        labels.insert("severity".to_string(), "critical".to_string());

        Self {
            name: "HighProcessingLatency".to_string(),
            expression: "eventflux_processing_latency_p99 > 1000".to_string(),
            for_duration: Duration::from_secs(60),
            labels,
            annotations: HashMap::new(),
        }
    }

    /// Checkpoint failure alert
    pub fn checkpoint_failure() -> Self {
        let mut labels = HashMap::new();
        labels.insert("severity".to_string(), "critical".to_string());

        Self {
            name: "CheckpointFailure".to_string(),
            expression: "eventflux_checkpoint_failure_rate > 0.1".to_string(),
            for_duration: Duration::from_secs(120),
            labels,
            annotations: HashMap::new(),
        }
    }
}

/// Notification channel for alerts
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NotificationChannel {
    /// Channel name
    pub name: String,

    /// Channel type
    pub channel_type: NotificationChannelType,

    /// Channel configuration
    pub config: HashMap<String, String>,

    /// Whether this channel is enabled
    pub enabled: bool,
}

impl NotificationChannel {
    /// Default Slack notification channel
    pub fn slack_default() -> Self {
        let mut config = HashMap::new();
        config.insert(
            "webhook_url".to_string(),
            "${SLACK_WEBHOOK_URL}".to_string(),
        );
        config.insert("channel".to_string(), "#eventflux-alerts".to_string());

        Self {
            name: "slack".to_string(),
            channel_type: NotificationChannelType::Slack,
            config,
            enabled: true,
        }
    }

    /// Default Email notification channel
    pub fn email_default() -> Self {
        let mut config = HashMap::new();
        config.insert("smtp_server".to_string(), "${SMTP_SERVER}".to_string());
        config.insert("from_address".to_string(), "${FROM_EMAIL}".to_string());
        config.insert(
            "to_addresses".to_string(),
            "${ALERT_EMAIL_LIST}".to_string(),
        );

        Self {
            name: "email".to_string(),
            channel_type: NotificationChannelType::Email,
            config,
            enabled: true,
        }
    }
}

/// Types of notification channels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NotificationChannelType {
    /// Slack notifications
    Slack,

    /// Email notifications
    Email,

    /// PagerDuty notifications
    PagerDuty,

    /// Webhook notifications
    Webhook,

    /// Custom notification channel
    Custom,
}

/// Observability configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ObservabilityConfig {
    /// Tracing configuration
    pub tracing: TracingConfig,

    /// Logging configuration
    pub logging: LoggingConfig,
}

impl ObservabilityConfig {
    /// Development configuration
    pub fn development() -> Self {
        Self {
            tracing: TracingConfig::development(),
            logging: LoggingConfig::development(),
        }
    }

    /// Production configuration
    pub fn production() -> Self {
        Self {
            tracing: TracingConfig::production(),
            logging: LoggingConfig::production(),
        }
    }

    /// Kubernetes configuration
    pub fn kubernetes() -> Self {
        Self {
            tracing: TracingConfig::kubernetes(),
            logging: LoggingConfig::kubernetes(),
        }
    }
}

/// Tracing configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TracingConfig {
    /// Enable distributed tracing
    pub enabled: bool,

    /// Tracing exporter type
    pub exporter: TracingExporter,

    /// Sample rate (0.0 to 1.0)
    pub sample_rate: f64,

    /// Service name for tracing
    pub service_name: String,

    /// Additional tracing configuration
    pub config: HashMap<String, String>,
}

impl TracingConfig {
    /// Development tracing
    pub fn development() -> Self {
        Self {
            enabled: true,
            exporter: TracingExporter::Console,
            sample_rate: 1.0, // Trace everything in dev
            service_name: "eventflux-dev".to_string(),
            config: HashMap::new(),
        }
    }

    /// Production tracing
    pub fn production() -> Self {
        let mut config = HashMap::new();
        config.insert(
            "endpoint".to_string(),
            "http://jaeger:14268/api/traces".to_string(),
        );

        Self {
            enabled: true,
            exporter: TracingExporter::Jaeger,
            sample_rate: 0.1, // Sample 10% in production
            service_name: "eventflux".to_string(),
            config,
        }
    }

    /// Kubernetes tracing
    pub fn kubernetes() -> Self {
        let mut config = HashMap::new();
        config.insert(
            "endpoint".to_string(),
            "http://jaeger-collector.monitoring.svc.cluster.local:14268/api/traces".to_string(),
        );

        Self {
            enabled: true,
            exporter: TracingExporter::Jaeger,
            sample_rate: 0.05, // Sample 5% in k8s
            service_name: "eventflux".to_string(),
            config,
        }
    }
}

/// Tracing exporter types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TracingExporter {
    /// Console output
    Console,

    /// Jaeger tracing
    Jaeger,

    /// Zipkin tracing  
    Zipkin,

    /// OpenTelemetry
    OpenTelemetry,
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LoggingConfig {
    /// Log level
    pub level: String,

    /// Log format
    pub format: LogFormat,

    /// Log output destinations
    pub outputs: Vec<LogOutput>,

    /// Structured logging fields
    pub structured_fields: Vec<String>,
}

impl LoggingConfig {
    /// Development logging
    pub fn development() -> Self {
        Self {
            level: "debug".to_string(),
            format: LogFormat::Pretty,
            outputs: vec![LogOutput::Console],
            structured_fields: vec![
                "timestamp".to_string(),
                "level".to_string(),
                "message".to_string(),
            ],
        }
    }

    /// Production logging
    pub fn production() -> Self {
        Self {
            level: "info".to_string(),
            format: LogFormat::Json,
            outputs: vec![LogOutput::Console, LogOutput::File],
            structured_fields: vec![
                "timestamp".to_string(),
                "level".to_string(),
                "message".to_string(),
                "service".to_string(),
                "trace_id".to_string(),
            ],
        }
    }

    /// Kubernetes logging
    pub fn kubernetes() -> Self {
        Self {
            level: "info".to_string(),
            format: LogFormat::Json,
            outputs: vec![LogOutput::Stdout],
            structured_fields: vec![
                "timestamp".to_string(),
                "level".to_string(),
                "message".to_string(),
                "service".to_string(),
                "pod_name".to_string(),
                "namespace".to_string(),
            ],
        }
    }
}

/// Log format types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LogFormat {
    /// Pretty printed logs
    Pretty,

    /// JSON structured logs
    Json,

    /// Plain text logs
    Plain,
}

/// Log output destinations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LogOutput {
    /// Console output
    Console,

    /// Standard output
    Stdout,

    /// File output
    File,

    /// Syslog output
    Syslog,
}

/// Endpoint configuration for monitoring services
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EndpointConfig {
    /// Health check endpoint
    pub health_endpoint: String,

    /// Metrics endpoint
    pub metrics_endpoint: String,

    /// Port for monitoring endpoints
    pub port: u16,

    /// Bind address
    pub bind_address: String,

    /// Enable HTTPS
    pub tls_enabled: bool,
}

impl Default for EndpointConfig {
    fn default() -> Self {
        Self {
            health_endpoint: "/health".to_string(),
            metrics_endpoint: "/metrics".to_string(),
            port: 8080,
            bind_address: "0.0.0.0".to_string(),
            tls_enabled: false,
        }
    }
}

impl EndpointConfig {
    /// Development configuration
    pub fn development() -> Self {
        Self {
            port: 8080,
            bind_address: "127.0.0.1".to_string(),
            tls_enabled: false,
            ..Self::default()
        }
    }

    /// Production configuration
    pub fn production() -> Self {
        Self {
            port: 8443,
            bind_address: "0.0.0.0".to_string(),
            tls_enabled: true,
            ..Self::default()
        }
    }

    /// Kubernetes configuration
    pub fn kubernetes() -> Self {
        Self {
            health_endpoint: "/healthz".to_string(), // K8s standard
            metrics_endpoint: "/metrics".to_string(),
            port: 8080,
            bind_address: "0.0.0.0".to_string(),
            tls_enabled: false, // TLS handled by service mesh
            ..Self::default()
        }
    }

    /// Merge another endpoint configuration
    pub fn merge(&mut self, other: EndpointConfig) {
        self.health_endpoint = other.health_endpoint;
        self.metrics_endpoint = other.metrics_endpoint;
        self.port = other.port;
        self.bind_address = other.bind_address;
        self.tls_enabled = other.tls_enabled;
    }
}

/// Health status representation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HealthStatus {
    /// Overall health status
    pub status: HealthState,

    /// Individual check results
    pub checks: HashMap<String, CheckResult>,

    /// Overall health score (0.0 to 1.0)
    pub score: f32,

    /// Last check timestamp
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Health state enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum HealthState {
    /// Healthy
    Healthy,

    /// Degraded but functional
    Degraded,

    /// Unhealthy
    Unhealthy,

    /// Unknown status
    Unknown,
}

/// Individual health check result
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckResult {
    /// Check status
    pub status: HealthState,

    /// Check message
    pub message: String,

    /// Check duration
    pub duration: Duration,

    /// Check-specific data
    pub data: HashMap<String, serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monitoring_config_default() {
        let config = MonitoringConfig::default();
        assert!(config.enabled);
        assert!(config.health.enabled);
        assert!(config.metrics.enabled);
        assert_eq!(config.endpoints.port, 8080);
    }

    #[test]
    fn test_monitoring_config_development() {
        let config = MonitoringConfig::development();
        assert!(config.enabled);
        assert_eq!(config.health.interval, Duration::from_secs(10));
        assert_eq!(config.metrics.collection_interval, Duration::from_secs(5));
        assert!(config.alerting.is_none());
    }

    #[test]
    fn test_monitoring_config_production() {
        let config = MonitoringConfig::production();
        assert!(config.enabled);
        assert_eq!(config.health.interval, Duration::from_secs(60));
        assert!(config.alerting.is_some());
        assert_eq!(config.endpoints.port, 8443);
        assert!(config.endpoints.tls_enabled);
    }

    #[test]
    fn test_monitoring_config_kubernetes() {
        let config = MonitoringConfig::kubernetes();
        assert!(config.enabled);
        assert_eq!(config.endpoints.health_endpoint, "/healthz");
        assert_eq!(config.endpoints.port, 8080);
        assert!(!config.endpoints.tls_enabled);

        let alerting = config.alerting.unwrap();
        assert!(alerting.enabled);
        assert_eq!(alerting.rules.len(), 4);
    }

    #[test]
    fn test_health_check_configuration() {
        let check = HealthCheck::system_resource();
        assert_eq!(check.name, "system_resources");
        assert!(matches!(check.check_type, HealthCheckType::SystemResource));
        assert!(check.critical);
        assert_eq!(check.weight, 0.3);
    }

    #[test]
    fn test_metrics_exporter_configuration() {
        let prometheus = MetricsExporter::prometheus_kubernetes();
        assert_eq!(prometheus.name, "prometheus-k8s");
        assert!(prometheus.enabled);
        assert_eq!(prometheus.config.get("port"), Some(&"8080".to_string()));
        assert_eq!(
            prometheus.config.get("namespace"),
            Some(&"eventflux".to_string())
        );
    }

    #[test]
    fn test_probe_configuration() {
        let startup = ProbeConfig::kubernetes_startup();
        assert_eq!(startup.initial_delay, Duration::from_secs(5));
        assert_eq!(startup.failure_threshold, 30);

        let liveness = ProbeConfig::kubernetes_liveness();
        assert_eq!(liveness.initial_delay, Duration::from_secs(30));
        assert_eq!(liveness.failure_threshold, 3);
    }

    #[test]
    fn test_alert_rules() {
        let cpu_alert = AlertRule::high_cpu_usage();
        assert_eq!(cpu_alert.name, "HighCpuUsage");
        assert_eq!(cpu_alert.expression, "eventflux_system_cpu_usage > 0.8");
        assert_eq!(cpu_alert.for_duration, Duration::from_secs(300));
        assert_eq!(
            cpu_alert.labels.get("severity"),
            Some(&"warning".to_string())
        );

        let latency_alert = AlertRule::high_processing_latency();
        assert_eq!(
            latency_alert.labels.get("severity"),
            Some(&"critical".to_string())
        );
    }

    #[test]
    fn test_observability_configuration() {
        let obs = ObservabilityConfig::production();
        assert!(obs.tracing.enabled);
        assert_eq!(obs.tracing.sample_rate, 0.1);
        assert_eq!(obs.logging.level, "info");
        assert!(matches!(obs.logging.format, LogFormat::Json));

        let k8s_obs = ObservabilityConfig::kubernetes();
        assert_eq!(k8s_obs.tracing.sample_rate, 0.05);
        assert_eq!(k8s_obs.logging.outputs.len(), 1);
        assert!(matches!(k8s_obs.logging.outputs[0], LogOutput::Stdout));
    }

    #[test]
    fn test_configuration_merging() {
        let mut base = MonitoringConfig::development();
        let production = MonitoringConfig::production();

        base.merge(production);

        assert_eq!(base.health.interval, Duration::from_secs(60));
        assert_eq!(base.endpoints.port, 8443);
        assert!(base.endpoints.tls_enabled);
        assert!(base.alerting.is_some());
    }
}
