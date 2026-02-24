// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for monitoring and health check configuration
//!
//! These tests demonstrate how the monitoring configuration system works with
//! health checks, metrics collection, alerting, and observability features.

use eventflux::core::config::{
    monitoring::{
        AlertRule, AlertingConfig, CheckResult, CustomMetric, EndpointConfig, HealthCheck,
        HealthCheckConfig, HealthCheckType, HealthState, HealthStatus, LoggingConfig, MetricType,
        MetricsConfig, MetricsExporter, MetricsExporterType, MonitoringConfig, NotificationChannel,
        NotificationChannelType, ObservabilityConfig, ProbeConfig, TracingConfig,
    },
    ConfigManager, EventFluxConfig,
};
use std::collections::HashMap;
use std::time::Duration;

/// Test basic monitoring configuration creation
#[test]
fn test_monitoring_config_creation() {
    let config = MonitoringConfig::new();

    assert!(config.enabled);
    assert!(config.health.enabled);
    assert!(config.metrics.enabled);
    assert_eq!(config.endpoints.port, 8080);
    assert_eq!(config.endpoints.health_endpoint, "/health");
    assert_eq!(config.endpoints.metrics_endpoint, "/metrics");
}

/// Test development monitoring configuration
#[test]
fn test_development_monitoring_config() {
    let config = MonitoringConfig::development();

    assert!(config.enabled);
    assert_eq!(config.health.interval, Duration::from_secs(10));
    assert_eq!(config.health.failure_threshold, 1);
    assert_eq!(config.metrics.collection_interval, Duration::from_secs(5));
    assert_eq!(config.endpoints.bind_address, "127.0.0.1");
    assert!(!config.endpoints.tls_enabled);
    assert!(config.alerting.is_none()); // No alerting in dev
}

/// Test production monitoring configuration
#[test]
fn test_production_monitoring_config() {
    let config = MonitoringConfig::production();

    assert!(config.enabled);
    assert_eq!(config.health.interval, Duration::from_secs(60));
    assert_eq!(config.health.failure_threshold, 5);
    assert_eq!(config.metrics.collection_interval, Duration::from_secs(30));
    assert_eq!(config.endpoints.port, 8443);
    assert!(config.endpoints.tls_enabled);
    assert!(config.alerting.is_some());
    assert!(config.observability.is_some());

    // Test metrics exporters for production
    assert_eq!(config.metrics.exporters.len(), 2);
    let exporter_names: Vec<&String> = config.metrics.exporters.iter().map(|e| &e.name).collect();
    assert!(exporter_names.contains(&&"prometheus".to_string()));
    assert!(exporter_names.contains(&&"opentelemetry".to_string()));
}

/// Test Kubernetes monitoring configuration
#[test]
fn test_kubernetes_monitoring_config() {
    let config = MonitoringConfig::kubernetes();

    assert!(config.enabled);
    assert_eq!(config.endpoints.health_endpoint, "/healthz");
    assert_eq!(config.endpoints.port, 8080);
    assert!(!config.endpoints.tls_enabled); // TLS handled by service mesh

    // Test alerting configuration
    let alerting = config.alerting.unwrap();
    assert!(alerting.enabled);
    assert_eq!(alerting.evaluation_interval, Duration::from_secs(30));
    assert_eq!(alerting.rules.len(), 4);

    // Verify alert rule names
    let rule_names: Vec<&String> = alerting.rules.iter().map(|r| &r.name).collect();
    assert!(rule_names.contains(&&"HighCpuUsage".to_string()));
    assert!(rule_names.contains(&&"HighMemoryUsage".to_string()));
    assert!(rule_names.contains(&&"HighProcessingLatency".to_string()));
    assert!(rule_names.contains(&&"CheckpointFailure".to_string()));

    // Test notification channels
    assert_eq!(alerting.notification_channels.len(), 1);
    assert_eq!(alerting.notification_channels[0].name, "slack");
}

/// Test health check configuration
#[test]
fn test_health_check_configuration() {
    let health_config = HealthCheckConfig::default();

    assert!(health_config.enabled);
    assert_eq!(health_config.interval, Duration::from_secs(30));
    assert_eq!(health_config.timeout, Duration::from_secs(5));
    assert_eq!(health_config.failure_threshold, 3);
    assert_eq!(health_config.success_threshold, 1);

    // Test default health checks
    assert_eq!(health_config.checks.len(), 3);
    let check_names: Vec<&String> = health_config.checks.iter().map(|c| &c.name).collect();
    assert!(check_names.contains(&&"system_resources".to_string()));
    assert!(check_names.contains(&&"event_processing".to_string()));
    assert!(check_names.contains(&&"state_persistence".to_string()));

    // Test probe configurations
    assert!(health_config.startup.is_some());
    assert!(health_config.readiness.is_some());
    assert!(health_config.liveness.is_some());
}

/// Test individual health check types
#[test]
fn test_health_check_types() {
    let system_check = HealthCheck::system_resource();
    assert_eq!(system_check.name, "system_resources");
    assert!(matches!(
        system_check.check_type,
        HealthCheckType::SystemResource
    ));
    assert!(system_check.critical);
    assert_eq!(system_check.weight, 0.3);
    assert_eq!(
        system_check.config.get("max_cpu_percent"),
        Some(&"80".to_string())
    );

    let processing_check = HealthCheck::event_processing();
    assert_eq!(processing_check.name, "event_processing");
    assert!(matches!(
        processing_check.check_type,
        HealthCheckType::EventProcessing
    ));
    assert_eq!(processing_check.weight, 0.4);
    assert_eq!(
        processing_check.config.get("max_processing_latency_ms"),
        Some(&"1000".to_string())
    );

    let persistence_check = HealthCheck::state_persistence();
    assert_eq!(persistence_check.name, "state_persistence");
    assert!(matches!(
        persistence_check.check_type,
        HealthCheckType::StatePersistence
    ));
    assert_eq!(persistence_check.weight, 0.3);
}

/// Test probe configurations for Kubernetes
#[test]
fn test_kubernetes_probe_configurations() {
    let startup = ProbeConfig::kubernetes_startup();
    assert_eq!(startup.initial_delay, Duration::from_secs(5));
    assert_eq!(startup.period, Duration::from_secs(10));
    assert_eq!(startup.timeout, Duration::from_secs(5));
    assert_eq!(startup.failure_threshold, 30); // Allow longer startup time
    assert_eq!(startup.success_threshold, 1);

    let readiness = ProbeConfig::kubernetes_readiness();
    assert_eq!(readiness.initial_delay, Duration::from_secs(5));
    assert_eq!(readiness.failure_threshold, 3);

    let liveness = ProbeConfig::kubernetes_liveness();
    assert_eq!(liveness.initial_delay, Duration::from_secs(30));
    assert_eq!(liveness.failure_threshold, 3);
}

/// Test metrics configuration
#[test]
fn test_metrics_configuration() {
    let metrics_config = MetricsConfig::default();

    assert!(metrics_config.enabled);
    assert_eq!(metrics_config.collection_interval, Duration::from_secs(15));
    assert_eq!(metrics_config.retention_period, Duration::from_secs(3600));
    assert_eq!(metrics_config.exporters.len(), 1);
    assert_eq!(metrics_config.exporters[0].name, "prometheus");

    // Test histogram buckets
    assert!(!metrics_config.histogram_buckets.is_empty());
    assert!(metrics_config.histogram_buckets.contains(&0.001));
    assert!(metrics_config.histogram_buckets.contains(&10.0));
}

/// Test metrics exporters
#[test]
fn test_metrics_exporters() {
    let prometheus = MetricsExporter::prometheus_default();
    assert_eq!(prometheus.name, "prometheus");
    assert!(matches!(
        prometheus.exporter_type,
        MetricsExporterType::Prometheus
    ));
    assert!(prometheus.enabled);
    assert_eq!(
        prometheus.config.get("endpoint"),
        Some(&"/metrics".to_string())
    );
    assert_eq!(prometheus.config.get("port"), Some(&"9090".to_string()));

    let prometheus_k8s = MetricsExporter::prometheus_kubernetes();
    assert_eq!(prometheus_k8s.name, "prometheus-k8s");
    assert_eq!(prometheus_k8s.config.get("port"), Some(&"8080".to_string()));
    assert_eq!(
        prometheus_k8s.config.get("namespace"),
        Some(&"eventflux".to_string())
    );

    let otel = MetricsExporter::opentelemetry_default();
    assert_eq!(otel.name, "opentelemetry");
    assert!(matches!(
        otel.exporter_type,
        MetricsExporterType::OpenTelemetry
    ));
    assert_eq!(
        otel.config.get("endpoint"),
        Some(&"http://localhost:4317".to_string())
    );
    assert_eq!(otel.config.get("protocol"), Some(&"grpc".to_string()));

    let otel_k8s = MetricsExporter::opentelemetry_kubernetes();
    assert_eq!(otel_k8s.name, "opentelemetry-k8s");
    assert!(otel_k8s
        .config
        .get("endpoint")
        .unwrap()
        .contains("otel-collector.monitoring.svc.cluster.local"));
}

/// Test custom metrics configuration
#[test]
fn test_custom_metrics() {
    let custom_metric = CustomMetric {
        name: "custom_throughput".to_string(),
        metric_type: MetricType::Gauge,
        description: "Custom throughput measurement".to_string(),
        labels: vec!["stream_name".to_string(), "query_name".to_string()],
        query: "SELECT count(*) FROM events_processed".to_string(),
    };

    assert_eq!(custom_metric.name, "custom_throughput");
    assert!(matches!(custom_metric.metric_type, MetricType::Gauge));
    assert_eq!(custom_metric.labels.len(), 2);
    assert!(custom_metric.labels.contains(&"stream_name".to_string()));
}

/// Test alert rules
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
    assert!(!cpu_alert.annotations.is_empty());

    let memory_alert = AlertRule::high_memory_usage();
    assert_eq!(memory_alert.name, "HighMemoryUsage");
    assert_eq!(
        memory_alert.expression,
        "eventflux_system_memory_usage > 0.85"
    );
    assert_eq!(
        memory_alert.labels.get("severity"),
        Some(&"warning".to_string())
    );

    let latency_alert = AlertRule::high_processing_latency();
    assert_eq!(latency_alert.name, "HighProcessingLatency");
    assert_eq!(
        latency_alert.expression,
        "eventflux_processing_latency_p99 > 1000"
    );
    assert_eq!(latency_alert.for_duration, Duration::from_secs(60));
    assert_eq!(
        latency_alert.labels.get("severity"),
        Some(&"critical".to_string())
    );

    let checkpoint_alert = AlertRule::checkpoint_failure();
    assert_eq!(checkpoint_alert.name, "CheckpointFailure");
    assert_eq!(
        checkpoint_alert.expression,
        "eventflux_checkpoint_failure_rate > 0.1"
    );
    assert_eq!(checkpoint_alert.for_duration, Duration::from_secs(120));
    assert_eq!(
        checkpoint_alert.labels.get("severity"),
        Some(&"critical".to_string())
    );
}

/// Test notification channels
#[test]
fn test_notification_channels() {
    let slack_channel = NotificationChannel::slack_default();
    assert_eq!(slack_channel.name, "slack");
    assert!(matches!(
        slack_channel.channel_type,
        NotificationChannelType::Slack
    ));
    assert!(slack_channel.enabled);
    assert_eq!(
        slack_channel.config.get("webhook_url"),
        Some(&"${SLACK_WEBHOOK_URL}".to_string())
    );
    assert_eq!(
        slack_channel.config.get("channel"),
        Some(&"#eventflux-alerts".to_string())
    );
}

/// Test observability configuration
#[test]
fn test_observability_configuration() {
    let dev_obs = ObservabilityConfig::development();
    assert!(dev_obs.tracing.enabled);
    assert_eq!(dev_obs.tracing.sample_rate, 1.0); // Trace everything in dev
    assert_eq!(dev_obs.tracing.service_name, "eventflux-dev");
    assert_eq!(dev_obs.logging.level, "debug");

    let prod_obs = ObservabilityConfig::production();
    assert_eq!(prod_obs.tracing.sample_rate, 0.1); // Sample 10% in production
    assert_eq!(prod_obs.tracing.service_name, "eventflux");
    assert_eq!(prod_obs.logging.level, "info");

    let k8s_obs = ObservabilityConfig::kubernetes();
    assert_eq!(k8s_obs.tracing.sample_rate, 0.05); // Sample 5% in k8s
    assert_eq!(k8s_obs.logging.outputs.len(), 1);
}

/// Test tracing configuration
#[test]
fn test_tracing_configuration() {
    let dev_tracing = TracingConfig::development();
    assert!(dev_tracing.enabled);
    assert_eq!(dev_tracing.sample_rate, 1.0);
    assert_eq!(dev_tracing.service_name, "eventflux-dev");

    let prod_tracing = TracingConfig::production();
    assert_eq!(prod_tracing.sample_rate, 0.1);
    assert_eq!(
        prod_tracing.config.get("endpoint"),
        Some(&"http://jaeger:14268/api/traces".to_string())
    );

    let k8s_tracing = TracingConfig::kubernetes();
    assert!(k8s_tracing
        .config
        .get("endpoint")
        .unwrap()
        .contains("jaeger-collector.monitoring.svc.cluster.local"));
}

/// Test logging configuration
#[test]
fn test_logging_configuration() {
    let dev_logging = LoggingConfig::development();
    assert_eq!(dev_logging.level, "debug");
    assert_eq!(dev_logging.structured_fields.len(), 3);

    let prod_logging = LoggingConfig::production();
    assert_eq!(prod_logging.level, "info");
    assert_eq!(prod_logging.structured_fields.len(), 5);
    assert!(prod_logging
        .structured_fields
        .contains(&"trace_id".to_string()));

    let k8s_logging = LoggingConfig::kubernetes();
    assert!(k8s_logging
        .structured_fields
        .contains(&"pod_name".to_string()));
    assert!(k8s_logging
        .structured_fields
        .contains(&"namespace".to_string()));
}

/// Test endpoint configuration
#[test]
fn test_endpoint_configuration() {
    let default_endpoints = EndpointConfig::default();
    assert_eq!(default_endpoints.health_endpoint, "/health");
    assert_eq!(default_endpoints.metrics_endpoint, "/metrics");
    assert_eq!(default_endpoints.port, 8080);
    assert_eq!(default_endpoints.bind_address, "0.0.0.0");
    assert!(!default_endpoints.tls_enabled);

    let dev_endpoints = EndpointConfig::development();
    assert_eq!(dev_endpoints.bind_address, "127.0.0.1");

    let prod_endpoints = EndpointConfig::production();
    assert_eq!(prod_endpoints.port, 8443);
    assert!(prod_endpoints.tls_enabled);

    let k8s_endpoints = EndpointConfig::kubernetes();
    assert_eq!(k8s_endpoints.health_endpoint, "/healthz");
    assert!(!k8s_endpoints.tls_enabled); // TLS handled by service mesh
}

/// Test configuration merging
#[test]
fn test_monitoring_config_merging() {
    let mut base = MonitoringConfig::development();
    let prod = MonitoringConfig::production();

    // Verify initial state
    assert_eq!(base.health.interval, Duration::from_secs(10));
    assert_eq!(base.endpoints.port, 8080);
    assert!(!base.endpoints.tls_enabled);
    assert!(base.alerting.is_none());

    // Merge production config
    base.merge(prod);

    // Verify merged state
    assert_eq!(base.health.interval, Duration::from_secs(60));
    assert_eq!(base.endpoints.port, 8443);
    assert!(base.endpoints.tls_enabled);
    assert!(base.alerting.is_some());

    // Verify alerting was properly merged
    let alerting = base.alerting.unwrap();
    assert!(alerting.enabled);
    assert_eq!(alerting.rules.len(), 4);
}

/// Test health status representation
#[test]
fn test_health_status() {
    let mut checks = HashMap::new();
    checks.insert(
        "system_resources".to_string(),
        CheckResult {
            status: HealthState::Healthy,
            message: "System resources within normal range".to_string(),
            duration: Duration::from_millis(10),
            data: HashMap::new(),
        },
    );

    checks.insert(
        "event_processing".to_string(),
        CheckResult {
            status: HealthState::Degraded,
            message: "Processing latency elevated but acceptable".to_string(),
            duration: Duration::from_millis(50),
            data: HashMap::new(),
        },
    );

    let health_status = HealthStatus {
        status: HealthState::Degraded,
        checks,
        score: 0.75,
        timestamp: chrono::Utc::now(),
    };

    assert!(matches!(health_status.status, HealthState::Degraded));
    assert_eq!(health_status.checks.len(), 2);
    assert_eq!(health_status.score, 0.75);
    assert!(health_status.checks.contains_key("system_resources"));
    assert!(health_status.checks.contains_key("event_processing"));
}

/// Test monitoring integration with ConfigManager
#[tokio::test]
async fn test_monitoring_config_manager_integration() {
    use std::fs;
    use tempfile::tempdir;

    let temp_dir = tempdir().unwrap();
    let config_file = temp_dir.path().join("eventflux-config.yaml");

    // Create a configuration file with monitoring settings
    let config_yaml = r#"
apiVersion: eventflux.io/v1
kind: EventFluxConfig
metadata:
  name: monitoring-test-config
eventflux:
  runtime:
    mode: "single-node"
    performance:
      thread_pool_size: 8
      event_buffer_size: 1024
  monitoring:
    enabled: true
    health:
      enabled: true
      interval: "30s"
      timeout: "5s"
      failure_threshold: 3
      success_threshold: 1
      checks:
        - name: "system_resources"
          check_type: "SystemResource"
          weight: 0.3
          critical: true
          config:
            max_cpu_percent: "80"
            max_memory_percent: "85"
    metrics:
      enabled: true
      collection_interval: "15s"
      retention_period: "1h"
      custom_metrics: []
      histogram_buckets: [0.001, 0.01, 0.1, 1.0, 10.0]
      exporters:
        - name: "prometheus"
          exporter_type: "Prometheus"
          enabled: true
          config:
            endpoint: "/metrics"
            port: "9090"
    endpoints:
      health_endpoint: "/health"
      metrics_endpoint: "/metrics"
      port: 8080
      bind_address: "0.0.0.0"
      tls_enabled: false
"#;

    fs::write(&config_file, config_yaml).unwrap();

    // Load configuration through ConfigManager
    let manager = ConfigManager::new();
    let config = manager.load_from_file(&config_file).await.unwrap();

    // Verify monitoring configuration was loaded correctly
    assert!(config.eventflux.monitoring.is_some());

    let monitoring = config.eventflux.monitoring.unwrap();
    assert!(monitoring.enabled);
    assert!(monitoring.health.enabled);
    assert_eq!(monitoring.health.interval, Duration::from_secs(30));
    assert_eq!(monitoring.health.timeout, Duration::from_secs(5));
    assert_eq!(monitoring.health.failure_threshold, 3);

    assert!(monitoring.metrics.enabled);
    assert_eq!(
        monitoring.metrics.collection_interval,
        Duration::from_secs(15)
    );
    assert_eq!(
        monitoring.metrics.retention_period,
        Duration::from_secs(3600)
    );
    assert_eq!(monitoring.metrics.exporters.len(), 1);
    assert_eq!(monitoring.metrics.exporters[0].name, "prometheus");

    assert_eq!(monitoring.endpoints.health_endpoint, "/health");
    assert_eq!(monitoring.endpoints.metrics_endpoint, "/metrics");
    assert_eq!(monitoring.endpoints.port, 8080);
    assert_eq!(monitoring.endpoints.bind_address, "0.0.0.0");
    assert!(!monitoring.endpoints.tls_enabled);
}

/// Example showing comprehensive monitoring configuration for production
#[test]
fn test_comprehensive_production_monitoring() {
    let config = MonitoringConfig::production();

    // Verify comprehensive production setup
    assert!(config.enabled);

    // Health checks configured for production reliability
    assert_eq!(config.health.interval, Duration::from_secs(60));
    assert_eq!(config.health.failure_threshold, 5); // More tolerant in prod
    assert_eq!(config.health.success_threshold, 2); // Require sustained health
    assert_eq!(config.health.checks.len(), 3); // All critical checks enabled

    // Metrics collection optimized for production
    assert_eq!(config.metrics.collection_interval, Duration::from_secs(30));
    assert_eq!(config.metrics.retention_period, Duration::from_secs(86400)); // 24 hours
    assert_eq!(config.metrics.exporters.len(), 2); // Prometheus + OpenTelemetry

    // Alerting enabled with rules
    let alerting = config.alerting.unwrap();
    assert!(alerting.enabled);
    assert_eq!(alerting.evaluation_interval, Duration::from_secs(60));
    assert_eq!(alerting.rules.len(), 4); // Default production rules

    // Observability configured for production
    let observability = config.observability.unwrap();
    assert_eq!(observability.tracing.sample_rate, 0.1); // 10% sampling
    assert_eq!(observability.logging.level, "info");

    // Endpoints secured
    assert_eq!(config.endpoints.port, 8443);
    assert!(config.endpoints.tls_enabled);

    println!("Production monitoring configuration:");
    println!(
        "  - Health checks every {} seconds with {} failure threshold",
        config.health.interval.as_secs(),
        config.health.failure_threshold
    );
    println!(
        "  - Metrics collection every {} seconds",
        config.metrics.collection_interval.as_secs()
    );
    println!(
        "  - {} metrics exporters configured",
        config.metrics.exporters.len()
    );
    println!("  - Alerting enabled: {}", alerting.enabled);
    println!("  - TLS enabled: {}", config.endpoints.tls_enabled);
    println!(
        "  - Tracing sample rate: {}%",
        observability.tracing.sample_rate * 100.0
    );
}
