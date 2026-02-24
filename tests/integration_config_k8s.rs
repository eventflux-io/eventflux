// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for Kubernetes configuration management
//!
//! These tests demonstrate how the configuration system works with Kubernetes
//! ConfigMaps, Secrets, and cloud-native patterns.

#[cfg(feature = "kubernetes")]
use eventflux::core::config::{
    loader::KubernetesConfigMapLoader, ApplicationConfig, ConfigManager, EventFluxConfig,
    PerformanceConfig, RuntimeMode,
};

#[cfg(feature = "kubernetes")]
use std::collections::HashMap;

/// Example test showing how to create a configuration manager for Kubernetes
#[tokio::test]
#[cfg(feature = "kubernetes")]
async fn test_kubernetes_config_manager_creation() {
    let manager = ConfigManager::builder()
        .with_kubernetes_configmap("eventflux-config")
        .with_environment_vars_prefix("EVENTFLUX_")
        .validation_enabled(true)
        .build();

    let loaders = manager.list_loaders();
    println!("Available loaders:");
    for loader in &loaders {
        println!(
            "  - {} (priority: {}, available: {})",
            loader.description, loader.priority, loader.available
        );
    }

    // Should have Kubernetes ConfigMap loader
    assert!(loaders
        .iter()
        .any(|l| l.description.contains("Kubernetes ConfigMap")));

    // Should have environment variables loader
    assert!(loaders
        .iter()
        .any(|l| l.description.contains("Environment variables")));
}

/// Test showing cloud-native configuration precedence
#[tokio::test]
#[cfg(feature = "kubernetes")]
async fn test_cloud_native_precedence() {
    // This test would work in a real Kubernetes environment with proper ConfigMaps
    // For now, it demonstrates the precedence logic

    // Set environment variable override
    std::env::set_var("EVENTFLUX_RUNTIME_PERFORMANCE_THREAD_POOL_SIZE", "32");

    let manager = ConfigManager::builder()
        .with_kubernetes_configmap("eventflux-config")
        .with_environment_vars_prefix("EVENTFLUX_")
        .build();

    // In a real environment, this would load from K8s ConfigMap first, then override with env vars
    let result = manager.list_loaders();

    // Verify precedence order (higher priority numbers should come later)
    let mut priorities: Vec<u32> = result.iter().map(|l| l.priority).collect();
    let original_priorities = priorities.clone();
    priorities.sort();
    assert_eq!(
        priorities, original_priorities,
        "Loaders should be ordered by priority"
    );

    std::env::remove_var("EVENTFLUX_RUNTIME_PERFORMANCE_THREAD_POOL_SIZE");
}

/// Example of configuring EventFlux for Kubernetes deployment
#[test]
#[cfg(feature = "kubernetes")]
fn test_kubernetes_deployment_config() {
    let mut config = EventFluxConfig::default();

    // Configure for Kubernetes deployment
    config.eventflux.runtime.mode = RuntimeMode::Distributed;
    config.eventflux.runtime.performance = PerformanceConfig {
        thread_pool_size: 16,
        event_buffer_size: 500000,
        batch_processing: true,
        batch_size: Some(1000),
        async_processing: true,
        backpressure_strategy: crate::core::config::BackpressureStrategy::Block,
    };

    // Add resource limits (typical for Kubernetes)
    config.eventflux.runtime.resources = Some(crate::core::config::ResourceConfig {
        memory: Some(crate::core::config::MemoryConfig {
            max_heap: "2Gi".to_string(),
            initial_heap: Some("1Gi".to_string()),
            gc_strategy: crate::core::config::GcStrategy::Adaptive,
        }),
        cpu: Some(crate::core::config::CpuConfig {
            max_cpu: "2".to_string(),
            requests: Some("1".to_string()),
        }),
        network: Some(crate::core::config::NetworkConfig {
            max_bandwidth: 1_000_000_000, // 1 Gbps
            max_connections: Some(10000),
        }),
        storage: Some(crate::core::config::StorageConfig {
            max_capacity: "10Gi".to_string(),
            storage_class: Some("fast-ssd".to_string()),
        }),
    });

    // Verify distributed configuration
    assert!(config.is_distributed_mode());
    assert_eq!(config.eventflux.runtime.performance.thread_pool_size, 16);

    // Verify resource configuration
    let resources = config.eventflux.runtime.resources.as_ref().unwrap();
    assert_eq!(resources.memory.as_ref().unwrap().max_heap, "2Gi");
    assert_eq!(resources.cpu.as_ref().unwrap().max_cpu, "2");
}

/// Test configuration validation for Kubernetes environments
#[test]
#[cfg(feature = "kubernetes")]
fn test_kubernetes_config_validation() {
    use crate::core::config::validator::{
        ApplicationValidationRule, ConfigValidator, SecurityValidationRule,
    };

    let mut validator = ConfigValidator::new();
    validator.add_rule(Box::new(ApplicationValidationRule));
    validator.add_rule(Box::new(SecurityValidationRule));

    let mut config = EventFluxConfig::default();
    config.eventflux.runtime.mode = RuntimeMode::Distributed;

    // This should fail validation because distributed mode requires distributed config
    let result = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async { validator.validate(&config).await });

    assert!(!result.is_valid());
    assert!(result
        .errors
        .iter()
        .any(|e| e.to_string().contains("distributed")));

    // Add distributed configuration
    config.eventflux.distributed = Some(crate::core::config::DistributedConfig::default());

    let result2 = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async { validator.validate(&config).await });

    assert!(result2.is_valid());
}

/// Example showing how applications would use the Kubernetes configuration
#[test]
#[cfg(feature = "kubernetes")]
fn test_application_usage_pattern() {
    use crate::core::config::eventflux_app_context::EventFluxAppContext;
    use crate::core::config::eventflux_context::EventFluxContext;
    use crate::query_api::eventflux_app::EventFluxApp;
    use std::sync::Arc;

    // Create a configuration for Kubernetes
    let mut config = EventFluxConfig::default();
    config.eventflux.runtime.mode = RuntimeMode::Distributed;
    config.eventflux.runtime.performance.thread_pool_size = 24;
    config.eventflux.runtime.performance.event_buffer_size = 2000000;

    // Add application-specific configuration
    let mut app_config = ApplicationConfig::default();
    app_config.persistence = Some(crate::core::config::PersistenceConfig {
        enabled: true,
        interval: Some(std::time::Duration::from_secs(60)),
        backend: "redis".to_string(),
        settings: {
            let mut settings = HashMap::new();
            settings.insert(
                "host".to_string(),
                "redis-cluster.eventflux.svc.cluster.local".to_string(),
            );
            settings.insert("port".to_string(), "6379".to_string());
            settings
        },
    });

    // Create EventFluxAppContext with configuration
    let eventflux_context = Arc::new(EventFluxContext::default());
    let eventflux_app = Arc::new(EventFluxApp::new("test-app".to_string()));
    let config_manager = Arc::new(
        ConfigManager::builder()
            .with_kubernetes_configmap("eventflux-config")
            .with_environment_vars()
            .build(),
    );

    let app_context = EventFluxAppContext::new_with_config(
        eventflux_context,
        "kubernetes-app".to_string(),
        eventflux_app,
        "/* EventFluxQL */".to_string(),
        Arc::new(config),
        Some(app_config),
        Some(config_manager),
    );

    // Verify configuration access
    assert_eq!(app_context.get_configured_thread_pool_size(), 24);
    assert_eq!(app_context.get_configured_event_buffer_size(), 2000000);
    assert!(app_context.is_distributed_mode());

    // Verify application-specific configuration
    let app_config_ref = app_context.get_app_config().unwrap();
    assert!(app_config_ref.persistence.as_ref().unwrap().enabled);

    println!("Kubernetes application context configured successfully:");
    println!("  - Runtime mode: {:?}", app_context.get_runtime_mode());
    println!(
        "  - Thread pool size: {}",
        app_context.get_configured_thread_pool_size()
    );
    println!(
        "  - Event buffer size: {}",
        app_context.get_configured_event_buffer_size()
    );
    println!(
        "  - Distributed mode: {}",
        app_context.is_distributed_mode()
    );
}
