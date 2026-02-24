// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/distributed/runtime_mode.rs

//! Runtime Mode Selection and Management
//!
//! This module implements the runtime mode selection mechanism that allows
//! EventFlux to operate in single-node or distributed mode without code changes.
//! Mode selection happens at initialization based on configuration.

use super::{DistributedConfig, DistributedError, DistributedResult, RuntimeMode};
use std::sync::Arc;

/// Runtime mode manager that handles mode-specific initialization
pub struct RuntimeModeManager {
    /// Current runtime mode
    mode: RuntimeMode,

    /// Configuration
    config: Arc<DistributedConfig>,

    /// Mode-specific implementation
    implementation: Box<dyn RuntimeModeImpl>,
}

impl RuntimeModeManager {
    /// Create a new runtime mode manager based on configuration
    pub fn new(config: DistributedConfig) -> DistributedResult<Self> {
        let mode = config.mode;
        let config = Arc::new(config);

        // Select implementation based on mode
        let implementation: Box<dyn RuntimeModeImpl> = match mode {
            RuntimeMode::SingleNode => Box::new(SingleNodeMode::new(Arc::clone(&config))?),
            RuntimeMode::Distributed => Box::new(DistributedMode::new(Arc::clone(&config))?),
            RuntimeMode::Hybrid => Box::new(HybridMode::new(Arc::clone(&config))?),
        };

        Ok(Self {
            mode,
            config,
            implementation,
        })
    }

    /// Get the current runtime mode
    pub fn mode(&self) -> RuntimeMode {
        self.mode
    }

    /// Check if running in distributed mode
    pub fn is_distributed(&self) -> bool {
        matches!(self.mode, RuntimeMode::Distributed | RuntimeMode::Hybrid)
    }

    /// Initialize the runtime mode
    pub async fn initialize(&mut self) -> DistributedResult<()> {
        self.implementation.initialize().await
    }

    /// Shutdown the runtime mode
    pub async fn shutdown(&mut self) -> DistributedResult<()> {
        self.implementation.shutdown().await
    }

    /// Get mode-specific capabilities
    pub fn capabilities(&self) -> ModeCapabilities {
        self.implementation.capabilities()
    }

    /// Perform mode-specific health check
    pub async fn health_check(&self) -> DistributedResult<HealthStatus> {
        self.implementation.health_check().await
    }
}

/// Trait for runtime mode implementations
#[async_trait::async_trait]
pub trait RuntimeModeImpl: Send + Sync {
    /// Initialize the mode
    async fn initialize(&mut self) -> DistributedResult<()>;

    /// Shutdown the mode
    async fn shutdown(&mut self) -> DistributedResult<()>;

    /// Get mode capabilities
    fn capabilities(&self) -> ModeCapabilities;

    /// Health check
    async fn health_check(&self) -> DistributedResult<HealthStatus>;
}

/// Capabilities provided by a runtime mode
#[derive(Debug, Clone)]
pub struct ModeCapabilities {
    /// Supports distributed queries
    pub distributed_queries: bool,

    /// Supports distributed state
    pub distributed_state: bool,

    /// Supports failover
    pub failover: bool,

    /// Supports dynamic scaling
    pub dynamic_scaling: bool,

    /// Maximum nodes supported
    pub max_nodes: Option<usize>,

    /// Supports exactly-once processing
    pub exactly_once: bool,

    /// Supports distributed transactions
    pub distributed_transactions: bool,
}

impl Default for ModeCapabilities {
    fn default() -> Self {
        Self {
            distributed_queries: false,
            distributed_state: false,
            failover: false,
            dynamic_scaling: false,
            max_nodes: Some(1),
            exactly_once: false,
            distributed_transactions: false,
        }
    }
}

/// Health status of the runtime mode
#[derive(Debug, Clone)]
pub struct HealthStatus {
    /// Overall health
    pub healthy: bool,

    /// Mode-specific status
    pub mode_status: ModeStatus,

    /// Component health
    pub components: Vec<ComponentHealth>,

    /// Warnings
    pub warnings: Vec<String>,
}

/// Mode-specific status information
#[derive(Debug, Clone)]
pub enum ModeStatus {
    /// Single-node status
    SingleNode {
        uptime: std::time::Duration,
        memory_usage: usize,
        cpu_usage: f64,
    },

    /// Distributed mode status
    Distributed {
        cluster_size: usize,
        healthy_nodes: usize,
        leader_node: Option<String>,
        partition_status: String,
    },

    /// Hybrid mode status
    Hybrid {
        local_status: Box<ModeStatus>,
        remote_status: Box<ModeStatus>,
    },
}

/// Component health information
#[derive(Debug, Clone)]
pub struct ComponentHealth {
    /// Component name
    pub name: String,

    /// Is healthy
    pub healthy: bool,

    /// Status message
    pub status: String,

    /// Last check time
    pub last_check: std::time::Instant,
}

/// Single-node mode implementation
struct SingleNodeMode {
    config: Arc<DistributedConfig>,
    start_time: std::time::Instant,
}

impl SingleNodeMode {
    fn new(config: Arc<DistributedConfig>) -> DistributedResult<Self> {
        // Validate single-node configuration
        if config.cluster.is_some() {
            return Err(DistributedError::ConfigurationError {
                message: "Cluster configuration not allowed in single-node mode".to_string(),
            });
        }

        Ok(Self {
            config,
            start_time: std::time::Instant::now(),
        })
    }
}

#[async_trait::async_trait]
impl RuntimeModeImpl for SingleNodeMode {
    async fn initialize(&mut self) -> DistributedResult<()> {
        // Single-node has minimal initialization
        println!("Initializing single-node mode");
        Ok(())
    }

    async fn shutdown(&mut self) -> DistributedResult<()> {
        println!("Shutting down single-node mode");
        Ok(())
    }

    fn capabilities(&self) -> ModeCapabilities {
        ModeCapabilities {
            distributed_queries: false,
            distributed_state: false,
            failover: false,
            dynamic_scaling: false,
            max_nodes: Some(1),
            exactly_once: true,
            distributed_transactions: false,
        }
    }

    async fn health_check(&self) -> DistributedResult<HealthStatus> {
        Ok(HealthStatus {
            healthy: true,
            mode_status: ModeStatus::SingleNode {
                uptime: self.start_time.elapsed(),
                memory_usage: 0, // Would get from system
                cpu_usage: 0.0,  // Would get from system
            },
            components: vec![
                ComponentHealth {
                    name: "EventProcessor".to_string(),
                    healthy: true,
                    status: "Running".to_string(),
                    last_check: std::time::Instant::now(),
                },
                ComponentHealth {
                    name: "StateStore".to_string(),
                    healthy: true,
                    status: "InMemory".to_string(),
                    last_check: std::time::Instant::now(),
                },
            ],
            warnings: vec![],
        })
    }
}

/// Distributed mode implementation
struct DistributedMode {
    config: Arc<DistributedConfig>,
    coordinator: Option<Arc<dyn super::coordinator::DistributedCoordinator>>,
    start_time: std::time::Instant,
}

impl DistributedMode {
    fn new(config: Arc<DistributedConfig>) -> DistributedResult<Self> {
        // Validate distributed configuration
        if config.cluster.is_none() {
            return Err(DistributedError::ConfigurationError {
                message: "Cluster configuration required for distributed mode".to_string(),
            });
        }

        if config.node.is_none() {
            return Err(DistributedError::ConfigurationError {
                message: "Node configuration required for distributed mode".to_string(),
            });
        }

        Ok(Self {
            config,
            coordinator: None,
            start_time: std::time::Instant::now(),
        })
    }
}

#[async_trait::async_trait]
impl RuntimeModeImpl for DistributedMode {
    async fn initialize(&mut self) -> DistributedResult<()> {
        println!("Initializing distributed mode");

        // Initialize coordinator based on configuration
        // This would create the actual coordinator implementation
        // self.coordinator = Some(create_coordinator(&self.config)?);

        // Join cluster
        // self.coordinator.join_cluster().await?;

        Ok(())
    }

    async fn shutdown(&mut self) -> DistributedResult<()> {
        println!("Shutting down distributed mode");

        // Leave cluster gracefully
        // if let Some(coordinator) = &self.coordinator {
        //     coordinator.leave_cluster().await?;
        // }

        Ok(())
    }

    fn capabilities(&self) -> ModeCapabilities {
        ModeCapabilities {
            distributed_queries: true,
            distributed_state: true,
            failover: true,
            dynamic_scaling: true,
            max_nodes: None,
            exactly_once: true,
            distributed_transactions: true,
        }
    }

    async fn health_check(&self) -> DistributedResult<HealthStatus> {
        let cluster_size = self
            .config
            .cluster
            .as_ref()
            .map(|c| c.expected_size)
            .unwrap_or(1);

        Ok(HealthStatus {
            healthy: true,
            mode_status: ModeStatus::Distributed {
                cluster_size,
                healthy_nodes: cluster_size, // Would get from coordinator
                leader_node: self.config.node.as_ref().map(|n| n.node_id.clone()),
                partition_status: "Healthy".to_string(),
            },
            components: vec![
                ComponentHealth {
                    name: "Coordinator".to_string(),
                    healthy: true,
                    status: "Leader".to_string(),
                    last_check: std::time::Instant::now(),
                },
                ComponentHealth {
                    name: "Transport".to_string(),
                    healthy: true,
                    status: "Connected".to_string(),
                    last_check: std::time::Instant::now(),
                },
                ComponentHealth {
                    name: "StateBackend".to_string(),
                    healthy: true,
                    status: "Synchronized".to_string(),
                    last_check: std::time::Instant::now(),
                },
            ],
            warnings: vec![],
        })
    }
}

/// Hybrid mode implementation (local processing with distributed state)
struct HybridMode {
    config: Arc<DistributedConfig>,
    single_node: SingleNodeMode,
    distributed_state: Option<Arc<dyn super::state_backend::StateBackend>>,
}

impl HybridMode {
    fn new(config: Arc<DistributedConfig>) -> DistributedResult<Self> {
        let single_node = SingleNodeMode::new(Arc::clone(&config))?;

        Ok(Self {
            config,
            single_node,
            distributed_state: None,
        })
    }
}

#[async_trait::async_trait]
impl RuntimeModeImpl for HybridMode {
    async fn initialize(&mut self) -> DistributedResult<()> {
        println!("Initializing hybrid mode");

        // Initialize single-node processing
        self.single_node.initialize().await?;

        // Initialize distributed state backend
        // self.distributed_state = Some(create_state_backend(&self.config)?);

        Ok(())
    }

    async fn shutdown(&mut self) -> DistributedResult<()> {
        println!("Shutting down hybrid mode");

        // Shutdown state backend
        // if let Some(state) = &self.distributed_state {
        //     state.shutdown().await?;
        // }

        // Shutdown single-node
        self.single_node.shutdown().await?;

        Ok(())
    }

    fn capabilities(&self) -> ModeCapabilities {
        ModeCapabilities {
            distributed_queries: false,
            distributed_state: true,
            failover: true,
            dynamic_scaling: false,
            max_nodes: Some(1),
            exactly_once: true,
            distributed_transactions: false,
        }
    }

    async fn health_check(&self) -> DistributedResult<HealthStatus> {
        let single_health = self.single_node.health_check().await?;

        Ok(HealthStatus {
            healthy: single_health.healthy,
            mode_status: ModeStatus::Hybrid {
                local_status: Box::new(single_health.mode_status.clone()),
                remote_status: Box::new(ModeStatus::SingleNode {
                    uptime: std::time::Duration::from_secs(0),
                    memory_usage: 0,
                    cpu_usage: 0.0,
                }),
            },
            components: single_health.components,
            warnings: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::distributed::{ClusterConfig, NodeCapabilities, NodeConfig, ResourceLimits};

    #[tokio::test]
    async fn test_single_node_mode() {
        let config = DistributedConfig::default();
        let mut manager = RuntimeModeManager::new(config).unwrap();

        assert_eq!(manager.mode(), RuntimeMode::SingleNode);
        assert!(!manager.is_distributed());

        assert!(manager.initialize().await.is_ok());

        let capabilities = manager.capabilities();
        assert!(!capabilities.distributed_queries);
        assert!(!capabilities.distributed_state);
        assert_eq!(capabilities.max_nodes, Some(1));

        let health = manager.health_check().await.unwrap();
        assert!(health.healthy);

        assert!(manager.shutdown().await.is_ok());
    }

    #[tokio::test]
    async fn test_distributed_mode_validation() {
        let mut config = DistributedConfig::default();
        config.mode = RuntimeMode::Distributed;

        // Should fail without cluster config
        let result = RuntimeModeManager::new(config.clone());
        assert!(result.is_err());

        // Add required configuration
        config.cluster = Some(ClusterConfig::default());
        config.node = Some(NodeConfig {
            node_id: "test-node".to_string(),
            endpoints: vec!["localhost:8080".to_string()],
            capabilities: NodeCapabilities::default(),
            resources: ResourceLimits::default(),
        });

        let manager = RuntimeModeManager::new(config).unwrap();
        assert_eq!(manager.mode(), RuntimeMode::Distributed);
        assert!(manager.is_distributed());

        let capabilities = manager.capabilities();
        assert!(capabilities.distributed_queries);
        assert!(capabilities.distributed_state);
        assert!(capabilities.failover);
    }

    #[tokio::test]
    async fn test_hybrid_mode() {
        let mut config = DistributedConfig::default();
        config.mode = RuntimeMode::Hybrid;

        let mut manager = RuntimeModeManager::new(config).unwrap();

        assert_eq!(manager.mode(), RuntimeMode::Hybrid);
        assert!(manager.is_distributed());

        assert!(manager.initialize().await.is_ok());

        let capabilities = manager.capabilities();
        assert!(!capabilities.distributed_queries);
        assert!(capabilities.distributed_state);
        assert!(capabilities.failover);

        assert!(manager.shutdown().await.is_ok());
    }
}
