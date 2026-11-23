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

// src/core/distributed/runtime_mode.rs

//! Runtime mode selection.
//!
//! Lets EventFlux run in single node, distributed or hybrid mode with no code
//! changes. The mode is picked from the settings when the manager is created.

use super::{DistributedConfig, DistributedError, DistributedResult, RuntimeMode};
use std::sync::Arc;

/// Sets up, checks and shuts down the current runtime mode.
#[allow(dead_code)]
pub struct RuntimeModeManager {
    mode: RuntimeMode,

    config: Arc<DistributedConfig>,

    implementation: Box<dyn RuntimeModeImpl>,
}

impl RuntimeModeManager {
    /// Create a manager for the mode in `config`.
    ///
    /// # Errors
    ///
    /// Returns `ConfigurationError` if single node or hybrid mode has cluster
    /// settings, or if distributed mode is missing cluster or node settings.
    pub fn new(config: DistributedConfig) -> DistributedResult<Self> {
        let mode = config.mode;
        let config = Arc::new(config);

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

    pub fn mode(&self) -> RuntimeMode {
        self.mode
    }

    /// Returns true in distributed or hybrid mode.
    pub fn is_distributed(&self) -> bool {
        matches!(self.mode, RuntimeMode::Distributed | RuntimeMode::Hybrid)
    }

    pub async fn initialize(&mut self) -> DistributedResult<()> {
        self.implementation.initialize().await
    }

    pub async fn shutdown(&mut self) -> DistributedResult<()> {
        self.implementation.shutdown().await
    }

    /// Returns what the current mode says it supports.
    pub fn capabilities(&self) -> ModeCapabilities {
        self.implementation.capabilities()
    }

    pub async fn health_check(&self) -> DistributedResult<HealthStatus> {
        self.implementation.health_check().await
    }
}

/// One runtime mode behind [`RuntimeModeManager`].
#[async_trait::async_trait]
pub trait RuntimeModeImpl: Send + Sync {
    async fn initialize(&mut self) -> DistributedResult<()>;

    async fn shutdown(&mut self) -> DistributedResult<()>;

    /// Returns what this mode says it supports.
    fn capabilities(&self) -> ModeCapabilities;

    async fn health_check(&self) -> DistributedResult<HealthStatus>;
}

/// What a runtime mode says it supports.
///
/// The default describes one node with no distributed features and no exactly
/// once processing.
#[derive(Debug, Clone)]
pub struct ModeCapabilities {
    /// Queries can run across nodes.
    pub distributed_queries: bool,

    /// State can be shared across nodes.
    pub distributed_state: bool,

    /// Work can move to another node when one fails.
    pub failover: bool,

    /// Nodes can be added or removed while running.
    pub dynamic_scaling: bool,

    /// Most nodes supported. `None` means no limit.
    pub max_nodes: Option<usize>,

    /// Each event is processed exactly once.
    pub exactly_once: bool,

    /// Transactions can span nodes.
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

/// Health of the runtime mode.
#[derive(Debug, Clone)]
pub struct HealthStatus {
    /// Overall health.
    pub healthy: bool,

    pub mode_status: ModeStatus,

    pub components: Vec<ComponentHealth>,

    pub warnings: Vec<String>,
}

/// Status details for each mode.
#[derive(Debug, Clone)]
pub enum ModeStatus {
    SingleNode {
        uptime: std::time::Duration,
        memory_usage: usize,
        cpu_usage: f64,
    },

    Distributed {
        cluster_size: usize,
        healthy_nodes: usize,
        leader_node: Option<String>,
        partition_status: String,
    },

    Hybrid {
        local_status: Box<ModeStatus>,
        remote_status: Box<ModeStatus>,
    },
}

/// Health of one component.
#[derive(Debug, Clone)]
pub struct ComponentHealth {
    pub name: String,

    pub healthy: bool,

    /// Short status text.
    pub status: String,

    /// When the component was last checked.
    pub last_check: std::time::Instant,
}

/// Single node mode.
#[allow(dead_code)]
struct SingleNodeMode {
    config: Arc<DistributedConfig>,
    start_time: std::time::Instant,
}

impl SingleNodeMode {
    /// # Errors
    ///
    /// Returns `ConfigurationError` if cluster settings are present.
    fn new(config: Arc<DistributedConfig>) -> DistributedResult<Self> {
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
        // Nothing to set up beyond logging.
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

    /// Always reports healthy with fixed component statuses. Only uptime is real.
    async fn health_check(&self) -> DistributedResult<HealthStatus> {
        Ok(HealthStatus {
            healthy: true,
            mode_status: ModeStatus::SingleNode {
                uptime: self.start_time.elapsed(),
                memory_usage: 0, // not measured yet
                cpu_usage: 0.0,  // not measured yet
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

/// Distributed mode.
///
/// No coordinator is created and the node does not join a cluster yet.
#[allow(dead_code)]
struct DistributedMode {
    config: Arc<DistributedConfig>,
    /// Always `None` for now.
    coordinator: Option<Arc<dyn super::coordinator::DistributedCoordinator>>,
    start_time: std::time::Instant,
}

impl DistributedMode {
    /// # Errors
    ///
    /// Returns `ConfigurationError` if cluster or node settings are missing.
    fn new(config: Arc<DistributedConfig>) -> DistributedResult<Self> {
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

        // TODO: create the coordinator from the settings and join the cluster.
        // self.coordinator = Some(create_coordinator(&self.config)?);

        // self.coordinator.join_cluster().await?;

        Ok(())
    }

    async fn shutdown(&mut self) -> DistributedResult<()> {
        println!("Shutting down distributed mode");

        // TODO: leave the cluster cleanly once a coordinator exists.
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

    /// Always reports healthy with fixed component statuses. Cluster size comes
    /// from `expected_size` in the settings, every node counts as healthy, and this
    /// node is reported as leader.
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
                healthy_nodes: cluster_size, // not read from a coordinator yet
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

/// Hybrid mode, meant for local processing with shared state.
///
/// Wraps single node mode. No shared state backend is created yet.
#[allow(dead_code)]
struct HybridMode {
    config: Arc<DistributedConfig>,
    single_node: SingleNodeMode,
    /// Always `None` for now.
    distributed_state: Option<Arc<dyn super::state_backend::StateBackend>>,
}

impl HybridMode {
    /// # Errors
    ///
    /// Returns `ConfigurationError` if cluster settings are present, because the
    /// inner single node mode rejects them.
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

        self.single_node.initialize().await?;

        // TODO: create the shared state backend from the settings.
        // self.distributed_state = Some(create_state_backend(&self.config)?);

        Ok(())
    }

    async fn shutdown(&mut self) -> DistributedResult<()> {
        println!("Shutting down hybrid mode");

        // TODO: shut down the shared state backend once it exists.
        // if let Some(state) = &self.distributed_state {
        //     state.shutdown().await?;
        // }

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

    /// Reports the local single node health. The remote status is a zeroed
    /// placeholder.
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
        let mut config = DistributedConfig {
            mode: RuntimeMode::Distributed,
            ..Default::default()
        };

        // Fails without cluster settings.
        let result = RuntimeModeManager::new(config.clone());
        assert!(result.is_err());

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
        let config = DistributedConfig {
            mode: RuntimeMode::Hybrid,
            ..Default::default()
        };

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
