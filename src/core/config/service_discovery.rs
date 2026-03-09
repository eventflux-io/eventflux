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

//! Service Discovery for Distributed EventFlux Clusters
//!
//! Provides service discovery mechanisms for distributed EventFlux deployments
//! including Kubernetes, Consul, Etcd, and static configuration support.

use crate::core::config::{ConfigError, ConfigResult};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Service discovery provider trait
#[async_trait::async_trait]
pub trait ServiceDiscovery: Send + Sync + std::fmt::Debug {
    /// Get the provider name
    fn name(&self) -> &str;

    /// Register this node with the service discovery system
    async fn register_node(&self, node_info: NodeInfo) -> ConfigResult<()>;

    /// Deregister this node from the service discovery system
    async fn deregister_node(&self, node_id: &str) -> ConfigResult<()>;

    /// Discover available nodes in the cluster
    async fn discover_nodes(&self) -> ConfigResult<Vec<NodeInfo>>;

    /// Watch for changes in cluster topology
    async fn watch_cluster_changes(&self) -> ConfigResult<ClusterChangeStream>;

    /// Health check for the service discovery system
    async fn health_check(&self) -> ConfigResult<ServiceHealth>;

    /// Get provider priority (higher = more important)
    fn priority(&self) -> u32 {
        50
    }
}

/// Information about a cluster node
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    /// Unique node identifier
    pub id: String,

    /// Node's network address
    pub address: SocketAddr,

    /// Node capabilities and metadata
    pub metadata: HashMap<String, String>,

    /// Node roles in the cluster
    pub roles: Vec<NodeRole>,

    /// Current health status
    pub health: NodeHealth,

    /// Node version
    pub version: String,

    /// Timestamp when node was last seen
    pub last_seen: std::time::SystemTime,
}

/// Node roles in a distributed cluster
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NodeRole {
    /// Coordinator node (can participate in leader election)
    Coordinator,

    /// Worker node (processes queries)
    Worker,

    /// Gateway node (handles external requests)
    Gateway,

    /// Storage node (manages persistent state)
    Storage,

    /// Custom role with specific capabilities
    Custom(String),
}

/// Node health status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeHealth {
    /// Node is healthy and ready
    Healthy,

    /// Node is starting up
    Starting,

    /// Node is shutting down gracefully
    Draining,

    /// Node is unhealthy but may recover
    Unhealthy,

    /// Node is unreachable
    Unreachable,
}

/// Service discovery health status
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceHealth {
    /// Overall health status
    pub status: HealthStatus,

    /// Health check message
    pub message: String,

    /// Number of discovered nodes
    pub node_count: usize,

    /// Last successful discovery time
    pub last_discovery: std::time::SystemTime,
}

/// Health status levels
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

/// Stream of cluster topology changes
pub type ClusterChangeStream = Box<dyn futures::Stream<Item = ClusterChange> + Send + Unpin>;

/// Types of cluster changes
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterChange {
    /// Node joined the cluster
    NodeJoined(NodeInfo),

    /// Node left the cluster
    NodeLeft(String), // node_id

    /// Node health status changed
    NodeHealthChanged {
        node_id: String,
        old_health: NodeHealth,
        new_health: NodeHealth,
    },

    /// Node metadata updated
    NodeUpdated(NodeInfo),

    /// Leadership change
    LeadershipChanged {
        old_leader: Option<String>,
        new_leader: Option<String>,
    },
}

/// Kubernetes service discovery implementation
#[cfg(feature = "kubernetes")]
#[derive(Debug, Clone)]
pub struct KubernetesServiceDiscovery {
    /// Namespace to discover services in
    namespace: String,

    /// Service label selector
    label_selector: String,

    /// Port name for EventFlux communication
    port_name: String,

    /// Cached node information
    cached_nodes: Arc<RwLock<Vec<NodeInfo>>>,
}

#[cfg(feature = "kubernetes")]
impl KubernetesServiceDiscovery {
    /// Create a new Kubernetes service discovery
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            label_selector: "app=eventflux".to_string(),
            port_name: "eventflux".to_string(),
            cached_nodes: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Set the label selector for discovering services
    pub fn with_label_selector(mut self, selector: impl Into<String>) -> Self {
        self.label_selector = selector.into();
        self
    }

    /// Set the port name for EventFlux communication
    pub fn with_port_name(mut self, port_name: impl Into<String>) -> Self {
        self.port_name = port_name.into();
        self
    }

    /// Discover nodes from Kubernetes endpoints
    async fn discover_from_k8s(&self) -> ConfigResult<Vec<NodeInfo>> {
        use k8s_openapi::api::core::v1::{Endpoints, Pod};
        use kube::{Api, Client};

        let client = Client::try_default().await.map_err(|e| {
            ConfigError::internal_error(format!("Failed to create K8s client: {}", e))
        })?;

        let endpoints_api: Api<Endpoints> = Api::namespaced(client.clone(), &self.namespace);
        let pods_api: Api<Pod> = Api::namespaced(client, &self.namespace);

        // List endpoints matching our label selector
        let lp = kube::api::ListParams::default().labels(&self.label_selector);
        let endpoints_list = endpoints_api
            .list(&lp)
            .await
            .map_err(|e| ConfigError::internal_error(format!("Failed to list endpoints: {}", e)))?;

        let mut nodes = Vec::new();

        for endpoint in endpoints_list.items {
            if let Some(subsets) = endpoint.subsets {
                for subset in subsets {
                    if let Some(addresses) = subset.addresses {
                        for address in addresses {
                            if let Some(ip) = address.ip {
                                // Find the appropriate port
                                let port = subset
                                    .ports
                                    .as_ref()
                                    .and_then(|ports| {
                                        ports
                                            .iter()
                                            .find(|p| p.name.as_deref() == Some(&self.port_name))
                                    })
                                    .map(|p| p.port as u16)
                                    .unwrap_or(8080); // Default port

                                let socket_addr =
                                    format!("{}:{}", ip, port).parse().map_err(|e| {
                                        ConfigError::internal_error(format!(
                                            "Invalid address: {}",
                                            e
                                        ))
                                    })?;

                                // Get additional metadata from the pod if available
                                let mut metadata = HashMap::new();
                                let mut roles = vec![NodeRole::Worker]; // Default role

                                if let Some(target_ref) = &address.target_ref {
                                    if target_ref.kind == "Pod" {
                                        if let Ok(pod) = pods_api.get(&target_ref.name).await {
                                            // Extract metadata from pod labels and annotations
                                            if let Some(labels) = pod.metadata.labels {
                                                for (key, value) in labels {
                                                    metadata
                                                        .insert(format!("label.{}", key), value);
                                                }

                                                // Determine roles from labels
                                                roles = Self::extract_roles_from_labels(&metadata);
                                            }

                                            if let Some(annotations) = pod.metadata.annotations {
                                                for (key, value) in annotations {
                                                    metadata.insert(
                                                        format!("annotation.{}", key),
                                                        value,
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }

                                let node_id = address
                                    .target_ref
                                    .as_ref()
                                    .map(|r| r.name.clone())
                                    .unwrap_or_else(|| format!("node-{}", ip.replace('.', "-")));

                                nodes.push(NodeInfo {
                                    id: node_id,
                                    address: socket_addr,
                                    metadata,
                                    roles,
                                    health: NodeHealth::Healthy, // Assume healthy if in endpoints
                                    version: env!("CARGO_PKG_VERSION").to_string(),
                                    last_seen: std::time::SystemTime::now(),
                                });
                            }
                        }
                    }
                }
            }
        }

        Ok(nodes)
    }

    /// Extract node roles from Kubernetes labels
    fn extract_roles_from_labels(metadata: &HashMap<String, String>) -> Vec<NodeRole> {
        let mut roles = vec![NodeRole::Worker]; // Default

        if let Some(role_str) = metadata.get("label.eventflux.role") {
            roles.clear();
            for role in role_str.split(',') {
                match role.trim() {
                    "coordinator" => roles.push(NodeRole::Coordinator),
                    "worker" => roles.push(NodeRole::Worker),
                    "gateway" => roles.push(NodeRole::Gateway),
                    "storage" => roles.push(NodeRole::Storage),
                    custom => roles.push(NodeRole::Custom(custom.to_string())),
                }
            }
        }

        roles
    }
}

#[cfg(feature = "kubernetes")]
#[async_trait::async_trait]
impl ServiceDiscovery for KubernetesServiceDiscovery {
    fn name(&self) -> &str {
        "kubernetes"
    }

    async fn register_node(&self, node_info: NodeInfo) -> ConfigResult<()> {
        // In Kubernetes, registration is handled by the platform
        // We could create/update a custom resource or annotation here
        println!(
            "Node registration handled by Kubernetes platform: {}",
            node_info.id
        );
        Ok(())
    }

    async fn deregister_node(&self, node_id: &str) -> ConfigResult<()> {
        // In Kubernetes, deregistration is handled by the platform
        println!(
            "Node deregistration handled by Kubernetes platform: {}",
            node_id
        );
        Ok(())
    }

    async fn discover_nodes(&self) -> ConfigResult<Vec<NodeInfo>> {
        let nodes = self.discover_from_k8s().await?;

        // Update cache
        {
            let mut cache = self.cached_nodes.write().await;
            *cache = nodes.clone();
        }

        Ok(nodes)
    }

    async fn watch_cluster_changes(&self) -> ConfigResult<ClusterChangeStream> {
        // Implementation pending: Kubernetes watch for real-time cluster changes
        // Future implementation will use the Kubernetes watch API to stream changes
        use futures::stream;

        let stream = stream::empty(); // Stub implementation
        Ok(Box::new(stream))
    }

    async fn health_check(&self) -> ConfigResult<ServiceHealth> {
        // Try to discover nodes as a health check
        match self.discover_nodes().await {
            Ok(nodes) => Ok(ServiceHealth {
                status: HealthStatus::Healthy,
                message: format!("Successfully discovered {} nodes", nodes.len()),
                node_count: nodes.len(),
                last_discovery: std::time::SystemTime::now(),
            }),
            Err(e) => Ok(ServiceHealth {
                status: HealthStatus::Unhealthy,
                message: format!("Discovery failed: {}", e),
                node_count: 0,
                last_discovery: std::time::SystemTime::now(),
            }),
        }
    }

    fn priority(&self) -> u32 {
        80 // High priority in Kubernetes environments
    }
}

/// Static service discovery (for testing or simple deployments)
#[derive(Debug, Clone)]
pub struct StaticServiceDiscovery {
    /// Static list of nodes
    nodes: Vec<NodeInfo>,
}

impl StaticServiceDiscovery {
    /// Create a new static service discovery with a list of nodes
    pub fn new(nodes: Vec<NodeInfo>) -> Self {
        Self { nodes }
    }

    /// Add a node to the static list
    pub fn add_node(&mut self, node: NodeInfo) {
        self.nodes.push(node);
    }
}

#[async_trait::async_trait]
impl ServiceDiscovery for StaticServiceDiscovery {
    fn name(&self) -> &str {
        "static"
    }

    async fn register_node(&self, node_info: NodeInfo) -> ConfigResult<()> {
        println!("Static discovery: Node {} registered (no-op)", node_info.id);
        Ok(())
    }

    async fn deregister_node(&self, node_id: &str) -> ConfigResult<()> {
        println!("Static discovery: Node {} deregistered (no-op)", node_id);
        Ok(())
    }

    async fn discover_nodes(&self) -> ConfigResult<Vec<NodeInfo>> {
        Ok(self.nodes.clone())
    }

    async fn watch_cluster_changes(&self) -> ConfigResult<ClusterChangeStream> {
        // Static discovery doesn't have changes
        use futures::stream;
        Ok(Box::new(stream::empty()))
    }

    async fn health_check(&self) -> ConfigResult<ServiceHealth> {
        Ok(ServiceHealth {
            status: HealthStatus::Healthy,
            message: format!("Static configuration with {} nodes", self.nodes.len()),
            node_count: self.nodes.len(),
            last_discovery: std::time::SystemTime::now(),
        })
    }

    fn priority(&self) -> u32 {
        10 // Low priority, fallback option
    }
}

/// Service discovery manager that coordinates multiple providers
#[derive(Debug)]
pub struct ServiceDiscoveryManager {
    /// Registered service discovery providers
    providers: Vec<Box<dyn ServiceDiscovery>>,

    /// Current active provider
    active_provider: Option<usize>,

    /// Cached cluster topology
    cached_topology: Arc<RwLock<Vec<NodeInfo>>>,
}

impl ServiceDiscoveryManager {
    /// Create a new service discovery manager
    pub fn new() -> Self {
        Self {
            providers: Vec::new(),
            active_provider: None,
            cached_topology: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Add a service discovery provider
    pub fn add_provider(&mut self, provider: Box<dyn ServiceDiscovery>) {
        self.providers.push(provider);
        // Sort by priority (highest first)
        self.providers
            .sort_by_key(|p| std::cmp::Reverse(p.priority()));
    }

    /// Initialize service discovery by selecting the best available provider
    pub async fn initialize(&mut self) -> ConfigResult<()> {
        for (index, provider) in self.providers.iter().enumerate() {
            match provider.health_check().await {
                Ok(health) if health.status == HealthStatus::Healthy => {
                    self.active_provider = Some(index);
                    println!("Selected service discovery provider: {}", provider.name());
                    return Ok(());
                }
                Ok(health) => {
                    println!("Provider {} unhealthy: {}", provider.name(), health.message);
                }
                Err(e) => {
                    println!("Provider {} failed health check: {}", provider.name(), e);
                }
            }
        }

        Err(ConfigError::internal_error(
            "No healthy service discovery provider found".to_string(),
        ))
    }

    /// Get the active service discovery provider
    fn get_active_provider(&self) -> ConfigResult<&dyn ServiceDiscovery> {
        match self.active_provider {
            Some(index) => Ok(self.providers[index].as_ref()),
            None => Err(ConfigError::internal_error(
                "No active service discovery provider".to_string(),
            )),
        }
    }

    /// Discover nodes in the cluster
    pub async fn discover_nodes(&self) -> ConfigResult<Vec<NodeInfo>> {
        let provider = self.get_active_provider()?;
        let nodes = provider.discover_nodes().await?;

        // Update cache
        {
            let mut cache = self.cached_topology.write().await;
            *cache = nodes.clone();
        }

        Ok(nodes)
    }

    /// Get cached topology without performing discovery
    pub async fn get_cached_topology(&self) -> Vec<NodeInfo> {
        let cache = self.cached_topology.read().await;
        cache.clone()
    }

    /// Register this node with the active provider
    pub async fn register_node(&self, node_info: NodeInfo) -> ConfigResult<()> {
        let provider = self.get_active_provider()?;
        provider.register_node(node_info).await
    }

    /// Deregister this node from the active provider
    pub async fn deregister_node(&self, node_id: &str) -> ConfigResult<()> {
        let provider = self.get_active_provider()?;
        provider.deregister_node(node_id).await
    }
}

impl Default for ServiceDiscoveryManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_node_info_creation() {
        let mut metadata = HashMap::new();
        metadata.insert("datacenter".to_string(), "us-west-1".to_string());

        let node = NodeInfo {
            id: "node-1".to_string(),
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 1, 100)), 8080),
            metadata,
            roles: vec![NodeRole::Worker, NodeRole::Coordinator],
            health: NodeHealth::Healthy,
            version: "1.0.0".to_string(),
            last_seen: std::time::SystemTime::now(),
        };

        assert_eq!(node.id, "node-1");
        assert_eq!(node.address.port(), 8080);
        assert!(node.roles.contains(&NodeRole::Worker));
        assert!(node.roles.contains(&NodeRole::Coordinator));
        assert_eq!(node.health, NodeHealth::Healthy);
    }

    #[tokio::test]
    async fn test_static_service_discovery() {
        let nodes = vec![
            NodeInfo {
                id: "node-1".to_string(),
                address: "10.0.1.100:8080".parse().unwrap(),
                metadata: HashMap::new(),
                roles: vec![NodeRole::Worker],
                health: NodeHealth::Healthy,
                version: "1.0.0".to_string(),
                last_seen: std::time::SystemTime::now(),
            },
            NodeInfo {
                id: "node-2".to_string(),
                address: "10.0.1.101:8080".parse().unwrap(),
                metadata: HashMap::new(),
                roles: vec![NodeRole::Coordinator],
                health: NodeHealth::Healthy,
                version: "1.0.0".to_string(),
                last_seen: std::time::SystemTime::now(),
            },
        ];

        let discovery = StaticServiceDiscovery::new(nodes.clone());

        assert_eq!(discovery.name(), "static");
        assert_eq!(discovery.priority(), 10);

        let discovered = discovery.discover_nodes().await.unwrap();
        assert_eq!(discovered.len(), 2);
        assert_eq!(discovered[0].id, "node-1");
        assert_eq!(discovered[1].id, "node-2");

        let health = discovery.health_check().await.unwrap();
        assert_eq!(health.status, HealthStatus::Healthy);
        assert_eq!(health.node_count, 2);
    }

    #[tokio::test]
    async fn test_service_discovery_manager() {
        let nodes = vec![NodeInfo {
            id: "test-node".to_string(),
            address: "127.0.0.1:8080".parse().unwrap(),
            metadata: HashMap::new(),
            roles: vec![NodeRole::Worker],
            health: NodeHealth::Healthy,
            version: "1.0.0".to_string(),
            last_seen: std::time::SystemTime::now(),
        }];

        let mut manager = ServiceDiscoveryManager::new();
        manager.add_provider(Box::new(StaticServiceDiscovery::new(nodes)));

        // Initialize should succeed
        manager.initialize().await.unwrap();

        // Should be able to discover nodes
        let discovered = manager.discover_nodes().await.unwrap();
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].id, "test-node");

        // Should have cached topology
        let cached = manager.get_cached_topology().await;
        assert_eq!(cached.len(), 1);
    }

    #[cfg(feature = "kubernetes")]
    #[test]
    fn test_kubernetes_service_discovery_creation() {
        let discovery = KubernetesServiceDiscovery::new("eventflux-system")
            .with_label_selector("app=eventflux,component=worker")
            .with_port_name("grpc");

        assert_eq!(discovery.name(), "kubernetes");
        assert_eq!(discovery.namespace, "eventflux-system");
        assert_eq!(discovery.label_selector, "app=eventflux,component=worker");
        assert_eq!(discovery.port_name, "grpc");
        assert_eq!(discovery.priority(), 80);
    }
}
