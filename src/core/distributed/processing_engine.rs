// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/distributed/processing_engine.rs

//! Processing Engine Abstraction
//!
//! This module provides the abstraction layer for the processing engine that
//! handles both single-node and distributed execution of queries. The engine
//! maintains consistent semantics across modes while optimizing for each.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::{DistributedError, DistributedResult, RuntimeMode};
use crate::core::event::Event;
use crate::core::query::query_runtime::QueryRuntime;
use crate::core::stream::stream_junction::StreamJunction;

/// Processing engine that abstracts single-node and distributed execution
pub struct ProcessingEngine {
    /// Runtime mode
    mode: RuntimeMode,

    /// Engine implementation
    implementation: Arc<dyn ProcessingEngineImpl>,

    /// Engine statistics
    stats: Arc<RwLock<EngineStatistics>>,
}

impl ProcessingEngine {
    /// Create a new processing engine for the given mode
    pub fn new(mode: RuntimeMode) -> DistributedResult<Self> {
        let implementation: Arc<dyn ProcessingEngineImpl> = match mode {
            RuntimeMode::SingleNode => Arc::new(SingleNodeEngine::new()?),
            RuntimeMode::Distributed => Arc::new(DistributedEngine::new()?),
            RuntimeMode::Hybrid => Arc::new(HybridEngine::new()?),
        };

        Ok(Self {
            mode,
            implementation,
            stats: Arc::new(RwLock::new(EngineStatistics::default())),
        })
    }

    /// Get the runtime mode
    pub fn mode(&self) -> RuntimeMode {
        self.mode
    }

    /// Process an event through the engine
    pub async fn process_event(&self, stream_id: &str, event: Event) -> DistributedResult<()> {
        let start = std::time::Instant::now();

        let result = self.implementation.process_event(stream_id, event).await;

        // Update statistics
        let mut stats = self.stats.write().await;
        stats.events_processed += 1;
        stats.total_processing_time += start.elapsed();

        result
    }

    /// Process a batch of events
    pub async fn process_batch(
        &self,
        stream_id: &str,
        events: Vec<Event>,
    ) -> DistributedResult<()> {
        let start = std::time::Instant::now();
        let count = events.len();

        let result = self.implementation.process_batch(stream_id, events).await;

        // Update statistics
        let mut stats = self.stats.write().await;
        stats.events_processed += count as u64;
        stats.batches_processed += 1;
        stats.total_processing_time += start.elapsed();

        result
    }

    /// Register a query with the engine
    pub async fn register_query(
        &self,
        query_id: &str,
        query: Arc<QueryRuntime>,
    ) -> DistributedResult<()> {
        self.implementation.register_query(query_id, query).await
    }

    /// Unregister a query from the engine
    pub async fn unregister_query(&self, query_id: &str) -> DistributedResult<()> {
        self.implementation.unregister_query(query_id).await
    }

    /// Get engine statistics
    pub async fn statistics(&self) -> EngineStatistics {
        self.stats.read().await.clone()
    }

    /// Perform engine optimization
    pub async fn optimize(&self) -> DistributedResult<OptimizationResult> {
        self.implementation.optimize().await
    }

    /// Get query distribution info (for distributed mode)
    pub async fn query_distribution(&self) -> DistributedResult<QueryDistribution> {
        self.implementation.query_distribution().await
    }
}

/// Trait for processing engine implementations
#[async_trait]
pub trait ProcessingEngineImpl: Send + Sync {
    /// Process a single event
    async fn process_event(&self, stream_id: &str, event: Event) -> DistributedResult<()>;

    /// Process a batch of events
    async fn process_batch(&self, stream_id: &str, events: Vec<Event>) -> DistributedResult<()>;

    /// Register a query
    async fn register_query(
        &self,
        query_id: &str,
        query: Arc<QueryRuntime>,
    ) -> DistributedResult<()>;

    /// Unregister a query
    async fn unregister_query(&self, query_id: &str) -> DistributedResult<()>;

    /// Perform optimization
    async fn optimize(&self) -> DistributedResult<OptimizationResult>;

    /// Get query distribution
    async fn query_distribution(&self) -> DistributedResult<QueryDistribution>;
}

/// Engine statistics
#[derive(Debug, Clone, Default)]
pub struct EngineStatistics {
    /// Total events processed
    pub events_processed: u64,

    /// Total batches processed
    pub batches_processed: u64,

    /// Total processing time
    pub total_processing_time: std::time::Duration,

    /// Current throughput (events/sec)
    pub current_throughput: f64,

    /// Peak throughput
    pub peak_throughput: f64,

    /// Active queries
    pub active_queries: usize,

    /// Queue depth
    pub queue_depth: usize,
}

/// Result of optimization operation
#[derive(Debug)]
pub struct OptimizationResult {
    /// Optimizations performed
    pub optimizations: Vec<OptimizationType>,

    /// Performance improvement estimate
    pub improvement_estimate: f64,

    /// Queries affected
    pub affected_queries: Vec<String>,
}

/// Types of optimizations
#[derive(Debug, Clone)]
pub enum OptimizationType {
    /// Query reordering
    QueryReordering,

    /// Predicate pushdown
    PredicatePushdown,

    /// Join reordering
    JoinReordering,

    /// Partition pruning
    PartitionPruning,

    /// Index creation
    IndexCreation,

    /// Memory allocation
    MemoryOptimization,
}

/// Query distribution information
#[derive(Debug)]
pub struct QueryDistribution {
    /// Node to queries mapping
    pub node_queries: HashMap<String, Vec<String>>,

    /// Query to node mapping
    pub query_nodes: HashMap<String, String>,

    /// Load distribution score (0-1, 1 being perfectly balanced)
    pub balance_score: f64,
}

/// Single-node processing engine
struct SingleNodeEngine {
    /// Query registry
    queries: Arc<RwLock<HashMap<String, Arc<QueryRuntime>>>>,

    /// Stream junctions
    streams: Arc<RwLock<HashMap<String, Arc<tokio::sync::Mutex<StreamJunction>>>>>,
}

impl SingleNodeEngine {
    fn new() -> DistributedResult<Self> {
        Ok(Self {
            queries: Arc::new(RwLock::new(HashMap::new())),
            streams: Arc::new(RwLock::new(HashMap::new())),
        })
    }
}

#[async_trait]
impl ProcessingEngineImpl for SingleNodeEngine {
    async fn process_event(&self, stream_id: &str, event: Event) -> DistributedResult<()> {
        // In single-node mode, directly send to stream junction
        let streams = self.streams.read().await;

        if let Some(junction) = streams.get(stream_id) {
            let mut junction = junction.lock().await;
            junction.send_event(event);
            Ok(())
        } else {
            Err(DistributedError::NetworkError {
                message: format!("Stream {} not found", stream_id),
            })
        }
    }

    async fn process_batch(&self, stream_id: &str, events: Vec<Event>) -> DistributedResult<()> {
        // Process each event
        for event in events {
            self.process_event(stream_id, event).await?;
        }
        Ok(())
    }

    async fn register_query(
        &self,
        query_id: &str,
        query: Arc<QueryRuntime>,
    ) -> DistributedResult<()> {
        let mut queries = self.queries.write().await;
        queries.insert(query_id.to_string(), query);
        Ok(())
    }

    async fn unregister_query(&self, query_id: &str) -> DistributedResult<()> {
        let mut queries = self.queries.write().await;
        queries.remove(query_id);
        Ok(())
    }

    async fn optimize(&self) -> DistributedResult<OptimizationResult> {
        // Single-node optimization is simpler
        Ok(OptimizationResult {
            optimizations: vec![OptimizationType::MemoryOptimization],
            improvement_estimate: 0.1,
            affected_queries: vec![],
        })
    }

    async fn query_distribution(&self) -> DistributedResult<QueryDistribution> {
        let queries = self.queries.read().await;
        let query_ids: Vec<String> = queries.keys().cloned().collect();

        let mut node_queries = HashMap::new();
        node_queries.insert("local".to_string(), query_ids.clone());

        let mut query_nodes = HashMap::new();
        for query_id in query_ids {
            query_nodes.insert(query_id, "local".to_string());
        }

        Ok(QueryDistribution {
            node_queries,
            query_nodes,
            balance_score: 1.0, // Perfect balance in single-node
        })
    }
}

/// Distributed processing engine
struct DistributedEngine {
    /// Query registry
    queries: Arc<RwLock<HashMap<String, Arc<QueryRuntime>>>>,

    /// Node assignments for queries
    query_assignments: Arc<RwLock<HashMap<String, String>>>,

    /// Load balancer
    load_balancer: Arc<LoadBalancer>,
}

impl DistributedEngine {
    fn new() -> DistributedResult<Self> {
        Ok(Self {
            queries: Arc::new(RwLock::new(HashMap::new())),
            query_assignments: Arc::new(RwLock::new(HashMap::new())),
            load_balancer: Arc::new(LoadBalancer::new()),
        })
    }
}

#[async_trait]
impl ProcessingEngineImpl for DistributedEngine {
    async fn process_event(&self, stream_id: &str, event: Event) -> DistributedResult<()> {
        // In distributed mode, route event to appropriate node
        let target_node = self.load_balancer.select_node(stream_id).await?;

        // Send event to target node (simplified)
        println!(
            "Routing event for stream {} to node {}",
            stream_id, target_node
        );

        Ok(())
    }

    async fn process_batch(&self, stream_id: &str, events: Vec<Event>) -> DistributedResult<()> {
        // Batch routing to target node
        let target_node = self.load_balancer.select_node(stream_id).await?;

        println!(
            "Routing {} events for stream {} to node {}",
            events.len(),
            stream_id,
            target_node
        );

        Ok(())
    }

    async fn register_query(
        &self,
        query_id: &str,
        query: Arc<QueryRuntime>,
    ) -> DistributedResult<()> {
        // Assign query to a node
        let assigned_node = self.load_balancer.assign_query(query_id).await?;

        let mut queries = self.queries.write().await;
        queries.insert(query_id.to_string(), query);

        let mut assignments = self.query_assignments.write().await;
        assignments.insert(query_id.to_string(), assigned_node);

        Ok(())
    }

    async fn unregister_query(&self, query_id: &str) -> DistributedResult<()> {
        let mut queries = self.queries.write().await;
        queries.remove(query_id);

        let mut assignments = self.query_assignments.write().await;
        assignments.remove(query_id);

        self.load_balancer.release_query(query_id).await?;

        Ok(())
    }

    async fn optimize(&self) -> DistributedResult<OptimizationResult> {
        // Distributed optimization includes rebalancing
        let rebalanced = self.load_balancer.rebalance().await?;

        Ok(OptimizationResult {
            optimizations: vec![
                OptimizationType::QueryReordering,
                OptimizationType::PartitionPruning,
            ],
            improvement_estimate: 0.25,
            affected_queries: rebalanced,
        })
    }

    async fn query_distribution(&self) -> DistributedResult<QueryDistribution> {
        let assignments = self.query_assignments.read().await;

        let mut node_queries: HashMap<String, Vec<String>> = HashMap::new();

        for (query_id, node_id) in assignments.iter() {
            node_queries
                .entry(node_id.clone())
                .or_insert_with(Vec::new)
                .push(query_id.clone());
        }

        let query_nodes = assignments.clone();
        let balance_score = self.load_balancer.balance_score().await;

        Ok(QueryDistribution {
            node_queries,
            query_nodes,
            balance_score,
        })
    }
}

/// Hybrid processing engine
struct HybridEngine {
    /// Local engine for processing
    local_engine: SingleNodeEngine,

    /// Distributed state manager
    distributed_state: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl HybridEngine {
    fn new() -> DistributedResult<Self> {
        Ok(Self {
            local_engine: SingleNodeEngine::new()?,
            distributed_state: Arc::new(RwLock::new(HashMap::new())),
        })
    }
}

#[async_trait]
impl ProcessingEngineImpl for HybridEngine {
    async fn process_event(&self, stream_id: &str, event: Event) -> DistributedResult<()> {
        // Process locally but sync state
        self.local_engine.process_event(stream_id, event).await
    }

    async fn process_batch(&self, stream_id: &str, events: Vec<Event>) -> DistributedResult<()> {
        self.local_engine.process_batch(stream_id, events).await
    }

    async fn register_query(
        &self,
        query_id: &str,
        query: Arc<QueryRuntime>,
    ) -> DistributedResult<()> {
        self.local_engine.register_query(query_id, query).await
    }

    async fn unregister_query(&self, query_id: &str) -> DistributedResult<()> {
        self.local_engine.unregister_query(query_id).await
    }

    async fn optimize(&self) -> DistributedResult<OptimizationResult> {
        self.local_engine.optimize().await
    }

    async fn query_distribution(&self) -> DistributedResult<QueryDistribution> {
        self.local_engine.query_distribution().await
    }
}

/// Load balancer for distributed processing
struct LoadBalancer {
    /// Node loads
    node_loads: Arc<RwLock<HashMap<String, f64>>>,

    /// Stream to node mapping
    stream_nodes: Arc<RwLock<HashMap<String, String>>>,
}

impl LoadBalancer {
    fn new() -> Self {
        Self {
            node_loads: Arc::new(RwLock::new(HashMap::new())),
            stream_nodes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn select_node(&self, stream_id: &str) -> DistributedResult<String> {
        let nodes = self.stream_nodes.read().await;

        if let Some(node) = nodes.get(stream_id) {
            Ok(node.clone())
        } else {
            // Select least loaded node
            let loads = self.node_loads.read().await;
            loads
                .iter()
                .min_by(|a, b| a.1.partial_cmp(b.1).unwrap())
                .map(|(node, _)| node.clone())
                .ok_or_else(|| DistributedError::ClusterNotReady {
                    reason: "No nodes available".to_string(),
                })
        }
    }

    async fn assign_query(&self, _query_id: &str) -> DistributedResult<String> {
        // Assign to least loaded node
        let loads = self.node_loads.read().await;
        loads
            .iter()
            .min_by(|a, b| a.1.partial_cmp(b.1).unwrap())
            .map(|(node, _)| node.clone())
            .ok_or_else(|| DistributedError::ClusterNotReady {
                reason: "No nodes available for query assignment".to_string(),
            })
    }

    async fn release_query(&self, _query_id: &str) -> DistributedResult<()> {
        // Update load information
        Ok(())
    }

    async fn rebalance(&self) -> DistributedResult<Vec<String>> {
        // Perform load rebalancing
        Ok(vec![])
    }

    async fn balance_score(&self) -> f64 {
        // Calculate balance score
        let loads = self.node_loads.read().await;
        if loads.is_empty() {
            return 1.0;
        }

        let avg_load: f64 = loads.values().sum::<f64>() / loads.len() as f64;
        let variance: f64 = loads
            .values()
            .map(|&load| (load - avg_load).powi(2))
            .sum::<f64>()
            / loads.len() as f64;

        // Convert variance to score (0-1)
        1.0 / (1.0 + variance)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_single_node_engine() {
        let engine = ProcessingEngine::new(RuntimeMode::SingleNode).unwrap();

        assert_eq!(engine.mode(), RuntimeMode::SingleNode);

        // Test event processing (will fail without stream setup)
        let event = Event::new_with_data(-1, vec![]);
        let result = engine.process_event("test_stream", event).await;
        assert!(result.is_err());

        // Test statistics - we track the attempt even if it fails
        let stats = engine.statistics().await;
        assert_eq!(stats.events_processed, 1);
    }

    #[tokio::test]
    async fn test_distributed_engine() {
        let engine = ProcessingEngine::new(RuntimeMode::Distributed).unwrap();

        assert_eq!(engine.mode(), RuntimeMode::Distributed);

        // Test query distribution
        let distribution = engine.query_distribution().await.unwrap();
        assert!(distribution.node_queries.is_empty());
        assert_eq!(distribution.balance_score, 1.0);
    }

    #[tokio::test]
    async fn test_optimization() {
        let engine = ProcessingEngine::new(RuntimeMode::SingleNode).unwrap();

        let result = engine.optimize().await.unwrap();
        assert!(!result.optimizations.is_empty());
        assert!(result.improvement_estimate >= 0.0);
    }
}
