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

// src/core/distributed/processing_engine.rs

//! Processing engine.
//!
//! One front for running queries in single node, distributed or hybrid mode. The
//! mode picks the engine behind it. Each engine has the same methods.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::{DistributedError, DistributedResult, RuntimeMode};
use crate::core::event::Event;
use crate::core::query::query_runtime::QueryRuntime;
use crate::core::stream::stream_junction::StreamJunction;

/// Runs events and queries through the engine for the current mode, and keeps
/// counters.
pub struct ProcessingEngine {
    mode: RuntimeMode,

    implementation: Arc<dyn ProcessingEngineImpl>,

    stats: Arc<RwLock<EngineStatistics>>,
}

impl ProcessingEngine {
    /// Create the engine for the given mode.
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

    pub fn mode(&self) -> RuntimeMode {
        self.mode
    }

    /// Process one event.
    ///
    /// The event and its time are counted even when processing fails. Returns the
    /// engine result.
    pub async fn process_event(&self, stream_id: &str, event: Event) -> DistributedResult<()> {
        let start = std::time::Instant::now();

        let result = self.implementation.process_event(stream_id, event).await;

        let mut stats = self.stats.write().await;
        stats.events_processed += 1;
        stats.total_processing_time += start.elapsed();

        result
    }

    /// Process a batch of events.
    ///
    /// The whole batch is counted even when processing fails part way. Returns the
    /// engine result.
    pub async fn process_batch(
        &self,
        stream_id: &str,
        events: Vec<Event>,
    ) -> DistributedResult<()> {
        let start = std::time::Instant::now();
        let count = events.len();

        let result = self.implementation.process_batch(stream_id, events).await;

        let mut stats = self.stats.write().await;
        stats.events_processed += count as u64;
        stats.batches_processed += 1;
        stats.total_processing_time += start.elapsed();

        result
    }

    pub async fn register_query(
        &self,
        query_id: &str,
        query: Arc<QueryRuntime>,
    ) -> DistributedResult<()> {
        self.implementation.register_query(query_id, query).await
    }

    pub async fn unregister_query(&self, query_id: &str) -> DistributedResult<()> {
        self.implementation.unregister_query(query_id).await
    }

    /// Returns a copy of the counters.
    pub async fn statistics(&self) -> EngineStatistics {
        self.stats.read().await.clone()
    }

    /// Ask the engine to optimize. The engines only report fixed results for now.
    pub async fn optimize(&self) -> DistributedResult<OptimizationResult> {
        self.implementation.optimize().await
    }

    /// Returns which node runs which query.
    pub async fn query_distribution(&self) -> DistributedResult<QueryDistribution> {
        self.implementation.query_distribution().await
    }
}

/// One engine behind [`ProcessingEngine`].
#[async_trait]
pub trait ProcessingEngineImpl: Send + Sync {
    async fn process_event(&self, stream_id: &str, event: Event) -> DistributedResult<()>;

    async fn process_batch(&self, stream_id: &str, events: Vec<Event>) -> DistributedResult<()>;

    async fn register_query(
        &self,
        query_id: &str,
        query: Arc<QueryRuntime>,
    ) -> DistributedResult<()>;

    async fn unregister_query(&self, query_id: &str) -> DistributedResult<()>;

    async fn optimize(&self) -> DistributedResult<OptimizationResult>;

    /// Returns which node runs which query.
    async fn query_distribution(&self) -> DistributedResult<QueryDistribution>;
}

/// Engine counters.
///
/// Only `events_processed`, `batches_processed` and `total_processing_time` are
/// updated. The other fields stay at zero.
#[derive(Debug, Clone, Default)]
pub struct EngineStatistics {
    pub events_processed: u64,

    pub batches_processed: u64,

    pub total_processing_time: std::time::Duration,

    /// Events per second.
    pub current_throughput: f64,

    /// Events per second.
    pub peak_throughput: f64,

    pub active_queries: usize,

    pub queue_depth: usize,
}

/// Result of an optimize call.
#[derive(Debug)]
pub struct OptimizationResult {
    /// Optimizations applied.
    pub optimizations: Vec<OptimizationType>,

    /// Estimated speed gain as a fraction, such as 0.1 for 10 percent.
    pub improvement_estimate: f64,

    pub affected_queries: Vec<String>,
}

/// Kinds of optimization.
#[derive(Debug, Clone)]
pub enum OptimizationType {
    QueryReordering,

    /// Apply filters as early as possible.
    PredicatePushdown,

    JoinReordering,

    /// Skip partitions that cannot match.
    PartitionPruning,

    IndexCreation,

    /// Better memory use.
    MemoryOptimization,
}

/// Which node runs which query.
#[derive(Debug)]
pub struct QueryDistribution {
    /// Query ids for each node id.
    pub node_queries: HashMap<String, Vec<String>>,

    /// Node id for each query id.
    pub query_nodes: HashMap<String, String>,

    /// How evenly load is spread, from 0 to 1. 1 means perfectly even.
    pub balance_score: f64,
}

/// Engine for single node mode.
///
/// Nothing fills `streams` yet, so `process_event` fails with stream not found
/// for every stream.
struct SingleNodeEngine {
    queries: Arc<RwLock<HashMap<String, Arc<QueryRuntime>>>>,

    /// Stream junctions, keyed by stream id.
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
        let streams = self.streams.read().await;

        if let Some(junction) = streams.get(stream_id) {
            let junction = junction.lock().await;
            junction
                .send_event(event)
                .map_err(|e| DistributedError::StateError {
                    message: format!("Failed to send event to stream {}: {}", stream_id, e),
                })?;
            Ok(())
        } else {
            Err(DistributedError::NetworkError {
                message: format!("Stream {} not found", stream_id),
            })
        }
    }

    async fn process_batch(&self, stream_id: &str, events: Vec<Event>) -> DistributedResult<()> {
        // Stops at the first failed event. Events before it stay sent.
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
        // Fixed result. Nothing is optimized yet.
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
            balance_score: 1.0, // one node is always perfectly balanced
        })
    }
}

/// Engine for distributed mode.
///
/// Picks a target node for each event but does not forward events yet.
struct DistributedEngine {
    queries: Arc<RwLock<HashMap<String, Arc<QueryRuntime>>>>,

    /// Assigned node id for each query id.
    query_assignments: Arc<RwLock<HashMap<String, String>>>,

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
    async fn process_event(&self, stream_id: &str, _event: Event) -> DistributedResult<()> {
        let target_node = self.load_balancer.select_node(stream_id).await?;

        // FIXME: the target node is picked but the event is dropped. Send `_event`
        // to `target_node` through the transport layer once the distributed message
        // protocol exists.
        log::warn!(
            "Distributed routing not yet implemented: event for stream '{}' \
             selected node '{}' but was not forwarded",
            stream_id,
            target_node
        );

        Ok(())
    }

    async fn process_batch(&self, stream_id: &str, events: Vec<Event>) -> DistributedResult<()> {
        let target_node = self.load_balancer.select_node(stream_id).await?;

        // Like process_event, the batch is only logged and then dropped.
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
        // Fails with ClusterNotReady when the balancer knows no nodes.
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
        // Rebalance, then report a fixed list of optimizations.
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
                .or_default()
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

/// Engine for hybrid mode. Every call goes to a local single node engine.
#[allow(dead_code)]
struct HybridEngine {
    local_engine: SingleNodeEngine,

    /// Shared state, keyed by name. Not used yet.
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
        // Processed locally only. State is not synced yet.
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

/// Picks nodes for streams and queries.
///
/// Nothing fills `node_loads` or `stream_nodes` yet, so every pick fails with
/// `ClusterNotReady`.
struct LoadBalancer {
    /// Load for each node id.
    node_loads: Arc<RwLock<HashMap<String, f64>>>,

    /// Fixed node id for each stream id.
    stream_nodes: Arc<RwLock<HashMap<String, String>>>,
}

impl LoadBalancer {
    fn new() -> Self {
        Self {
            node_loads: Arc::new(RwLock::new(HashMap::new())),
            stream_nodes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Returns the node fixed for the stream, or else the least loaded node.
    ///
    /// Panics if a load is NaN.
    async fn select_node(&self, stream_id: &str) -> DistributedResult<String> {
        let nodes = self.stream_nodes.read().await;

        if let Some(node) = nodes.get(stream_id) {
            Ok(node.clone())
        } else {
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

    /// Returns the least loaded node. Does not add load for the query.
    ///
    /// Panics if a load is NaN.
    async fn assign_query(&self, _query_id: &str) -> DistributedResult<String> {
        let loads = self.node_loads.read().await;
        loads
            .iter()
            .min_by(|a, b| a.1.partial_cmp(b.1).unwrap())
            .map(|(node, _)| node.clone())
            .ok_or_else(|| DistributedError::ClusterNotReady {
                reason: "No nodes available for query assignment".to_string(),
            })
    }

    /// Does nothing yet. Load is not tracked per query.
    async fn release_query(&self, _query_id: &str) -> DistributedResult<()> {
        Ok(())
    }

    /// Does nothing yet. Always returns no moved queries.
    async fn rebalance(&self) -> DistributedResult<Vec<String>> {
        Ok(vec![])
    }

    /// Returns `1 / (1 + variance)` of the node loads, or 1.0 when there are no
    /// nodes.
    async fn balance_score(&self) -> f64 {
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

        // Fails because no stream is set up.
        let event = Event::new_with_data(-1, vec![]);
        let result = engine.process_event("test_stream", event).await;
        assert!(result.is_err());

        // The failed attempt is still counted.
        let stats = engine.statistics().await;
        assert_eq!(stats.events_processed, 1);
    }

    #[tokio::test]
    async fn test_distributed_engine() {
        let engine = ProcessingEngine::new(RuntimeMode::Distributed).unwrap();

        assert_eq!(engine.mode(), RuntimeMode::Distributed);

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
