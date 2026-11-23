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

// src/core/distributed/distributed_runtime.rs

//! Distributed runtime.
//!
//! Wraps an `EventFluxAppRuntime` and adds distributed features based on the
//! settings. In single node mode events go straight to the core runtime. In
//! distributed and hybrid mode they go through the processing engine.

use std::sync::Arc;
use tokio::sync::RwLock;

use crate::core::config::eventflux_context::EventFluxContext;
use crate::core::event::Event;
use crate::core::eventflux_app_runtime::EventFluxAppRuntime;
use crate::core::stream::input::input_handler::InputHandler;
use crate::query_api::EventFluxApp as ApiEventFluxApp;
use std::sync::Mutex;

use super::{
    processing_engine::{EngineStatistics, ProcessingEngine},
    runtime_mode::{HealthStatus, RuntimeModeManager},
    DistributedConfig, DistributedError, DistributedResult, RuntimeMode,
};

/// Wraps an `EventFluxAppRuntime` with distributed features.
pub struct DistributedRuntime {
    /// Core runtime. Present in every mode.
    core_runtime: Arc<EventFluxAppRuntime>,

    mode_manager: Arc<RwLock<RuntimeModeManager>>,

    processing_engine: Arc<ProcessingEngine>,

    config: Arc<DistributedConfig>,

    state: Arc<RwLock<RuntimeState>>,

    /// Taken from the node settings, or `node-<uuid>` when none are given.
    node_id: String,
}

impl DistributedRuntime {
    /// Build the core runtime, mode manager and processing engine, and initialize
    /// the mode manager. The runtime starts in the `Initializing` state.
    ///
    /// # Errors
    ///
    /// Returns `ConfigurationError` if the core runtime cannot be built. Also
    /// returns any error from creating or initializing the mode manager or the
    /// processing engine.
    pub async fn new(
        api_eventflux_app: Arc<ApiEventFluxApp>,
        eventflux_context: Arc<EventFluxContext>,
        config: DistributedConfig,
    ) -> DistributedResult<Self> {
        let node_id = config
            .node
            .as_ref()
            .map(|n| n.node_id.clone())
            .unwrap_or_else(|| format!("node-{}", uuid::Uuid::new_v4()));

        let core_runtime = Arc::new(
            EventFluxAppRuntime::new(
                Arc::clone(&api_eventflux_app),
                Arc::clone(&eventflux_context),
                None,
            )
            .map_err(|e| DistributedError::ConfigurationError {
                message: format!("Failed to create core runtime: {}", e),
            })?,
        );

        let mut mode_manager = RuntimeModeManager::new(config.clone())?;

        let processing_engine = Arc::new(ProcessingEngine::new(config.mode)?);

        mode_manager.initialize().await?;

        let runtime = Self {
            core_runtime,
            mode_manager: Arc::new(RwLock::new(mode_manager)),
            processing_engine,
            config: Arc::new(config),
            state: Arc::new(RwLock::new(RuntimeState::Initializing)),
            node_id,
        };

        Ok(runtime)
    }

    /// Start the core runtime and register its queries with the processing engine.
    ///
    /// Queries are registered as `query_<index>` in the order the core runtime
    /// holds them. The state moves to `Starting`, then to `Running` on success.
    ///
    /// # Errors
    ///
    /// Returns `ConfigurationError` if the core runtime fails to start, or any
    /// error from registering a query. The state stays `Starting` on failure.
    pub async fn start(&self) -> DistributedResult<()> {
        {
            let mut state = self.state.write().await;
            *state = RuntimeState::Starting;
        }

        self.core_runtime
            .start()
            .map_err(|e| DistributedError::ConfigurationError {
                message: format!("Failed to start core runtime: {}", e),
            })?;

        for (query_id, query_runtime) in self.core_runtime.query_runtimes.iter().enumerate() {
            self.processing_engine
                .register_query(&format!("query_{}", query_id), Arc::clone(query_runtime))
                .await?;
        }

        {
            let mut state = self.state.write().await;
            *state = RuntimeState::Running;
        }

        println!(
            "Distributed runtime started in {:?} mode on node {}",
            self.config.mode, self.node_id
        );

        Ok(())
    }

    /// Shut down the core runtime and the mode manager.
    ///
    /// The state moves to `Stopping`, then to `Stopped` on success.
    ///
    /// # Errors
    ///
    /// Returns any error from shutting down the mode manager. The state stays
    /// `Stopping` in that case.
    pub async fn stop(&self) -> DistributedResult<()> {
        {
            let mut state = self.state.write().await;
            *state = RuntimeState::Stopping;
        }

        // Run the core shutdown on a blocking thread. shutdown() drains async
        // junctions with bounded sleeps, up to the drain and settle timeouts, and
        // would otherwise stall this tokio worker for seconds. Call it inline when
        // there is no tokio runtime, or when the blocking task is cancelled because
        // the tokio runtime is itself shutting down. The core shutdown must run
        // either way, and is safe to repeat because of its state guard. stop() must
        // not return early before the mode manager and state updates below.
        if tokio::runtime::Handle::try_current().is_ok() {
            let core_runtime = Arc::clone(&self.core_runtime);
            if tokio::task::spawn_blocking(move || core_runtime.shutdown())
                .await
                .is_err()
            {
                self.core_runtime.shutdown();
            }
        } else {
            self.core_runtime.shutdown();
        }

        {
            let mut manager = self.mode_manager.write().await;
            manager.shutdown().await?;
        }

        {
            let mut state = self.state.write().await;
            *state = RuntimeState::Stopped;
        }

        println!("Distributed runtime stopped");

        Ok(())
    }

    /// Returns the core runtime input handler for a stream, or `None` if the stream
    /// does not exist.
    pub fn get_input_handler(&self, stream_id: &str) -> Option<Arc<Mutex<InputHandler>>> {
        self.core_runtime.get_input_handler(stream_id)
    }

    /// Send one event to a stream.
    ///
    /// Single node mode sends it straight to the core input handler. Distributed
    /// and hybrid mode hand it to the processing engine.
    ///
    /// # Errors
    ///
    /// In single node mode, returns `NetworkError` if the stream does not exist or
    /// the send fails. Otherwise returns any error from the processing engine.
    pub async fn send_event(&self, stream_id: &str, event: Event) -> DistributedResult<()> {
        match self.config.mode {
            RuntimeMode::SingleNode => {
                if let Some(handler) = self.get_input_handler(stream_id) {
                    let handler = handler.lock().unwrap();
                    handler
                        .send_single_event(event)
                        .map_err(|e| DistributedError::NetworkError {
                            message: format!("Failed to send event: {}", e),
                        })
                } else {
                    Err(DistributedError::NetworkError {
                        message: format!("Stream {} not found", stream_id),
                    })
                }
            }
            RuntimeMode::Distributed | RuntimeMode::Hybrid => {
                self.processing_engine.process_event(stream_id, event).await
            }
        }
    }

    /// Send a batch of events to a stream.
    ///
    /// Routes the same way as [`send_event`](Self::send_event).
    ///
    /// # Errors
    ///
    /// In single node mode, returns `NetworkError` if the stream does not exist or
    /// the send fails. Otherwise returns any error from the processing engine.
    pub async fn send_batch(&self, stream_id: &str, events: Vec<Event>) -> DistributedResult<()> {
        match self.config.mode {
            RuntimeMode::SingleNode => {
                if let Some(handler) = self.get_input_handler(stream_id) {
                    let handler = handler.lock().unwrap();
                    handler.send_multiple_events(events).map_err(|e| {
                        DistributedError::NetworkError {
                            message: format!("Failed to send batch: {}", e),
                        }
                    })
                } else {
                    Err(DistributedError::NetworkError {
                        message: format!("Stream {} not found", stream_id),
                    })
                }
            }
            RuntimeMode::Distributed | RuntimeMode::Hybrid => {
                self.processing_engine
                    .process_batch(stream_id, events)
                    .await
            }
        }
    }

    pub fn mode(&self) -> RuntimeMode {
        self.config.mode
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Returns true in distributed or hybrid mode.
    pub fn is_distributed(&self) -> bool {
        matches!(
            self.config.mode,
            RuntimeMode::Distributed | RuntimeMode::Hybrid
        )
    }

    /// Returns the health status reported by the mode manager.
    pub async fn health_check(&self) -> DistributedResult<HealthStatus> {
        let manager = self.mode_manager.read().await;
        manager.health_check().await
    }

    /// Returns the current state and engine counters. Uptime is always zero
    /// because it is not tracked yet.
    pub async fn statistics(&self) -> RuntimeStatistics {
        let engine_stats = self.processing_engine.statistics().await;
        let state = self.state.read().await.clone();

        RuntimeStatistics {
            node_id: self.node_id.clone(),
            mode: self.config.mode,
            state,
            engine_stats,
            uptime: std::time::Duration::from_secs(0), // not tracked yet
        }
    }

    /// Returns checkpoint info with a new id and the current time.
    ///
    /// No state is saved yet. The type is `Local` in single node mode and
    /// `Distributed` otherwise.
    pub async fn checkpoint(&self) -> DistributedResult<CheckpointInfo> {
        match self.config.mode {
            RuntimeMode::SingleNode => Ok(CheckpointInfo {
                checkpoint_id: uuid::Uuid::new_v4().to_string(),
                timestamp: std::time::SystemTime::now(),
                node_id: self.node_id.clone(),
                checkpoint_type: CheckpointType::Local,
            }),
            RuntimeMode::Distributed | RuntimeMode::Hybrid => Ok(CheckpointInfo {
                checkpoint_id: uuid::Uuid::new_v4().to_string(),
                timestamp: std::time::SystemTime::now(),
                node_id: self.node_id.clone(),
                checkpoint_type: CheckpointType::Distributed,
            }),
        }
    }

    /// Restore from a checkpoint.
    ///
    /// Only prints the checkpoint id. No state is restored yet.
    pub async fn restore(&self, checkpoint_id: &str) -> DistributedResult<()> {
        println!("Restoring from checkpoint: {}", checkpoint_id);
        // TODO: restore state from the checkpoint.
        Ok(())
    }

    /// Add a node to the cluster.
    ///
    /// Only prints the node id. The node is not added yet.
    ///
    /// # Errors
    ///
    /// Returns `ConfigurationError` in single node mode.
    pub async fn scale_out(&self, node_config: super::NodeConfig) -> DistributedResult<()> {
        if !self.is_distributed() {
            return Err(DistributedError::ConfigurationError {
                message: "Scale out only available in distributed mode".to_string(),
            });
        }

        println!("Adding node {} to cluster", node_config.node_id);
        // TODO: add the node to the cluster.
        Ok(())
    }

    /// Remove a node from the cluster.
    ///
    /// Only prints the node id. The node is not removed yet.
    ///
    /// # Errors
    ///
    /// Returns `ConfigurationError` in single node mode.
    pub async fn scale_in(&self, node_id: &str) -> DistributedResult<()> {
        if !self.is_distributed() {
            return Err(DistributedError::ConfigurationError {
                message: "Scale in only available in distributed mode".to_string(),
            });
        }

        println!("Removing node {} from cluster", node_id);
        // TODO: remove the node from the cluster.
        Ok(())
    }
}

/// Lifecycle state of a [`DistributedRuntime`].
#[derive(Debug, Clone)]
pub enum RuntimeState {
    Initializing,
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed { reason: String },
}

/// Snapshot of runtime state and counters.
#[derive(Debug)]
pub struct RuntimeStatistics {
    pub node_id: String,

    pub mode: RuntimeMode,

    pub state: RuntimeState,

    pub engine_stats: EngineStatistics,

    /// Always zero for now.
    pub uptime: std::time::Duration,
}

/// Details of a checkpoint.
#[derive(Debug)]
pub struct CheckpointInfo {
    pub checkpoint_id: String,

    pub timestamp: std::time::SystemTime,

    /// Node that created the checkpoint.
    pub node_id: String,

    pub checkpoint_type: CheckpointType,
}

/// Kind of checkpoint.
#[derive(Debug)]
pub enum CheckpointType {
    Local,
    Distributed,
    Incremental,
    Full,
}

/// Builder for [`DistributedRuntime`]. Starts from the default settings.
pub struct DistributedRuntimeBuilder {
    config: DistributedConfig,
    eventflux_context: Option<Arc<EventFluxContext>>,
}

impl DistributedRuntimeBuilder {
    pub fn new() -> Self {
        Self {
            config: DistributedConfig::default(),
            eventflux_context: None,
        }
    }

    pub fn mode(mut self, mode: RuntimeMode) -> Self {
        self.config.mode = mode;
        self
    }

    pub fn node(mut self, node: super::NodeConfig) -> Self {
        self.config.node = Some(node);
        self
    }

    pub fn cluster(mut self, cluster: super::ClusterConfig) -> Self {
        self.config.cluster = Some(cluster);
        self
    }

    /// Set the EventFlux context. Required before [`build`](Self::build).
    pub fn eventflux_context(mut self, context: Arc<EventFluxContext>) -> Self {
        self.eventflux_context = Some(context);
        self
    }

    /// Build the runtime.
    ///
    /// # Errors
    ///
    /// Returns `ConfigurationError` if no EventFlux context was set. Also returns
    /// any error from [`DistributedRuntime::new`].
    pub async fn build(
        self,
        api_eventflux_app: Arc<ApiEventFluxApp>,
    ) -> DistributedResult<DistributedRuntime> {
        let eventflux_context =
            self.eventflux_context
                .ok_or_else(|| DistributedError::ConfigurationError {
                    message: "EventFluxContext required".to_string(),
                })?;

        DistributedRuntime::new(api_eventflux_app, eventflux_context, self.config).await
    }
}

impl Default for DistributedRuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_api::EventFluxApp;

    async fn create_test_runtime(mode: RuntimeMode) -> DistributedRuntime {
        let api_app = Arc::new(EventFluxApp::default());
        let context = Arc::new(EventFluxContext::default());

        let mut config = DistributedConfig {
            mode,
            ..Default::default()
        };

        if mode == RuntimeMode::Distributed {
            config.node = Some(super::super::NodeConfig {
                node_id: "test-node".to_string(),
                endpoints: vec!["localhost:8080".to_string()],
                capabilities: super::super::NodeCapabilities::default(),
                resources: super::super::ResourceLimits::default(),
            });
            config.cluster = Some(super::super::ClusterConfig::default());
        }

        DistributedRuntime::new(api_app, context, config)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_single_node_runtime() {
        let runtime = create_test_runtime(RuntimeMode::SingleNode).await;

        assert_eq!(runtime.mode(), RuntimeMode::SingleNode);
        assert!(!runtime.is_distributed());

        assert!(runtime.start().await.is_ok());

        let stats = runtime.statistics().await;
        assert_eq!(stats.mode, RuntimeMode::SingleNode);

        assert!(runtime.stop().await.is_ok());
    }

    #[tokio::test]
    async fn test_distributed_runtime() {
        let runtime = create_test_runtime(RuntimeMode::Distributed).await;

        assert_eq!(runtime.mode(), RuntimeMode::Distributed);
        assert!(runtime.is_distributed());
        assert_eq!(runtime.node_id(), "test-node");

        assert!(runtime.start().await.is_ok());
        assert!(runtime.stop().await.is_ok());
    }

    #[tokio::test]
    async fn test_checkpoint() {
        let runtime = create_test_runtime(RuntimeMode::SingleNode).await;

        assert!(runtime.start().await.is_ok());

        let checkpoint = runtime.checkpoint().await.unwrap();
        assert!(!checkpoint.checkpoint_id.is_empty());
        assert!(matches!(checkpoint.checkpoint_type, CheckpointType::Local));

        assert!(runtime.restore(&checkpoint.checkpoint_id).await.is_ok());

        assert!(runtime.stop().await.is_ok());
    }

    #[tokio::test]
    async fn test_builder() {
        let context = Arc::new(EventFluxContext::default());
        let api_app = Arc::new(EventFluxApp::default());

        let runtime = DistributedRuntimeBuilder::new()
            .mode(RuntimeMode::SingleNode)
            .eventflux_context(context)
            .build(api_app)
            .await
            .unwrap();

        assert_eq!(runtime.mode(), RuntimeMode::SingleNode);
    }
}
