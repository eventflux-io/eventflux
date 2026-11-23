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

// src/core/distributed/coordinator.rs

//! Cluster coordination.
//!
//! Defines the trait for leader election, agreement between nodes, and cluster
//! membership.

use super::DistributedResult;
use async_trait::async_trait;

/// Coordinates nodes in a cluster.
#[async_trait]
pub trait DistributedCoordinator: Send + Sync {
    /// Join the cluster.
    async fn join_cluster(&self) -> DistributedResult<()>;

    /// Leave the cluster.
    async fn leave_cluster(&self) -> DistributedResult<()>;

    /// Returns the current leader id, or `None` if there is no leader.
    async fn get_leader(&self) -> DistributedResult<Option<String>>;

    /// Returns true if this node is the leader.
    async fn is_leader(&self) -> bool;

    /// Returns the ids of the cluster members.
    async fn get_members(&self) -> DistributedResult<Vec<String>>;

    /// Starts a checkpoint across the cluster.
    async fn initiate_checkpoint(&self, checkpoint_id: &str) -> DistributedResult<()>;
}

/// Placeholder Raft coordinator.
///
/// Does no real work. It always reports `"leader-node"` as leader, says this node is
/// not the leader, and returns no members.
pub struct RaftCoordinator;

#[async_trait]
impl DistributedCoordinator for RaftCoordinator {
    async fn join_cluster(&self) -> DistributedResult<()> {
        Ok(())
    }

    async fn leave_cluster(&self) -> DistributedResult<()> {
        Ok(())
    }

    async fn get_leader(&self) -> DistributedResult<Option<String>> {
        Ok(Some("leader-node".to_string()))
    }

    async fn is_leader(&self) -> bool {
        false
    }

    async fn get_members(&self) -> DistributedResult<Vec<String>> {
        Ok(vec![])
    }

    async fn initiate_checkpoint(&self, _checkpoint_id: &str) -> DistributedResult<()> {
        Ok(())
    }
}
