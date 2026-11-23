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

// src/core/persistence/incremental/distributed_coordinator.rs

//! Distributed checkpoint coordinator.
//!
//! Coordinates checkpoints across a cluster. The leader starts a checkpoint,
//! nodes report back, and the checkpoint succeeds once enough nodes finish.
//!
//! The Raft style coordinator is a sketch. Nothing is sent over the network,
//! messages are only printed, and a node only wins an election when it has no
//! other cluster members. The in memory coordinator is for tests.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use super::{ClusterHealth, DistributedConfig, DistributedCoordinator, PartitionStatus};
use crate::core::persistence::state_holder::{CheckpointId, StateError};

/// Coordinator modeled on Raft leader election.
pub struct RaftDistributedCoordinator {
    config: DistributedConfig,

    /// This node's role, term and health.
    node_state: Arc<RwLock<NodeState>>,

    /// Other cluster members, keyed by node id. Never filled outside tests.
    cluster_members: Arc<RwLock<HashMap<String, NodeInfo>>>,

    /// Checkpoints in progress. Entries are never removed.
    active_checkpoints: Arc<Mutex<HashMap<CheckpointId, CheckpointProgress>>>,

    election_state: Arc<Mutex<ElectionState>>,

    /// Log of coordination events. Only appended and counted, never replicated.
    consensus_log: Arc<Mutex<Vec<ConsensusEntry>>>,

    stats: Arc<Mutex<CoordinatorStatistics>>,
}

/// In memory coordinator for tests. Any 2 successful reports complete a
/// checkpoint.
pub struct InMemoryDistributedCoordinator {
    node_id: String,

    cluster_state: Arc<RwLock<SimulatedClusterState>>,

    active_checkpoints: Arc<Mutex<HashMap<CheckpointId, CheckpointProgress>>>,
}

/// State of this node.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct NodeState {
    node_id: String,

    role: NodeRole,

    /// Election term.
    current_term: u64,

    /// Node voted for in the current term.
    voted_for: Option<String>,

    /// Last heartbeat received. Never updated yet.
    last_heartbeat: Instant,

    health: NodeHealth,
}

/// Role of a node in leader election.
#[derive(Debug, Clone, PartialEq)]
enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum NodeHealth {
    Healthy,
    Degraded { reason: String },
    Failed { reason: String },
}

/// A cluster member.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct NodeInfo {
    node_id: String,

    endpoints: Vec<String>,

    last_seen: Instant,

    capabilities: NodeCapabilities,

    health: NodeHealth,
}

/// What a cluster node can do.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct NodeCapabilities {
    /// Can stand in leader elections.
    can_be_leader: bool,

    /// Bytes.
    storage_capacity: u64,

    /// CPU cores.
    processing_capacity: u32,

    /// Bytes per second.
    network_bandwidth: u64,
}

/// Progress of one distributed checkpoint.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct CheckpointProgress {
    checkpoint_id: CheckpointId,

    /// Nodes that started the checkpoint. Never filled yet.
    nodes_started: HashSet<String>,

    nodes_completed: HashSet<String>,

    nodes_failed: HashSet<String>,

    start_time: Instant,

    expected_completion: Option<Instant>,

    metadata: CheckpointMetadata,
}

/// Details of a distributed checkpoint.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct CheckpointMetadata {
    /// Free text such as `full` or `simulated`.
    checkpoint_type: String,

    /// Node that started the checkpoint.
    coordinator_node: String,

    /// Not enforced. `wait_for_completion` uses its own timeout.
    timeout: Duration,

    /// Successful reports needed for the checkpoint to count as done.
    required_quorum: usize,
}

/// Leader election state.
#[derive(Debug)]
struct ElectionState {
    current_term: u64,

    /// When to start the next election if no leader is found.
    election_timeout: Instant,

    /// Nodes that voted for this node in the current term.
    votes_received: HashSet<String>,

    election_in_progress: bool,
}

/// One entry in the coordination log.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ConsensusEntry {
    /// Election term when the entry was added.
    term: u64,

    /// Position in the log, starting at 0.
    index: u64,

    entry_type: ConsensusEntryType,

    /// For checkpoint entries, the checkpoint id as little endian bytes.
    data: Vec<u8>,

    timestamp: Instant,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum ConsensusEntryType {
    CheckpointInitiation,
    CheckpointCompletion,
    NodeJoin,
    NodeLeave,
    Configuration,
}

/// Coordinator counters.
///
/// Only the checkpoint counts and `avg_coordination_time` are updated.
#[derive(Debug, Default)]
#[allow(dead_code)]
struct CoordinatorStatistics {
    /// Checkpoints started.
    pub total_checkpoints: u64,

    /// Counted on every successful report once quorum is reached, so one
    /// checkpoint can be counted more than once.
    pub successful_checkpoints: u64,

    pub failed_checkpoints: u64,

    /// Kept in whole milliseconds.
    pub avg_coordination_time: Duration,

    pub leader_elections: u64,

    pub network_partitions: u64,

    pub avg_cluster_size: f64,
}

/// Fake cluster for the in memory coordinator.
#[derive(Debug, Default)]
#[allow(dead_code)]
struct SimulatedClusterState {
    nodes: HashMap<String, NodeInfo>,

    /// Never set yet.
    leader: Option<String>,

    /// Not used. Checkpoints live in the coordinator itself.
    checkpoints: HashMap<CheckpointId, CheckpointProgress>,

    /// Recorded but not used when computing health.
    partitions: Vec<Vec<String>>,
}

impl RaftDistributedCoordinator {
    /// Create a coordinator that starts as a follower in term 0.
    pub fn new(config: DistributedConfig) -> Self {
        let node_state = NodeState {
            node_id: config.node_id.clone(),
            role: NodeRole::Follower,
            current_term: 0,
            voted_for: None,
            last_heartbeat: Instant::now(),
            health: NodeHealth::Healthy,
        };

        let election_state = ElectionState {
            current_term: 0,
            election_timeout: Instant::now() + config.election_timeout,
            votes_received: HashSet::new(),
            election_in_progress: false,
        };

        Self {
            config,
            node_state: Arc::new(RwLock::new(node_state)),
            cluster_members: Arc::new(RwLock::new(HashMap::new())),
            active_checkpoints: Arc::new(Mutex::new(HashMap::new())),
            election_state: Arc::new(Mutex::new(election_state)),
            consensus_log: Arc::new(Mutex::new(Vec::new())),
            stats: Arc::new(Mutex::new(CoordinatorStatistics::default())),
        }
    }

    /// Start the heartbeat, election and log threads.
    ///
    /// The threads loop forever. There is no way to stop them, and calling this
    /// again starts another set.
    pub fn start(&self) -> Result<(), StateError> {
        println!(
            "Starting distributed coordinator for node: {}",
            self.config.node_id
        );

        self.start_heartbeat_thread();

        self.start_election_thread();

        self.start_consensus_thread();

        Ok(())
    }

    /// Start a thread that, while this node is leader, prints a heartbeat line
    /// for each member every `heartbeat_interval`. Nothing is sent.
    fn start_heartbeat_thread(&self) {
        let node_state = Arc::clone(&self.node_state);
        let cluster_members = Arc::clone(&self.cluster_members);
        let heartbeat_interval = self.config.heartbeat_interval;

        thread::spawn(move || {
            loop {
                thread::sleep(heartbeat_interval);

                let state = node_state.read().unwrap();
                if state.role == NodeRole::Leader {
                    let members = cluster_members.read().unwrap();
                    for (node_id, _node_info) in members.iter() {
                        if node_id != &state.node_id {
                            // Printed only. No heartbeat is sent.
                            println!("Sending heartbeat to node: {node_id}");
                        }
                    }
                }
            }
        });
    }

    /// Start a thread that runs an election when the election timeout passes and
    /// this node is not leader.
    ///
    /// The node votes for itself and only prints vote requests, so it wins only
    /// when it has no other members. If it does not win, `election_in_progress`
    /// stays set and no later election starts.
    fn start_election_thread(&self) {
        let node_state = Arc::clone(&self.node_state);
        let election_state = Arc::clone(&self.election_state);
        let cluster_members = Arc::clone(&self.cluster_members);
        let election_timeout = self.config.election_timeout;

        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_millis(100));

                let mut election = election_state.lock().unwrap();
                let mut state = node_state.write().unwrap();

                if state.role != NodeRole::Leader && Instant::now() > election.election_timeout {
                    // Election timed out with no leader, so start one unless one is running
                    if !election.election_in_progress {
                        println!("Starting leader election for node: {}", state.node_id);

                        state.role = NodeRole::Candidate;
                        state.current_term += 1;
                        state.voted_for = Some(state.node_id.clone());

                        election.current_term = state.current_term;
                        election.election_timeout = Instant::now() + election_timeout;
                        election.votes_received.clear();
                        election.votes_received.insert(state.node_id.clone());
                        election.election_in_progress = true;

                        let members = cluster_members.read().unwrap();
                        for (node_id, _node_info) in members.iter() {
                            if node_id != &state.node_id {
                                // Printed only. No vote request is sent.
                                println!("Requesting vote from node: {node_id}");
                            }
                        }

                        // Only this node's own vote is ever counted.
                        let total_nodes = members.len() + 1; // +1 for self
                        let required_votes = (total_nodes / 2) + 1;

                        if election.votes_received.len() >= required_votes {
                            state.role = NodeRole::Leader;
                            election.election_in_progress = false;
                            println!("Node {} became leader", state.node_id);
                        }
                    }
                }
            }
        });
    }

    /// Start a thread that prints the log size every second when the log is not
    /// empty. Entries are not processed or replicated.
    fn start_consensus_thread(&self) {
        let consensus_log = Arc::clone(&self.consensus_log);

        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(1));

            let log = consensus_log.lock().unwrap();
            if !log.is_empty() {
                println!("Processing {} consensus entries", log.len());
            }
        });
    }

    /// Append an entry to the log, tagged with the current term.
    fn add_consensus_entry(
        &self,
        entry_type: ConsensusEntryType,
        data: Vec<u8>,
    ) -> Result<(), StateError> {
        let mut log = self.consensus_log.lock().unwrap();
        let state = self.node_state.read().unwrap();

        let entry = ConsensusEntry {
            term: state.current_term,
            index: log.len() as u64,
            entry_type,
            data,
            timestamp: Instant::now(),
        };

        log.push(entry);
        Ok(())
    }

    fn is_leader(&self) -> bool {
        let state = self.node_state.read().unwrap();
        state.role == NodeRole::Leader
    }

    /// Returns the majority size of the cluster, counting this node. That is
    /// half the nodes rounded down, plus one.
    fn get_quorum_size(&self) -> usize {
        let members = self.cluster_members.read().unwrap();
        let total_nodes = members.len() + 1; // +1 for self
        (total_nodes / 2) + 1
    }
}

impl DistributedCoordinator for RaftDistributedCoordinator {
    /// Start tracking a checkpoint and log it. Barriers to members are only
    /// printed.
    ///
    /// The checkpoint needs a majority of successful reports and expects to
    /// finish within 60 seconds.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStateData` if this node is not the leader.
    fn initiate_checkpoint(&self, checkpoint_id: CheckpointId) -> Result<(), StateError> {
        if !self.is_leader() {
            return Err(StateError::InvalidStateData {
                message: "Only leader can initiate checkpoints".to_string(),
            });
        }

        println!("Initiating distributed checkpoint: {checkpoint_id}");

        let quorum_size = self.get_quorum_size();

        let progress = CheckpointProgress {
            checkpoint_id,
            nodes_started: HashSet::new(),
            nodes_completed: HashSet::new(),
            nodes_failed: HashSet::new(),
            start_time: Instant::now(),
            expected_completion: Some(Instant::now() + Duration::from_secs(60)),
            metadata: CheckpointMetadata {
                checkpoint_type: "full".to_string(),
                coordinator_node: self.config.node_id.clone(),
                timeout: Duration::from_secs(60),
                required_quorum: quorum_size,
            },
        };

        {
            let mut checkpoints = self.active_checkpoints.lock().unwrap();
            checkpoints.insert(checkpoint_id, progress);
        }

        self.add_consensus_entry(
            ConsensusEntryType::CheckpointInitiation,
            checkpoint_id.to_le_bytes().to_vec(),
        )?;

        // Printed only. No barrier is sent.
        let members = self.cluster_members.read().unwrap();
        for (node_id, _node_info) in members.iter() {
            println!("Sending checkpoint barrier to node: {node_id}");
        }

        {
            let mut stats = self.stats.lock().unwrap();
            stats.total_checkpoints += 1;
        }

        Ok(())
    }

    /// Record a node's result for a checkpoint. Unknown checkpoint ids are
    /// ignored.
    ///
    /// Once successful reports reach the quorum, the checkpoint counts as done.
    /// While they are still below it, the failure check subtracts the quorum
    /// from the size of `nodes_started`, which is never filled, so it underflows.
    /// That panics in debug builds.
    fn report_completion(
        &self,
        checkpoint_id: CheckpointId,
        node_id: &str,
        success: bool,
    ) -> Result<(), StateError> {
        println!("Node {node_id} reported checkpoint {checkpoint_id} completion: {success}");

        let mut checkpoints = self.active_checkpoints.lock().unwrap();

        if let Some(progress) = checkpoints.get_mut(&checkpoint_id) {
            if success {
                progress.nodes_completed.insert(node_id.to_string());
            } else {
                progress.nodes_failed.insert(node_id.to_string());
            }

            let quorum_size = progress.metadata.required_quorum;
            let completed_count = progress.nodes_completed.len();
            let failed_count = progress.nodes_failed.len();

            if completed_count >= quorum_size {
                println!("Checkpoint {checkpoint_id} completed successfully with {completed_count} nodes");

                // Running average in whole milliseconds.
                {
                    let mut stats = self.stats.lock().unwrap();
                    stats.successful_checkpoints += 1;
                    let duration = progress.start_time.elapsed();
                    stats.avg_coordination_time = Duration::from_millis(
                        (stats.avg_coordination_time.as_millis() as u64
                            * (stats.successful_checkpoints - 1)
                            + duration.as_millis() as u64)
                            / stats.successful_checkpoints,
                    );
                }

                self.add_consensus_entry(
                    ConsensusEntryType::CheckpointCompletion,
                    checkpoint_id.to_le_bytes().to_vec(),
                )?;
            } else if failed_count > (progress.nodes_started.len() - quorum_size) {
                // Meant to fire when so many nodes failed that quorum can no
                // longer be reached.
                println!("Checkpoint {checkpoint_id} failed with {failed_count} failures");

                {
                    let mut stats = self.stats.lock().unwrap();
                    stats.failed_checkpoints += 1;
                }
            }
        }

        Ok(())
    }

    /// Poll every 100 ms until the checkpoint reaches quorum or `timeout` passes.
    ///
    /// Returns true on quorum and false on timeout. Has the same underflow in
    /// its failure check as `report_completion`. The checkpoint lock stays held
    /// during each 100 ms sleep, which blocks reports in the meantime.
    fn wait_for_completion(
        &self,
        checkpoint_id: CheckpointId,
        timeout: Duration,
    ) -> Result<bool, StateError> {
        let start_time = Instant::now();

        loop {
            if start_time.elapsed() > timeout {
                return Ok(false); // timed out
            }

            let checkpoints = self.active_checkpoints.lock().unwrap();
            if let Some(progress) = checkpoints.get(&checkpoint_id) {
                let quorum_size = progress.metadata.required_quorum;

                if progress.nodes_completed.len() >= quorum_size {
                    return Ok(true);
                }

                if progress.nodes_failed.len() > (progress.nodes_started.len() - quorum_size) {
                    return Ok(false); // quorum can no longer be reached
                }
            }

            thread::sleep(Duration::from_millis(100));
        }
    }

    /// Report cluster health from the member list.
    ///
    /// This node always counts as healthy. The leader is only known when it is
    /// this node. A lack of healthy majority is reported as `MajorPartition` with
    /// only this node listed.
    fn cluster_health(&self) -> Result<ClusterHealth, StateError> {
        let members = self.cluster_members.read().unwrap();
        let state = self.node_state.read().unwrap();

        let total_nodes = members.len() + 1; // +1 for self
        let mut healthy_nodes = 1; // this node always counts as healthy

        for (_node_id, node_info) in members.iter() {
            if let NodeHealth::Healthy = node_info.health {
                healthy_nodes += 1
            }
        }

        let leader_node = if state.role == NodeRole::Leader {
            Some(state.node_id.clone())
        } else {
            None
        };

        let partition_status = if healthy_nodes < (total_nodes / 2) + 1 {
            PartitionStatus::MajorPartition {
                majority_nodes: vec![state.node_id.clone()],
            }
        } else {
            PartitionStatus::Healthy
        };

        Ok(ClusterHealth {
            total_nodes,
            healthy_nodes,
            leader_node,
            partition_status,
        })
    }
}

impl InMemoryDistributedCoordinator {
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            cluster_state: Arc::new(RwLock::new(SimulatedClusterState::default())),
            active_checkpoints: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Add a healthy fake node with made up capabilities.
    pub fn add_node(&self, node_id: String) {
        let mut state = self.cluster_state.write().unwrap();
        let node_info = NodeInfo {
            node_id: node_id.clone(),
            endpoints: vec![format!("http://localhost:800{}", node_id.len())],
            last_seen: Instant::now(),
            capabilities: NodeCapabilities {
                can_be_leader: true,
                storage_capacity: 1024 * 1024 * 1024,  // 1 GiB
                processing_capacity: 4,                // 4 cores
                network_bandwidth: 1024 * 1024 * 1024, // 1 GiB/s
            },
            health: NodeHealth::Healthy,
        };
        state.nodes.insert(node_id, node_info);
    }

    /// Record a fake network split into two groups. Not used when computing
    /// health yet.
    pub fn simulate_partition(&self, partition1: Vec<String>, partition2: Vec<String>) {
        let mut state = self.cluster_state.write().unwrap();
        state.partitions = vec![partition1, partition2];
    }
}

impl DistributedCoordinator for InMemoryDistributedCoordinator {
    fn initiate_checkpoint(&self, checkpoint_id: CheckpointId) -> Result<(), StateError> {
        println!("Simulating checkpoint initiation: {checkpoint_id}");

        let progress = CheckpointProgress {
            checkpoint_id,
            nodes_started: HashSet::new(),
            nodes_completed: HashSet::new(),
            nodes_failed: HashSet::new(),
            start_time: Instant::now(),
            expected_completion: Some(Instant::now() + Duration::from_secs(10)),
            metadata: CheckpointMetadata {
                checkpoint_type: "simulated".to_string(),
                coordinator_node: self.node_id.clone(),
                timeout: Duration::from_secs(30),
                required_quorum: 2,
            },
        };

        let mut checkpoints = self.active_checkpoints.lock().unwrap();
        checkpoints.insert(checkpoint_id, progress);

        Ok(())
    }

    fn report_completion(
        &self,
        checkpoint_id: CheckpointId,
        node_id: &str,
        success: bool,
    ) -> Result<(), StateError> {
        println!("Simulating completion report from {node_id}: {checkpoint_id} ({success})");

        let mut checkpoints = self.active_checkpoints.lock().unwrap();
        if let Some(progress) = checkpoints.get_mut(&checkpoint_id) {
            if success {
                progress.nodes_completed.insert(node_id.to_string());
            } else {
                progress.nodes_failed.insert(node_id.to_string());
            }
        }

        Ok(())
    }

    fn wait_for_completion(
        &self,
        checkpoint_id: CheckpointId,
        timeout: Duration,
    ) -> Result<bool, StateError> {
        let start_time = Instant::now();

        loop {
            if start_time.elapsed() > timeout {
                return Ok(false);
            }

            let checkpoints = self.active_checkpoints.lock().unwrap();
            if let Some(progress) = checkpoints.get(&checkpoint_id) {
                if progress.nodes_completed.len() >= progress.metadata.required_quorum {
                    return Ok(true);
                }
            }

            thread::sleep(Duration::from_millis(10));
        }
    }

    /// Count this node and the fake nodes. The partition status is always
    /// `Healthy`, even after `simulate_partition`.
    fn cluster_health(&self) -> Result<ClusterHealth, StateError> {
        let state = self.cluster_state.read().unwrap();

        let total_nodes = state.nodes.len() + 1; // +1 for self
        let healthy_nodes = state
            .nodes
            .values()
            .filter(|node| matches!(node.health, NodeHealth::Healthy))
            .count()
            + 1; // +1 for self

        Ok(ClusterHealth {
            total_nodes,
            healthy_nodes,
            leader_node: state.leader.clone(),
            partition_status: PartitionStatus::Healthy,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_coordinator() {
        let coordinator = InMemoryDistributedCoordinator::new("node1".to_string());

        // Add some nodes
        coordinator.add_node("node2".to_string());
        coordinator.add_node("node3".to_string());

        // Initiate checkpoint
        assert!(coordinator.initiate_checkpoint(1).is_ok());

        // Report completions
        assert!(coordinator.report_completion(1, "node2", true).is_ok());
        assert!(coordinator.report_completion(1, "node3", true).is_ok());

        // Wait for completion
        let result = coordinator
            .wait_for_completion(1, Duration::from_secs(1))
            .unwrap();
        assert!(result);

        // Check cluster health
        let health = coordinator.cluster_health().unwrap();
        assert_eq!(health.total_nodes, 3);
        assert_eq!(health.healthy_nodes, 3);
    }

    #[test]
    fn test_raft_coordinator_creation() {
        let config = DistributedConfig {
            node_id: "test_node".to_string(),
            cluster_endpoints: vec!["http://localhost:8001".to_string()],
            election_timeout: Duration::from_secs(5),
            heartbeat_interval: Duration::from_secs(1),
            partition_tolerance: Duration::from_secs(10),
        };

        let coordinator = RaftDistributedCoordinator::new(config);

        assert_eq!(coordinator.config.node_id, "test_node");
        assert!(!coordinator.is_leader()); // starts as a follower
    }

    #[test]
    fn test_checkpoint_progress_tracking() {
        let coordinator = InMemoryDistributedCoordinator::new("coordinator".to_string());

        // Initiate checkpoint
        coordinator.initiate_checkpoint(1).unwrap();

        // Verify progress tracking
        let checkpoints = coordinator.active_checkpoints.lock().unwrap();
        let progress = checkpoints.get(&1).unwrap();

        assert_eq!(progress.checkpoint_id, 1);
        assert_eq!(progress.metadata.coordinator_node, "coordinator");
        assert_eq!(progress.metadata.required_quorum, 2);
    }

    #[test]
    fn test_quorum_calculation() {
        let config = DistributedConfig {
            node_id: "test".to_string(),
            cluster_endpoints: vec![],
            election_timeout: Duration::from_secs(5),
            heartbeat_interval: Duration::from_secs(1),
            partition_tolerance: Duration::from_secs(10),
        };

        let coordinator = RaftDistributedCoordinator::new(config);

        // Add some cluster members
        {
            let mut members = coordinator.cluster_members.write().unwrap();
            members.insert(
                "node1".to_string(),
                NodeInfo {
                    node_id: "node1".to_string(),
                    endpoints: vec![],
                    last_seen: Instant::now(),
                    capabilities: NodeCapabilities {
                        can_be_leader: true,
                        storage_capacity: 0,
                        processing_capacity: 0,
                        network_bandwidth: 0,
                    },
                    health: NodeHealth::Healthy,
                },
            );
            members.insert(
                "node2".to_string(),
                NodeInfo {
                    node_id: "node2".to_string(),
                    endpoints: vec![],
                    last_seen: Instant::now(),
                    capabilities: NodeCapabilities {
                        can_be_leader: true,
                        storage_capacity: 0,
                        processing_capacity: 0,
                        network_bandwidth: 0,
                    },
                    health: NodeHealth::Healthy,
                },
            );
        }

        // Two members plus this node is 3 nodes, so the quorum is 2.
        assert_eq!(coordinator.get_quorum_size(), 2);
    }
}
