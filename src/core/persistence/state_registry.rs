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

// src/core/persistence/state_registry.rs

//! State registry.
//!
//! [`StateRegistry`] keeps a list of every stateful component, what each one
//! depends on, and the order to recover them in.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use super::state_holder::{AccessPattern, ComponentId, StateError, StateHolder, StateSize};

/// Registry details for one component.
#[derive(Debug, Clone)]
pub struct ComponentMetadata {
    pub component_id: ComponentId,
    pub component_type: String,
    /// Components that must be recovered before this one.
    pub dependencies: Vec<ComponentId>,
    /// Components that depend on this one. Only read on unregister.
    pub dependents: Vec<ComponentId>,
    pub priority: ComponentPriority,
    pub resource_requirements: ResourceRequirements,
}

/// Recovery priority. Not used for ordering yet, which follows dependencies only.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ComponentPriority {
    Critical = 0, // must recover first, such as sources
    High = 1,     // needed for the system to work
    Medium = 2,   // normal processing components
    Low = 3,      // optional or derived components
}

/// Expected resource use of a component.
#[derive(Debug, Clone)]
pub struct ResourceRequirements {
    pub memory_estimate: usize,
    pub cpu_weight: f64,
    pub io_intensity: IoIntensity,
    pub network_requirements: NetworkRequirements,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IoIntensity {
    None,
    Light,
    Moderate,
    Heavy,
}

#[derive(Debug, Clone)]
pub struct NetworkRequirements {
    pub requires_replication: bool,
    pub bandwidth_estimate: Option<u64>, // bytes per second
}

/// Components, their dependency graph, and the recovery order.
#[derive(Debug, Clone)]
pub struct StateTopology {
    pub components: HashMap<ComponentId, ComponentMetadata>,
    pub dependency_graph: DependencyGraph,
    /// Recovery order as stages. Every component in a stage depends only on
    /// earlier stages. Only components that appear in the dependency graph are
    /// listed, so a component with no dependencies and no dependents is missing.
    pub recovery_stages: Vec<Vec<ComponentId>>,
}

impl Default for StateTopology {
    fn default() -> Self {
        Self::new()
    }
}

impl StateTopology {
    pub fn new() -> Self {
        Self {
            components: HashMap::new(),
            dependency_graph: DependencyGraph::new(),
            recovery_stages: Vec::new(),
        }
    }

    /// Rebuild `recovery_stages` from the dependency graph.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStateData` if the dependencies form a cycle. The old
    /// stages are kept in that case.
    pub fn calculate_recovery_order(&mut self) -> Result<(), StateError> {
        let stages = self.dependency_graph.topological_sort()?;
        self.recovery_stages = stages;
        Ok(())
    }

    /// Returns the recovery stages. Components in one stage can be recovered in
    /// parallel.
    pub fn get_parallel_recovery_groups(&self) -> Vec<Vec<ComponentId>> {
        self.recovery_stages.clone()
    }
}

/// Which components depend on which.
#[derive(Debug, Clone)]
pub struct DependencyGraph {
    /// For each component, the components that depend on it.
    pub edges: HashMap<ComponentId, HashSet<ComponentId>>,
    /// For each component, the components it depends on.
    pub reverse_edges: HashMap<ComponentId, HashSet<ComponentId>>,
}

impl Default for DependencyGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl DependencyGraph {
    pub fn new() -> Self {
        Self {
            edges: HashMap::new(),
            reverse_edges: HashMap::new(),
        }
    }

    /// Record that `from` depends on `to`, so `to` must be recovered first.
    pub fn add_dependency(&mut self, from: ComponentId, to: ComponentId) {
        // Stored as an edge from `to` to `from`, so `to` comes first in the
        // topological order.
        self.edges
            .entry(to.clone())
            .or_default()
            .insert(from.clone());
        self.reverse_edges.entry(from).or_default().insert(to);
    }

    /// Remove the record that `from` depends on `to`. Leaves both nodes in the
    /// graph, even with no edges left.
    pub fn remove_dependency(&mut self, from: &ComponentId, to: &ComponentId) {
        if let Some(deps) = self.edges.get_mut(to) {
            deps.remove(from);
        }
        if let Some(reverse_deps) = self.reverse_edges.get_mut(from) {
            reverse_deps.remove(to);
        }
    }

    /// Sort the components into recovery stages.
    ///
    /// Each stage holds the components whose dependencies are all in earlier
    /// stages. Order inside a stage is not fixed.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStateData` if the dependencies form a cycle.
    pub fn topological_sort(&self) -> Result<Vec<Vec<ComponentId>>, StateError> {
        // in_degree counts how many dependencies each node still waits on.
        let mut in_degree: HashMap<ComponentId, usize> = HashMap::new();
        let mut all_nodes: HashSet<ComponentId> = HashSet::new();

        for (from, tos) in &self.edges {
            all_nodes.insert(from.clone());
            in_degree.entry(from.clone()).or_insert(0);

            for to in tos {
                all_nodes.insert(to.clone());
                *in_degree.entry(to.clone()).or_insert(0) += 1;
            }
        }

        let mut result = Vec::new();
        let mut queue: Vec<ComponentId> = in_degree
            .iter()
            .filter(|(_, &degree)| degree == 0)
            .map(|(node, _)| node.clone())
            .collect();

        while !queue.is_empty() {
            // Every node in the queue now has no pending dependencies, so they
            // form one stage.
            result.push(queue.clone());

            let current_level = queue.clone();
            queue.clear();

            for node in current_level {
                if let Some(neighbors) = self.edges.get(&node) {
                    for neighbor in neighbors {
                        if let Some(degree) = in_degree.get_mut(neighbor) {
                            *degree -= 1;
                            if *degree == 0 {
                                queue.push(neighbor.clone());
                            }
                        }
                    }
                }
            }
        }

        // Nodes in a cycle never reach zero, so they are left out.
        let processed_count: usize = result.iter().map(|stage| stage.len()).sum();
        if processed_count != all_nodes.len() {
            return Err(StateError::InvalidStateData {
                message: "Circular dependency detected in state components".to_string(),
            });
        }

        Ok(result)
    }

    /// Find dependency cycles.
    ///
    /// Uses a depth first search, not Tarjan's algorithm. Returns at most one
    /// cycle per search start, so it may not list every cycle.
    pub fn detect_cycles(&self) -> Vec<Vec<ComponentId>> {
        let mut visited = HashSet::new();
        let mut cycles = Vec::new();

        for node in self.edges.keys() {
            if !visited.contains(node) {
                let mut path = Vec::new();
                let mut current_visited = HashSet::new();
                if self.dfs_cycle_detection(node, &mut visited, &mut current_visited, &mut path) {
                    cycles.push(path);
                }
            }
        }

        cycles
    }

    fn dfs_cycle_detection(
        &self,
        node: &ComponentId,
        visited: &mut HashSet<ComponentId>,
        current_visited: &mut HashSet<ComponentId>,
        path: &mut Vec<ComponentId>,
    ) -> bool {
        if current_visited.contains(node) {
            // Back on the current path, so this is a cycle. Trim the path to it.
            if let Some(cycle_start) = path.iter().position(|n| n == node) {
                *path = path[cycle_start..].to_vec();
                return true;
            }
        }

        if visited.contains(node) {
            return false;
        }

        visited.insert(node.clone());
        current_visited.insert(node.clone());
        path.push(node.clone());

        if let Some(neighbors) = self.edges.get(node) {
            for neighbor in neighbors {
                if self.dfs_cycle_detection(neighbor, visited, current_visited, path) {
                    return true;
                }
            }
        }

        current_visited.remove(node);
        path.pop();
        false
    }
}

/// Result of a dependency analysis.
#[derive(Debug, Clone)]
pub struct StateDependencyGraph {
    pub total_components: usize,
    /// Number of recovery stages.
    pub dependency_depth: usize,
    /// Size of the largest recovery stage.
    pub parallel_recovery_width: usize,
    pub circular_dependencies: Vec<Vec<ComponentId>>,
    /// Meant to be the longest dependency chain. See
    /// [`StateRegistry::analyze_dependencies`] for what it holds today.
    pub critical_path: Vec<ComponentId>,
}

/// Registry of every stateful component.
pub struct StateRegistry {
    components: RwLock<HashMap<ComponentId, Arc<dyn StateHolder>>>,
    metadata: RwLock<HashMap<ComponentId, ComponentMetadata>>,
    topology: RwLock<StateTopology>,
}

impl StateRegistry {
    pub fn new() -> Self {
        Self {
            components: RwLock::new(HashMap::new()),
            metadata: RwLock::new(HashMap::new()),
            topology: RwLock::new(StateTopology::new()),
        }
    }

    /// Register a stateful component and rebuild the recovery order.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStateData` if `id` is empty, already registered, or its
    /// dependencies create a cycle. In the cycle case the component and its
    /// dependencies stay registered.
    pub fn register(
        &self,
        id: ComponentId,
        component: Arc<dyn StateHolder>,
        metadata: ComponentMetadata,
    ) -> Result<(), StateError> {
        if id.is_empty() {
            return Err(StateError::InvalidStateData {
                message: "Component ID cannot be empty".to_string(),
            });
        }

        {
            let mut components = self.components.write().unwrap();
            if components.contains_key(&id) {
                return Err(StateError::InvalidStateData {
                    message: format!("Component '{id}' already registered"),
                });
            }
            components.insert(id.clone(), component);
        }

        {
            let mut metadata_map = self.metadata.write().unwrap();
            let mut topology = self.topology.write().unwrap();

            for dep in &metadata.dependencies {
                topology
                    .dependency_graph
                    .add_dependency(id.clone(), dep.clone());
            }

            metadata_map.insert(id.clone(), metadata.clone());
            topology.components.insert(id, metadata);

            topology.calculate_recovery_order()?;
        }

        Ok(())
    }

    /// Remove a component and its dependency edges, then rebuild the recovery
    /// order.
    ///
    /// Edges are removed using the `dependencies` and `dependents` lists in the
    /// component's metadata. Edges added by other components that are not in
    /// `dependents` stay in the graph.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStateData` if the component is not registered, or if the
    /// remaining dependencies form a cycle.
    pub fn unregister(&self, id: &ComponentId) -> Result<(), StateError> {
        let mut components = self.components.write().unwrap();
        let mut metadata_map = self.metadata.write().unwrap();
        let mut topology = self.topology.write().unwrap();

        if !components.contains_key(id) {
            return Err(StateError::InvalidStateData {
                message: format!("Component '{id}' not found"),
            });
        }

        components.remove(id);

        if let Some(metadata) = metadata_map.remove(id) {
            for dep in &metadata.dependencies {
                topology.dependency_graph.remove_dependency(id, dep);
            }
            for dependent in &metadata.dependents {
                topology.dependency_graph.remove_dependency(dependent, id);
            }
        }

        topology.components.remove(id);
        topology.calculate_recovery_order()?;

        Ok(())
    }

    pub fn get_component(&self, id: &ComponentId) -> Option<Arc<dyn StateHolder>> {
        let components = self.components.read().unwrap();
        components.get(id).cloned()
    }

    pub fn get_metadata(&self, id: &ComponentId) -> Option<ComponentMetadata> {
        let metadata = self.metadata.read().unwrap();
        metadata.get(id).cloned()
    }

    /// Returns every registered component id, in no set order.
    pub fn get_all_component_ids(&self) -> Vec<ComponentId> {
        let components = self.components.read().unwrap();
        components.keys().cloned().collect()
    }

    /// Returns a copy of the topology.
    pub fn get_topology(&self) -> StateTopology {
        let topology = self.topology.read().unwrap();
        topology.clone()
    }

    /// Summarize the component dependencies.
    ///
    /// `critical_path` is the node list from one depth first walk of the graph,
    /// which includes side branches. It is not strictly the longest chain.
    pub fn analyze_dependencies(&self) -> StateDependencyGraph {
        let topology = self.topology.read().unwrap();
        let circular_deps = topology.dependency_graph.detect_cycles();

        let recovery_stages = &topology.recovery_stages;
        let parallel_width = recovery_stages
            .iter()
            .map(|stage| stage.len())
            .max()
            .unwrap_or(0);

        let critical_path = self.find_critical_path(&topology);

        StateDependencyGraph {
            total_components: topology.components.len(),
            dependency_depth: recovery_stages.len(),
            parallel_recovery_width: parallel_width,
            circular_dependencies: circular_deps,
            critical_path,
        }
    }

    /// Returns the sum of every component's size estimate.
    pub fn estimate_total_state_size(&self) -> StateSize {
        let components = self.components.read().unwrap();
        let mut total_bytes = 0;
        let mut total_entries = 0;
        let mut total_growth_rate = 0.0;

        for component in components.values() {
            let size = component.estimate_size();
            total_bytes += size.bytes;
            total_entries += size.entries;
            total_growth_rate += size.estimated_growth_rate;
        }

        StateSize {
            bytes: total_bytes,
            entries: total_entries,
            estimated_growth_rate: total_growth_rate,
        }
    }

    /// Returns the ids of components with the given access pattern.
    pub fn get_components_by_access_pattern(&self, pattern: AccessPattern) -> Vec<ComponentId> {
        let components = self.components.read().unwrap();
        components
            .iter()
            .filter(|(_, component)| component.access_pattern() == pattern)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Returns the largest node list from a depth first walk started at each
    /// unvisited component. Meant to find the longest dependency chain, but the
    /// walk never pops nodes, so side branches are included too.
    fn find_critical_path(&self, topology: &StateTopology) -> Vec<ComponentId> {
        let mut longest_path = Vec::new();
        let mut visited = HashSet::new();

        for component_id in topology.components.keys() {
            if !visited.contains(component_id) {
                let mut current_path = Vec::new();
                Self::dfs_longest_path(
                    component_id,
                    &topology.dependency_graph,
                    &mut visited,
                    &mut current_path,
                );

                if current_path.len() > longest_path.len() {
                    longest_path = current_path;
                }
            }
        }

        longest_path
    }

    fn dfs_longest_path(
        node: &ComponentId,
        graph: &DependencyGraph,
        visited: &mut HashSet<ComponentId>,
        current_path: &mut Vec<ComponentId>,
    ) {
        visited.insert(node.clone());
        current_path.push(node.clone());

        if let Some(neighbors) = graph.edges.get(node) {
            for neighbor in neighbors {
                if !visited.contains(neighbor) {
                    Self::dfs_longest_path(neighbor, graph, visited, current_path);
                }
            }
        }
    }
}

impl Default for StateRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::persistence::state_holder::{
        ChangeLog, SchemaVersion, SerializationHints, StateSnapshot,
    };

    // Only the size, access pattern and metadata methods work.
    struct MockStateHolder {
        id: String,
    }

    impl StateHolder for MockStateHolder {
        fn schema_version(&self) -> SchemaVersion {
            SchemaVersion::new(1, 0, 0)
        }

        fn serialize_state(
            &self,
            _hints: &SerializationHints,
        ) -> Result<StateSnapshot, StateError> {
            unimplemented!()
        }

        fn deserialize_state(&self, _snapshot: &StateSnapshot) -> Result<(), StateError> {
            unimplemented!()
        }

        fn get_changelog(
            &self,
            _since: super::super::state_holder::CheckpointId,
        ) -> Result<ChangeLog, StateError> {
            unimplemented!()
        }

        fn apply_changelog(&self, _changes: &ChangeLog) -> Result<(), StateError> {
            unimplemented!()
        }

        fn estimate_size(&self) -> StateSize {
            StateSize {
                bytes: 100,
                entries: 1,
                estimated_growth_rate: 0.0,
            }
        }

        fn access_pattern(&self) -> AccessPattern {
            AccessPattern::Warm
        }

        fn component_metadata(&self) -> super::super::state_holder::StateMetadata {
            super::super::state_holder::StateMetadata::new(
                self.id.clone(),
                "MockComponent".to_string(),
            )
        }
    }

    #[test]
    fn test_registry_registration() {
        let registry = StateRegistry::new();
        let component = Arc::new(MockStateHolder {
            id: "test1".to_string(),
        });

        let metadata = ComponentMetadata {
            component_id: "test1".to_string(),
            component_type: "MockComponent".to_string(),
            dependencies: Vec::new(),
            dependents: Vec::new(),
            priority: ComponentPriority::Medium,
            resource_requirements: ResourceRequirements {
                memory_estimate: 1024,
                cpu_weight: 1.0,
                io_intensity: IoIntensity::Light,
                network_requirements: NetworkRequirements {
                    requires_replication: false,
                    bandwidth_estimate: None,
                },
            },
        };

        assert!(registry
            .register("test1".to_string(), component, metadata)
            .is_ok());
        assert!(registry.get_component(&"test1".to_string()).is_some());
    }

    #[test]
    fn test_dependency_graph_topological_sort() {
        let mut graph = DependencyGraph::new();

        // A depends on B, and B depends on C, so the recovery order must be
        // C, then B, then A. add_dependency(from, to) means from depends on to.
        graph.add_dependency("A".to_string(), "B".to_string());
        graph.add_dependency("B".to_string(), "C".to_string());

        let result = graph.topological_sort().unwrap();

        println!("Topological sort result: {:?}", result);

        let mut c_pos = None;
        let mut b_pos = None;
        let mut a_pos = None;

        for (stage_idx, stage) in result.iter().enumerate() {
            if stage.contains(&"C".to_string()) {
                c_pos = Some(stage_idx);
            }
            if stage.contains(&"B".to_string()) {
                b_pos = Some(stage_idx);
            }
            if stage.contains(&"A".to_string()) {
                a_pos = Some(stage_idx);
            }
        }

        println!("Positions - C: {:?}, B: {:?}, A: {:?}", c_pos, b_pos, a_pos);

        // Dependencies are recovered first, so C comes before B, and B before A.
        assert!(c_pos.is_some());
        assert!(b_pos.is_some());
        assert!(a_pos.is_some());
        assert!(c_pos.unwrap() < b_pos.unwrap());
        assert!(b_pos.unwrap() < a_pos.unwrap());
    }

    #[test]
    fn test_cycle_detection() {
        let mut graph = DependencyGraph::new();

        // A depends on B, B on C, and C on A, which is a cycle.
        graph.add_dependency("A".to_string(), "B".to_string());
        graph.add_dependency("B".to_string(), "C".to_string());
        graph.add_dependency("C".to_string(), "A".to_string());

        let cycles = graph.detect_cycles();
        assert!(!cycles.is_empty());
    }
}
