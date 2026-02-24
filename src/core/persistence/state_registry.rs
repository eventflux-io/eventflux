// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/persistence/state_registry.rs

//! State Registry for centralized state component management
//!
//! The StateRegistry maintains a catalog of all stateful components in the system,
//! their dependencies, and topology information for optimization.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use super::state_holder::{AccessPattern, ComponentId, StateError, StateHolder, StateSize};

/// Component metadata for registry
#[derive(Debug, Clone)]
pub struct ComponentMetadata {
    pub component_id: ComponentId,
    pub component_type: String,
    pub dependencies: Vec<ComponentId>,
    pub dependents: Vec<ComponentId>,
    pub priority: ComponentPriority,
    pub resource_requirements: ResourceRequirements,
}

/// Priority for component recovery ordering
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ComponentPriority {
    Critical = 0, // Must recover first (e.g., sources)
    High = 1,     // Important for system function
    Medium = 2,   // Normal processing components
    Low = 3,      // Optional or derived components
}

/// Resource requirements for optimization
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

/// State topology for dependency analysis
#[derive(Debug, Clone)]
pub struct StateTopology {
    pub components: HashMap<ComponentId, ComponentMetadata>,
    pub dependency_graph: DependencyGraph,
    pub recovery_stages: Vec<Vec<ComponentId>>, // Ordered recovery stages
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

    /// Calculate optimal recovery order based on dependencies
    pub fn calculate_recovery_order(&mut self) -> Result<(), StateError> {
        let stages = self.dependency_graph.topological_sort()?;
        self.recovery_stages = stages;
        Ok(())
    }

    /// Get components that can be recovered in parallel
    pub fn get_parallel_recovery_groups(&self) -> Vec<Vec<ComponentId>> {
        self.recovery_stages.clone()
    }
}

/// Dependency graph for state components
#[derive(Debug, Clone)]
pub struct DependencyGraph {
    pub edges: HashMap<ComponentId, HashSet<ComponentId>>,
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

    /// Add a dependency edge: from depends on to
    /// This means 'to' must be recovered before 'from' can be recovered
    pub fn add_dependency(&mut self, from: ComponentId, to: ComponentId) {
        // 'from' depends on 'to', so 'to' -> 'from' in the dependency graph
        // This means 'to' must come before 'from' in topological order
        self.edges
            .entry(to.clone())
            .or_default()
            .insert(from.clone());
        self.reverse_edges.entry(from).or_default().insert(to);
    }

    /// Remove a dependency
    pub fn remove_dependency(&mut self, from: &ComponentId, to: &ComponentId) {
        // 'from' depends on 'to', so 'to' -> 'from' in the dependency graph
        if let Some(deps) = self.edges.get_mut(to) {
            deps.remove(from);
        }
        if let Some(reverse_deps) = self.reverse_edges.get_mut(from) {
            reverse_deps.remove(to);
        }
    }

    /// Perform topological sort for recovery ordering
    pub fn topological_sort(&self) -> Result<Vec<Vec<ComponentId>>, StateError> {
        let mut in_degree: HashMap<ComponentId, usize> = HashMap::new();
        let mut all_nodes: HashSet<ComponentId> = HashSet::new();

        // Initialize in-degrees
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
            // All nodes in current queue can be processed in parallel
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

        // Check for cycles
        let processed_count: usize = result.iter().map(|stage| stage.len()).sum();
        if processed_count != all_nodes.len() {
            return Err(StateError::InvalidStateData {
                message: "Circular dependency detected in state components".to_string(),
            });
        }

        Ok(result)
    }

    /// Detect strongly connected components (cycles)
    pub fn detect_cycles(&self) -> Vec<Vec<ComponentId>> {
        // Implementation of Tarjan's algorithm for SCC detection
        // This is a simplified version - could be enhanced
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
            // Found a cycle
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

/// State dependency analysis results
#[derive(Debug, Clone)]
pub struct StateDependencyGraph {
    pub total_components: usize,
    pub dependency_depth: usize,
    pub parallel_recovery_width: usize,
    pub circular_dependencies: Vec<Vec<ComponentId>>,
    pub critical_path: Vec<ComponentId>,
}

/// Central registry for all stateful components
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

    /// Register a stateful component
    pub fn register(
        &self,
        id: ComponentId,
        component: Arc<dyn StateHolder>,
        metadata: ComponentMetadata,
    ) -> Result<(), StateError> {
        // Validate component
        if id.is_empty() {
            return Err(StateError::InvalidStateData {
                message: "Component ID cannot be empty".to_string(),
            });
        }

        // Register component
        {
            let mut components = self.components.write().unwrap();
            if components.contains_key(&id) {
                return Err(StateError::InvalidStateData {
                    message: format!("Component '{id}' already registered"),
                });
            }
            components.insert(id.clone(), component);
        }

        // Register metadata and update topology
        {
            let mut metadata_map = self.metadata.write().unwrap();
            let mut topology = self.topology.write().unwrap();

            // Add dependencies to topology
            for dep in &metadata.dependencies {
                topology
                    .dependency_graph
                    .add_dependency(id.clone(), dep.clone());
            }

            metadata_map.insert(id.clone(), metadata.clone());
            topology.components.insert(id, metadata);

            // Recalculate recovery order
            topology.calculate_recovery_order()?;
        }

        Ok(())
    }

    /// Unregister a component
    pub fn unregister(&self, id: &ComponentId) -> Result<(), StateError> {
        let mut components = self.components.write().unwrap();
        let mut metadata_map = self.metadata.write().unwrap();
        let mut topology = self.topology.write().unwrap();

        if !components.contains_key(id) {
            return Err(StateError::InvalidStateData {
                message: format!("Component '{id}' not found"),
            });
        }

        // Remove from components
        components.remove(id);

        // Remove from metadata and topology
        if let Some(metadata) = metadata_map.remove(id) {
            // Remove dependencies
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

    /// Get a component by ID
    pub fn get_component(&self, id: &ComponentId) -> Option<Arc<dyn StateHolder>> {
        let components = self.components.read().unwrap();
        components.get(id).cloned()
    }

    /// Get component metadata
    pub fn get_metadata(&self, id: &ComponentId) -> Option<ComponentMetadata> {
        let metadata = self.metadata.read().unwrap();
        metadata.get(id).cloned()
    }

    /// Get all component IDs
    pub fn get_all_component_ids(&self) -> Vec<ComponentId> {
        let components = self.components.read().unwrap();
        components.keys().cloned().collect()
    }

    /// Get state topology for optimization
    pub fn get_topology(&self) -> StateTopology {
        let topology = self.topology.read().unwrap();
        topology.clone()
    }

    /// Analyze state dependencies
    pub fn analyze_dependencies(&self) -> StateDependencyGraph {
        let topology = self.topology.read().unwrap();
        let circular_deps = topology.dependency_graph.detect_cycles();

        let recovery_stages = &topology.recovery_stages;
        let parallel_width = recovery_stages
            .iter()
            .map(|stage| stage.len())
            .max()
            .unwrap_or(0);

        // Find critical path (longest dependency chain)
        let critical_path = self.find_critical_path(&topology);

        StateDependencyGraph {
            total_components: topology.components.len(),
            dependency_depth: recovery_stages.len(),
            parallel_recovery_width: parallel_width,
            circular_dependencies: circular_deps,
            critical_path,
        }
    }

    /// Get total state size estimate
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

    /// Get components by access pattern
    pub fn get_components_by_access_pattern(&self, pattern: AccessPattern) -> Vec<ComponentId> {
        let components = self.components.read().unwrap();
        components
            .iter()
            .filter(|(_, component)| component.access_pattern() == pattern)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Find the critical path (longest dependency chain)
    fn find_critical_path(&self, topology: &StateTopology) -> Vec<ComponentId> {
        let mut longest_path = Vec::new();
        let mut visited = HashSet::new();

        for component_id in topology.components.keys() {
            if !visited.contains(component_id) {
                let mut current_path = Vec::new();
                self.dfs_longest_path(
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
        &self,
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
                    self.dfs_longest_path(neighbor, graph, visited, current_path);
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

    // Mock component for testing
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

        // Create a simple dependency chain where:
        // - A depends on B (A cannot start until B is recovered)
        // - B depends on C (B cannot start until C is recovered)
        // So recovery order should be: C -> B -> A
        //
        // In our implementation: add_dependency(from, to) means "from depends on to"
        // So A->B means A depends on B, B must come before A in topological order
        graph.add_dependency("A".to_string(), "B".to_string());
        graph.add_dependency("B".to_string(), "C".to_string());

        let result = graph.topological_sort().unwrap();

        println!("Topological sort result: {:?}", result);

        // Find the positions of each node
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

        // For recovery: C should come before B, and B should come before A
        // (dependencies must be recovered first)
        assert!(c_pos.is_some());
        assert!(b_pos.is_some());
        assert!(a_pos.is_some());
        assert!(c_pos.unwrap() < b_pos.unwrap());
        assert!(b_pos.unwrap() < a_pos.unwrap());
    }

    #[test]
    fn test_cycle_detection() {
        let mut graph = DependencyGraph::new();

        // Create a cycle: A -> B -> C -> A
        graph.add_dependency("A".to_string(), "B".to_string());
        graph.add_dependency("B".to_string(), "C".to_string());
        graph.add_dependency("C".to_string(), "A".to_string());

        let cycles = graph.detect_cycles();
        assert!(!cycles.is_empty());
    }
}
