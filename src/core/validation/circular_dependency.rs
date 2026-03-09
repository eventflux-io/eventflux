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

//! Circular Dependency Detection
//!
//! Implements Phase 1 validation to detect circular dependencies in stream definitions.
//! Uses Depth-First Search (DFS) to detect cycles in the dependency graph.

use crate::core::exception::{EventFluxError, EventFluxResult};
use std::collections::{HashMap, HashSet};

/// Detect circular dependencies in query definitions
///
/// Uses DFS to detect cycles in the dependency graph where:
/// - Nodes are target streams
/// - Edges are dependencies (FROM and JOIN sources)
///
/// # Arguments
/// * `dependencies` - Map of target_stream → [source_streams]
///
/// # Returns
/// * `Ok(())` if no cycles detected
/// * `Err(EventFluxError)` with cycle path if circular dependency found
///
/// # Example Cycle Detection
/// ```text
/// A depends on B
/// B depends on C
/// C depends on A
/// → Cycle: A → B → C → A
/// ```
pub fn detect_circular_dependencies(
    dependencies: &HashMap<String, HashSet<String>>,
) -> EventFluxResult<()> {
    // Track globally visited nodes (completed DFS)
    let mut visited_global = HashSet::new();

    // Check each node as potential cycle start
    for start_node in dependencies.keys() {
        if !visited_global.contains(start_node) {
            // Track nodes in current DFS path (for cycle detection)
            let mut visited_path = HashSet::new();
            // Track path for error reporting
            let mut path = Vec::new();

            dfs_check_cycle(
                start_node,
                dependencies,
                &mut visited_global,
                &mut visited_path,
                &mut path,
            )?;
        }
    }

    // ✅ No cycles detected
    Ok(())
}

/// DFS traversal to detect cycles
///
/// # Arguments
/// * `node` - Current node being visited
/// * `dependencies` - Dependency graph
/// * `visited_global` - Nodes that completed DFS (no cycles from them)
/// * `visited_path` - Nodes in current DFS path (cycle if revisited)
/// * `path` - Current path for error reporting
fn dfs_check_cycle(
    node: &String,
    dependencies: &HashMap<String, HashSet<String>>,
    visited_global: &mut HashSet<String>,
    visited_path: &mut HashSet<String>,
    path: &mut Vec<String>,
) -> EventFluxResult<()> {
    // If node already in current path → CYCLE DETECTED
    if visited_path.contains(node) {
        path.push(node.clone());
        let cycle_path = path.join(" → ");
        return Err(EventFluxError::validation_failed(format!(
            "Circular dependency detected: {}\nStreams cannot form dependency cycles.",
            cycle_path
        )));
    }

    // If already visited in previous DFS, skip (already validated)
    if visited_global.contains(node) {
        return Ok(());
    }

    // Mark as visited in current path
    visited_path.insert(node.clone());
    path.push(node.clone());

    // Visit all dependencies
    if let Some(deps) = dependencies.get(node) {
        for dep in deps {
            dfs_check_cycle(dep, dependencies, visited_global, visited_path, path)?;
        }
    }

    // Backtrack: remove from current path
    visited_path.remove(node);
    path.pop();

    // Mark as globally visited (completed DFS from this node)
    visited_global.insert(node.clone());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_dependencies(edges: Vec<(&str, Vec<&str>)>) -> HashMap<String, HashSet<String>> {
        edges
            .into_iter()
            .map(|(target, sources)| {
                (
                    target.to_string(),
                    sources.into_iter().map(String::from).collect(),
                )
            })
            .collect()
    }

    #[test]
    fn test_no_dependencies_empty() {
        let deps = HashMap::new();
        let result = detect_circular_dependencies(&deps);
        assert!(result.is_ok());
    }

    #[test]
    fn test_linear_dependency_chain() {
        // A ← B ← C ← D (no cycles)
        let deps = create_dependencies(vec![("B", vec!["A"]), ("C", vec!["B"]), ("D", vec!["C"])]);

        let result = detect_circular_dependencies(&deps);
        assert!(result.is_ok());
    }

    #[test]
    fn test_direct_circular_dependency() {
        // A → B, B → A (direct cycle)
        let deps = create_dependencies(vec![("A", vec!["B"]), ("B", vec!["A"])]);

        let result = detect_circular_dependencies(&deps);
        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("Circular dependency detected"));
        assert!(err_msg.contains("A") || err_msg.contains("B"));
    }

    #[test]
    fn test_multi_level_circular_dependency() {
        // A → B → C → A (3-node cycle)
        let deps = create_dependencies(vec![("A", vec!["B"]), ("B", vec!["C"]), ("C", vec!["A"])]);

        let result = detect_circular_dependencies(&deps);
        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("Circular dependency detected"));
        // Should show the cycle path
        assert!(err_msg.contains("→"));
    }

    #[test]
    fn test_self_referencing_stream() {
        // A → A (self-reference)
        let deps = create_dependencies(vec![("A", vec!["A"])]);

        let result = detect_circular_dependencies(&deps);
        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("Circular dependency detected"));
        assert!(err_msg.contains("A → A"));
    }

    #[test]
    fn test_complex_graph_with_cycle() {
        // A → B → C → D
        //     ↓       ↑
        //     E ------+ (cycle: B → E → D → C → B)
        let deps = create_dependencies(vec![
            ("A", vec!["B"]),
            ("B", vec!["C"]),
            ("C", vec!["D"]),
            ("B", vec!["E"]),
            ("E", vec!["D"]),
            ("D", vec!["C"]),
            ("C", vec!["B"]),
        ]);

        let result = detect_circular_dependencies(&deps);
        assert!(result.is_err());
    }

    #[test]
    fn test_diamond_dependency_no_cycle() {
        //     A
        //    / \
        //   B   C
        //    \ /
        //     D
        let deps = create_dependencies(vec![
            ("B", vec!["A"]),
            ("C", vec!["A"]),
            ("D", vec!["B", "C"]),
        ]);

        let result = detect_circular_dependencies(&deps);
        assert!(result.is_ok());
    }

    #[test]
    fn test_multiple_independent_chains() {
        // Chain 1: A ← B ← C
        // Chain 2: X ← Y ← Z
        let deps = create_dependencies(vec![
            ("B", vec!["A"]),
            ("C", vec!["B"]),
            ("Y", vec!["X"]),
            ("Z", vec!["Y"]),
        ]);

        let result = detect_circular_dependencies(&deps);
        assert!(result.is_ok());
    }

    #[test]
    fn test_one_cycle_in_multiple_chains() {
        // Chain 1: A ← B ← C (ok)
        // Chain 2: X → Y → X (cycle)
        let deps = create_dependencies(vec![
            ("B", vec!["A"]),
            ("C", vec!["B"]),
            ("X", vec!["Y"]),
            ("Y", vec!["X"]),
        ]);

        let result = detect_circular_dependencies(&deps);
        assert!(result.is_err());
    }
}
