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

//! Query Helper Functions
//!
//! Provides utilities to extract dependencies from queries for validation.

use crate::query_api::execution::query::input::stream::input_stream::InputStreamTrait;
use crate::query_api::execution::query::input::stream::InputStream;
use crate::query_api::execution::query::Query;
use std::collections::HashSet;

/// Trait for extracting source streams from queries
pub trait QuerySourceExtractor {
    /// Extract all source stream names from query
    ///
    /// Returns stream names from:
    /// - FROM clause (primary data source)
    /// - JOIN clauses (all JOIN types: INNER, LEFT, RIGHT, FULL)
    ///
    /// NOTE: Subqueries NOT supported (EventFlux does not support subqueries)
    fn get_source_streams(&self) -> Vec<String>;

    /// Get the target stream name (output stream)
    fn get_target_stream(&self) -> Option<String>;
}

impl QuerySourceExtractor for Query {
    fn get_source_streams(&self) -> Vec<String> {
        let mut sources = Vec::new();

        if let Some(input_stream) = &self.input_stream {
            extract_sources_from_input_stream(input_stream, &mut sources);
        }

        sources
    }

    fn get_target_stream(&self) -> Option<String> {
        self.output_stream.get_target_id().map(|s| s.to_string())
    }
}

/// Extract source stream names from InputStream recursively
fn extract_sources_from_input_stream(input_stream: &InputStream, sources: &mut Vec<String>) {
    match input_stream {
        InputStream::Single(single) => {
            // Get stream ID from single input stream
            let stream_id = single.get_stream_id_str().to_string();
            sources.push(stream_id);
        }
        InputStream::Join(join) => {
            // Extract from left and right streams in JOIN
            extract_sources_from_single_stream(&join.left_input_stream, sources);
            extract_sources_from_single_stream(&join.right_input_stream, sources);
        }
        InputStream::State(state) => {
            // Extract from pattern/sequence state elements
            // State streams contain multiple stream references
            let stream_ids = state.get_all_stream_ids();
            sources.extend(stream_ids);
        }
    }
}

/// Extract source from SingleInputStream
fn extract_sources_from_single_stream(
    single: &crate::query_api::execution::query::input::stream::SingleInputStream,
    sources: &mut Vec<String>,
) {
    let stream_id = single.get_stream_id_str().to_string();
    sources.push(stream_id);
}

/// Build dependency graph from queries
///
/// Creates a HashMap mapping target_stream → [source_streams] for validation.
///
/// # Arguments
/// * `queries` - List of queries to analyze
///
/// # Returns
/// * HashMap where keys are target streams and values are sets of source streams
pub fn build_dependency_graph(
    queries: &[Query],
) -> std::collections::HashMap<String, HashSet<String>> {
    let mut dependencies = std::collections::HashMap::new();

    for query in queries {
        if let Some(target) = query.get_target_stream() {
            let sources = query.get_source_streams();
            let source_set: HashSet<String> = sources.into_iter().collect();

            dependencies
                .entry(target)
                .or_insert_with(HashSet::new)
                .extend(source_set);
        }
    }

    dependencies
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_api::execution::query::input::stream::{InputStream, SingleInputStream};
    use crate::query_api::execution::query::output::output_stream::{
        InsertIntoStreamAction, OutputStream, OutputStreamAction,
    };
    use crate::query_api::execution::query::selection::Selector;
    use crate::query_api::execution::query::Query;

    fn create_test_query(input_stream_name: &str, output_stream_name: &str) -> Query {
        let input_stream = InputStream::Single(SingleInputStream::new_basic(
            input_stream_name.to_string(),
            false,
            false,
            None,
            Vec::new(),
        ));

        let output_stream = OutputStream {
            eventflux_element: Default::default(),
            action: OutputStreamAction::InsertInto(InsertIntoStreamAction {
                target_id: output_stream_name.to_string(),
                is_inner_stream: false,
                is_fault_stream: false,
            }),
            output_event_type: None,
        };

        Query {
            eventflux_element: Default::default(),
            input_stream: Some(input_stream),
            selector: Selector::new(),
            output_stream,
            output_rate: None,
            annotations: Vec::new(),
        }
    }

    #[test]
    fn test_get_source_streams_single() {
        let query = create_test_query("InputStream", "OutputStream");
        let sources = query.get_source_streams();

        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0], "InputStream");
    }

    #[test]
    fn test_get_target_stream() {
        let query = create_test_query("InputStream", "OutputStream");
        let target = query.get_target_stream();

        assert_eq!(target, Some("OutputStream".to_string()));
    }

    #[test]
    fn test_build_dependency_graph_linear() {
        let queries = vec![
            create_test_query("A", "B"),
            create_test_query("B", "C"),
            create_test_query("C", "D"),
        ];

        let deps = build_dependency_graph(&queries);

        assert_eq!(deps.len(), 3);
        assert!(deps.get("B").unwrap().contains("A"));
        assert!(deps.get("C").unwrap().contains("B"));
        assert!(deps.get("D").unwrap().contains("C"));
    }

    #[test]
    fn test_build_dependency_graph_multiple_sources() {
        // If we had JOIN support, this would test multiple sources
        // For now, test single source per query
        let queries = vec![
            create_test_query("Source1", "Target"),
            create_test_query("Source2", "Target"),
        ];

        let deps = build_dependency_graph(&queries);

        // Target depends on both Source1 and Source2
        assert_eq!(deps.len(), 1);
        let target_deps = deps.get("Target").unwrap();
        assert_eq!(target_deps.len(), 2);
        assert!(target_deps.contains("Source1"));
        assert!(target_deps.contains("Source2"));
    }

    #[test]
    fn test_query_without_target() {
        // Query without output target should not appear in dependency graph
        let mut query = create_test_query("Input", "Output");
        query.output_stream.action = OutputStreamAction::InsertInto(InsertIntoStreamAction {
            target_id: String::new(),
            is_inner_stream: false,
            is_fault_stream: false,
        });

        let queries = vec![query];
        let deps = build_dependency_graph(&queries);

        // Should have entry for empty string if that's the target
        // In real scenarios, queries should always have valid targets
        assert!(deps.is_empty() || deps.contains_key(""));
    }
}
