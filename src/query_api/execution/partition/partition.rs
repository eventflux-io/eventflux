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

use super::partition_type::PartitionType;
use crate::query_api::annotation::Annotation;
use crate::query_api::execution::query::Query;
use std::collections::HashMap;

use super::range_partition_type::{RangePartitionProperty, RangePartitionType};
use super::value_partition_type::ValuePartitionType;
use crate::query_api::expression::Expression;

/// A group of queries that run once per partition key.
#[derive(Clone, Debug, PartialEq)]
pub struct Partition {
    // Position in the query text. Held directly instead of through EventFluxElement.
    pub query_context_start_index: Option<(i32, i32)>,
    pub query_context_end_index: Option<(i32, i32)>,

    /// How each stream is partitioned, keyed by the stream id.
    pub partition_type_map: HashMap<String, PartitionType>,
    pub query_list: Vec<Query>,
    pub annotations: Vec<Annotation>,
    // Query names are not stored. add_query() scans query_list to warn on duplicates.
}

impl Partition {
    pub fn new() -> Self {
        Partition {
            query_context_start_index: None,
            query_context_end_index: None,
            partition_type_map: HashMap::new(),
            query_list: Vec::new(),
            annotations: Vec::new(),
        }
    }

    /// Same as `new`. Starts a builder chain.
    #[allow(clippy::self_named_constructors)]
    pub fn partition() -> Self {
        Self::new()
    }

    /// Build one range for `with_range_partition`. Events matching `condition` get
    /// `partition_key` as their key.
    pub fn range_property(partition_key: String, condition: Expression) -> RangePartitionProperty {
        RangePartitionProperty::new(partition_key, condition)
    }

    /// Partition `stream_id` by the value of `expression`.
    ///
    /// Replaces any earlier partition for the same stream and prints a warning to stderr.
    pub fn with_value_partition(mut self, stream_id: String, expression: Expression) -> Self {
        if self.partition_type_map.contains_key(&stream_id) {
            eprintln!(
                "Warning: Duplicate partition definition for stream '{}'",
                stream_id
            );
        }

        let value_partition = ValuePartitionType::new(stream_id.clone(), expression);
        self.partition_type_map
            .insert(stream_id, PartitionType::new_value(value_partition));
        self
    }

    /// Partition `stream_id` by a list of ranges.
    ///
    /// Replaces any earlier partition for the same stream and prints a warning to stderr.
    pub fn with_range_partition(
        mut self,
        stream_id: String,
        range_props: Vec<RangePartitionProperty>,
    ) -> Self {
        if self.partition_type_map.contains_key(&stream_id) {
            eprintln!(
                "Warning: Duplicate partition definition for stream '{}'",
                stream_id
            );
        }

        let range_partition = RangePartitionType::new(stream_id.clone(), range_props);
        self.partition_type_map
            .insert(stream_id, PartitionType::new_range(range_partition));
        self
    }

    /// Add a ready made partition type, keyed by its stream id.
    ///
    /// Replaces any earlier partition for the same stream and prints a warning to stderr.
    pub fn with_partition_type(mut self, partition_type: PartitionType) -> Self {
        let stream_id = partition_type.get_stream_id().to_string();

        if self.partition_type_map.contains_key(&stream_id) {
            eprintln!(
                "Warning: Duplicate partition definition for stream '{}'",
                stream_id
            );
        }

        self.partition_type_map.insert(stream_id, partition_type);
        self
    }

    /// Add a query to the partition.
    ///
    /// A query whose @info(name) matches an earlier one is still added. It only prints a
    /// warning to stderr.
    pub fn add_query(mut self, query: Query) -> Self {
        if let Some(query_name) = self.extract_query_name(&query) {
            for existing_query in &self.query_list {
                if let Some(existing_name) = self.extract_query_name(existing_query) {
                    if existing_name == query_name {
                        eprintln!(
                            "Warning: Duplicate query name '{}' in partition",
                            query_name
                        );
                        break;
                    }
                }
            }
        }

        self.query_list.push(query);
        self
    }

    // The name from the query's @info(name = '...') annotation, if any.
    fn extract_query_name(&self, query: &Query) -> Option<String> {
        for annotation in query.get_annotations() {
            if annotation.name == "info" {
                for element in &annotation.elements {
                    if element.key == "name" {
                        return Some(element.value.clone());
                    }
                }
            }
        }
        None
    }

    pub fn annotation(mut self, annotation: Annotation) -> Self {
        self.annotations.push(annotation);
        self
    }
}

impl Default for Partition {
    fn default() -> Self {
        Self::new()
    }
}

use crate::query_api::execution::execution_element::ExecutionElementTrait;
impl ExecutionElementTrait for Partition {
    fn get_annotations(&self) -> &Vec<Annotation> {
        &self.annotations
    }
}
