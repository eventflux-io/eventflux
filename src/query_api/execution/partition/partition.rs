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
use crate::query_api::annotation::Annotation; // Using actual Annotation
use crate::query_api::execution::query::Query; // Actual Query
use std::collections::HashMap;

// For builder methods:
use super::range_partition_type::{RangePartitionProperty, RangePartitionType};
use super::value_partition_type::ValuePartitionType;
use crate::query_api::expression::Expression;

#[derive(Clone, Debug, PartialEq)]
pub struct Partition {
    // EventFluxElement fields for Partition itself
    pub query_context_start_index: Option<(i32, i32)>,
    pub query_context_end_index: Option<(i32, i32)>,

    // Partition fields from Java
    pub partition_type_map: HashMap<String, PartitionType>, // Key is streamId being partitioned
    pub query_list: Vec<Query>,
    pub annotations: Vec<Annotation>,
    // queryNameList from Java is for validation, might not be needed as a field in Rust
}

impl Partition {
    // Constructor
    pub fn new() -> Self {
        Partition {
            query_context_start_index: None,
            query_context_end_index: None,
            partition_type_map: HashMap::new(),
            query_list: Vec::new(),
            annotations: Vec::new(),
        }
    }

    // Static factory `partition()` from Java
    #[allow(clippy::self_named_constructors)]
    pub fn partition() -> Self {
        Self::new()
    }

    // Static factory `range()` from Java Partition for creating RangePartitionProperty
    pub fn range_property(partition_key: String, condition: Expression) -> RangePartitionProperty {
        RangePartitionProperty::new(partition_key, condition)
    }

    // Builder method `with(String streamId, Expression expression)`
    pub fn with_value_partition(mut self, stream_id: String, expression: Expression) -> Self {
        // Check for duplicate stream partition definition and warn
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

    // Builder method `with(String streamId, RangePartitionProperty... rangePartitionProperties)`
    pub fn with_range_partition(
        mut self,
        stream_id: String,
        range_props: Vec<RangePartitionProperty>,
    ) -> Self {
        // Check for duplicate stream partition definition and warn
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

    // Builder method `with(PartitionType partitionType)`
    pub fn with_partition_type(mut self, partition_type: PartitionType) -> Self {
        let stream_id = partition_type.get_stream_id().to_string();

        // Check for duplicate stream partition definition and warn
        if self.partition_type_map.contains_key(&stream_id) {
            eprintln!(
                "Warning: Duplicate partition definition for stream '{}'",
                stream_id
            );
        }

        self.partition_type_map.insert(stream_id, partition_type);
        self
    }

    // Builder method `addQuery(Query query)`
    pub fn add_query(mut self, query: Query) -> Self {
        // Check for duplicate query names and warn
        if let Some(query_name) = self.extract_query_name(&query) {
            // Check if this name already exists
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

    // Helper method to extract query name from annotations
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

    // Builder method `annotation(Annotation annotation)`
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

// Implement ExecutionElement for Partition (conceptual, actual trait defined elsewhere)
use crate::query_api::execution::execution_element::ExecutionElementTrait;
impl ExecutionElementTrait for Partition {
    fn get_annotations(&self) -> &Vec<Annotation> {
        &self.annotations
    }
}
