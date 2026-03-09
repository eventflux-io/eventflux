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

// Corresponds to io.eventflux.query.api.execution.ExecutionElement

use crate::query_api::annotation::Annotation;
use crate::query_api::execution::partition::Partition;
use crate::query_api::execution::query::Query;

// This enum will wrap concrete types that are considered ExecutionElements.
#[derive(Clone, Debug, PartialEq)]
pub enum ExecutionElement {
    Query(Query),
    Partition(Partition),
}

// Trait for common behavior (get_annotations)
// Implemented by Partition and Query structs.
pub trait ExecutionElementTrait {
    fn get_annotations(&self) -> &Vec<Annotation>;
    // Potentially add other common methods if ExecutionElement interface in Java had more.
}

// Implement the trait for the enum by dispatching to variants.
impl ExecutionElementTrait for ExecutionElement {
    fn get_annotations(&self) -> &Vec<Annotation> {
        match self {
            ExecutionElement::Query(q) => q.get_annotations(), // Assumes Query has get_annotations()
            ExecutionElement::Partition(p) => p.get_annotations(), // Assumes Partition has get_annotations()
        }
    }
}

// Individual Query and Partition structs should also implement ExecutionElementTrait directly.
// Example (in query.rs):
// impl ExecutionElementTrait for Query {
//     fn get_annotations(&self) -> &Vec<Annotation> { &self.annotations }
// }
// Example (in partition.rs - already done in this subtask):
// impl ExecutionElementTrait for Partition {
//     fn get_annotations(&self) -> &Vec<Annotation> { &self.annotations }
// }

// Note: The prompt for Partition.rs in this subtask included:
// use crate::query_api::execution::execution_element::ExecutionElementTrait;
// impl ExecutionElementTrait for Partition { ... }
// This is consistent. Query.rs would need a similar direct implementation if not already present.
// The Java interface `ExecutionElement` only has `getAnnotations()`.
// `EventFluxElement` (for query context) is implemented by `Query` and `Partition` separately.
