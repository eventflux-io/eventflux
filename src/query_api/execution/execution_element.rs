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

use crate::query_api::annotation::Annotation;
use crate::query_api::execution::partition::Partition;
use crate::query_api::execution::query::Query;

/// A top level runnable unit of an app. Either a query or a partition.
#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum ExecutionElement {
    Query(Query),
    Partition(Partition),
}

/// Behaviour shared by queries and partitions. Implemented by Query, Partition and the
/// ExecutionElement enum.
pub trait ExecutionElementTrait {
    fn get_annotations(&self) -> &Vec<Annotation>;
}

// The enum passes the call on to the wrapped query or partition.
impl ExecutionElementTrait for ExecutionElement {
    fn get_annotations(&self) -> &Vec<Annotation> {
        match self {
            ExecutionElement::Query(q) => q.get_annotations(),
            ExecutionElement::Partition(p) => p.get_annotations(),
        }
    }
}

// Query and Partition each implement the trait in their own files, like this.
// In query.rs:
// impl ExecutionElementTrait for Query {
//     fn get_annotations(&self) -> &Vec<Annotation> { &self.annotations }
// }
// In partition.rs:
// impl ExecutionElementTrait for Partition {
//     fn get_annotations(&self) -> &Vec<Annotation> { &self.annotations }
// }

// The trait only covers annotations. Query and Partition each keep their own query
// position fields.
