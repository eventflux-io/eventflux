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

use crate::query_api::expression::Expression;

/// One range of a range partition. Events matching `condition` get `partition_key`.
#[derive(Clone, Debug, PartialEq)]
pub struct RangePartitionProperty {
    pub query_context_start_index: Option<(i32, i32)>,
    pub query_context_end_index: Option<(i32, i32)>,
    pub partition_key: String,
    pub condition: Expression,
}

impl RangePartitionProperty {
    pub fn new(partition_key: String, condition: Expression) -> Self {
        RangePartitionProperty {
            query_context_start_index: None,
            query_context_end_index: None,
            partition_key,
            condition,
        }
    }
}

/// Splits one stream into partitions by a list of conditions.
#[derive(Clone, Debug, PartialEq)]
pub struct RangePartitionType {
    // Position in the query text. Held directly instead of through EventFluxElement.
    pub query_context_start_index: Option<(i32, i32)>,
    pub query_context_end_index: Option<(i32, i32)>,

    /// Id of the stream being partitioned.
    pub stream_id: String,

    pub range_partition_properties: Vec<RangePartitionProperty>,
}

impl RangePartitionType {
    pub fn new(stream_id: String, range_partition_properties: Vec<RangePartitionProperty>) -> Self {
        RangePartitionType {
            query_context_start_index: None,
            query_context_end_index: None,
            stream_id,
            range_partition_properties,
        }
    }
}
