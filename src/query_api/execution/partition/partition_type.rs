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

use super::range_partition_type::RangePartitionType;
use super::value_partition_type::ValuePartitionType;

#[derive(Clone, Debug, PartialEq)]
pub enum PartitionTypeVariant {
    Value(ValuePartitionType),
    Range(RangePartitionType),
}

/// How one stream in a partition is split. Wraps a value or range variant.
#[derive(Clone, Debug, PartialEq)]
pub struct PartitionType {
    // Position in the query text of the partition type itself.
    pub query_context_start_index: Option<(i32, i32)>,
    pub query_context_end_index: Option<(i32, i32)>,

    pub variant: PartitionTypeVariant,
}

impl PartitionType {
    /// Wrap a value partition. The query positions start unset and are not copied from
    /// the variant.
    pub fn new_value(value_type: ValuePartitionType) -> Self {
        PartitionType {
            query_context_start_index: None,
            query_context_end_index: None,
            variant: PartitionTypeVariant::Value(value_type),
        }
    }

    /// Wrap a range partition. The query positions start unset and are not copied from
    /// the variant.
    pub fn new_range(range_type: RangePartitionType) -> Self {
        PartitionType {
            query_context_start_index: None,
            query_context_end_index: None,
            variant: PartitionTypeVariant::Range(range_type),
        }
    }

    /// Id of the stream this partition type applies to.
    pub fn get_stream_id(&self) -> &str {
        match &self.variant {
            PartitionTypeVariant::Value(v) => &v.stream_id,
            PartitionTypeVariant::Range(r) => &r.stream_id,
        }
    }
}
