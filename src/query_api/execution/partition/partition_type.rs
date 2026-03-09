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

// In Java, PartitionType is an interface.
// This struct wraps the variant and implements common traits/functionality.
#[derive(Clone, Debug, PartialEq)]
pub struct PartitionType {
    // Common EventFluxElement context for the PartitionType itself
    pub query_context_start_index: Option<(i32, i32)>,
    pub query_context_end_index: Option<(i32, i32)>,

    pub variant: PartitionTypeVariant,
}

impl PartitionType {
    // Constructors for value and range types
    pub fn new_value(value_type: ValuePartitionType) -> Self {
        // Context for PartitionType itself might be distinct from the inner variant's context,
        // or it could be derived/copied. For now, initializing as None.
        PartitionType {
            query_context_start_index: None, // Or copy from value_type if appropriate
            query_context_end_index: None,   // Or copy from value_type if appropriate
            variant: PartitionTypeVariant::Value(value_type),
        }
    }

    pub fn new_range(range_type: RangePartitionType) -> Self {
        PartitionType {
            query_context_start_index: None, // Or copy from range_type
            query_context_end_index: None,   // Or copy from range_type
            variant: PartitionTypeVariant::Range(range_type),
        }
    }

    // getStreamId() from Java's PartitionType interface
    pub fn get_stream_id(&self) -> &str {
        match &self.variant {
            PartitionTypeVariant::Value(v) => &v.stream_id,
            PartitionTypeVariant::Range(r) => &r.stream_id,
        }
    }
}

// The impl EventFluxElement for PartitionType block has been removed.
// PartitionType now directly holds its EventFluxElement-like fields.
