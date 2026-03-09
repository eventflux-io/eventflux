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

pub mod partition;
pub mod partition_type;
pub mod range_partition_type;
pub mod value_partition_type;

pub use self::partition::Partition;
pub use self::partition_type::{PartitionType, PartitionTypeVariant};
pub use self::range_partition_type::{RangePartitionProperty, RangePartitionType};
pub use self::value_partition_type::ValuePartitionType;
