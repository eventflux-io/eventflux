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

pub mod execution_element;
pub mod partition;
pub mod query;

// Re-export top-level executable units
pub use self::execution_element::{ExecutionElement, ExecutionElementTrait}; // Assuming ExecutionElementTrait exists
pub use self::query::Query;
// If OnDemandQuery can be a top-level execution element (it's usually part of a StoreQuery or used directly by API)
// pub use self::query::OnDemandQuery;
pub use self::partition::Partition;

// Re-export common partition types for convenience
pub use self::partition::{
    PartitionType, PartitionTypeVariant, RangePartitionProperty, RangePartitionType,
    ValuePartitionType,
};

// Re-export key sub-modules or types from query if they are frequently accessed via `execution::`
pub use self::query::input;
pub use self::query::output;
pub use self::query::selection;
// For example, to allow `execution::input::InputStream`
// or `execution::output::OutputStream`
// or `execution::selection::Selector`
