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

// In eventflux/src/query_api/execution/query/mod.rs

// Existing modules
pub mod input;
pub mod on_demand_query;
#[allow(clippy::module_inception)]
pub mod query;
pub mod selection;
pub mod store_query;

// Ensure output module is declared
pub mod output;

// Re-exports
pub use self::on_demand_query::{OnDemandQuery, OnDemandQueryType};
pub use self::query::Query;
pub use self::store_query::{StoreQuery, StoreQueryType};

pub use self::input::InputStream;
pub use self::selection::Order as OrderByOrder;
pub use self::selection::OrderByAttribute;
pub use self::selection::OutputAttribute;
pub use self::selection::Selector;

// Re-exports for the output types, using the actual names from their defining modules
pub use self::output::OutputEventType;
pub use self::output::OutputRate;
pub use self::output::OutputStream;
pub use self::output::SetAttribute; // Changed from SetAttributePlaceholder

// The aliases ActualOutputStream etc. are no longer needed if the output/mod.rs directly exports OutputStream.
// The previous `output/mod.rs` was:
// pub use self::output_stream::{OutputStream as ActualOutputStream, OutputEventType as ActualOutputEventType};
// pub use self.ratelimit::OutputRate;
// I will assume `output/mod.rs` now directly exports `OutputStream` and `OutputEventType` without aliases.
// If `output_stream.rs` defines `OutputStream` and `output/mod.rs` has `pub use self::output_stream::OutputStream;`, then this is fine.
// The `overwrite_file_with_block` for `output/mod.rs` in the previous step was:
// pub use self::output_stream::{OutputStream, OutputEventType, SetAttributePlaceholder};
// pub use self::ratelimit::{OutputRate, OutputRateVariant, OutputRateBehavior, EventsOutputRate, TimeOutputRate, SnapshotOutputRate};
// This is good. So, the re-exports above are correct.
