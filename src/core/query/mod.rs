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

// src/core/query/mod.rs

pub mod input;
pub mod processor; // Processor trait and the processors that run on streams,
                   // such as filters and windows
pub mod output; // Output processors for callbacks, streams, tables and aggregations
pub mod selector; // SELECT handling with SelectProcessor, attributes and aggregators
                  // pub mod stream; // This was for query_api::execution::query::input::stream, not core stream processors

// Left over from removing a second declaration. The `pub mod processor;` above is the
// real one.
// pub mod processor; // THIS IS THE DUPLICATE - REMOVING

// Query runtimes. Only QueryRuntime exists. OnDemandQueryRuntime is not ported.
// pub mod query_runtime;
// pub mod on_demand_query_runtime;
pub mod query_runtime;

pub use self::input::stream::join::{
    JoinProcessor, JoinProcessorSide, JoinSide, JoinStreamRuntime,
};
pub use self::processor::{CommonProcessorMeta, FilterProcessor, ProcessingMode, Processor};
pub use self::query_runtime::QueryRuntime;
pub use self::selector::{OutputAttributeProcessor, SelectProcessor};
