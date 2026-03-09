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
pub mod processor; // For join stream runtimes and other input handling
                   // Other query submodules will be added here: input, output, selector (core internal versions)
pub mod output; // For core query output components (callbacks, rate limiters)
pub mod selector; // For core query selector components (QuerySelector/SelectProcessor)
                  // pub mod stream; // This was for query_api::execution::query::input::stream, not core stream processors

// pub mod processor; // THIS IS THE DUPLICATE - REMOVING
// The first `pub mod processor;` at the top of the file is correct.

// For top-level query runtime classes like QueryRuntime, OnDemandQueryRuntime
// pub mod query_runtime;
// pub mod on_demand_query_runtime;
pub mod query_runtime; // Added
                       // etc.

// Re-export items from the processor and selector modules
pub use self::input::stream::join::{
    JoinProcessor, JoinProcessorSide, JoinSide, JoinStreamRuntime,
};
pub use self::processor::{CommonProcessorMeta, FilterProcessor, ProcessingMode, Processor};
pub use self::query_runtime::QueryRuntime; // Added
pub use self::selector::{OutputAttributeProcessor, SelectProcessor}; // Kept one
