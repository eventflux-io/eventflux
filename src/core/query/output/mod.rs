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

// src/core/query/output/mod.rs

// This module is for components related to query output processing in the core engine,
// like output callbacks, specific output processors (e.g., for INSERT INTO, DELETE, UPDATE),
// and rate limiters that operate on core event chunks.

pub mod delete_table_processor;
pub mod insert_into_aggregation_processor;
pub mod insert_into_stream_processor;
pub mod insert_into_table_processor;
pub mod update_table_processor;
pub mod upsert_table_processor;
// pub mod output_rate_limiter; // Core engine's rate limiter
pub mod callback_processor; // Added

// Note: core::stream::output::StreamCallback is for external callbacks on streams.
// Query-specific output callbacks (QueryCallback in Java) might also go here or a sub-module.

pub use self::callback_processor::CallbackProcessor;
pub use self::delete_table_processor::DeleteTableProcessor;
pub use self::insert_into_aggregation_processor::InsertIntoAggregationProcessor;
pub use self::insert_into_stream_processor::InsertIntoStreamProcessor;
pub use self::insert_into_table_processor::InsertIntoTableProcessor;
pub use self::update_table_processor::UpdateTableProcessor;
pub use self::upsert_table_processor::UpsertTableProcessor;
