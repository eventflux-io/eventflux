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

// Store inputs for on demand queries. Reads from tables, windows and aggregations.

pub mod aggregation_input_store;
pub mod condition_input_store;
pub mod input_store; // the InputStore enum and InputStoreTrait
#[allow(clippy::module_inception)]
pub mod store; // Store, a SingleInputStream that names the store

pub use self::aggregation_input_store::AggregationInputStore;
pub use self::condition_input_store::ConditionInputStore;
pub use self::input_store::InputStore;
pub use self::store::Store;

// An early sketch of the enum, kept for reference. The real InputStore enum in
// input_store.rs also has a Store variant.
// pub enum InputStoreType { // Renaming from InputStore to avoid conflict if InputStore is a trait
//     Condition(ConditionInputStore),
//     Aggregation(AggregationInputStore),
//     // Store itself is a kind of BasicSingleInputStream, so it might not be a variant here
//     // if ConditionInputStore and AggregationInputStore directly embed Store.
// }
// Store is the starting point. Its on_condition() returns a ConditionInputStore and its
// on_aggregation_* methods return an AggregationInputStore. Store, ConditionInputStore and
// AggregationInputStore all implement InputStoreTrait, which gives the store id and alias.
