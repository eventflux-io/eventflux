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

// src/core/query/selector/mod.rs
pub mod attribute; // For OutputAttributeProcessor and future aggregators/processors
pub mod group_by_key_generator;
pub mod order_by_event_comparator; // For OrderByEventComparator.java
pub mod select_processor; // Corresponds to QuerySelector.java // For GroupByKeyGenerator.java

pub use self::attribute::OutputAttributeProcessor; // Re-export for convenience
pub use self::group_by_key_generator::GroupByKeyGenerator;
pub use self::order_by_event_comparator::OrderByEventComparator;
pub use self::select_processor::SelectProcessor;

// Other components like OrderByEventComparator, GroupByKeyGenerator would be exported here.
