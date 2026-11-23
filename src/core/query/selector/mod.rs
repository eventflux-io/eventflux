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
pub mod attribute; // OutputAttributeProcessor and the aggregators
pub mod group_by_key_generator; // Port of GroupByKeyGenerator.java
pub mod order_by_event_comparator; // Port of OrderByEventComparator.java
pub mod select_processor; // Port of QuerySelector.java

pub use self::attribute::OutputAttributeProcessor;
pub use self::group_by_key_generator::GroupByKeyGenerator;
pub use self::order_by_event_comparator::OrderByEventComparator;
pub use self::select_processor::SelectProcessor;
