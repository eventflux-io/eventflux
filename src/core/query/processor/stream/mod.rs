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

// src/core/query/processor/stream/mod.rs
pub mod filter;
pub mod join;
pub mod window; // Window processors such as length and time
                // pub mod single; // If SingleStreamProcessor becomes a struct/module later
                // pub mod function; // For stream function processors

pub use self::filter::FilterProcessor;
pub use self::join::{JoinProcessor, JoinProcessorSide, JoinSide};
pub use self::window::{
    LengthBatchWindowProcessor, LengthWindowProcessor, TimeBatchWindowProcessor,
    TimeWindowProcessor,
};
// Re-export further stream processor types here as they are added.

// Java's AbstractStreamProcessor and StreamProcessor map to the Processor trait in
// core/query/processor/mod.rs, with shared fields in CommonProcessorMeta. Stream
// processors such as FilterProcessor and the window processors implement Processor.
