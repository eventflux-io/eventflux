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

//! Constants used inside the EventFlux core. They copy the values from the Java
//! `io.eventflux.core.util.EventFluxConstants` class, but only the ones the Rust code
//! needs so far.

// Which data array of a `StreamEvent` or `StateEvent` an attribute lives in

pub const BEFORE_WINDOW_DATA_INDEX: usize = 0;
pub const ON_AFTER_WINDOW_DATA_INDEX: usize = 1;
pub const OUTPUT_DATA_INDEX: usize = 2;
pub const STATE_OUTPUT_DATA_INDEX: usize = 3;

// Slots of the four element position array (see VariableExpressionExecutor)
pub const STREAM_EVENT_CHAIN_INDEX: usize = 0;
pub const STREAM_EVENT_INDEX_IN_CHAIN: usize = 1;
pub const STREAM_ATTRIBUTE_TYPE_INDEX: usize = 2;
pub const STREAM_ATTRIBUTE_INDEX_IN_TYPE: usize = 3;

// Special index values `StateEvent` uses when walking event chains
pub const CURRENT: i32 = -1;
pub const LAST: i32 = -2;
pub const ANY: i32 = -1;
pub const UNKNOWN_STATE: i32 = -1;

/// Delimiter used to join parts of compound keys, for example group by keys.
pub const KEY_DELIMITER: &str = ":-:";

// Add new constants here so the mapping to the Java class stays clear.

// Re-export the query API constants so users of `core` only need one import.
pub use crate::query_api::constants::*;

/// Empty struct kept for older code that expects a type named `EventFluxConstants` in
/// this module. New code should use the constants above directly.
pub struct EventFluxConstants;
