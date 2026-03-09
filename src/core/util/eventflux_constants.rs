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

// src/core/util/eventflux_constants.rs

//! Constants used inside the EventFlux core implementation.  These mirror the
//! values found in the Java `io.eventflux.core.util.EventFluxConstants` class that are
//! required by the currently ported modules.  Only the subset needed by the Rust
//! code base is included here.

// Position indexes for attribute arrays used by `StreamEvent`/`StateEvent`.
pub const BEFORE_WINDOW_DATA_INDEX: usize = 0;
pub const ON_AFTER_WINDOW_DATA_INDEX: usize = 1;
pub const OUTPUT_DATA_INDEX: usize = 2;
pub const STATE_OUTPUT_DATA_INDEX: usize = 3;

pub const STREAM_EVENT_CHAIN_INDEX: usize = 0;
pub const STREAM_EVENT_INDEX_IN_CHAIN: usize = 1;
pub const STREAM_ATTRIBUTE_TYPE_INDEX: usize = 2;
pub const STREAM_ATTRIBUTE_INDEX_IN_TYPE: usize = 3;

// Misc index values used by `StateEvent` when navigating chains
pub const CURRENT: i32 = -1;
pub const LAST: i32 = -2;
pub const ANY: i32 = -1;
pub const UNKNOWN_STATE: i32 = -1;

/// Delimiter used when constructing compound keys (e.g., for group-by).
pub const KEY_DELIMITER: &str = ":-:";

// When additional constants become necessary they should be added here to keep
// the mapping with the Java implementation explicit.

// Re‑export the query API constants so users of `core` only need one import.
pub use crate::query_api::constants::*;

/// Empty struct kept for backwards compatibility with earlier code that
/// expected a type named `EventFluxConstants` in this module.  New code should
/// directly use the constants above.
pub struct EventFluxConstants;
