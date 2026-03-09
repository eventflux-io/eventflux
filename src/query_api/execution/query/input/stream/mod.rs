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

// Corresponds to package io.eventflux.query.api.execution.query.input.stream

// Individual stream types
// pub mod anonymous_input_stream; // Functionality merged into single_input_stream.rs
// pub mod basic_single_input_stream; // Functionality merged into single_input_stream.rs
pub mod input_stream; // Defines the main InputStream enum and its factory methods
pub mod join_input_stream;
pub mod single_input_stream;
pub mod state_input_stream;

// Re-export key types for easier access from parent modules (e.g., query_api::execution::query::input)
pub use self::input_stream::{InputStream, InputStreamTrait}; // This is the main enum & trait
pub use self::single_input_stream::{SingleInputStream, SingleInputStreamKind};
// BasicSingleInputStream and AnonymousInputStream are not top-level structs anymore.
// Their logic is within SingleInputStream or specific constructors.
// pub use self::basic_single_input_stream::BasicSingleInputStream; // Removed
// pub use self::anonymous_input_stream::AnonymousInputStream; // Removed
pub use self::join_input_stream::{
    EventTrigger as JoinEventTrigger, JoinInputStream, Type as JoinType,
}; // Removed Within from here
pub use self::state_input_stream::{StateInputStream, Type as StateInputStreamType};
pub use crate::query_api::aggregation::Within as JoinWithin; // Import Within directly and alias

// Comments updated:
// - `input_stream.rs` defines `pub enum InputStream { Single(SingleInputStream), Join(JoinInputStream), State(StateInputStream) }` and `InputStreamTrait`.
// - `single_input_stream.rs` defines `pub struct SingleInputStream { kind: SingleInputStreamKind }` where kind can be Basic or Anonymous.
// - `join_input_stream.rs` defines `pub struct JoinInputStream { ... }` and its enums `Type` and `EventTrigger`.
// - `state_input_stream.rs` defines `pub struct StateInputStream { ... }` and its enum `Type`.
// All structs that are variants or composed within these (e.g. SingleInputStream, JoinInputStream, StateInputStream)
// must compose `eventflux_element: EventFluxElement`.
// The `InputStream` enum implements `EventFluxElement` and `InputStreamTrait` by dispatching to its variants.
