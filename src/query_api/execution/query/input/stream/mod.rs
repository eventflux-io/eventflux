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

// Input stream types for the FROM part of a query.
//
// - `input_stream.rs` has the `InputStream` enum (Single, Join, State) and `InputStreamTrait`.
// - `single_input_stream.rs` has `SingleInputStream`, whose kind is Basic or Anonymous.
// - `join_input_stream.rs` has `JoinInputStream` and its `Type` and `EventTrigger` enums.
// - `state_input_stream.rs` has `StateInputStream` for patterns and sequences, and its `Type`.
//
// Each stream struct holds an `eventflux_element: EventFluxElement` field. `InputStream`
// implements `InputStreamTrait` by passing calls on to the wrapped stream.

// pub mod anonymous_input_stream; // merged into single_input_stream.rs
// pub mod basic_single_input_stream; // merged into single_input_stream.rs
pub mod input_stream;
pub mod join_input_stream;
pub mod single_input_stream;
pub mod state_input_stream;

pub use self::input_stream::{InputStream, InputStreamTrait};
pub use self::single_input_stream::{SingleInputStream, SingleInputStreamKind};
// These types no longer exist. Use SingleInputStream with the matching kind.
// pub use self::basic_single_input_stream::BasicSingleInputStream; // Removed
// pub use self::anonymous_input_stream::AnonymousInputStream; // Removed
pub use self::join_input_stream::{
    EventTrigger as JoinEventTrigger, JoinInputStream, Type as JoinType,
};
pub use self::state_input_stream::{StateInputStream, Type as StateInputStreamType};
pub use crate::query_api::aggregation::Within as JoinWithin;
