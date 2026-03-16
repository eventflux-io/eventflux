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

// Corresponds to package io.eventflux.query.api.execution.query.input.state

pub mod absent_stream_state_element;
pub mod count_state_element;
pub mod every_state_element;
pub mod logical_state_element;
pub mod next_state_element;
#[allow(clippy::module_inception)]
pub mod state; // This is the factory class with static methods
pub mod state_element; // Interface, will be an enum or trait
pub mod stream_state_element;

// Re-export key types
pub use self::absent_stream_state_element::AbsentStreamStateElement;
pub use self::count_state_element::{CountStateElement, ANY_COUNT}; // ANY_COUNT instead of ANY to be more specific
pub use self::every_state_element::EveryStateElement;
pub use self::logical_state_element::{LogicalStateElement, Type as LogicalStateElementType};
pub use self::next_state_element::NextStateElement;
pub use self::state::State; // Factory methods
pub use self::state_element::StateElement; // Main enum for different state types
pub use self::stream_state_element::StreamStateElement;

// `StateElement` will be an enum:
// pub enum StateElement {
//     Stream(StreamStateElement),
//     AbsentStream(AbsentStreamStateElement), // Not directly in Java's StateElement interface, but logical
//     Logical(LogicalStateElement),
//     Next(Box<NextStateElement>), // Boxed due to recursion
//     Count(CountStateElement),
//     Every(Box<EveryStateElement>), // Boxed due to recursion
// }
// Each struct will implement EventFluxElement.
// The State.java class contains static factory methods to construct these StateElements.
// These will be translated to associated functions on the `State` struct or directly on the `StateElement` enum.
