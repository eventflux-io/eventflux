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

// Building blocks of pattern and sequence queries.

pub mod absent_stream_state_element;
pub mod count_state_element;
pub mod every_state_element;
pub mod logical_state_element;
pub mod next_state_element;
#[allow(clippy::module_inception)]
pub mod state; // factory functions for StateElement values
pub mod state_element; // the StateElement enum
pub mod stream_state_element;

pub use self::absent_stream_state_element::AbsentStreamStateElement;
pub use self::count_state_element::{CountStateElement, ANY_COUNT};
pub use self::every_state_element::EveryStateElement;
pub use self::logical_state_element::{LogicalStateElement, Type as LogicalStateElementType};
pub use self::next_state_element::NextStateElement;
pub use self::state::State;
pub use self::state_element::StateElement;
pub use self::stream_state_element::StreamStateElement;

// Sketch of the StateElement enum. The real one is in state_element.rs.
// pub enum StateElement {
//     Stream(StreamStateElement),
//     AbsentStream(AbsentStreamStateElement), // Not directly in Java's StateElement interface, but logical
//     Logical(LogicalStateElement),
//     Next(Box<NextStateElement>), // Boxed due to recursion
//     Count(CountStateElement),
//     Every(Box<EveryStateElement>), // Boxed due to recursion
// }
// Each element struct holds its query positions in an EventFluxElement field.
// The `State` struct has the factory functions that build these elements.
