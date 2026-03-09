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

// Corresponds to package io.eventflux.query.api.execution.query.input

pub mod handler;
pub mod state;
pub mod store;
pub mod stream;

// Re-export key elements from the stream module as it defines the main InputStream enum/struct
pub use self::stream::{
    InputStream,
    JoinEventTrigger, // Enum from JoinInputStream
    JoinInputStream,
    // BasicSingleInputStream, // Removed
    // AnonymousInputStream, // Removed
    JoinType, // Enum from JoinInputStream
    SingleInputStream,
    StateInputStream,
    StateInputStreamType, // Enum from StateInputStream
};

// Re-export key elements from handler module
pub use self::handler::{
    Filter,
    StreamFunction,
    StreamHandler, // Enum or Trait
    WindowHandler, // Use the already aliased WindowHandler from handler/mod.rs
};

// Re-export key elements from state module
pub use self::state::{
    AbsentStreamStateElement,
    CountStateElement,
    EveryStateElement,
    LogicalStateElement,
    LogicalStateElementType, // Enum from LogicalStateElement
    NextStateElement,
    State,        // Utility struct with static methods
    StateElement, // Enum or Trait
    StreamStateElement,
};

// Re-export key elements from store module
pub use self::store::{
    AggregationInputStore,
    ConditionInputStore,
    InputStore, // Trait
    Store,
};
