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

// The FROM side of a query. Input streams, their handlers, pattern states and stores.

pub mod handler;
pub mod state;
pub mod store;
pub mod stream;

pub use self::stream::{
    InputStream,
    JoinEventTrigger,
    JoinInputStream,
    // BasicSingleInputStream, // merged into SingleInputStream
    // AnonymousInputStream, // removed
    JoinType,
    SingleInputStream,
    StateInputStream,
    StateInputStreamType,
};

pub use self::handler::{
    Filter,
    StreamFunction,
    StreamHandler, // enum
    WindowHandler, // handler::Window, renamed in handler/mod.rs
};

pub use self::state::{
    AbsentStreamStateElement,
    CountStateElement,
    EveryStateElement,
    LogicalStateElement,
    LogicalStateElementType, // logical_state_element::Type
    NextStateElement,
    State,        // holds factory functions only
    StateElement, // enum
    StreamStateElement,
};

pub use self::store::{
    AggregationInputStore,
    ConditionInputStore,
    InputStore, // enum
    Store,
};
