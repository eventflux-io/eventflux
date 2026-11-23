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

use std::cell::RefCell;
use std::collections::HashMap;

// Per thread flag that asks for a full snapshot instead of an incremental one
thread_local! {
    static REQUEST_FULL: RefCell<bool> = const { RefCell::new(false) };
}

/// Turns the full snapshot request on or off for the current thread only.
pub fn request_for_full_snapshot(enable: bool) {
    REQUEST_FULL.with(|f| *f.borrow_mut() = enable);
}

/// Whether the current thread asked for a full snapshot. False by default.
pub fn is_request_for_full_snapshot() -> bool {
    REQUEST_FULL.with(|f| *f.borrow())
}

/// Placeholder for serialized incremental snapshot data.
#[derive(Debug, Default, Clone)]
pub struct IncrementalSnapshot {
    pub incremental_state: HashMap<String, HashMap<String, Vec<u8>>>,
    pub incremental_state_base: HashMap<String, HashMap<String, Vec<u8>>>,
    pub periodic_state: HashMap<String, HashMap<String, Vec<u8>>>,
}

/// Returned when a snapshot is persisted. Holds the revision it was saved under.
#[derive(Debug, Clone)]
pub struct PersistenceReference {
    pub revision: String,
}

impl PersistenceReference {
    pub fn new(revision: String) -> Self {
        Self { revision }
    }
}

/// Snapshot and restore trait for pattern processing components, used for
/// checkpointing and recovery
pub mod state {
    use serde_json::Value;
    use std::collections::HashMap;

    /// State of a component that can be saved and restored, used to persist and
    /// recover pattern processing state
    pub trait State {
        /// Returns the current state as a map of state keys to JSON values
        fn snapshot(&self) -> HashMap<String, Value>;

        /// Replaces the internal state with one taken by `snapshot`
        fn restore(&mut self, state: HashMap<String, Value>);

        /// True when the state is empty or idle and can be cleaned up safely
        fn can_destroy(&self) -> bool;
    }
}
