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

// Thread local flag used to request a full snapshot.
thread_local! {
    static REQUEST_FULL: RefCell<bool> = const { RefCell::new(false) };
}

/// Enable or disable full snapshot for the current thread.
pub fn request_for_full_snapshot(enable: bool) {
    REQUEST_FULL.with(|f| *f.borrow_mut() = enable);
}

/// Whether the current thread requested a full snapshot.
pub fn is_request_for_full_snapshot() -> bool {
    REQUEST_FULL.with(|f| *f.borrow())
}

/// Serialized incremental snapshot information placeholder.
#[derive(Debug, Default, Clone)]
pub struct IncrementalSnapshot {
    pub incremental_state: HashMap<String, HashMap<String, Vec<u8>>>,
    pub incremental_state_base: HashMap<String, HashMap<String, Vec<u8>>>,
    pub periodic_state: HashMap<String, HashMap<String, Vec<u8>>>,
}

/// Reference to persistence futures returned when persisting snapshots.
#[derive(Debug, Clone)]
pub struct PersistenceReference {
    pub revision: String,
}

impl PersistenceReference {
    pub fn new(revision: String) -> Self {
        Self { revision }
    }
}

/// State management trait for pattern processing components
/// Provides snapshot/restore capabilities for checkpointing and recovery
pub mod state {
    use serde_json::Value;
    use std::collections::HashMap;

    /// State trait for components that need snapshot/restore capabilities
    /// Used for pattern processing state persistence and recovery
    pub trait State {
        /// Take a snapshot of the current state
        /// Returns a map of state keys to JSON values for persistence
        fn snapshot(&self) -> HashMap<String, Value>;

        /// Restore state from a snapshot
        /// Takes a map of state keys to JSON values and restores internal state
        fn restore(&mut self, state: HashMap<String, Value>);

        /// Check if this state can be safely destroyed
        /// Returns true if the state is empty/idle and can be cleaned up
        fn can_destroy(&self) -> bool;
    }
}
