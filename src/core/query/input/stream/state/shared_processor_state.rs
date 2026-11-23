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

//! State shared by a PreStateProcessor and its PostStateProcessor.
//!
//! Lets the two processors signal each other through an atomic flag, without either
//! one locking the other.
//!
//! ## Why
//!
//! PostStateProcessor has to tell PreStateProcessor that the state changed. A direct
//! method call would deadlock:
//! 1. PreStateProcessor is already locked while it processes an event
//! 2. PostStateProcessor would have to lock it again to call the method
//! 3. Rust's Mutex cannot be locked twice by the same caller, so it deadlocks
//!
//! Instead both hold an atomic flag they can read and write without locking.
//!
//! ## Benefits
//! - No lock, so no waiting and no deadlock
//! - Atomic reads and writes are cheap
//! - Safe to use from several threads

use std::sync::atomic::{AtomicBool, Ordering};

/// State shared by a PreStateProcessor and its PostStateProcessor.
///
/// Every field is atomic, so either side can use it from any thread without a lock.
#[derive(Debug)]
pub struct ProcessorSharedState {
    /// True once the processor state has changed, for example when a state is added,
    /// a match moves forward, or a state is removed. Stays true until
    /// `reset_state_changed` is called.
    state_changed: AtomicBool,
}

impl ProcessorSharedState {
    /// Create shared state with the changed flag cleared
    pub fn new() -> Self {
        Self {
            state_changed: AtomicBool::new(false),
        }
    }

    /// Set the changed flag.
    ///
    /// Called by either processor when it changes the pending states. Uses
    /// `Ordering::Release` so writes made before it are visible to a thread that later
    /// reads the flag.
    #[inline]
    pub fn mark_state_changed(&self) {
        self.state_changed.store(true, Ordering::Release);
    }

    /// True when the flag is set. Reading does not clear it.
    ///
    /// Uses `Ordering::Acquire` so the reader sees every write made before the flag
    /// was set.
    #[inline]
    pub fn is_state_changed(&self) -> bool {
        self.state_changed.load(Ordering::Acquire)
    }

    /// Clear the changed flag. Uses `Ordering::Release` to match mark_state_changed.
    #[inline]
    pub fn reset_state_changed(&self) {
        self.state_changed.store(false, Ordering::Release);
    }
}

impl Default for ProcessorSharedState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_basic_state_change() {
        let state = ProcessorSharedState::new();

        assert!(!state.is_state_changed());

        state.mark_state_changed();
        assert!(state.is_state_changed());

        state.reset_state_changed();
        assert!(!state.is_state_changed());
    }

    #[test]
    fn test_concurrent_access() {
        use std::time::Duration;

        let state = Arc::new(ProcessorSharedState::new());
        let state1 = state.clone();
        let state2 = state.clone();

        // Thread 1: Mark state changed
        let t1 = thread::spawn(move || {
            for _ in 0..1000 {
                state1.mark_state_changed();
                thread::sleep(Duration::from_micros(10)); // Give thread 2 time to observe
            }
        });

        // Thread 2: Check state changed
        let t2 = thread::spawn(move || {
            let mut saw_changed = false;
            for _ in 0..1000 {
                if state2.is_state_changed() {
                    saw_changed = true;
                    state2.reset_state_changed();
                }
                thread::sleep(Duration::from_micros(10)); // Give thread 1 time to mark
            }
            saw_changed
        });

        t1.join().unwrap();
        let saw_changed = t2.join().unwrap();

        // Should have seen at least one state change
        assert!(saw_changed);
    }

    #[test]
    fn test_multiple_readers() {
        let state = Arc::new(ProcessorSharedState::new());
        state.mark_state_changed();

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let state_clone = state.clone();
                thread::spawn(move || state_clone.is_state_changed())
            })
            .collect();

        // All readers should see the change
        for handle in handles {
            assert!(handle.join().unwrap());
        }
    }
}
