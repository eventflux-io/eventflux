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

//! Shared state between Pre and Post state processors
//!
//! This module provides lock-free shared state that allows PreStateProcessor and
//! PostStateProcessor to communicate without requiring locks on each other.
//!
//! ## Design Rationale
//!
//! PostStateProcessor needs to notify PreStateProcessor of state changes without
//! causing deadlock. Direct method calls would require:
//! 1. PreStateProcessor is already locked during event processing
//! 2. PostStateProcessor needs to lock it to call methods
//! 3. Rust Mutex is not reentrant → deadlock
//!
//! Solution: Share atomic state that both can access without locking each other.
//!
//! ## Benefits:
//! - Lock-free: No contention, faster
//! - No deadlock possibility
//! - Cache-friendly atomic operations
//! - Better for multi-threaded scenarios

use std::sync::atomic::{AtomicBool, Ordering};

/// Shared state between PreStateProcessor and PostStateProcessor
///
/// This allows both processors to coordinate without locking each other.
/// All fields use atomic operations for thread-safe, lock-free access.
#[derive(Debug)]
pub struct ProcessorSharedState {
    /// Flag indicating if processor state has changed
    ///
    /// Set to true when:
    /// - New state added to pending list
    /// - State matched and progressed
    /// - State expired or removed
    ///
    /// Used for optimization: skip processing if nothing changed
    state_changed: AtomicBool,
}

impl ProcessorSharedState {
    /// Create new shared state
    pub fn new() -> Self {
        Self {
            state_changed: AtomicBool::new(false),
        }
    }

    /// Mark that state has changed
    ///
    /// Called by both PreStateProcessor and PostStateProcessor when they
    /// modify pending states.
    ///
    /// Uses `Ordering::Release` to ensure all prior writes are visible
    /// to threads that subsequently read the flag.
    #[inline]
    pub fn mark_state_changed(&self) {
        self.state_changed.store(true, Ordering::Release);
    }

    /// Check if state has changed
    ///
    /// Uses `Ordering::Acquire` to ensure we see all writes that happened
    /// before the flag was set.
    ///
    /// Returns true if state changed since last check
    #[inline]
    pub fn is_state_changed(&self) -> bool {
        self.state_changed.load(Ordering::Acquire)
    }

    /// Reset the state changed flag
    ///
    /// Called after processing state changes to reset for next check.
    ///
    /// Uses `Ordering::Release` for consistency with mark_state_changed.
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
