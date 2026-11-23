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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};

/// Gate that can stop threads from entering, like the Java ThreadBarrier.
///
/// Threads call `enter` and `exit` around their work. While locked, `enter` blocks.
/// Locking does not wait for threads already inside, so callers check
/// `get_active_threads` for that.
#[derive(Debug)]
pub struct ThreadBarrier {
    locked: Mutex<bool>,
    cvar: Condvar,
    counter: AtomicUsize,
}

impl ThreadBarrier {
    pub fn new() -> Self {
        Self {
            locked: Mutex::new(false),
            cvar: Condvar::new(),
            counter: AtomicUsize::new(0),
        }
    }

    /// Blocks while the barrier is locked, then counts this thread as active.
    pub fn enter(&self) {
        let mut locked = self.locked.lock().unwrap();
        while *locked {
            locked = self.cvar.wait(locked).unwrap();
        }
        self.counter.fetch_add(1, Ordering::SeqCst);
    }

    /// Counts this thread as no longer active. Call once per `enter`.
    pub fn exit(&self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }

    /// Number of active threads inside the barrier.
    pub fn get_active_threads(&self) -> usize {
        self.counter.load(Ordering::SeqCst)
    }

    /// Locks the barrier so new threads block in `enter`.
    pub fn lock(&self) {
        let mut locked = self.locked.lock().unwrap();
        *locked = true;
    }

    /// Unlocks the barrier and wakes all waiting threads.
    pub fn unlock(&self) {
        let mut locked = self.locked.lock().unwrap();
        *locked = false;
        self.cvar.notify_all();
    }
}

impl Default for ThreadBarrier {
    fn default() -> Self {
        Self::new()
    }
}
