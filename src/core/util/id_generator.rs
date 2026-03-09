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

// src/core/util/id_generator.rs
// Lightweight equivalent of io.eventflux.core.util.IdGenerator.
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct IdGenerator {
    counter: AtomicU64,
}

impl Clone for IdGenerator {
    fn clone(&self) -> Self {
        Self {
            counter: AtomicU64::new(self.counter.load(Ordering::SeqCst)),
        }
    }
}

impl Default for IdGenerator {
    fn default() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }
}

impl IdGenerator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a new monotonically increasing id as a String.
    pub fn create_new_id(&self) -> String {
        let next = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        next.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_incrementing_ids() {
        let gen = IdGenerator::new();
        assert_eq!(gen.create_new_id(), "1");
        assert_eq!(gen.create_new_id(), "2");
    }
}
