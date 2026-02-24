// SPDX-License-Identifier: MIT OR Apache-2.0

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
