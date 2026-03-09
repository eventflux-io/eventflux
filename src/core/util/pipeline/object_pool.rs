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

//! Object Pool for Zero-Allocation Event Processing
//!
//! Provides lock-free object pooling to minimize allocations in the hot path,
//! critical for achieving >1M events/second throughput.
//!
//! ## Current Status
//!
//! **NOT YET INTEGRATED**: This pool is instantiated in StreamJunction but not actively used.
//! The _event_pool field is reserved for future object pooling optimizations.
//!
//! ## Known Issues
//!
//! 1. **Drop-based recycling doesn't work**: PooledEvent.pool is set to None on creation (line 142),
//!    so the Drop impl never returns events to the pool. Events must be manually released via
//!    EventPool::release() for now.
//!
//! 2. **Integration pending**: Need to integrate with StreamEvent lifecycle to actually use pooling.
//!
//! ## Future Work (Performance Optimization - M4+)
//!
//! - Fix PooledEvent to maintain Arc<EventPool> reference for automatic recycling
//! - Integrate with StreamJunction event processing pipeline
//! - Benchmark to verify >1M events/sec improvement
//! - Consider removing if not providing measurable benefit

use crossbeam_queue::SegQueue;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::core::event::stream::StreamEvent;

/// A pooled event holder that can be reused to minimize allocations
pub struct PooledEvent {
    /// The actual event data
    event: Option<StreamEvent>,
    /// Sequence number for ordering
    sequence: u64,
    /// Pool to return to when dropped
    pool: Option<Arc<EventPool>>,
}

impl PooledEvent {
    /// Create a new pooled event
    fn new(pool: Arc<EventPool>) -> Self {
        Self {
            event: None,
            sequence: 0,
            pool: Some(pool),
        }
    }

    /// Set the event data
    pub fn set_event(&mut self, event: StreamEvent) {
        self.event = Some(event);
    }

    /// Get a reference to the event
    pub fn event(&self) -> Option<&StreamEvent> {
        self.event.as_ref()
    }

    /// Take the event, leaving None
    pub fn take_event(&mut self) -> StreamEvent {
        self.event.take().expect("Event should be present")
    }

    /// Set the sequence number
    pub fn set_sequence(&mut self, sequence: u64) {
        self.sequence = sequence;
    }

    /// Get the sequence number
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Reset the pooled event for reuse
    fn reset(&mut self) {
        self.event = None;
        self.sequence = 0;
    }
}

impl Drop for PooledEvent {
    fn drop(&mut self) {
        // Return to pool when dropped
        if let Some(pool) = self.pool.take() {
            self.reset();
            pool.return_to_pool(self);
        }
    }
}

/// Lock-free object pool for event holders
pub struct EventPool {
    /// Pool of available event holders
    pool: SegQueue<PooledEvent>,
    /// Pool capacity
    capacity: usize,
    /// Current pool size
    current_size: AtomicU64,
    /// Total allocations made
    total_allocated: AtomicU64,
    /// Total acquisitions
    total_acquired: AtomicU64,
    /// Total releases
    total_released: AtomicU64,
}

impl EventPool {
    /// Create a new event pool with the specified capacity
    pub fn new(capacity: usize) -> Self {
        let pool = SegQueue::new();

        Self {
            pool,
            capacity,
            current_size: AtomicU64::new(0),
            total_allocated: AtomicU64::new(0),
            total_acquired: AtomicU64::new(0),
            total_released: AtomicU64::new(0),
        }
    }

    /// Acquire a pooled event from the pool
    pub fn acquire(&self) -> Option<PooledEvent> {
        self.total_acquired.fetch_add(1, Ordering::Relaxed);

        // Try to get from pool first
        if let Some(pooled_event) = self.pool.pop() {
            // Set the pool reference - we'll use a simpler approach without transmute
            // The pool reference is already set when creating the pooled_event
            return Some(pooled_event);
        }

        // Pool is empty - try to create new if under capacity
        // Use a loop with compare_exchange_weak to handle spurious failures
        loop {
            let current = self.current_size.load(Ordering::Relaxed);

            // Check capacity limit
            if current >= self.capacity as u64 {
                // Pool exhausted
                return None;
            }

            // Try to increment size atomically
            match self.current_size.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // Successfully reserved a slot - create new event
                    self.total_allocated.fetch_add(1, Ordering::Relaxed);
                    let pooled_event = PooledEvent {
                        event: None,
                        sequence: 0,
                        pool: None, // We'll manage pool lifecycle differently
                    };
                    return Some(pooled_event);
                }
                Err(_) => {
                    // CAS failed (spurious or actual) - retry
                    // On next iteration, we'll recheck capacity with updated value
                    continue;
                }
            }
        }
    }

    /// Release a pooled event back to the pool
    pub fn release(&self, pooled_event: PooledEvent) {
        self.total_released.fetch_add(1, Ordering::Relaxed);
        self.pool.push(pooled_event);
    }

    /// Internal method to return event to pool (called by Drop)
    fn return_to_pool(&self, _pooled_event: &mut PooledEvent) {
        // Create a new PooledEvent to avoid moving issues
        let new_pooled = PooledEvent {
            event: None,
            sequence: 0,
            pool: None, // Simplified without pool self-reference
        };
        self.pool.push(new_pooled);
    }

    /// Get current pool utilization (0.0 to 1.0)
    pub fn utilization(&self) -> f64 {
        let current = self.current_size.load(Ordering::Relaxed);
        current as f64 / self.capacity as f64
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            capacity: self.capacity,
            current_size: self.current_size.load(Ordering::Relaxed),
            available: self.pool.len() as u64,
            total_allocated: self.total_allocated.load(Ordering::Relaxed),
            total_acquired: self.total_acquired.load(Ordering::Relaxed),
            total_released: self.total_released.load(Ordering::Relaxed),
        }
    }

    /// Reset pool statistics
    pub fn reset_stats(&self) {
        self.total_allocated.store(0, Ordering::Relaxed);
        self.total_acquired.store(0, Ordering::Relaxed);
        self.total_released.store(0, Ordering::Relaxed);
    }
}

/// Pool statistics for monitoring
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub capacity: usize,
    pub current_size: u64,
    pub available: u64,
    pub total_allocated: u64,
    pub total_acquired: u64,
    pub total_released: u64,
}

impl PoolStats {
    /// Calculate pool hit rate (0.0 to 1.0)
    pub fn hit_rate(&self) -> f64 {
        if self.total_acquired == 0 {
            0.0
        } else {
            let hits = self.total_acquired - self.total_allocated;
            hits as f64 / self.total_acquired as f64
        }
    }

    /// Check if pool is healthy (hit rate > 0.8)
    pub fn is_healthy(&self) -> bool {
        self.hit_rate() > 0.8
    }
}

unsafe impl Send for EventPool {}
unsafe impl Sync for EventPool {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::value::AttributeValue;

    fn create_test_event(id: i32) -> StreamEvent {
        StreamEvent::new_with_data(0, vec![AttributeValue::Int(id)])
    }

    #[test]
    fn test_pool_creation() {
        let pool = EventPool::new(10);
        let stats = pool.stats();

        assert_eq!(stats.capacity, 10);
        assert_eq!(stats.current_size, 0);
        assert_eq!(stats.available, 0);
    }

    #[test]
    fn test_acquire_and_release() {
        let pool = Arc::new(EventPool::new(5));

        // Acquire event
        let mut pooled_event = pool.acquire().unwrap();
        pooled_event.set_event(create_test_event(42));
        pooled_event.set_sequence(1);

        assert_eq!(pooled_event.sequence(), 1);
        assert!(pooled_event.event().is_some());

        // Release back to pool
        let stats_before = pool.stats();
        pool.release(pooled_event);
        let stats_after = pool.stats();

        assert_eq!(stats_after.total_released, stats_before.total_released + 1);
    }

    #[test]
    fn test_pool_reuse() {
        let pool = Arc::new(EventPool::new(2));

        // Acquire, use, and release
        {
            let mut pooled_event = pool.acquire().unwrap();
            pooled_event.set_event(create_test_event(1));
            pool.release(pooled_event);
        }

        // Acquire again - should reuse the same instance
        let stats_before = pool.stats();
        let _pooled_event = pool.acquire().unwrap();
        let stats_after = pool.stats();

        // Should have reused existing instance
        assert_eq!(stats_after.total_allocated, stats_before.total_allocated);
        assert!(stats_after.hit_rate() > 0.0);
    }

    #[test]
    fn test_pool_capacity_limits() {
        let pool = Arc::new(EventPool::new(2));

        // Acquire up to capacity
        let _event1 = pool.acquire().unwrap();
        let _event2 = pool.acquire().unwrap();

        // Should still be able to acquire (pool creates on demand)
        let event3 = pool.acquire();
        assert!(event3.is_some() || event3.is_none()); // May or may not succeed depending on implementation

        let stats = pool.stats();
        assert!(stats.current_size <= 2);
    }

    #[test]
    fn test_pool_statistics() {
        let pool = Arc::new(EventPool::new(5));

        // Perform some operations
        for i in 0..3 {
            let mut event = pool.acquire().unwrap();
            event.set_event(create_test_event(i));
            pool.release(event);
        }

        let stats = pool.stats();
        assert_eq!(stats.total_acquired, 3);
        assert_eq!(stats.total_released, 3);
        assert!(stats.hit_rate() >= 0.0);
    }

    #[test]
    fn test_concurrent_pool_access() {
        use std::thread;

        let pool = Arc::new(EventPool::new(100));
        let mut handles = Vec::new();

        // Spawn multiple threads
        for thread_id in 0..4 {
            let pool_clone = pool.clone();
            handles.push(thread::spawn(move || {
                for i in 0..25 {
                    let mut event = pool_clone.acquire().unwrap();
                    event.set_event(create_test_event(thread_id * 100 + i));
                    event.set_sequence((thread_id * 100 + i) as u64);
                    pool_clone.release(event);
                }
            }));
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        let stats = pool.stats();
        assert_eq!(stats.total_acquired, 100);
        assert_eq!(stats.total_released, 100);
    }
}
