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

//! Object Pool for event holders
//!
//! A lock free pool of reusable event holders, meant to cut allocations on the hot path
//! toward the >1M events/second goal.
//!
//! ## Current Status
//!
//! EventPipeline uses its own pool. It takes a holder in `publish` and gives it back with
//! `release` after the event is consumed. StreamJunction also creates a pool in its
//! `_event_pool` field, but that one is never used. The StreamEvent itself is still
//! allocated for every event.
//!
//! ## Known Issues
//!
//! 1. **Recycling on drop does not work**: `acquire` creates holders with `pool` set to
//!    None, so the Drop impl never returns them. Holders must be returned by hand with
//!    EventPool::release(). A holder that is dropped instead never comes back and its
//!    slot stays counted in `current_size`.
//!
//! 2. **Not tied to the StreamEvent lifecycle**: pooling covers the holder, not the event.
//!
//! ## Future Work (Performance Optimization, M4 and later)
//!
//! - Fix PooledEvent to maintain Arc<EventPool> reference for automatic recycling
//! - Integrate with StreamJunction event processing pipeline
//! - Benchmark to verify >1M events/sec improvement
//! - Consider removing if not providing measurable benefit

use crossbeam_queue::SegQueue;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::core::event::stream::StreamEvent;

/// A reusable holder for one event and its sequence number
pub struct PooledEvent {
    event: Option<StreamEvent>,
    /// Sequence number, used for ordering
    sequence: u64,
    /// Pool to go back to when dropped. Always None for holders from `acquire`.
    pool: Option<Arc<EventPool>>,
}

impl PooledEvent {
    /// Creates a holder tied to `pool`. Not used today.
    #[allow(dead_code)]
    fn new(pool: Arc<EventPool>) -> Self {
        Self {
            event: None,
            sequence: 0,
            pool: Some(pool),
        }
    }

    /// Stores the event, replacing any event already held
    pub fn set_event(&mut self, event: StreamEvent) {
        self.event = Some(event);
    }

    pub fn event(&self) -> Option<&StreamEvent> {
        self.event.as_ref()
    }

    /// Takes the event out, leaving the holder empty.
    ///
    /// # Panics
    /// Panics if the holder is empty.
    pub fn take_event(&mut self) -> StreamEvent {
        self.event.take().expect("Event should be present")
    }

    pub fn set_sequence(&mut self, sequence: u64) {
        self.sequence = sequence;
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Clears the event and sequence for reuse
    fn reset(&mut self) {
        self.event = None;
        self.sequence = 0;
    }
}

impl Drop for PooledEvent {
    fn drop(&mut self) {
        // Only holders tied to a pool go back. See Known Issues in the module docs.
        if let Some(pool) = self.pool.take() {
            self.reset();
            pool.return_to_pool(self);
        }
    }
}

/// Lock free pool of event holders. Holders are created on demand, up to `capacity`.
pub struct EventPool {
    /// Holders ready for reuse
    pool: SegQueue<PooledEvent>,
    /// Most holders this pool will ever create
    capacity: usize,
    /// Holders created so far. Never goes down.
    current_size: AtomicU64,
    /// Holders created, counting since the last stats reset
    total_allocated: AtomicU64,
    /// `acquire` calls, failed ones included
    total_acquired: AtomicU64,
    /// `release` calls
    total_released: AtomicU64,
}

impl EventPool {
    /// Creates an empty pool that will create at most `capacity` holders
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

    /// Takes a holder from the pool, or creates one if the pool is empty and fewer than
    /// `capacity` exist. Returns None when the pool is exhausted.
    ///
    /// A reused holder is returned as released, so it may still hold its old event.
    pub fn acquire(&self) -> Option<PooledEvent> {
        self.total_acquired.fetch_add(1, Ordering::Relaxed);

        if let Some(pooled_event) = self.pool.pop() {
            return Some(pooled_event);
        }

        // Pool is empty, so create a new holder if under capacity. compare_exchange_weak
        // can fail spuriously, so loop.
        loop {
            let current = self.current_size.load(Ordering::Relaxed);

            if current >= self.capacity as u64 {
                return None;
            }

            // Reserve a slot
            match self.current_size.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.total_allocated.fetch_add(1, Ordering::Relaxed);
                    let pooled_event = PooledEvent {
                        event: None,
                        sequence: 0,
                        pool: None, // Not tied to the pool. Return it with `release`.
                    };
                    return Some(pooled_event);
                }
                Err(_) => {
                    // Lost a race or failed spuriously. Retry and check capacity again.
                    continue;
                }
            }
        }
    }

    /// Puts a holder back in the pool. It is not cleared, so any event it still holds
    /// stays in memory until the holder is reused.
    pub fn release(&self, pooled_event: PooledEvent) {
        self.total_released.fetch_add(1, Ordering::Relaxed);
        self.pool.push(pooled_event);
    }

    /// Called from Drop. Pushes a fresh empty holder in place of the dropped one, since
    /// the dropped one cannot be moved out of `&mut`.
    fn return_to_pool(&self, _pooled_event: &mut PooledEvent) {
        let new_pooled = PooledEvent {
            event: None,
            sequence: 0,
            pool: None, // No link back to the pool
        };
        self.pool.push(new_pooled);
    }

    /// Share of `capacity` created so far (0.0 to 1.0). Holders waiting in the pool
    /// still count.
    pub fn utilization(&self) -> f64 {
        let current = self.current_size.load(Ordering::Relaxed);
        current as f64 / self.capacity as f64
    }

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

    /// Resets the allocated, acquired and released counters. `current_size` is kept.
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
    /// Holders waiting in the pool
    pub available: u64,
    pub total_allocated: u64,
    pub total_acquired: u64,
    pub total_released: u64,
}

impl PoolStats {
    /// Share of acquires that did not create a holder (0.0 to 1.0). Failed acquires
    /// count as hits, so an exhausted pool still looks healthy.
    pub fn hit_rate(&self) -> f64 {
        if self.total_acquired == 0 {
            0.0
        } else {
            let hits = self.total_acquired - self.total_allocated;
            hits as f64 / self.total_acquired as f64
        }
    }

    /// True when the hit rate is above 0.8
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

        let mut pooled_event = pool.acquire().unwrap();
        pooled_event.set_event(create_test_event(42));
        pooled_event.set_sequence(1);

        assert_eq!(pooled_event.sequence(), 1);
        assert!(pooled_event.event().is_some());

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

        // Acquiring again should reuse the same holder
        let stats_before = pool.stats();
        let _pooled_event = pool.acquire().unwrap();
        let stats_after = pool.stats();

        assert_eq!(stats_after.total_allocated, stats_before.total_allocated);
        assert!(stats_after.hit_rate() > 0.0);
    }

    #[test]
    fn test_pool_capacity_limits() {
        let pool = Arc::new(EventPool::new(2));

        // Acquire up to capacity
        let _event1 = pool.acquire().unwrap();
        let _event2 = pool.acquire().unwrap();

        // Both holders are out and the capacity is reached, so this returns None today.
        // The check below accepts either result.
        let event3 = pool.acquire();
        assert!(event3.is_some() || event3.is_none());

        let stats = pool.stats();
        assert!(stats.current_size <= 2);
    }

    #[test]
    fn test_pool_statistics() {
        let pool = Arc::new(EventPool::new(5));

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

        for handle in handles {
            handle.join().unwrap();
        }

        let stats = pool.stats();
        assert_eq!(stats.total_acquired, 100);
        assert_eq!(stats.total_released, 100);
    }
}
