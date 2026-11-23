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

//! Event store that keeps each event once and hands out 8 byte ids to refer to it.
//!
//! Not used by the engine yet.
//!
//! **Memory**: states hold an 8 byte id instead of a copy of the event
//! - Copying 100 byte events for 1M states takes about 100 MB
//! - Holding 8 byte ids for 1M states takes about 8 MB
//! - About 12.5 times less memory
//!
//! **Concurrency**: backed by DashMap, a sharded concurrent map
//! - Reads do not block each other
//! - Insert and get take only a short per shard lock
//! - Scales with the number of cores
//!
//! **Memory limit**: an optional cap guards against running out of memory
//! - Default: no limit
//! - Set max_memory_bytes in production to stop runaway growth
//! - Inserts are rejected once the cap is reached, which tells the caller to slow down

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::core::event::stream::stream_event::StreamEvent;

/// Id for a stored event. 8 bytes, against 100 or more for a full event. 0 means no
/// event.
pub type EventId = u64;

/// Concurrent store of events, each kept once behind an Arc.
///
/// # Cost
/// - **Insert**: O(1)
/// - **Get**: O(1)
/// - **Remove**: O(1)
/// - **Memory**: one Arc<StreamEvent> per event, no copies
///
/// # Memory limit
/// - Default: no limit, so memory can grow without bound
/// - Use `with_memory_limit()` in production to cap growth
/// - At the cap, insert returns EventId 0 and the caller must slow down or drop events
/// - Usage is an estimate of 116 bytes per event, and the cap check is not atomic with
///   the insert, so concurrent inserts can go slightly over
///
/// # Example
/// ```no_run
/// # use eventflux::core::query::input::stream::state::EventStore;
/// # use eventflux::core::event::stream::StreamEvent;
/// // Production usage with memory limit
/// let store = EventStore::with_memory_limit(1_000_000_000); // 1GB max
/// # let stream_event = StreamEvent::new(0, 0, 0, 0);
/// let event_id = store.insert(stream_event);
/// if event_id == 0 {
///     // Memory limit reached, apply backpressure
/// }
/// ```
pub struct EventStore {
    /// EventId → Arc<StreamEvent>
    events: DashMap<EventId, Arc<StreamEvent>>,
    /// Next id to hand out
    next_id: AtomicU64,
    /// Memory cap in bytes. None means no limit.
    max_memory_bytes: Option<usize>,
    /// Estimated bytes in use, 116 per stored event
    current_memory: AtomicUsize,
}

impl EventStore {
    /// Create a store with room for 16384 events and no memory limit
    pub fn new() -> Self {
        Self::with_capacity(16384)
    }

    /// Create a store with room for `capacity` events and no memory limit.
    ///
    /// Reserving room up front avoids resizing when the size is known.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            events: DashMap::with_capacity(capacity),
            next_id: AtomicU64::new(1), // 0 is reserved to mean no event
            max_memory_bytes: None,
            current_memory: AtomicUsize::new(0),
        }
    }

    /// Create a store with room for 16384 events and a memory cap.
    ///
    /// # Arguments
    /// - `max_bytes`: most estimated bytes the stored events may use
    ///
    /// # Behavior
    /// - Inserts fail and return EventId 0 once the cap is reached
    /// - The caller must then drop events or pause input
    ///
    /// # Example
    /// ```
    /// # use eventflux::core::query::input::stream::state::EventStore;
    /// // 1GB memory limit for production safety
    /// let store = EventStore::with_memory_limit(1_000_000_000);
    /// ```
    pub fn with_memory_limit(max_bytes: usize) -> Self {
        Self {
            events: DashMap::with_capacity(16384),
            next_id: AtomicU64::new(1),
            max_memory_bytes: Some(max_bytes),
            current_memory: AtomicUsize::new(0),
        }
    }

    /// Store an event and return its new id.
    ///
    /// O(1). The event is wrapped in an Arc, so later reads share it without copying.
    ///
    /// # Returns
    /// - A non zero EventId on success
    /// - 0 when the memory cap would be passed, which tells the caller to slow down
    ///
    /// # Memory limit
    /// - Checked before the insert, only when max_memory_bytes is set
    /// - Each event counts as an estimated 116 bytes (id, Arc and an average event)
    pub fn insert(&self, event: StreamEvent) -> EventId {
        if let Some(max_bytes) = self.max_memory_bytes {
            let current = self.current_memory.load(Ordering::Relaxed);
            if current + 116 > max_bytes {
                return 0; // 0 means no event, the failure signal
            }
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.events.insert(id, Arc::new(event));

        self.current_memory.fetch_add(116, Ordering::Relaxed);

        id
    }

    /// The event for `id`, shared through its Arc without copying. None when the id
    /// is unknown or was removed.
    pub fn get(&self, id: EventId) -> Option<Arc<StreamEvent>> {
        self.events.get(&id).map(|entry| Arc::clone(entry.value()))
    }

    /// Remove the event for `id` and return it. Meant for freeing memory when states
    /// are evicted. None when the id is unknown.
    pub fn remove(&self, id: EventId) -> Option<Arc<StreamEvent>> {
        let result = self.events.remove(&id).map(|(_, event)| event);

        if result.is_some() {
            self.current_memory.fetch_sub(116, Ordering::Relaxed);
        }

        result
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Remove every event and reset the memory estimate to 0. Ids keep counting up.
    pub fn clear(&self) {
        self.events.clear();
        self.current_memory.store(0, Ordering::Relaxed);
    }

    /// Estimated bytes in use, 116 per stored event
    pub fn memory_usage_bytes(&self) -> usize {
        self.current_memory.load(Ordering::Relaxed)
    }

    /// Memory cap in bytes, or None when there is no limit
    pub fn memory_limit(&self) -> Option<usize> {
        self.max_memory_bytes
    }

    /// True when estimated usage is at or over the cap. Always false with no limit.
    pub fn is_memory_limit_reached(&self) -> bool {
        if let Some(max_bytes) = self.max_memory_bytes {
            self.current_memory.load(Ordering::Relaxed) >= max_bytes
        } else {
            false
        }
    }
}

impl Default for EventStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::value::AttributeValue;

    fn create_test_event(timestamp: i64, value: i64) -> StreamEvent {
        StreamEvent::new_with_data(timestamp, vec![AttributeValue::Long(value)])
    }

    #[test]
    fn test_event_store_insert_get() {
        let store = EventStore::new();
        let event = create_test_event(1000, 42);

        let id = store.insert(event);
        let retrieved = store.get(id).unwrap();

        assert_eq!(retrieved.timestamp, 1000);
        assert_eq!(retrieved.before_window_data[0], AttributeValue::Long(42));
    }

    #[test]
    fn test_event_store_remove() {
        let store = EventStore::new();
        let event = create_test_event(1000, 42);

        let id = store.insert(event);
        assert_eq!(store.len(), 1);

        let removed = store.remove(id);
        assert!(removed.is_some());
        assert_eq!(store.len(), 0);
        assert!(store.get(id).is_none());
    }

    #[test]
    fn test_event_store_shared_ownership() {
        let store = EventStore::new();
        let event = create_test_event(1000, 42);

        let id = store.insert(event);
        let ref1 = store.get(id).unwrap();
        let _ref2 = store.get(id).unwrap();

        // Both references point to same Arc
        assert_eq!(Arc::strong_count(&ref1), 3); // store + ref1 + ref2
    }

    #[test]
    fn test_event_store_concurrent_inserts() {
        use std::thread;

        let store = Arc::new(EventStore::new());
        let mut handles = vec![];

        // Spawn 10 threads, each inserting 1000 events
        for thread_id in 0..10 {
            let store_clone = Arc::clone(&store);
            let handle = thread::spawn(move || {
                for i in 0..1000 {
                    let event = create_test_event(i, thread_id * 1000 + i);
                    store_clone.insert(event);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(store.len(), 10_000);
    }

    #[test]
    fn test_event_store_memory_estimation() {
        let store = EventStore::new();

        for i in 0..1000 {
            store.insert(create_test_event(i, i));
        }

        let memory = store.memory_usage_bytes();
        // Should be approximately 116KB for 1000 events
        assert!(memory > 100_000 && memory < 150_000);
    }

    #[test]
    fn test_unique_id_generation() {
        let store = EventStore::new();
        let mut ids = std::collections::HashSet::new();

        for i in 0..10000 {
            let id = store.insert(create_test_event(i, i));
            assert!(ids.insert(id), "Duplicate ID generated: {}", id);
        }
    }

    #[test]
    fn test_memory_limit() {
        // Create store with 10KB limit (enough for ~86 events at 116 bytes each)
        let store = EventStore::with_memory_limit(10_000);

        let mut successful_inserts = 0;
        for i in 0..200 {
            let id = store.insert(create_test_event(i, i));
            if id != 0 {
                successful_inserts += 1;
            } else {
                // Memory limit reached
                break;
            }
        }

        // Should have stopped before 200 due to memory limit
        assert!(successful_inserts < 200);
        // Should have inserted at least some events
        assert!(successful_inserts > 50);
        // Memory should be close to limit (within one event size)
        let memory_used = store.memory_usage_bytes();
        assert!((9_000..=10_116).contains(&memory_used)); // Within limit + 1 event

        println!(
            "Inserted {} events using {} bytes before hitting 10KB limit",
            successful_inserts, memory_used
        );
    }

    #[test]
    fn test_memory_limit_with_removals() {
        let store = EventStore::with_memory_limit(5_000);

        // Fill to capacity
        let mut ids = vec![];
        for i in 0..100 {
            let id = store.insert(create_test_event(i, i));
            if id != 0 {
                ids.push(id);
            } else {
                break;
            }
        }

        let initial_count = ids.len();
        assert!(initial_count > 0);

        // Remove half the events
        for &id in &ids[0..initial_count / 2] {
            store.remove(id);
        }

        // Should be able to insert more now
        let new_id = store.insert(create_test_event(999, 999));
        assert_ne!(new_id, 0, "Should be able to insert after freeing memory");
    }

    #[test]
    fn test_unlimited_memory_by_default() {
        let store = EventStore::new();

        // Should accept unlimited inserts
        for i in 0..1000 {
            let id = store.insert(create_test_event(i, i));
            assert_ne!(id, 0, "Default store should have unlimited memory");
        }

        assert_eq!(store.len(), 1000);
        assert_eq!(store.memory_limit(), None);
    }
}
