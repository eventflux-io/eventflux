// SPDX-License-Identifier: MIT OR Apache-2.0

//! Production-Optimized Event Store with ID-based References
//!
//! **Memory Optimization**: Store events once, reference by 8-byte ID
//! - Before: Clone 100-byte events = 100MB for 1M states
//! - After: Store IDs (8 bytes) = 8MB for 1M states
//! - **12.5x memory reduction**
//!
//! **Lock-Free Design**: DashMap for zero contention
//! - Concurrent reads without blocking
//! - Lock-free insert/get operations
//! - Scales linearly with cores
//!
//! **Memory Safety**: Configurable memory limits prevent OOM
//! - Default: Unlimited (for backward compatibility)
//! - Production: Set max_memory_bytes to prevent runaway growth
//! - Rejects inserts when limit reached (backpressure signal)

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::core::event::stream::stream_event::StreamEvent;

/// Event identifier - 8 bytes vs 100+ bytes for full event
pub type EventId = u64;

/// Global event store with lock-free concurrent access
///
/// # Performance Characteristics
/// - **Insert**: O(1) lock-free
/// - **Get**: O(1) lock-free
/// - **Remove**: O(1) lock-free
/// - **Memory**: Single Arc<StreamEvent> per unique event (no clones)
///
/// # Memory Limits
/// - Default: Unlimited (backward compatible, but risk of OOM)
/// - Production: Use `with_memory_limit()` to prevent unbounded growth
/// - When limit reached: Returns EventId = 0 (null), caller must handle backpressure
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
    /// Lock-free concurrent hash map: EventId → Arc<StreamEvent>
    events: DashMap<EventId, Arc<StreamEvent>>,
    /// Atomic counter for generating unique IDs
    next_id: AtomicU64,
    /// Maximum memory bytes allowed (None = unlimited)
    max_memory_bytes: Option<usize>,
    /// Current approximate memory usage
    current_memory: AtomicUsize,
}

impl EventStore {
    /// Create a new event store with default capacity (unlimited memory)
    pub fn new() -> Self {
        Self::with_capacity(16384) // 16K initial capacity
    }

    /// Create event store with specific initial capacity (unlimited memory)
    ///
    /// Pre-allocating capacity reduces resizing overhead for known workloads
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            events: DashMap::with_capacity(capacity),
            next_id: AtomicU64::new(1), // Start from 1 (0 reserved for null)
            max_memory_bytes: None,     // Unlimited
            current_memory: AtomicUsize::new(0),
        }
    }

    /// Create event store with memory limit (production-safe)
    ///
    /// # Arguments
    /// - `max_bytes`: Maximum memory allowed for event storage
    ///
    /// # Behavior
    /// - Inserts will fail (return EventId = 0) when limit reached
    /// - Caller must handle backpressure by dropping events or pausing input
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

    /// Insert event and return unique ID
    ///
    /// # Performance
    /// - O(1) lock-free insertion
    /// - Event wrapped in Arc for shared ownership
    /// - Zero-copy when retrieving
    ///
    /// # Returns
    /// - EventId (non-zero) on success
    /// - 0 if memory limit reached (backpressure signal)
    ///
    /// # Memory Limits
    /// - If max_memory_bytes is set, checks before insertion
    /// - Approximate check: 116 bytes per event (ID + Arc + avg event)
    /// - Returns 0 when limit would be exceeded
    pub fn insert(&self, event: StreamEvent) -> EventId {
        // Check memory limit before insertion
        if let Some(max_bytes) = self.max_memory_bytes {
            let current = self.current_memory.load(Ordering::Relaxed);
            if current + 116 > max_bytes {
                // Memory limit would be exceeded, reject insertion
                return 0; // 0 = null EventId (failure signal)
            }
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.events.insert(id, Arc::new(event));

        // Update memory tracking (approximate)
        self.current_memory.fetch_add(116, Ordering::Relaxed);

        id
    }

    /// Get event by ID (returns Arc for zero-copy sharing)
    ///
    /// # Returns
    /// - `Some(Arc<StreamEvent>)` if event exists
    /// - `None` if event not found or was removed
    pub fn get(&self, id: EventId) -> Option<Arc<StreamEvent>> {
        self.events.get(&id).map(|entry| Arc::clone(entry.value()))
    }

    /// Remove event by ID
    ///
    /// Called during state eviction to free memory
    pub fn remove(&self, id: EventId) -> Option<Arc<StreamEvent>> {
        let result = self.events.remove(&id).map(|(_, event)| event);

        // Update memory tracking if removal succeeded
        if result.is_some() {
            self.current_memory.fetch_sub(116, Ordering::Relaxed);
        }

        result
    }

    /// Get current number of events stored
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Check if store is empty
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Clear all events (used for reset/cleanup)
    pub fn clear(&self) {
        self.events.clear();
        self.current_memory.store(0, Ordering::Relaxed);
    }

    /// Get memory usage estimate in bytes
    ///
    /// Returns tracked memory usage (updated on insert/remove)
    pub fn memory_usage_bytes(&self) -> usize {
        self.current_memory.load(Ordering::Relaxed)
    }

    /// Get configured memory limit
    ///
    /// Returns None if unlimited
    pub fn memory_limit(&self) -> Option<usize> {
        self.max_memory_bytes
    }

    /// Check if memory limit is reached
    pub fn is_memory_limit_reached(&self) -> bool {
        if let Some(max_bytes) = self.max_memory_bytes {
            self.current_memory.load(Ordering::Relaxed) >= max_bytes
        } else {
            false // Unlimited
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
        let ref2 = store.get(id).unwrap();

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
        assert!(memory_used >= 9_000 && memory_used <= 10_116); // Within limit + 1 event

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
