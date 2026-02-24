// SPDX-License-Identifier: MIT OR Apache-2.0

//! Timer Wheel for O(1) Event Scheduling
//!
//! **Performance Optimization**: O(1) insert instead of O(log n) BinaryHeap
//! - Before: BinaryHeap insert = O(log n), pop = O(log n)
//! - After: Timer wheel insert = O(1), trigger = O(expired_items)
//! - **Speedup**: Constant time insertion regardless of scheduled items
//!
//! **Algorithm**: Time-bucketed array with circular indexing
//! - Array of buckets (e.g., 3600 for 1 hour at 1-second resolution)
//! - current_index advances with time
//! - Insert: bucket_index = (current + offset) % capacity
//! - Trigger: advance wheel, collect expired buckets
//!
//! **Use Case**: Absent patterns like `NOT A FOR 10 SECONDS`

/// Timer wheel for O(1) event scheduling
///
/// # Algorithm
/// - Array of time buckets (e.g., 3600 buckets for 1 hour at 1-second ticks)
/// - current_index advances with time like a clock
/// - Insert: calculate bucket offset, add to bucket (O(1))
/// - Trigger: advance wheel, collect expired items (O(expired), NOT O(total))
///
/// # Performance
/// - **Insert**: O(1) - just push to bucket
/// - **Trigger**: O(ticks_advanced + expired_items) - NOT O(total_scheduled)
/// - **Memory**: num_buckets × Vec overhead + items
///
/// # Example
/// ```ignore
/// use eventflux::core::query::input::stream::state::TimerWheel;
/// let mut wheel = TimerWheel::new(3600_000, 1000); // 1 hour, 1-second ticks
/// wheel.set_start_time(0);
/// wheel.schedule(state_handle, trigger_at_ms);
/// let expired = wheel.advance_to(current_time_ms);
/// ```
pub struct TimerWheel<T> {
    /// Array of time buckets
    buckets: Box<[Vec<T>]>,

    /// Current bucket index (advances with time)
    current_index: usize,

    /// Start time (milliseconds)
    start_time: i64,

    /// Tick duration (milliseconds per bucket)
    tick_duration_ms: i64,
}

impl<T> TimerWheel<T> {
    /// Create new timer wheel
    ///
    /// # Arguments
    /// - `duration_ms`: Total time span covered (e.g., 3600_000 for 1 hour)
    /// - `tick_ms`: Time per bucket (e.g., 1000 for 1 second)
    ///
    /// # Buckets
    /// - num_buckets = duration_ms / tick_ms
    /// - Example: 3600_000 / 1000 = 3600 buckets (1 hour at 1-second resolution)
    ///
    /// # Trade-offs
    /// - More buckets: Higher resolution, more memory
    /// - Fewer buckets: Coarser resolution, less memory
    /// - Recommendation: 100ms - 1000ms tick duration
    pub fn new(duration_ms: i64, tick_ms: i64) -> Self {
        let num_buckets = (duration_ms / tick_ms) as usize;
        let buckets = (0..num_buckets)
            .map(|_| Vec::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            buckets,
            current_index: 0,
            start_time: 0,
            tick_duration_ms: tick_ms,
        }
    }

    /// Initialize start time
    ///
    /// Must be called before scheduling events
    pub fn set_start_time(&mut self, start_time: i64) {
        self.start_time = start_time;
    }

    /// Schedule item to trigger at specific time
    ///
    /// # Performance
    /// - O(1) insert - just push to bucket
    /// - No heap operations, no sorting
    ///
    /// # Behavior
    /// - If trigger_at is beyond wheel capacity, wraps around
    /// - Items in same bucket trigger together (bucket resolution)
    pub fn schedule(&mut self, item: T, trigger_at: i64) {
        let elapsed = trigger_at - self.start_time;
        let ticks = (elapsed / self.tick_duration_ms) as usize;
        let bucket_index = (self.current_index + ticks) % self.buckets.len();

        self.buckets[bucket_index].push(item);
    }

    /// Advance wheel to current time and collect expired items
    ///
    /// # Performance
    /// - O(ticks_to_advance + expired_items)
    /// - NOT O(total_scheduled)!
    /// - Only processes buckets between old and new time
    ///
    /// # Returns
    /// Vec of all items that should trigger at or before current_time
    pub fn advance_to(&mut self, current_time: i64) -> Vec<T> {
        let elapsed = current_time - self.start_time;
        let target_index = (elapsed / self.tick_duration_ms) as usize % self.buckets.len();

        let mut triggered = Vec::new();

        // Edge case: if current_index == target_index, collect items at current bucket
        // This handles items scheduled at exactly current_time
        if self.current_index == target_index {
            triggered.extend(self.buckets[self.current_index].drain(..));
            return triggered;
        }

        // Advance wheel, collect expired items from each passed bucket
        while self.current_index != target_index {
            self.current_index = (self.current_index + 1) % self.buckets.len();
            triggered.extend(self.buckets[self.current_index].drain(..));
        }

        triggered
    }

    /// Get number of scheduled items
    pub fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.len()).sum()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.buckets.iter().all(|b| b.is_empty())
    }

    /// Clear all scheduled items
    pub fn clear(&mut self) {
        self.buckets.iter_mut().for_each(|b| b.clear());
    }

    /// Get number of buckets
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    /// Get tick duration in milliseconds
    pub fn tick_duration(&self) -> i64 {
        self.tick_duration_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timer_wheel_schedule_and_trigger() {
        let mut wheel = TimerWheel::new(10_000, 1000); // 10 seconds, 1-second ticks
        wheel.set_start_time(0);

        // Schedule events
        wheel.schedule(1u32, 1000); // Trigger at 1 second
        wheel.schedule(2u32, 2000); // Trigger at 2 seconds
        wheel.schedule(3u32, 5000); // Trigger at 5 seconds

        assert_eq!(wheel.len(), 3);

        // Advance to 2 seconds
        let expired = wheel.advance_to(2000);
        assert_eq!(expired.len(), 2); // Items 1 and 2
        assert!(expired.contains(&1));
        assert!(expired.contains(&2));

        // Advance to 5 seconds
        let expired = wheel.advance_to(5000);
        assert_eq!(expired.len(), 1); // Item 3
        assert!(expired.contains(&3));
    }

    #[test]
    fn test_timer_wheel_wrap_around() {
        let mut wheel = TimerWheel::new(5_000, 1000); // 5 seconds, 1-second ticks (5 buckets)
        wheel.set_start_time(0);

        // Schedule event beyond wheel capacity (should wrap around)
        wheel.schedule(1u32, 6000); // 6 seconds (wraps to bucket 1)

        // Advance past wrap point
        let expired = wheel.advance_to(6000);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0], 1);
    }

    #[test]
    fn test_timer_wheel_no_trigger_before_time() {
        let mut wheel = TimerWheel::new(10_000, 1000);
        wheel.set_start_time(0);

        wheel.schedule(1u32, 5000); // Trigger at 5 seconds

        // Advance to 3 seconds (before trigger time)
        let expired = wheel.advance_to(3000);
        assert!(expired.is_empty());

        // Advance to 5 seconds (at trigger time)
        let expired = wheel.advance_to(5000);
        assert_eq!(expired.len(), 1);
    }

    #[test]
    fn test_timer_wheel_multiple_items_same_bucket() {
        let mut wheel = TimerWheel::new(10_000, 1000);
        wheel.set_start_time(0);

        // Schedule multiple items in same bucket
        wheel.schedule(1u32, 2000);
        wheel.schedule(2u32, 2000);
        wheel.schedule(3u32, 2000);

        let expired = wheel.advance_to(2000);
        assert_eq!(expired.len(), 3);
        assert!(expired.contains(&1));
        assert!(expired.contains(&2));
        assert!(expired.contains(&3));
    }

    #[test]
    fn test_timer_wheel_clear() {
        let mut wheel = TimerWheel::new(10_000, 1000);
        wheel.set_start_time(0);

        wheel.schedule(1u32, 1000);
        wheel.schedule(2u32, 2000);

        assert_eq!(wheel.len(), 2);

        wheel.clear();

        assert_eq!(wheel.len(), 0);
        assert!(wheel.is_empty());
    }

    #[test]
    fn test_timer_wheel_performance_10k_schedules() {
        use std::time::Instant;

        let mut wheel = TimerWheel::new(3600_000, 1000); // 1 hour, 1-second ticks
        wheel.set_start_time(0);

        // Schedule 10,000 items
        let start = Instant::now();
        for i in 0..10_000 {
            wheel.schedule(i, i * 100); // Spread across time
        }
        let duration = start.elapsed();

        println!("Scheduled 10,000 items in {:?}", duration);

        // Should be <10ms for 10k inserts (O(1) per insert)
        // Relaxed threshold for CI environments (was 1ms, then 5ms)
        assert!(duration.as_micros() < 10000);
    }

    #[test]
    fn test_timer_wheel_o1_advance() {
        let mut wheel = TimerWheel::new(3600_000, 1000);
        wheel.set_start_time(0);

        // Schedule many items
        for i in 0..10_000 {
            wheel.schedule(i, (i % 100) * 1000);
        }

        // Advance 10 seconds (should only process 10 buckets, not all items)
        use std::time::Instant;
        let start = Instant::now();
        let expired = wheel.advance_to(10_000);
        let duration = start.elapsed();

        println!(
            "Advanced wheel and collected {} items in {:?}",
            expired.len(),
            duration
        );

        // Should be fast regardless of total scheduled items
        // Relaxed threshold for CI environments (virtualization overhead)
        assert!(duration.as_millis() < 5);
    }

    #[test]
    fn test_timer_wheel_incremental_advance() {
        let mut wheel = TimerWheel::new(10_000, 1000);
        wheel.set_start_time(0);

        wheel.schedule(1u32, 1000);
        wheel.schedule(2u32, 3000);
        wheel.schedule(3u32, 5000);

        // Advance incrementally
        let e1 = wheel.advance_to(1000);
        assert_eq!(e1.len(), 1);
        assert_eq!(e1[0], 1);

        let e2 = wheel.advance_to(3000);
        assert_eq!(e2.len(), 1);
        assert_eq!(e2[0], 2);

        let e3 = wheel.advance_to(5000);
        assert_eq!(e3.len(), 1);
        assert_eq!(e3[0], 3);
    }
}
