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

//! Timer wheel that schedules items in constant time.
//!
//! **Why**: inserting is O(1), where a BinaryHeap insert and pop are each O(log n).
//! Firing costs O(expired items). Insert time stays the same however many items are
//! scheduled.
//!
//! **How**: a ring of time buckets
//! - An array of buckets, for example 3600 for 1 hour at 1 second per bucket
//! - current_index moves forward as time passes
//! - Insert: bucket_index = (current + offset) % capacity
//! - Fire: move the wheel forward and collect the buckets passed
//!
//! **Meant for**: absent patterns like `NOT A FOR 10 SECONDS`. Not used by the engine
//! yet.

/// Timer wheel that schedules items in constant time.
///
/// # How it works
/// - An array of time buckets, for example 3600 buckets for 1 hour at 1 second ticks
/// - current_index moves forward with time, like a clock hand
/// - Insert: work out the bucket and push the item there, O(1)
/// - Fire: move forward and collect the items passed, O(expired), not O(total)
///
/// # Cost
/// - **Insert**: O(1), a push into one bucket
/// - **Fire**: O(ticks moved + expired items), not O(total scheduled)
/// - **Memory**: one Vec per bucket, plus the items
///
/// # Example
/// ```ignore
/// use eventflux::core::query::input::stream::state::TimerWheel;
/// let mut wheel = TimerWheel::new(3_600_000, 1000); // 1 hour, 1-second ticks
/// wheel.set_start_time(0);
/// wheel.schedule(state_handle, trigger_at_ms);
/// let expired = wheel.advance_to(current_time_ms);
/// ```
pub struct TimerWheel<T> {
    buckets: Box<[Vec<T>]>,

    /// Bucket the wheel points at now. Moves forward with time.
    current_index: usize,

    /// Time the wheel starts from, in milliseconds
    start_time: i64,

    /// Milliseconds covered by one bucket
    tick_duration_ms: i64,
}

impl<T> TimerWheel<T> {
    /// Create a timer wheel with start time 0.
    ///
    /// # Arguments
    /// - `duration_ms`: total time span covered, for example 3_600_000 for 1 hour
    /// - `tick_ms`: time per bucket, for example 1000 for 1 second
    ///
    /// # Buckets
    /// - num_buckets = duration_ms / tick_ms, rounded down
    /// - Example: 3_600_000 / 1000 = 3600 buckets, 1 hour at 1 second per bucket
    ///
    /// # Panics
    /// Panics when `tick_ms` is 0. When `duration_ms` is less than `tick_ms` there
    /// are no buckets, and later calls to `schedule` or `advance_to` panic.
    ///
    /// # Trade offs
    /// - More buckets give finer timing and use more memory
    /// - Fewer buckets give coarser timing and use less memory
    /// - A tick of 100 ms to 1000 ms is recommended
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

    /// Set the start time in milliseconds. Call it before scheduling items.
    pub fn set_start_time(&mut self, start_time: i64) {
        self.start_time = start_time;
    }

    /// Schedule `item` to fire at `trigger_at` milliseconds.
    ///
    /// O(1), a push into one bucket with no heap work or sorting.
    ///
    /// # Behavior
    /// - The bucket is current_index plus the ticks from start_time to trigger_at.
    ///   The ticks are counted from start_time, not from the current position, so
    ///   after the wheel has moved the item lands later than asked.
    /// - A time past the wheel's span wraps around
    /// - Items in the same bucket fire together
    pub fn schedule(&mut self, item: T, trigger_at: i64) {
        let elapsed = trigger_at - self.start_time;
        let ticks = (elapsed / self.tick_duration_ms) as usize;
        let bucket_index = (self.current_index + ticks) % self.buckets.len();

        self.buckets[bucket_index].push(item);
    }

    /// Move the wheel to `current_time` and return the items that fired.
    ///
    /// Costs O(ticks moved + expired items), not O(total scheduled). Only the buckets
    /// between the old and new position are visited.
    ///
    /// # Returns
    /// The items in the buckets passed on the way to `current_time`. When the wheel is
    /// already at the target bucket, returns that bucket's items. Otherwise the bucket
    /// it starts on is not collected.
    pub fn advance_to(&mut self, current_time: i64) -> Vec<T> {
        let elapsed = current_time - self.start_time;
        let target_index = (elapsed / self.tick_duration_ms) as usize % self.buckets.len();

        let mut triggered = Vec::new();

        // Already at the target bucket, so return its items. Covers items scheduled at
        // exactly current_time.
        if self.current_index == target_index {
            triggered.append(&mut self.buckets[self.current_index]);
            return triggered;
        }

        // Step forward one bucket at a time and collect each bucket passed
        while self.current_index != target_index {
            self.current_index = (self.current_index + 1) % self.buckets.len();
            triggered.append(&mut self.buckets[self.current_index]);
        }

        triggered
    }

    /// Number of scheduled items. Walks every bucket, so O(buckets).
    pub fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.buckets.iter().all(|b| b.is_empty())
    }

    /// Drop every scheduled item. The position and start time stay as they are.
    pub fn clear(&mut self) {
        self.buckets.iter_mut().for_each(|b| b.clear());
    }

    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    /// Milliseconds per bucket
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

        let mut wheel = TimerWheel::new(3_600_000, 1000); // 1 hour, 1-second ticks
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
        let mut wheel = TimerWheel::new(3_600_000, 1000);
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
