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

// Simple executor service backed by a rayon thread pool.

use rayon::ThreadPool;
use rayon::ThreadPoolBuilder;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Counts tasks still running that were submitted through
/// [`ExecutorService::execute_tracked`].
///
/// A task counts from the moment it is submitted, not when it starts running, until
/// its closure returns or unwinds.
///
/// Memory ordering caveat. A reader that has no happens before link to a `register()`
/// call can see an old zero from before the registration. So `in_flight() == 0` means
/// "no tracked work" only for readers that are synchronized with every registration.
/// The junction drain is one. It reads the consumed counter before the tracker, and
/// consumers register a dispatch before they increment the consumed counter with
/// Release, which gives it that link.
#[derive(Debug, Default)]
pub struct TaskTracker {
    // CachePadded keeps the frequent per dispatch increments and decrements off the
    // cache line that holds the Arc reference counts.
    in_flight: crossbeam_utils::CachePadded<AtomicU64>,
}

impl TaskTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Counts one more task. The returned guard takes it off the count when dropped,
    /// including during a panic unwind.
    pub fn register(self: &Arc<Self>) -> TaskGuard {
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        TaskGuard {
            tracker: Arc::clone(self),
        }
    }

    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Acquire)
    }

    /// Polls until `in_flight` reaches zero or `timeout` passes, sleeping
    /// `poll_interval` between checks. Returns whether zero was reached.
    /// A timeout too large to add to an `Instant` means wait forever.
    pub fn wait_for_zero(
        &self,
        timeout: std::time::Duration,
        poll_interval: std::time::Duration,
    ) -> bool {
        let deadline = std::time::Instant::now().checked_add(timeout);
        while self.in_flight() != 0 {
            if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
                return false;
            }
            std::thread::sleep(poll_interval);
        }
        true
    }
}

/// Guard for one tracked task. Dropping it takes the task off the count. See
/// [`TaskTracker::register`].
#[derive(Debug)]
pub struct TaskGuard {
    tracker: Arc<TaskTracker>,
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.tracker.in_flight.fetch_sub(1, Ordering::Release);
    }
}

/// Thread pool for running tasks in the background.
///
/// The default pool size comes from `EVENTFLUX_EXECUTOR_THREADS`, or the number of CPUs
/// when that is unset or not a number.
#[derive(Debug)]
pub struct ExecutorService {
    pool: ThreadPool,
    threads: usize,
}

impl Default for ExecutorService {
    fn default() -> Self {
        let threads = std::env::var("EVENTFLUX_EXECUTOR_THREADS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or_else(|| num_cpus::get().max(1));
        ExecutorService::new("executor", threads)
    }
}

impl ExecutorService {
    /// Creates an executor with the given number of worker threads, named
    /// "<name>-<index>". A panicking task is logged and the pool keeps running.
    ///
    /// # Panics
    /// Panics if rayon cannot build the thread pool.
    pub fn new(name: &str, threads: usize) -> Self {
        let name_str = name.to_string();
        let name_for_panic = name.to_string();
        let pool = ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(move |i| format!("{name_str}-{i}"))
            // Without a panic handler rayon aborts the whole process when a
            // spawned task panics. Log it and keep the pool alive instead.
            .panic_handler(move |panic| {
                let msg = panic
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "unknown panic".to_string());
                log::error!("[{name_for_panic}] executor task panicked: {msg}");
            })
            .build()
            .expect("failed to build thread pool");
        Self { pool, threads }
    }

    /// Queues a task to run on the pool and returns at once.
    pub fn execute<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.pool.spawn(task);
    }

    /// Queues a task whose completion can be watched through `tracker`.
    ///
    /// The guard is created on the caller's thread before the task is queued,
    /// so a queued task is never left uncounted.
    pub fn execute_tracked<F>(&self, tracker: &Arc<TaskTracker>, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let guard = tracker.register();
        self.pool.spawn(move || {
            let _guard = guard;
            task();
        });
    }

    /// Does NOT wait. It does nothing, because rayon manages its own threads and has
    /// no way to wait for queued tasks. Callers that need to know when tasks finish
    /// must use [`ExecutorService::execute_tracked`] and poll the [`TaskTracker`].
    /// Making this a real wait is tracked in #75.
    pub fn wait_all(&self) {}

    pub fn pool_size(&self) -> usize {
        self.threads
    }
}

impl Drop for ExecutorService {
    fn drop(&mut self) {}
}

/// Reads the thread count from `EVENTFLUX_POOL_<NAME>_THREADS`, with the name upper
/// cased. Falls back to `default` when it is unset or not a number.
pub fn pool_size_from_env(name: &str, default: usize) -> usize {
    let var = format!("EVENTFLUX_POOL_{}_THREADS", name.to_uppercase());
    std::env::var(&var)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

/// Holds named executor services.
#[derive(Debug, Default)]
pub struct ExecutorServiceRegistry {
    pools: RwLock<HashMap<String, Arc<ExecutorService>>>,
}

impl ExecutorServiceRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a pool under the given name, replacing any pool with that name.
    pub fn register_named(&self, name: String, exec: Arc<ExecutorService>) {
        self.pools.write().unwrap().insert(name, exec);
    }

    /// Returns the pool with this name, if any.
    pub fn get(&self, name: &str) -> Option<Arc<ExecutorService>> {
        self.pools.read().unwrap().get(name).cloned()
    }

    /// Returns the pool with this name, or creates and registers one with `threads`
    /// threads. The check and insert are not atomic, so two callers racing can each
    /// create a pool and the last one registered wins.
    pub fn get_or_create(&self, name: &str, threads: usize) -> Arc<ExecutorService> {
        if let Some(e) = self.get(name) {
            return e;
        }
        let exec = Arc::new(ExecutorService::new(name, threads));
        self.register_named(name.to_string(), Arc::clone(&exec));
        exec
    }

    /// Same as `get_or_create`, with the size from `pool_size_from_env(name, default)`.
    pub fn get_or_create_from_env(&self, name: &str, default: usize) -> Arc<ExecutorService> {
        let threads = pool_size_from_env(name, default);
        self.get_or_create(name, threads)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    fn wait_for_zero(tracker: &TaskTracker, timeout: Duration) -> bool {
        tracker.wait_for_zero(timeout, Duration::from_micros(50))
    }

    #[test]
    fn test_tracked_task_counted_before_run_and_zero_after() {
        let exec = ExecutorService::new("tracked-test", 2);
        let tracker = Arc::new(TaskTracker::new());
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let (started_tx, started_rx) = mpsc::channel::<()>();

        exec.execute_tracked(&tracker, move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });

        // Counted from submission, before the task even starts
        assert_eq!(tracker.in_flight(), 1);
        started_rx.recv().unwrap();
        assert_eq!(tracker.in_flight(), 1);

        release_tx.send(()).unwrap();
        assert!(
            wait_for_zero(&tracker, Duration::from_secs(5)),
            "tracker should reach zero after task completes"
        );
    }

    #[test]
    fn test_tracked_task_panic_still_decrements() {
        let exec = ExecutorService::new("panic-test", 2);
        let tracker = Arc::new(TaskTracker::new());

        // Needs the pool's panic_handler. Without it rayon aborts the process and
        // this test could never see the decrement.
        exec.execute_tracked(&tracker, || panic!("subscriber blew up"));

        assert!(
            wait_for_zero(&tracker, Duration::from_secs(5)),
            "guard must decrement during panic unwind"
        );
    }

    #[test]
    fn test_plain_guard_drop_decrements() {
        let tracker = Arc::new(TaskTracker::new());
        let guard = tracker.register();
        assert_eq!(tracker.in_flight(), 1);
        drop(guard);
        assert_eq!(tracker.in_flight(), 0);
    }
}
