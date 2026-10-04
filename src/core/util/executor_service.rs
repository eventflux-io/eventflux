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

// src/core/util/executor_service.rs
// Simple executor service backed by rayon thread pool.

use rayon::ThreadPool;
use rayon::ThreadPoolBuilder;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Counts in-flight tasks submitted via [`ExecutorService::execute_tracked`].
///
/// A task is "in flight" from the moment it is submitted (not when it starts
/// running) until its closure returns or unwinds, so `in_flight() == 0` means
/// no tracked work is queued or executing.
#[derive(Debug, Default)]
pub struct TaskTracker {
    in_flight: AtomicU64,
}

impl TaskTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one unit of in-flight work. The returned guard decrements the
    /// counter when dropped, including during a panic unwind.
    pub fn register(self: &Arc<Self>) -> TaskGuard {
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        TaskGuard {
            tracker: Arc::clone(self),
        }
    }

    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Acquire)
    }
}

/// RAII guard for one tracked task; see [`TaskTracker::register`].
#[derive(Debug)]
pub struct TaskGuard {
    tracker: Arc<TaskTracker>,
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.tracker.in_flight.fetch_sub(1, Ordering::Release);
    }
}

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
    /// Create a new executor with the given number of worker threads.
    pub fn new(name: &str, threads: usize) -> Self {
        let name_str = name.to_string();
        let name_for_panic = name.to_string();
        let pool = ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(move |i| format!("{name_str}-{i}"))
            // Without a panic handler rayon aborts the whole process when a
            // spawned task panics; log and keep the pool alive instead.
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

    /// Submit a task for asynchronous execution.
    pub fn execute<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.pool.spawn(task);
    }

    /// Submit a task whose completion is observable through `tracker`.
    ///
    /// The guard is created on the caller's thread before the task is queued,
    /// so there is no window where the task is enqueued but uncounted.
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

    /// Block until queued tasks complete. Rayon manages its threads so this is
    /// a no-op; callers that need completion signalling can opt in via
    /// [`ExecutorService::execute_tracked`] and poll the [`TaskTracker`].
    /// Making this wait for all untracked tasks is tracked in #75.
    pub fn wait_all(&self) {}

    pub fn pool_size(&self) -> usize {
        self.threads
    }
}

impl Drop for ExecutorService {
    fn drop(&mut self) {}
}

/// Determine thread count from an environment variable of the form
/// `EVENTFLUX_POOL_<NAME>_THREADS`. Falls back to the provided default.
pub fn pool_size_from_env(name: &str, default: usize) -> usize {
    let var = format!("EVENTFLUX_POOL_{}_THREADS", name.to_uppercase());
    std::env::var(&var)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

/// Registry for managing multiple named executor services.
#[derive(Debug, Default)]
pub struct ExecutorServiceRegistry {
    pools: RwLock<HashMap<String, Arc<ExecutorService>>>,
}

impl ExecutorServiceRegistry {
    /// Create a new, empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a pool under the given name.
    pub fn register_named(&self, name: String, exec: Arc<ExecutorService>) {
        self.pools.write().unwrap().insert(name, exec);
    }

    /// Get a pool by name if present.
    pub fn get(&self, name: &str) -> Option<Arc<ExecutorService>> {
        self.pools.read().unwrap().get(name).cloned()
    }

    /// Get a pool if it exists or create one with the provided size.
    pub fn get_or_create(&self, name: &str, threads: usize) -> Arc<ExecutorService> {
        if let Some(e) = self.get(name) {
            return e;
        }
        let exec = Arc::new(ExecutorService::new(name, threads));
        self.register_named(name.to_string(), Arc::clone(&exec));
        exec
    }

    /// Get or create a pool using `pool_size_from_env` with the provided default.
    pub fn get_or_create_from_env(&self, name: &str, default: usize) -> Arc<ExecutorService> {
        let threads = pool_size_from_env(name, default);
        self.get_or_create(name, threads)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    fn wait_for_zero(tracker: &TaskTracker, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while tracker.in_flight() != 0 {
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::yield_now();
        }
        true
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

        // Requires the pool's panic_handler — without it rayon aborts the
        // process and this test could never observe the decrement.
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
