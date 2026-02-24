// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/util/executor_service.rs
// Simple executor service backed by rayon thread pool.

use rayon::ThreadPool;
use rayon::ThreadPoolBuilder;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

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
        let pool = ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(move |i| format!("{name_str}-{i}"))
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

    /// Block until queued tasks complete. Rayon manages its threads so this is a no-op.
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
