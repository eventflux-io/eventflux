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

#[cfg(feature = "kafka")]
pub mod kafka_source;
#[cfg(feature = "rabbitmq")]
pub mod rabbitmq_source;
pub mod timer_source;
#[cfg(feature = "websocket")]
pub mod websocket_source;

use crate::core::exception::EventFluxError;
use crate::core::stream::input::input_handler::InputHandler;
use crate::core::stream::input::mapper::SourceMapper;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Callback for receiving data from sources
///
/// Sources produce raw bytes from external systems and deliver them via this callback.
/// The callback is responsible for parsing bytes into Events and delivering to InputHandler.
pub trait SourceCallback: Debug + Send + Sync {
    /// Called when source has new data available
    ///
    /// # Arguments
    /// * `data` - Raw bytes from external system (JSON, CSV, binary, etc.)
    ///
    /// # Returns
    /// * `Ok(())` - Data processed successfully
    /// * `Err(EventFluxError)` - Processing failed
    fn on_data(&self, data: &[u8]) -> Result<(), EventFluxError>;
}

pub trait Source: Debug + Send + Sync {
    /// Start the source with a callback for data delivery
    ///
    /// Sources read from external systems and deliver raw bytes via the callback.
    /// The callback handles parsing (via SourceMapper) and event delivery.
    ///
    /// # Architecture
    /// ```text
    /// Source::read() → bytes → SourceCallback::on_data() → SourceMapper → Events → InputHandler
    /// ```
    fn start(&mut self, callback: Arc<dyn SourceCallback>);

    /// Stop the source.
    ///
    /// Contract: synchronous (no callback may fire after it returns) and
    /// idempotent. Do not implement by hand — embed a [`SourceWorker`] and
    /// delegate to [`SourceWorker::stop`], which guarantees both.
    fn stop(&mut self);

    fn clone_box(&self) -> Box<dyn Source>;

    /// Phase 2 validation: Verify connectivity and external resource availability
    ///
    /// This method is called during application initialization (Phase 2) to validate
    /// that external systems are reachable and properly configured.
    ///
    /// **Fail-Fast Principle**: Application should NOT start if transports are not ready.
    ///
    /// # Default Implementation
    ///
    /// Returns Ok by default - sources that don't need external validation can use this.
    /// Sources with external dependencies (Kafka, HTTP, etc.) MUST override this method.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - External system is reachable and properly configured
    /// * `Err(EventFluxError)` - Validation failed, application should not start
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// // Kafka source validates broker connectivity
    /// fn validate_connectivity(&self) -> Result<(), EventFluxError> {
    ///     // 1. Validate brokers are reachable
    ///     let metadata = self.consumer.fetch_metadata(None, Duration::from_secs(10))?;
    ///
    ///     // 2. Validate topic exists
    ///     if !metadata.topics().iter().any(|t| t.name() == self.topic) {
    ///         return Err(EventFluxError::configuration(
    ///             format!("Topic '{}' does not exist", self.topic)
    ///         ));
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    fn validate_connectivity(&self) -> Result<(), crate::core::exception::EventFluxError> {
        Ok(()) // Default: no validation needed
    }

    /// Set the DLQ junction for error routing
    ///
    /// This method allows setting the DLQ junction after source creation,
    /// enabling DLQ strategy when the factory creates a source before the DLQ
    /// junction is available. The stream_initializer wires it after creation.
    ///
    /// Sources that support DLQ error handling should override this method.
    /// The default implementation is a no-op for sources that don't support DLQ.
    ///
    /// # Arguments
    /// * `_junction` - The InputHandler for the DLQ stream
    fn set_error_dlq_junction(&mut self, _junction: Arc<Mutex<InputHandler>>) {
        // Default: no-op for sources that don't support DLQ
    }
}

impl Clone for Box<dyn Source> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Default grace period sources wait for their worker thread in `stop()`.
pub const SOURCE_STOP_GRACE_PERIOD: Duration = Duration::from_secs(5);

/// Lifecycle handle for a source's worker thread.
///
/// Every thread-based source embeds one of these instead of hand-rolling a
/// `running: Arc<AtomicBool>` + `JoinHandle` pair. It captures the shutdown
/// pattern once so implementations cannot get it wrong:
///
/// - [`start`](Self::start) sets the running flag and spawns the worker
/// - [`stop`](Self::stop) clears the flag and joins the thread (bounded by
///   [`SOURCE_STOP_GRACE_PERIOD`]), making `Source::stop()` synchronous —
///   no callback can fire after it returns. Idempotent.
/// - `Drop` calls `stop()`, so a source that is dropped without being
///   stopped still shuts its worker down (RAII)
/// - `Clone` yields a fresh, unstarted worker — runtime state never leaks
///   into clones
///
/// Worker loops that wait between iterations should use
/// [`sleep_while_running`] instead of `thread::sleep` so long intervals don't
/// delay shutdown.
///
/// # Usage
///
/// ```rust,ignore
/// pub struct MySource {
///     config: MyConfig,
///     worker: SourceWorker,
/// }
///
/// impl Source for MySource {
///     fn start(&mut self, callback: Arc<dyn SourceCallback>) {
///         let config = self.config.clone();
///         self.worker.start(move |running| {
///             while running.load(Ordering::SeqCst) {
///                 // poll `running` at least every ~100ms so stop() is fast
///             }
///         });
///     }
///
///     fn stop(&mut self) {
///         self.worker.stop();
///     }
///     // ...
/// }
/// ```
#[derive(Debug)]
pub struct SourceWorker {
    /// Source name used in shutdown log messages
    name: &'static str,
    /// Running flag polled by the worker loop
    running: Arc<AtomicBool>,
    /// Worker thread handle, joined in `stop()`
    handle: Option<JoinHandle<()>>,
}

impl SourceWorker {
    /// Create an unstarted worker. `name` appears in shutdown log messages.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            running: Arc::new(AtomicBool::new(false)),
            handle: None,
        }
    }

    /// Set the running flag and spawn the worker thread.
    ///
    /// The closure receives the running flag and must poll it at least every
    /// ~100ms so that `stop()` returns promptly (use [`sleep_while_running`]
    /// for waits between iterations).
    pub fn start<F>(&mut self, body: F)
    where
        F: FnOnce(Arc<AtomicBool>) + Send + 'static,
    {
        self.running.store(true, Ordering::SeqCst);
        let running = Arc::clone(&self.running);
        self.handle = Some(std::thread::spawn(move || body(running)));
    }

    /// Signal shutdown and join the worker thread.
    ///
    /// Bounded by [`SOURCE_STOP_GRACE_PERIOD`]; a worker stuck in slow I/O is
    /// detached with a warning. Idempotent — safe to call without `start()`
    /// or multiple times.
    pub fn stop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            join_with_timeout(handle, SOURCE_STOP_GRACE_PERIOD, self.name);
        }
    }
}

impl Drop for SourceWorker {
    fn drop(&mut self) {
        // RAII: a source dropped without stop() still shuts its worker down.
        self.stop();
    }
}

impl Clone for SourceWorker {
    fn clone(&self) -> Self {
        // Runtime state (flag, handle) never leaks into clones.
        Self::new(self.name)
    }
}

/// Sleep for `duration`, waking early when `running` is cleared.
///
/// Worker loops use this instead of `thread::sleep` for waits between
/// iterations, so a long interval (e.g. a 60s timer) never delays `stop()`:
/// the sleep is taken in ≤100ms slices with the flag checked between slices.
pub fn sleep_while_running(running: &AtomicBool, duration: Duration) {
    let deadline = Instant::now() + duration;
    while running.load(Ordering::SeqCst) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }
        std::thread::sleep(remaining.min(Duration::from_millis(100)));
    }
}

/// Join a worker thread, bounded by `timeout`. Internal to [`SourceWorker`].
///
/// std has no native timed join, so this polls [`JoinHandle::is_finished`]
/// until the deadline. On timeout the worker is detached (its handle is
/// dropped) with a warning — no extra threads, nothing left parked.
fn join_with_timeout(handle: JoinHandle<()>, timeout: Duration, source_name: &str) {
    let deadline = Instant::now() + timeout;
    while !handle.is_finished() {
        if Instant::now() >= deadline {
            log::warn!("[{source_name}] Worker thread did not stop within {timeout:?}; detaching");
            return; // dropping the handle detaches the thread
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    if handle.join().is_err() {
        log::warn!("[{source_name}] Worker thread panicked during shutdown");
    }
}

/// Adapter that connects the Source architecture (produces bytes) with SourceMapper and InputHandler
///
/// This adapter implements the clean architecture flow:
/// ```text
/// Source → bytes → SourceCallback::on_data() → SourceMapper → Events → InputHandler
/// ```
///
/// The adapter receives raw bytes from sources, uses the mapper to parse them into Events,
/// and delivers the Events to the InputHandler for processing.
#[derive(Debug)]
pub struct SourceCallbackAdapter {
    mapper: Arc<Mutex<Box<dyn SourceMapper>>>,
    handler: Arc<Mutex<InputHandler>>,
}

impl SourceCallbackAdapter {
    pub fn new(
        mapper: Arc<Mutex<Box<dyn SourceMapper>>>,
        handler: Arc<Mutex<InputHandler>>,
    ) -> Self {
        Self { mapper, handler }
    }
}

impl SourceCallback for SourceCallbackAdapter {
    fn on_data(&self, data: &[u8]) -> Result<(), EventFluxError> {
        // Transform bytes → Events via mapper
        // Mapper can now report parsing errors instead of silently dropping data
        let events = self.mapper.lock().unwrap().map(data)?;

        // Deliver Events to InputHandler
        for event in events.iter() {
            self.handler
                .lock()
                .unwrap()
                .send_single_event(event.clone())
                .map_err(|e| {
                    log::error!("[SourceCallbackAdapter] Failed to send event: {}", e);
                    EventFluxError::app_runtime(format!("Failed to send event: {}", e))
                })?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    #[test]
    fn test_join_with_timeout_completes_promptly() {
        let finished = Arc::new(AtomicBool::new(false));
        let finished_clone = Arc::clone(&finished);
        let handle = std::thread::spawn(move || {
            finished_clone.store(true, Ordering::SeqCst);
        });

        let start = Instant::now();
        join_with_timeout(handle, Duration::from_secs(5), "TestSource");

        assert!(finished.load(Ordering::SeqCst), "worker must have finished");
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "join must return promptly for a fast worker, not wait for the timeout"
        );
    }

    #[test]
    fn test_join_with_timeout_detaches_slow_worker() {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = Arc::clone(&running);
        let handle = std::thread::spawn(move || {
            while running_clone.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        let start = Instant::now();
        join_with_timeout(handle, Duration::from_millis(100), "TestSource");
        let elapsed = start.elapsed();

        assert!(
            elapsed >= Duration::from_millis(100),
            "join must wait the full grace period before detaching"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "join must not block past the grace period"
        );

        // Let the detached worker exit so the test doesn't leak a spinning thread
        running.store(false, Ordering::SeqCst);
    }

    #[test]
    fn test_join_with_timeout_survives_worker_panic() {
        let handle = std::thread::spawn(|| {
            panic!("intentional test panic");
        });

        // Must log and return, not propagate the panic
        join_with_timeout(handle, Duration::from_secs(5), "TestSource");
    }

    // =========================================================================
    // SourceWorker Tests
    // =========================================================================

    use std::sync::atomic::AtomicU64;

    #[test]
    fn test_source_worker_stop_joins_thread() {
        let ticks = Arc::new(AtomicU64::new(0));
        let ticks_clone = Arc::clone(&ticks);

        let mut worker = SourceWorker::new("TestSource");
        worker.start(move |running| {
            while running.load(Ordering::SeqCst) {
                ticks_clone.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(5));
            }
        });

        std::thread::sleep(Duration::from_millis(30));
        worker.stop();

        // stop() joined the worker — the tick count must be frozen
        let at_stop = ticks.load(Ordering::SeqCst);
        assert!(at_stop > 0, "worker should have run");
        std::thread::sleep(Duration::from_millis(30));
        assert_eq!(
            ticks.load(Ordering::SeqCst),
            at_stop,
            "worker must not run after stop() returns"
        );
    }

    #[test]
    fn test_source_worker_stop_is_idempotent() {
        let mut worker = SourceWorker::new("TestSource");
        worker.stop(); // Without start — no-op
        worker.start(|running| while running.load(Ordering::SeqCst) {});
        worker.stop();
        worker.stop(); // Handle already taken — no-op
    }

    #[test]
    fn test_source_worker_drop_stops_thread() {
        let exited = Arc::new(AtomicBool::new(false));
        let exited_clone = Arc::clone(&exited);

        {
            let mut worker = SourceWorker::new("TestSource");
            worker.start(move |running| {
                while running.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(5));
                }
                exited_clone.store(true, Ordering::SeqCst);
            });
            // Dropped without stop()
        }

        assert!(
            exited.load(Ordering::SeqCst),
            "drop must signal and join the worker (RAII)"
        );
    }

    #[test]
    fn test_source_worker_clone_is_unstarted() {
        let mut worker = SourceWorker::new("TestSource");
        worker.start(|running| while running.load(Ordering::SeqCst) {});

        let clone = worker.clone();
        assert!(
            clone.handle.is_none(),
            "a cloned worker must start unstarted"
        );
        assert!(
            !clone.running.load(Ordering::SeqCst),
            "a cloned worker must not be running"
        );

        worker.stop();
    }
}
