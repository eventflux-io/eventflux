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

#[cfg(feature = "file")]
pub mod file_source;
#[cfg(feature = "http")]
pub mod http_source;
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

/// Receives data from a source.
///
/// Sources read raw bytes from outside systems and pass them to this callback.
/// The callback turns the bytes into events and hands them to the InputHandler.
pub trait SourceCallback: Debug + Send + Sync {
    /// Called when the source has new data.
    ///
    /// # Arguments
    /// * `data` - Raw bytes from the outside system, such as JSON, CSV, or binary
    ///
    /// # Returns
    /// * `Ok(())` - The data was processed
    /// * `Err(EventFluxError)` - Processing failed
    fn on_data(&self, data: &[u8]) -> Result<(), EventFluxError>;
}

/// A connector that reads data from an outside system into a stream.
pub trait Source: Debug + Send + Sync {
    /// Starts the source and gives it the callback for its data.
    ///
    /// Sources read from outside systems and pass raw bytes to the callback.
    /// The callback parses them with a SourceMapper and delivers the events.
    ///
    /// # Architecture
    /// ```text
    /// Source::read() → bytes → SourceCallback::on_data() → SourceMapper → Events → InputHandler
    /// ```
    fn start(&mut self, callback: Arc<dyn SourceCallback>);

    /// Stops the source.
    ///
    /// It must block until the source has stopped, so no callback fires after
    /// it returns, and it must be safe to call more than once. Do not write
    /// this by hand. Hold a [`SourceWorker`] and call [`SourceWorker::stop`],
    /// which gives both.
    fn stop(&mut self);

    fn clone_box(&self) -> Box<dyn Source>;

    /// Phase 2 check that outside systems can be reached and are set up right.
    ///
    /// Called during app startup (Phase 2). The app should NOT start when a
    /// transport is not ready, so problems show up early.
    ///
    /// # Default Implementation
    ///
    /// Returns Ok. Sources with no outside system to check can keep it.
    /// Sources that depend on one (Kafka, HTTP, and so on) MUST override it.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - The outside system can be reached and is set up right
    /// * `Err(EventFluxError)` - The check failed and the app should not start
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
        Ok(())
    }

    /// Sets the DLQ junction that failed events are sent to.
    ///
    /// The factory builds a source before the DLQ junction exists, so the
    /// junction is set afterwards. `EventFluxAppRuntime` calls this when it
    /// attaches the source to its stream.
    ///
    /// Sources that support the DLQ strategy must override this. The default
    /// returns a configuration error, so a DLQ set on a source that cannot use
    /// one fails at attach time instead of silently dropping failed events.
    ///
    /// # Arguments
    /// * `stream_name` - Name of the stream this source feeds. DLQ events
    ///   report it as `streamName`
    /// * `_junction` - The InputHandler for the DLQ stream
    fn set_error_dlq_junction(
        &mut self,
        stream_name: &str,
        _junction: Arc<Mutex<InputHandler>>,
    ) -> Result<(), EventFluxError> {
        Err(EventFluxError::configuration_with_key(
            format!("The source for stream '{stream_name}' does not support DLQ routing"),
            crate::core::error::DLQ_STREAM_KEY,
        ))
    }
}

impl Clone for Box<dyn Source> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// How long `stop()` waits for a source's worker thread before giving up on it.
pub const SOURCE_STOP_GRACE_PERIOD: Duration = Duration::from_secs(5);

/// Starts and stops a source's worker thread.
///
/// Every source that runs a thread holds one of these instead of its own
/// `running: Arc<AtomicBool>` and `JoinHandle` pair. The shutdown logic lives
/// here once so sources cannot get it wrong.
///
/// - [`start`](Self::start) sets the running flag and starts the thread
/// - [`stop`](Self::stop) clears the flag and joins the thread, waiting up to
///   [`SOURCE_STOP_GRACE_PERIOD`]. So `Source::stop()` blocks and no callback
///   fires after it returns, unless the thread missed the grace period and was
///   left running. Safe to call more than once.
/// - `Drop` calls `stop()`, so a source dropped without being stopped still
///   shuts its thread down
/// - `Clone` gives a new worker that has not started. Runtime state never
///   carries over to clones.
///
/// Worker loops that wait between rounds should use [`sleep_while_running`]
/// instead of `thread::sleep`, so a long interval does not delay shutdown.
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
    /// Running flag that the worker loop checks
    running: Arc<AtomicBool>,
    /// Worker thread handle, joined in `stop()`
    handle: Option<JoinHandle<()>>,
}

impl SourceWorker {
    /// Creates a worker that has not started. `name` appears in shutdown log
    /// messages.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            running: Arc::new(AtomicBool::new(false)),
            handle: None,
        }
    }

    /// Sets the running flag and starts the worker thread.
    ///
    /// The closure gets the running flag and must check it about every 100 ms
    /// or more often, so `stop()` returns quickly. Use [`sleep_while_running`]
    /// for waits between rounds. Call `stop()` before starting again, since a
    /// second start replaces the old handle without joining it.
    pub fn start<F>(&mut self, body: F)
    where
        F: FnOnce(Arc<AtomicBool>) + Send + 'static,
    {
        self.running.store(true, Ordering::SeqCst);
        let running = Arc::clone(&self.running);
        self.handle = Some(std::thread::spawn(move || body(running)));
    }

    /// Tells the worker to stop and joins its thread.
    ///
    /// Waits up to [`SOURCE_STOP_GRACE_PERIOD`]. A worker stuck in slow I/O is
    /// left running on its own, with a warning. Safe to call before `start()`
    /// or more than once.
    pub fn stop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            join_with_timeout(handle, SOURCE_STOP_GRACE_PERIOD, self.name);
        }
    }
}

impl Drop for SourceWorker {
    fn drop(&mut self) {
        // A source dropped without stop() still shuts its worker down
        self.stop();
    }
}

impl Clone for SourceWorker {
    fn clone(&self) -> Self {
        // The running flag and thread handle never carry over to clones
        Self::new(self.name)
    }
}

/// Sleeps for `duration`, waking early when `running` is cleared.
///
/// Worker loops use this instead of `thread::sleep` for waits between rounds,
/// so a long interval, such as a 60s timer, never delays `stop()`. It sleeps in
/// slices of at most 100 ms and checks the flag between slices.
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

/// Joins a worker thread, waiting at most `timeout`. Used only by
/// [`SourceWorker`].
///
/// std has no join with a timeout, so this checks [`JoinHandle::is_finished`]
/// every 10 ms until the deadline. On timeout it logs a warning and drops the
/// handle, which leaves the thread running on its own. No extra thread is
/// started. A worker that panicked is logged, not passed on.
fn join_with_timeout(handle: JoinHandle<()>, timeout: Duration, source_name: &str) {
    let deadline = Instant::now() + timeout;
    while !handle.is_finished() {
        if Instant::now() >= deadline {
            log::warn!("[{source_name}] Worker thread did not stop within {timeout:?}; detaching");
            return; // dropping the handle leaves the thread running on its own
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    if handle.join().is_err() {
        log::warn!("[{source_name}] Worker thread panicked during shutdown");
    }
}

/// Connects a source, which produces bytes, to a SourceMapper and an InputHandler.
///
/// ```text
/// Source → bytes → SourceCallback::on_data() → SourceMapper → Events → InputHandler
/// ```
///
/// It takes raw bytes from the source, parses them into events with the
/// mapper, and sends each event to the InputHandler.
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
        // A parse error is returned to the source instead of dropping the data.
        // A send error stops at the failing event, so earlier events in the
        // batch are already delivered.
        let events = self.mapper.lock().unwrap().map(data)?;

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

        // Let the abandoned worker exit so the test does not leave a thread spinning
        running.store(false, Ordering::SeqCst);
    }

    #[test]
    fn test_join_with_timeout_survives_worker_panic() {
        let handle = std::thread::spawn(|| {
            panic!("intentional test panic");
        });

        // Must log and return, not pass the panic on
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

        // stop() joined the worker, so the tick count must not change now
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
        worker.stop(); // before start, does nothing
        worker.start(|running| while running.load(Ordering::SeqCst) {});
        worker.stop();
        worker.stop(); // handle already taken, does nothing
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
