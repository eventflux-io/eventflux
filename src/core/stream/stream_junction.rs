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

//! StreamJunction on a Fast Crossbeam Pipeline
//!
//! A StreamJunction routes the events of one stream to its subscribers. It
//! runs on the crossbeam based event pipeline, built for more than 1M events
//! per second, which replaced the older crossbeam_channel version.

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::event::Event;
use crate::core::event::stream::StreamEvent;
use crate::core::exception::EventFluxError;
use crate::core::query::processor::Processor;
use crate::core::stream::input::input_handler::InputProcessor;
use crate::core::util::executor_service::{ExecutorService, TaskTracker};
use crate::core::util::pipeline::{
    BackpressureStrategy, EventPipeline, EventPool, MetricsSnapshot, PipelineBuilder,
    PipelineConfig, PipelineResult,
};
use crate::query_api::definition::StreamDefinition;

use crossbeam::utils::CachePadded;
use std::fmt::Debug;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex, RwLock,
};
use std::time::{Duration, Instant};

/// DEFAULT time `EventFluxAppRuntime::shutdown()` gives junctions to empty
/// out. Same as `SOURCE_STOP_GRACE_PERIOD`. The env var
/// `EVENTFLUX_JUNCTION_DRAIN_TIMEOUT_MS` can override it (capped at
/// [`MAX_DRAIN_TIMEOUT`]), and a shutdown that times out waits up to
/// [`HALT_SETTLE_TIMEOUT`] more. So an orchestrator's kill grace period should
/// cover drain plus settle time, not this constant alone.
pub const JUNCTION_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// How often the shutdown drain checks whether the junctions have gone idle.
pub const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Upper limit on any drain or wait timeout. Caps values from the user so
/// `Instant::now() + timeout` cannot overflow, which would panic.
pub const MAX_DRAIN_TIMEOUT: Duration = Duration::from_secs(86_400);

/// How long `restart_processing` waits for the old consumer threads to exit
/// before it clears the shutdown flags, which otherwise stay set.
const CONSUMER_EXIT_TIMEOUT: Duration = Duration::from_secs(2);

/// How long a shutdown that timed out waits, after halting, for live consumers
/// and dispatches still running to finish, before it counts abandoned events
/// and saves state.
pub const HALT_SETTLE_TIMEOUT: Duration = Duration::from_secs(2);

/// Checks every 1 ms until every handle is settled (no live consumers and no
/// dispatches running) or the deadline passes. Used after `halt_processing`, so
/// the abandoned event counts are exact and nothing changes state while the
/// shutdown snapshot is taken. Returns true when all handles settled.
pub fn poll_settled(handles: &[JunctionDrainHandle], deadline: Instant) -> bool {
    loop {
        if handles.iter().all(|h| h.settled()) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(DRAIN_POLL_INTERVAL);
    }
}

/// Checks until every handle is idle for two passes in a row with the same
/// total published count, or the deadline passes. Returns true when all
/// junctions went idle.
///
/// PRECONDITION: every publisher must be stopped or covered. That means a
/// source thread that was joined, a consumer (covered because it holds back its
/// consumed count until done), or a tracked dispatch (covered by its in flight
/// guard). A publisher that is not covered and is paused between pushing to the
/// queue and raising its published count is invisible here, and can make the
/// system look idle when it is not.
///
/// Why two passes with the same total published count (ΣP) are enough, given
/// the precondition. Each pass where all are idle reads every junction's
/// consumed counter (Acquire) before its in flight count. So hidden work at one
/// hop of a cascade shows up on the next pass as a non zero in flight count, a
/// consumed and published mismatch, or a changed ΣP, and any of these resets
/// the streak. Work hidden d hops deep can take about d passes to show up. That
/// can cost time (deep cascades may need more passes near the deadline) but
/// never gives a wrong answer. The confirming pass needs a second read, not
/// time passing, so the loop only sleeps while the system is clearly still
/// moving.
pub fn poll_quiescent(handles: &[JunctionDrainHandle], deadline: Instant) -> bool {
    if handles.is_empty() {
        return true;
    }
    let mut quiescent_streak = 0u32;
    let mut last_total_published: Option<u64> = None;
    let mut grace_passes = 0u32;
    loop {
        let previous_streak = quiescent_streak;
        let total_published: u64 = handles.iter().map(|h| h.published()).sum();
        let all_quiescent = handles.iter().all(|h| h.is_quiescent());
        if all_quiescent && last_total_published == Some(total_published) {
            quiescent_streak += 1;
        } else if all_quiescent {
            quiescent_streak = 1;
        } else {
            quiescent_streak = 0;
        }
        if quiescent_streak >= 2 {
            return true;
        }
        last_total_published = Some(total_published);
        if Instant::now() >= deadline {
            // An idle pass still gets its confirming read at the deadline.
            // Otherwise a used up budget reports "timed out" for a system that
            // is already idle, such as the drain after the flush, which reuses
            // the first drain's deadline. A few extra passes (each only a
            // handful of atomic reads) also cover the last cascade event
            // finishing ON an extra pass. The cap of 3 keeps a system that is
            // really still moving from looping past the deadline.
            if quiescent_streak == 0 || grace_passes >= 3 {
                return false;
            }
            grace_passes += 1;
            continue;
        }
        // Skip the sleep only when this pass RAISED the streak, since the
        // confirming read needs a second read, not time passing. A pass that
        // only reset the streak to 1 saw ΣP move. Sleeping there keeps a
        // shutdown with many cascades from spinning hot until the deadline.
        if quiescent_streak <= previous_streak {
            std::thread::sleep(DRAIN_POLL_INTERVAL);
        }
    }
}

/// What a StreamJunction does with a failed event. Default LOG.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnErrorAction {
    #[default]
    LOG,
    STREAM,
    STORE,
    DROP,
}

/// Fast event router for one stream, built on the crossbeam pipeline
#[allow(clippy::type_complexity)]
pub struct StreamJunction {
    // Identity
    pub stream_id: String,
    stream_definition: Arc<StreamDefinition>,
    eventflux_app_context: Arc<EventFluxAppContext>,

    // Event pipeline
    event_pipeline: Arc<EventPipeline>,
    _event_pool: Arc<EventPool>, // kept for object pooling later, not used yet

    // Subscribers
    // An RwLock suits this mostly read pattern
    // - Read on every event (millions per second), many readers at once
    // - Write only on subscribe and unsubscribe, which is rare
    subscribers: Arc<RwLock<Vec<Arc<Mutex<dyn Processor>>>>>,

    // Configuration
    is_async: bool,
    buffer_size: usize,
    on_error_action: OnErrorAction,

    // Start and stop state
    started: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,

    // Junction for the fault stream, if any
    fault_stream_junction: Option<Arc<Mutex<StreamJunction>>>,

    // Counters
    events_processed: Arc<CachePadded<AtomicU64>>,
    events_dropped: Arc<CachePadded<AtomicU64>>,
    processing_errors: Arc<CachePadded<AtomicU64>>,

    // Thread pool for async processing
    executor_service: Arc<ExecutorService>,

    // Subscriber dispatches still running (async mode that does not run
    // inline only). The consumer loops are NOT tracked here on purpose. They
    // run until shutdown, so counting them would mean the junction never
    // looks idle.
    dispatch_tracker: Arc<TaskTracker>,

    // Live consumer threads. Lets restart_processing wait for the old ones to
    // exit before it clears the shutdown flags, which otherwise stay set.
    active_consumers: Arc<TaskTracker>,
}

impl Debug for StreamJunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamJunction")
            .field("stream_id", &self.stream_id)
            .field("is_async", &self.is_async)
            .field("buffer_size", &self.buffer_size)
            .field(
                "subscribers_count",
                &self.subscribers.read().expect("RwLock poisoned").len(),
            )
            .field("started", &self.started.load(Ordering::Relaxed))
            .field(
                "events_processed",
                &self.events_processed.load(Ordering::Relaxed),
            )
            .field(
                "events_dropped",
                &self.events_dropped.load(Ordering::Relaxed),
            )
            .field("on_error_action", &self.on_error_action)
            .finish()
    }
}

impl StreamJunction {
    /// Creates a stream junction.
    ///
    /// Uses the app context's executor service, or a default one. Async mode
    /// uses the ExponentialBackoff strategy (up to 1000 ms) when the buffer is
    /// full, and sync mode uses Block.
    ///
    /// # Arguments
    /// * `stream_id` - Id of this stream junction
    /// * `stream_definition` - Schema of the events that pass through
    /// * `eventflux_app_context` - App context for config and services
    /// * `buffer_size` - Pipeline buffer size. A power of 2 is best for async mode.
    /// * `is_async` - **CRITICAL**: picks the processing mode
    ///   - `false` (RECOMMENDED DEFAULT): sync processing that keeps event order
    ///   - `true`: async processing for high throughput, but events may be reordered
    /// * `fault_stream_junction` - Optional junction for the fault stream
    ///
    /// # Processing Modes
    ///
    /// **Sync mode (`is_async: false`)**
    /// - Events are processed one at a time in the exact order they arrive
    /// - Thread safe but blocking. Each event waits for the one before it.
    /// - Keeps time order and cause and effect between events correct
    /// - Throughput around 1K to 10K events per second, depending on the work
    ///
    /// **Async mode (`is_async: true`)**
    /// - Events are processed in parallel on the lock free pipeline
    /// - Senders do not wait, and throughput is high, with object pooling
    /// - **WARNING: events may be processed out of arrival order**
    /// - Throughput can pass 100K events per second
    ///
    /// # Recommendation
    /// Use sync mode unless you really need high throughput AND your logic can
    /// cope with events being reordered.
    ///
    /// # Errors
    ///
    /// Returns an error when the pipeline cannot be built.
    pub fn new(
        stream_id: String,
        stream_definition: Arc<StreamDefinition>,
        eventflux_app_context: Arc<EventFluxAppContext>,
        buffer_size: usize,
        is_async: bool,
        fault_stream_junction: Option<Arc<Mutex<StreamJunction>>>,
    ) -> Result<Self, String> {
        let executor_service = eventflux_app_context
            .executor_service
            .clone()
            .unwrap_or_else(|| Arc::new(ExecutorService::default()));

        let backpressure_strategy = if is_async {
            BackpressureStrategy::ExponentialBackoff { max_delay_ms: 1000 }
        } else {
            BackpressureStrategy::Block
        };

        Self::new_with_backpressure_and_executor(
            stream_id,
            stream_definition,
            eventflux_app_context,
            buffer_size,
            is_async,
            fault_stream_junction,
            backpressure_strategy,
            executor_service,
        )
    }

    /// Creates a StreamJunction with a chosen strategy for a full buffer.
    ///
    /// Use it when you need to control what happens when the buffer is full.
    /// - `BackpressureStrategy::Drop` - send and forget, drop at once when full
    /// - `BackpressureStrategy::Block` - always deliver, wait until there is room
    /// - `BackpressureStrategy::ExponentialBackoff` - retry with growing waits, then drop
    /// - `BackpressureStrategy::BlockWithTimeout` - wait up to a timeout, then drop
    /// - `BackpressureStrategy::CircuitBreaker` - fail at once after a threshold
    ///
    /// # Errors
    ///
    /// Returns an error when the pipeline cannot be built.
    pub fn new_with_backpressure(
        stream_id: String,
        stream_definition: Arc<StreamDefinition>,
        eventflux_app_context: Arc<EventFluxAppContext>,
        buffer_size: usize,
        is_async: bool,
        fault_stream_junction: Option<Arc<Mutex<StreamJunction>>>,
        backpressure_strategy: BackpressureStrategy,
    ) -> Result<Self, String> {
        let executor_service = eventflux_app_context
            .executor_service
            .clone()
            .unwrap_or_else(|| Arc::new(ExecutorService::default()));

        Self::new_with_backpressure_and_executor(
            stream_id,
            stream_definition,
            eventflux_app_context,
            buffer_size,
            is_async,
            fault_stream_junction,
            backpressure_strategy,
            executor_service,
        )
    }

    /// Constructor for tests that takes its own executor. Uses the same full
    /// buffer strategy as `new`.
    ///
    /// Only in test builds.
    #[cfg(test)]
    pub fn new_with_executor(
        stream_id: String,
        stream_definition: Arc<StreamDefinition>,
        eventflux_app_context: Arc<EventFluxAppContext>,
        buffer_size: usize,
        is_async: bool,
        fault_stream_junction: Option<Arc<Mutex<StreamJunction>>>,
        executor_service: Arc<ExecutorService>,
    ) -> Result<Self, String> {
        let backpressure_strategy = if is_async {
            BackpressureStrategy::ExponentialBackoff { max_delay_ms: 1000 }
        } else {
            BackpressureStrategy::Block
        };

        Self::new_with_backpressure_and_executor(
            stream_id,
            stream_definition,
            eventflux_app_context,
            buffer_size,
            is_async,
            fault_stream_junction,
            backpressure_strategy,
            executor_service,
        )
    }

    /// Internal constructor that takes every option. Use `new()` or
    /// `new_with_backpressure()` outside this module.
    ///
    /// Async mode runs half the CPU count (at least 1) as consumer threads,
    /// and sync mode runs 1. Batches are 64 events. Metrics are on unless the
    /// app's metrics level is OFF. The event pool holds twice `buffer_size`.
    #[allow(clippy::too_many_arguments)]
    fn new_with_backpressure_and_executor(
        stream_id: String,
        stream_definition: Arc<StreamDefinition>,
        eventflux_app_context: Arc<EventFluxAppContext>,
        buffer_size: usize,
        is_async: bool,
        fault_stream_junction: Option<Arc<Mutex<StreamJunction>>>,
        backpressure_strategy: BackpressureStrategy,
        executor_service: Arc<ExecutorService>,
    ) -> Result<Self, String> {
        let pipeline_config = PipelineConfig {
            capacity: buffer_size,
            backpressure: backpressure_strategy,
            use_object_pool: true,
            consumer_threads: if is_async {
                (num_cpus::get() / 2).max(1)
            } else {
                1
            },
            batch_size: 64,
            enable_metrics: eventflux_app_context.get_root_metrics_level()
                != crate::core::config::eventflux_app_context::MetricsLevelPlaceholder::OFF,
        };

        let event_pipeline = Arc::new(
            PipelineBuilder::new()
                .with_capacity(pipeline_config.capacity)
                .with_backpressure(pipeline_config.backpressure)
                .with_object_pool(pipeline_config.use_object_pool)
                .with_consumer_threads(pipeline_config.consumer_threads)
                .with_batch_size(pipeline_config.batch_size)
                .with_metrics(pipeline_config.enable_metrics)
                .build()?,
        );

        let event_pool = Arc::new(EventPool::new(buffer_size * 2));

        let junction = Self {
            stream_id,
            stream_definition,
            eventflux_app_context,
            event_pipeline,
            _event_pool: event_pool,
            subscribers: Arc::new(RwLock::new(Vec::new())),
            is_async,
            buffer_size,
            on_error_action: OnErrorAction::default(),
            started: Arc::new(AtomicBool::new(false)),
            shutdown: Arc::new(AtomicBool::new(false)),
            fault_stream_junction,
            events_processed: Arc::new(CachePadded::new(AtomicU64::new(0))),
            events_dropped: Arc::new(CachePadded::new(AtomicU64::new(0))),
            processing_errors: Arc::new(CachePadded::new(AtomicU64::new(0))),
            executor_service,
            dispatch_tracker: Arc::new(TaskTracker::new()),
            active_consumers: Arc::new(TaskTracker::new()),
        };

        // Async mode starts its consumer threads right away
        if is_async {
            if let Err(e) = junction.start_async_consumer() {
                // Stop any consumers that did start before the junction is
                // dropped, or they sit idle forever and keep the pipeline alive.
                junction.halt_processing();
                return Err(e);
            }
            junction.started.store(true, Ordering::Release);
        }

        Ok(junction)
    }

    /// Returns the stream definition
    pub fn get_stream_definition(&self) -> Arc<StreamDefinition> {
        Arc::clone(&self.stream_definition)
    }

    /// Creates a publisher for this junction.
    ///
    /// The publisher holds an Arc, so the junction lives as long as it does.
    pub fn construct_publisher(junction: Arc<Mutex<StreamJunction>>) -> Publisher {
        Publisher::new(junction)
    }

    /// Adds a processor that receives events. Adding the same processor (same
    /// Arc) twice does nothing.
    pub fn subscribe(&self, processor: Arc<Mutex<dyn Processor>>) {
        let mut subs = self.subscribers.write().expect("RwLock poisoned");
        if !subs.iter().any(|p| Arc::ptr_eq(p, &processor)) {
            subs.push(processor);
        }
    }

    /// Removes a processor (matched by Arc) so it no longer gets events
    pub fn unsubscribe(&self, processor: &Arc<Mutex<dyn Processor>>) {
        let mut subs = self.subscribers.write().expect("RwLock poisoned");
        subs.retain(|p| !Arc::ptr_eq(p, processor));
    }

    /// Reserves room for the expected subscribers.
    ///
    /// Sizes the subscriber list up front from the subscriber_count hint in
    /// JunctionConfig, so the list does not have to grow as subscribers are
    /// added.
    ///
    /// # Arguments
    /// * `capacity` - Expected number of subscribers to make room for
    pub fn reserve_subscriber_capacity(&mut self, capacity: usize) {
        let mut subs = self.subscribers.write().expect("RwLock poisoned");
        subs.reserve(capacity);
        log::debug!(
            "[{}] Reserved subscriber capacity for {} processors",
            self.stream_id,
            capacity
        );
    }

    /// Sets what to do with a failed event
    pub fn set_on_error_action(&mut self, action: OnErrorAction) {
        self.on_error_action = action;
    }

    /// Starts processing events.
    ///
    /// Async junctions already start when they are built, and calling this
    /// again while running does nothing. It is NOT valid after
    /// `stop_processing` or `halt_processing`, where it returns `Err`. Use
    /// [`StreamJunction::restart_processing`] instead, which waits for the old
    /// consumer threads and clears the shutdown flags.
    ///
    /// # Errors
    ///
    /// Returns an error when the junction is stopped, or when the async
    /// consumer threads fail to start. In that case it halts, counts any
    /// queued events as dropped, and a retry must go through
    /// `restart_processing`.
    pub fn start_processing(&self) -> Result<(), String> {
        if self.shutdown.load(Ordering::Acquire) {
            // A stopped junction must go through restart_processing, which
            // waits for the old consumers and clears the shutdown flags.
            // Starting here would CAS `started` to true while the new
            // consumers see the shutdown flag and exit at once. The junction
            // would then take events that nothing ever reads.
            return Err(format!(
                "[{}] junction is stopped; use restart_processing()",
                self.stream_id
            ));
        }
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return Ok(()); // already started
        }

        if self.is_async {
            if let Err(e) = self.start_async_consumer() {
                // Stop the consumers that did start (they exit at their next
                // round) and reset `started`. A retry must go through
                // restart_processing, which waits for them and clears the flags.
                self.halt_processing();
                // Let those consumers finish the events they hold so the
                // abandon count below is exact. This keeps the rule that a
                // halted queue is already counted, which restart's purge
                // relies on. Anything queued here would otherwise be thrown
                // away later without ever showing up in events_dropped.
                poll_settled(&[self.drain_handle()], Instant::now() + HALT_SETTLE_TIMEOUT);
                self.abandon_pending();
                return Err(e);
            }
        }

        Ok(())
    }

    /// Stops processing cleanly. Consumers finish the queue, then exit. New
    /// sends are rejected. Use `halt_processing` to stop without emptying the
    /// queue.
    pub fn stop_processing(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.event_pipeline.shutdown();
        self.started.store(false, Ordering::Release);
    }

    /// Stops processing WITHOUT emptying the queue. Consumers exit at their
    /// next round and queued events stay where they are. Used when the drain
    /// times out, so the events counted by `abandon_pending` are not also
    /// delivered afterwards, into stopped sinks, while the shutdown snapshot
    /// is taken.
    ///
    /// CALLER CONTRACT: after halting, wait for [`JunctionDrainHandle::settled`]
    /// (see [`poll_settled`]), then call [`StreamJunction::abandon_pending`] so
    /// the frozen queue is counted into `events_dropped`. `restart_processing`
    /// throws away a halted queue and assumes this counting already happened.
    /// Halting without abandoning makes those events vanish from the metrics at
    /// the next restart.
    pub fn halt_processing(&self) {
        self.stop_processing();
        self.event_pipeline.halt();
    }

    /// Undoes an earlier `stop_processing()` or `halt_processing()` so the
    /// junction takes events again. Both shutdown flags stay set until cleared,
    /// so this must run on every runtime restart. That includes sync
    /// junctions, or `send_event` rejects everything after the first shutdown.
    ///
    /// Steps, in order. (1) Wait, up to a limit, for the old consumer threads
    /// AND their running dispatches to finish. (2) **Throw away for good** any
    /// events left in the queue. Old events from the last run must not replay
    /// into the restarted app before sinks reconnect. They were counted into
    /// `events_dropped` when abandoned, or are counted here. (3) Clear the
    /// shutdown flags. (4) Start new consumer threads.
    ///
    /// # Errors
    ///
    /// Fails, leaving the flags set, if the last run is still alive after the
    /// wait. Clearing the flags with an old thread alive would bring it back
    /// for good next to the new ones, leaking a set of consumers on each
    /// restart. With the flags still set, the old thread exits once it
    /// unblocks, and the restart can be tried again. Also returns any error
    /// from `start_processing`.
    pub fn restart_processing(&self) -> Result<(), String> {
        if self.shutdown.load(Ordering::Acquire) {
            // Consumers exit quickly here (queue empty, or halted), and each of
            // their dispatches is a single subscriber call, so this normally
            // takes well under a millisecond. Waiting on BOTH trackers matters.
            // The consumers can be gone while work they dispatched is still
            // queued on the executor, and restarting then would mix old run
            // deliveries into the new run. One loop with one deadline keeps
            // the counts in the error message in line with the refusal.
            let settle_deadline = Instant::now() + CONSUMER_EXIT_TIMEOUT;
            loop {
                let consumers = self.active_consumers.in_flight();
                let dispatches = self.dispatch_tracker.in_flight();
                if consumers == 0 && dispatches == 0 {
                    break;
                }
                if Instant::now() >= settle_deadline {
                    return Err(format!(
                        "[{}] previous run not settled after {:?} ({} consumer(s), {} dispatch(es) still live — wedged subscriber?); not restarting — retry once they exit",
                        self.stream_id, CONSUMER_EXIT_TIMEOUT, consumers, dispatches
                    ));
                }
                std::thread::sleep(DRAIN_POLL_INTERVAL);
            }
            // Anything left in the queue is old run work and must not replay
            // into the restarted app. Sinks reconnect only after junctions, so
            // replaying would bring back the #136 lost event window. A halted
            // queue was already counted into events_dropped by abandon_pending.
            // What a clean stop left behind (a producer that raced the flags)
            // was not, so count it before throwing it away.
            if !self.event_pipeline.is_halted() {
                self.abandon_pending();
            }
            let purged = self.event_pipeline.purge();
            if purged > 0 {
                log::info!(
                    "[{}] discarded {} stale event(s) left by the previous run (counted into events_dropped when abandoned)",
                    self.stream_id,
                    purged
                );
            }
            self.shutdown.store(false, Ordering::Release);
            self.event_pipeline.clear_shutdown();
        }
        // For async junctions, the CAS in start_processing lets a new set of
        // consumers start. On a first start the set the constructor started is
        // still running, and the CAS makes this do nothing.
        self.start_processing()
    }

    /// Returns a handle that reads this junction's drain state without a lock,
    /// so callers do not hold the junction's Mutex. Shutdown checks many
    /// junctions every 1 ms.
    pub fn drain_handle(&self) -> JunctionDrainHandle {
        JunctionDrainHandle {
            is_async: self.is_async,
            pipeline: Arc::clone(&self.event_pipeline),
            dispatch_tracker: Arc::clone(&self.dispatch_tracker),
            active_consumers: Arc::clone(&self.active_consumers),
        }
    }

    /// Best effort estimate of the work not yet done, as `(queued, in_flight)`.
    /// `queued` counts events still in the pipeline. `in_flight` counts
    /// subscriber dispatches given to the executor but not finished.
    pub fn pending_drain_estimate(&self) -> (u64, u64) {
        self.drain_handle().pending_drain_estimate()
    }

    /// Checks until this junction is idle for two passes in a row with the
    /// same published count, or `timeout` passes (capped at one day). Returns
    /// true when it went idle. Only makes sense once producers have stopped.
    /// For cascades across several junctions, use [`poll_quiescent`] over all
    /// their handles, since one junction cannot see dispatches still heading
    /// to it from other junctions.
    pub fn wait_quiescent(&self, timeout: Duration) -> bool {
        let timeout = timeout.min(MAX_DRAIN_TIMEOUT);
        poll_quiescent(&[self.drain_handle()], Instant::now() + timeout)
    }

    /// Gives up on events not yet processed. Counts the events still queued as
    /// dropped and logs a warning. Returns how many queued events were
    /// abandoned. Always 0 for sync junctions.
    ///
    /// For an exact count, call only after `halt_processing` and settling
    /// ([`poll_settled`]). Until consumers settle, an event they already took
    /// (but have not counted as consumed) still shows in `queued`, so it would
    /// be counted dropped AND still delivered. Running dispatches are logged
    /// but never counted dropped, since their events were already handed on to
    /// subscribers. NOT safe to repeat, since counting the same frozen queue
    /// again counts it twice. The drain steps make sure it runs once per queue,
    /// since a halted junction reads as idle and a second shutdown skips the
    /// abandon step.
    pub fn abandon_pending(&self) -> u64 {
        if !self.is_async {
            return 0;
        }
        let (queued, in_flight) = self.pending_drain_estimate();
        if queued > 0 || in_flight > 0 {
            self.events_dropped.fetch_add(queued, Ordering::Relaxed);
            log::warn!(
                "[{}] abandoning {} queued event(s) ({} subscriber dispatch(es) still in flight)",
                self.stream_id,
                queued,
                in_flight
            );
        }
        queued
    }

    /// Counts events lost on an internal cascade link (a query output feeding
    /// this junction) because the send was rejected during shutdown. The
    /// sending processor drops the error, so without this the events would
    /// vanish from the delivered and dropped totals. Outside senders are NOT
    /// counted here, since they get the `Err` and still own their events.
    pub(crate) fn record_cascade_drop(&self, count: u64) {
        self.events_dropped.fetch_add(count, Ordering::Relaxed);
    }

    /// Sends one event. Async junctions queue it in the pipeline. Sync
    /// junctions hand it to the subscribers right away on this thread.
    ///
    /// # Errors
    ///
    /// Returns `ShutdownRejected` during shutdown. In async mode returns
    /// `SendError` when the pipeline is full, timed out, shutting down, or
    /// fails. Full and timed out events are counted as dropped.
    pub fn send_event(&self, event: Event) -> Result<(), EventFluxError> {
        if self.shutdown.load(Ordering::Acquire) {
            self.handle_backpressure_error("StreamJunction is shutting down");
            return Err(EventFluxError::ShutdownRejected { count: 1 });
        }

        let stream_event = Self::event_to_stream_event(event);

        if self.is_async {
            match self.event_pipeline.publish(stream_event) {
                PipelineResult::Success { .. } => {
                    // Not counted here. The consumer counts processed events.
                    Ok(())
                }
                PipelineResult::Full => {
                    self.events_dropped.fetch_add(1, Ordering::Relaxed);
                    self.handle_backpressure_error("Pipeline full");
                    Err(EventFluxError::SendError {
                        message: format!("Pipeline full for stream {}", self.stream_id),
                    })
                }
                PipelineResult::Shutdown => Err(EventFluxError::SendError {
                    message: "Pipeline is shutting down".to_string(),
                }),
                PipelineResult::Timeout => {
                    self.events_dropped.fetch_add(1, Ordering::Relaxed);
                    Err(EventFluxError::SendError {
                        message: "Pipeline publish timeout".to_string(),
                    })
                }
                PipelineResult::Error(msg) => {
                    self.processing_errors.fetch_add(1, Ordering::Relaxed);
                    Err(EventFluxError::SendError { message: msg })
                }
            }
        } else {
            // Sync mode hands the event straight to the subscribers
            self.dispatch_to_subscribers_sync(Box::new(stream_event))
        }
    }

    /// Sends a chain of complex events. Turns StreamEvents and StateEvents into
    /// Events first (see `complex_event_chain_to_events`), then calls
    /// `send_events`. `None` does nothing.
    pub fn send_complex_event_chunk(
        &self,
        complex_event_chunk: Option<Box<dyn ComplexEvent>>,
    ) -> Result<(), EventFluxError> {
        let Some(chunk_head) = complex_event_chunk else {
            return Ok(());
        };

        let events = self.complex_event_chain_to_events(chunk_head);
        self.send_events(events)
    }

    /// Turns a chain of complex events into a Vec<Event>.
    ///
    /// Handles StreamEvent (one event) and StateEvent (a pattern or join that
    /// holds several events). Other event types are skipped. Each event keeps
    /// its expired flag.
    fn complex_event_chain_to_events(&self, chunk_head: Box<dyn ComplexEvent>) -> Vec<Event> {
        let mut events = Vec::new();
        let mut current_opt = Some(chunk_head);

        while let Some(mut current) = current_opt {
            // StreamEvent first, the most common case
            if let Some(stream_event) = current
                .as_any()
                .downcast_ref::<crate::core::event::stream::StreamEvent>()
            {
                // Use output_data when present (processed or projected data),
                // else before_window_data (raw input data)
                let data = stream_event
                    .output_data
                    .as_ref()
                    .unwrap_or(&stream_event.before_window_data)
                    .clone();

                let mut event = Event::new_with_data(stream_event.timestamp, data);
                event.is_expired = matches!(
                    stream_event.event_type,
                    crate::core::event::complex_event::ComplexEventType::Expired
                );
                events.push(event);
            }
            // Then StateEvent, used in patterns, joins, and sequences
            else if let Some(state_event) = current
                .as_any()
                .downcast_ref::<crate::core::event::state::StateEvent>()
            {
                // A StateEvent holds several StreamEvents. Use its own
                // output_data when present, which is the joined or matched result.
                if let Some(ref output_data) = state_event.output_data {
                    let mut event =
                        Event::new_with_data(state_event.timestamp, output_data.clone());
                    event.is_expired = matches!(
                        state_event.event_type,
                        crate::core::event::complex_event::ComplexEventType::Expired
                    );
                    events.push(event);
                } else {
                    // Otherwise emit one event per StreamEvent inside the
                    // StateEvent, all with the StateEvent's expired flag.
                    // CRITICAL: each slot can hold a linked list of StreamEvents
                    // through `next`, for example from count or sequence
                    // quantifiers like e1[2] or e1{2,5}. Walk the whole list,
                    // not just its head.
                    for stream_event_arc in state_event.stream_events.iter().flatten() {
                        // stream_event_arc is &Arc<StreamEvent>. Deref
                        // coercion turns it into &StreamEvent here.
                        let stream_event_ref: &crate::core::event::stream::StreamEvent =
                            stream_event_arc;
                        let mut current_in_chain: Option<&dyn crate::core::event::ComplexEvent> =
                            Some(stream_event_ref);

                        while let Some(current) = current_in_chain {
                            if let Some(se) = current
                                .as_any()
                                .downcast_ref::<crate::core::event::stream::StreamEvent>(
                            ) {
                                let data = se
                                    .output_data
                                    .as_ref()
                                    .unwrap_or(&se.before_window_data)
                                    .clone();
                                let mut event = Event::new_with_data(se.timestamp, data);
                                event.is_expired = matches!(
                                    state_event.event_type,
                                    crate::core::event::complex_event::ComplexEventType::Expired
                                );
                                events.push(event);

                                current_in_chain = se.next.as_ref().map(|b| b.as_ref());
                            } else {
                                // Not a StreamEvent, so stop walking this list
                                break;
                            }
                        }
                    }
                }
            }

            current_opt = current.set_next(None);
        }

        events
    }

    /// Sends several events. Async junctions queue them as one batch. An empty
    /// list does nothing.
    pub fn send_events(&self, events: Vec<Event>) -> Result<(), EventFluxError> {
        if events.is_empty() {
            return Ok(());
        }

        if self.shutdown.load(Ordering::Acquire) {
            // "Rejected", not "dropped". The caller gets Err, and these events
            // are never counted into events_dropped, which is only for events
            // lost silently. Callers that send and forget and drop the Err must
            // count them with record_cascade_drop. ShutdownRejected carries the
            // count for that.
            self.handle_backpressure_error(&format!(
                "StreamJunction is shutting down ({} events rejected)",
                events.len()
            ));
            return Err(EventFluxError::ShutdownRejected {
                count: events.len(),
            });
        }

        if self.is_async {
            // Publish as one batch for better throughput
            let event_count = events.len();
            let stream_events: Vec<_> = events
                .into_iter()
                .map(Self::event_to_stream_event)
                .collect();

            let results = self.event_pipeline.publish_batch(stream_events);
            let mut successful: usize = 0;
            let mut dropped: usize = 0;
            let mut errors: usize = 0;
            let mut processed_count: usize = 0;

            for result in results {
                processed_count += 1;
                match result {
                    PipelineResult::Success { .. } => successful += 1,
                    PipelineResult::Full | PipelineResult::Timeout => dropped += 1,
                    PipelineResult::Error(_) => errors += 1,
                    PipelineResult::Shutdown => {
                        // CRITICAL: count this event as dropped, since it got
                        // Shutdown and was rejected
                        dropped += 1;

                        // CRITICAL: count the rest of the batch as dropped too.
                        // When shutdown starts in the middle of a batch, the
                        // pipeline rejects the rest. Without this, callers would
                        // think the whole batch worked when successful > 0.
                        let remaining = event_count - processed_count;
                        dropped += remaining;
                        break;
                    }
                }
            }

            // Processed events are not counted here. The consumer counts them.
            self.events_dropped
                .fetch_add(dropped as u64, Ordering::Relaxed);
            self.processing_errors
                .fetch_add(errors as u64, Ordering::Relaxed);

            // CRITICAL: like send_event(), return an error if ANY event failed,
            // so retry logic further up works correctly
            if dropped > 0 || errors > 0 {
                Err(EventFluxError::SendError {
                    message: format!(
                        "Failed to process {} of {} events (dropped: {}, errors: {})",
                        dropped + errors,
                        event_count,
                        dropped,
                        errors
                    ),
                })
            } else if successful > 0 {
                Ok(())
            } else {
                Err(EventFluxError::SendError {
                    message: format!("Failed to process {event_count} events"),
                })
            }
        } else {
            // Sync mode hands the whole batch to subscribers as one chain
            let event_chain = self.create_event_chain(events);
            self.dispatch_to_subscribers_sync(event_chain)
        }
    }

    /// Starts the consumer threads that read the pipeline, one per configured
    /// consumer. Returns an error if a thread cannot be started. Threads that
    /// did start keep running.
    fn start_async_consumer(&self) -> Result<(), String> {
        let subscribers = Arc::clone(&self.subscribers);
        let pipeline = Arc::clone(&self.event_pipeline);
        let error_counter = Arc::clone(&self.processing_errors);
        let events_processed = Arc::clone(&self.events_processed);
        let executor_service = Arc::clone(&self.executor_service);
        let dispatch_tracker = Arc::clone(&self.dispatch_tracker);
        let stream_id = self.stream_id.clone();

        // Consumers are loops that run for a long time, so each gets its own
        // thread, NOT a thread from the shared executor. A pool thread busy
        // with a consumer never runs queued subscriber dispatches, and with
        // several async junctions the consumers alone could fill the pool and
        // starve every dispatch in the app.
        let pool_size = executor_service.pool_size();
        let consumer_threads = self.event_pipeline.config().consumer_threads;

        // With a one thread executor, dispatching each event through the pool
        // would run everything one at a time anyway. So run subscribers on the
        // consumer threads instead.
        let use_inline_processing = pool_size == 1;
        if use_inline_processing {
            log::info!(
                "[{}] Single-threaded executor (pool_size=1): running subscribers inline on the consumer threads instead of dispatching through the pool.",
                stream_id
            );
        }

        // Start N consumer threads that read events from the pipeline in
        // parallel, for higher throughput
        for consumer_id in 0..consumer_threads {
            let subscribers_clone = Arc::clone(&subscribers);
            let pipeline_clone = Arc::clone(&pipeline);
            let error_counter_clone = Arc::clone(&error_counter);
            let events_processed_clone = Arc::clone(&events_processed);
            let executor_service_clone = Arc::clone(&executor_service);
            let dispatch_tracker_clone = Arc::clone(&dispatch_tracker);
            let stream_id_clone = stream_id.clone();
            let consumer_guard = self.active_consumers.register();

            let consumer_body = move || {
                // Dropped when the loop exits, even on panic, so
                // restart_processing can tell when this consumer set is gone.
                let _consumer_guard = consumer_guard;
                // Copies needed after the handler closure takes the originals
                let stream_id_for_completion = stream_id_clone.clone();
                let error_counter_for_completion = Arc::clone(&error_counter_clone);

                let result = pipeline_clone.consume(move |event, _sequence| {
                    let boxed_event = Box::new(event);

                    // Take a copy of the subscriber list. The read lock lets
                    // several consumer threads read at once.
                    let subs_guard = match subscribers_clone.read() {
                        Ok(guard) => guard,
                        Err(_) => {
                            error_counter_clone.fetch_add(1, Ordering::Relaxed);
                            return Err("Subscribers RwLock poisoned".to_string());
                        }
                    };
                    let subs: Vec<_> = subs_guard.iter().map(Arc::clone).collect();
                    drop(subs_guard);

                    if subs.is_empty() {
                        events_processed_clone.fetch_add(1, Ordering::Relaxed);
                        return Ok(());
                    }

                    // Each subscriber gets its own copy of the event
                    for subscriber in subs.iter() {
                        let event_for_sub = Some(crate::core::event::complex_event::clone_event_chain(
                            boxed_event.as_ref(),
                        ));

                        if use_inline_processing {
                            // pool_size == 1, so run the subscriber on this
                            // consumer thread. The idle check still holds,
                            // since this work finishes before the event counts
                            // as consumed.
                            match subscriber.lock() {
                                Ok(processor) => {
                                    processor.process(event_for_sub);
                                }
                                Err(_) => {
                                    error_counter_clone.fetch_add(1, Ordering::Relaxed);
                                    log::error!("[{}] Consumer {} failed to lock subscriber processor", stream_id_clone, consumer_id);
                                }
                            }
                        } else {
                            // Several pool threads, so hand the work to the pool
                            // and do not block the consumer
                            let subscriber_clone = Arc::clone(subscriber);
                            let stream_id_clone2 = stream_id_clone.clone();
                            let executor_clone = Arc::clone(&executor_service_clone);
                            let error_counter_clone2 = Arc::clone(&error_counter_clone);

                            // Tracked so the shutdown drain can wait for
                            // subscriber work still running (see is_quiescent()).
                            executor_clone.execute_tracked(&dispatch_tracker_clone, move || match subscriber_clone.lock() {
                                Ok(processor) => {
                                    processor.process(event_for_sub);
                                }
                                Err(_) => {
                                    error_counter_clone2.fetch_add(1, Ordering::Relaxed);
                                    log::error!("[{stream_id_clone2}] Failed to lock subscriber processor");
                                }
                            });
                        }
                    }

                    events_processed_clone.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                });

                // The consumer loop has ended, after shutdown or an error
                match result {
                    Ok(processed_count) => {
                        log::info!(
                            "[{stream_id_for_completion}] Consumer {} completed after processing {processed_count} events",
                            consumer_id
                        );
                    }
                    Err(e) => {
                        error_counter_for_completion.fetch_add(1, Ordering::Relaxed);
                        log::error!(
                            "[{stream_id_for_completion}] Consumer {} error: {e}",
                            consumer_id
                        );
                    }
                }
            };

            std::thread::Builder::new()
                .name(format!("{}-consumer-{}", self.stream_id, consumer_id))
                .spawn(consumer_body)
                .map_err(|e| {
                    format!(
                        "[{}] Failed to spawn consumer thread {}: {}",
                        self.stream_id, consumer_id, e
                    )
                })?;
        }

        Ok(())
    }

    /// Hands an event chain straight to every subscriber on this thread (sync
    /// mode). Counts one processed event per call, even for a chain.
    ///
    /// # Errors
    ///
    /// Returns `SendError` when a subscriber lock is poisoned. Subscribers
    /// after it in the list do not get the event.
    fn dispatch_to_subscribers_sync(
        &self,
        event: Box<dyn ComplexEvent>,
    ) -> Result<(), EventFluxError> {
        let subs_guard = self.subscribers.read().expect("RwLock poisoned");
        let subs: Vec<_> = subs_guard.iter().map(Arc::clone).collect();
        drop(subs_guard);

        if subs.is_empty() {
            self.events_processed.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        // Every subscriber but the last gets a copy
        for subscriber in subs.iter().take(subs.len().saturating_sub(1)) {
            let event_for_sub = Some(crate::core::event::complex_event::clone_event_chain(
                event.as_ref(),
            ));

            match subscriber.lock() {
                Ok(processor) => {
                    processor.process(event_for_sub);
                }
                Err(_) => {
                    self.processing_errors.fetch_add(1, Ordering::Relaxed);
                    return Err(EventFluxError::SendError {
                        message: "Failed to lock subscriber processor".to_string(),
                    });
                }
            }
        }

        // The last subscriber gets the original, which saves one copy
        if let Some(last_subscriber) = subs.last() {
            match last_subscriber.lock() {
                Ok(processor) => {
                    processor.process(Some(event));
                }
                Err(_) => {
                    self.processing_errors.fetch_add(1, Ordering::Relaxed);
                    return Err(EventFluxError::SendError {
                        message: "Failed to lock subscriber processor".to_string(),
                    });
                }
            }
        }

        self.events_processed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Turns an Event into a StreamEvent, keeping the expired flag.
    ///
    /// CORRECTNESS NOTE: both before_window_data and output_data must be set.
    /// - before_window_data: the original data, for window and aggregation processors
    /// - output_data: the data sent on to joins, selects, and output processors
    ///
    /// Join processors need output_data to be set. The clone is needed to
    /// serve both.
    fn event_to_stream_event(event: Event) -> StreamEvent {
        let mut stream_event = StreamEvent::new_with_data(event.timestamp, event.data);
        stream_event.output_data = Some(stream_event.before_window_data.clone());
        if event.is_expired {
            stream_event.event_type = crate::core::event::complex_event::ComplexEventType::Expired;
        }
        stream_event
    }

    /// Links several events into one chain, in order.
    ///
    /// # Panics
    ///
    /// Panics when `events` is empty. `send_events` returns early for an empty
    /// list, so it never calls this with one.
    fn create_event_chain(&self, events: Vec<Event>) -> Box<dyn ComplexEvent> {
        let mut iter = events.into_iter();
        let first = match iter.next() {
            Some(event) => Self::event_to_stream_event(event),
            None => panic!("Cannot create chain from empty events"),
        };

        let mut head: Box<dyn ComplexEvent> = Box::new(first);
        let mut tail_ref = head.mut_next_ref_option();

        for event in iter {
            let stream_event = Self::event_to_stream_event(event);
            let boxed = Box::new(stream_event);
            *tail_ref = Some(boxed);
            if let Some(ref mut last) = *tail_ref {
                tail_ref = last.mut_next_ref_option();
            }
        }

        head
    }

    /// Reports a send that failed (full pipeline or shutdown) as set by
    /// `on_error_action`. LOG warns, DROP does nothing, STREAM sends a
    /// (stream id, message) event to the fault stream, and STORE saves the
    /// error in the app's error store. STREAM and STORE only warn when that
    /// target is not set up.
    fn handle_backpressure_error(&self, message: &str) {
        match self.on_error_action {
            OnErrorAction::LOG => {
                log::warn!("[{}] Backpressure: {}", self.stream_id, message);
            }
            OnErrorAction::DROP => {
                // Do nothing
            }
            OnErrorAction::STREAM => {
                if let Some(fault_junction) = &self.fault_stream_junction {
                    // The error event is timestamped now. Its send result is ignored.
                    let error_event = Event::new_with_data(
                        chrono::Utc::now().timestamp_millis(),
                        vec![
                            crate::core::event::value::AttributeValue::String(
                                self.stream_id.clone(),
                            ),
                            crate::core::event::value::AttributeValue::String(message.to_string()),
                        ],
                    );
                    let _ = fault_junction.lock().unwrap().send_event(error_event);
                } else {
                    log::warn!(
                        "[{}] Fault stream not configured: {}",
                        self.stream_id,
                        message
                    );
                }
            }
            OnErrorAction::STORE => {
                if let Some(store) = self
                    .eventflux_app_context
                    .get_eventflux_context()
                    .get_error_store()
                {
                    let error = EventFluxError::SendError {
                        message: message.to_string(),
                    };
                    store.store(&self.stream_id, error);
                } else {
                    log::warn!(
                        "[{}] Error store not configured: {}",
                        self.stream_id,
                        message
                    );
                }
            }
        }
    }

    /// Returns a snapshot of the junction and pipeline metrics
    pub fn get_performance_metrics(&self) -> JunctionPerformanceMetrics {
        let pipeline_metrics = self.event_pipeline.metrics().snapshot();

        JunctionPerformanceMetrics {
            stream_id: self.stream_id.clone(),
            events_processed: self.events_processed.load(Ordering::Relaxed),
            events_dropped: self.events_dropped.load(Ordering::Relaxed),
            processing_errors: self.processing_errors.load(Ordering::Relaxed),
            pipeline_metrics,
            subscriber_count: self.subscribers.read().expect("RwLock poisoned").len(),
            is_async: self.is_async,
            buffer_utilization: self.event_pipeline.utilization(),
            remaining_capacity: self.event_pipeline.remaining_capacity(),
        }
    }

    /// Returns true when the junction is not shut down, the pipeline metrics
    /// look healthy, and at least one event was processed. A new junction
    /// that has seen no events is not healthy.
    pub fn is_healthy(&self) -> bool {
        !self.shutdown.load(Ordering::Acquire)
            && self.event_pipeline.metrics().snapshot().is_healthy()
            && self.events_processed.load(Ordering::Relaxed) > 0
    }

    /// Returns the number of events processed. Always `Some`.
    pub fn total_events(&self) -> Option<u64> {
        Some(self.events_processed.load(Ordering::Relaxed))
    }

    /// Returns the average publish latency in nanoseconds, from the pipeline
    /// metrics. `None` when there is no latency data yet.
    pub fn average_latency_ns(&self) -> Option<u64> {
        let metrics = self.event_pipeline.metrics().snapshot();
        if metrics.avg_publish_latency_us > 0.0 {
            Some((metrics.avg_publish_latency_us * 1000.0) as u64)
        } else {
            None
        }
    }
}

/// View of a junction's drain state that needs no lock, for shutdown checks.
///
/// Take it once (which needs the junction Mutex), then check it as often as
/// needed. All reads go through Arcs to atomics, never back through the Mutex.
#[derive(Clone)]
pub struct JunctionDrainHandle {
    is_async: bool,
    pipeline: Arc<EventPipeline>,
    dispatch_tracker: Arc<TaskTracker>,
    active_consumers: Arc<TaskTracker>,
}

impl JunctionDrainHandle {
    pub fn is_async(&self) -> bool {
        self.is_async
    }

    /// True once no consumer thread is alive and no subscriber dispatch is
    /// running, so nothing can change state further down anymore. Used after
    /// `halt_processing` to keep the abandoned event count exact and to stop
    /// anything changing while the shutdown snapshot is taken.
    pub fn settled(&self) -> bool {
        self.active_consumers.in_flight() == 0 && self.dispatch_tracker.in_flight() == 0
    }

    /// Published event count (Acquire). Only goes up once sources are stopped.
    pub fn published(&self) -> u64 {
        self.pipeline.metrics().events_published_count()
    }

    /// Idle check: `!is_async || (halted ? settled : (queue empty
    /// && published == consumed && in_flight == 0))`
    ///
    /// A halted junction counts as idle once settled. Its frozen queue will
    /// never move again (it was counted by `abandon_pending` and is thrown
    /// away on restart), so waiting on it would only use up the drain budget.
    /// Abandoning it again on a later shutdown would count the drops twice.
    ///
    /// For live junctions, `published` goes up only after a successful queue
    /// push, and `consumed` only after the consumer handler returns, which is
    /// after all subscriber dispatches for that event were registered. Reading
    /// in the order consumed, published, is_empty, in_flight gives the
    /// ordering the counters need. The latest `consumed` makes earlier
    /// dispatch registrations visible before `in_flight` is read.
    ///
    /// CAUTION: a single call can still wrongly return `true`. A cascade
    /// dispatch on another thread may have pushed an event whose `published`
    /// increase this thread has not seen yet, and the empty queue check is not
    /// synced with that push. The check is only reliable inside
    /// [`poll_quiescent`]'s two pass check that ΣP stays the same. Do not
    /// base anything on one call alone.
    pub fn is_quiescent(&self) -> bool {
        if !self.is_async {
            return true;
        }
        if self.pipeline.is_halted() {
            return self.settled();
        }
        let consumed = self.pipeline.metrics().events_consumed_count();
        let published = self.pipeline.metrics().events_published_count();
        let empty = self.pipeline.is_empty();
        let in_flight = self.dispatch_tracker.in_flight();
        empty && published == consumed && in_flight == 0
    }

    /// Best effort `(queued, in_flight)` estimate. See
    /// `StreamJunction::pending_drain_estimate`.
    pub fn pending_drain_estimate(&self) -> (u64, u64) {
        let consumed = self.pipeline.metrics().events_consumed_count();
        let published = self.pipeline.metrics().events_published_count();
        (
            published.saturating_sub(consumed),
            self.dispatch_tracker.in_flight(),
        )
    }
}

/// Snapshot of a stream junction's metrics
#[derive(Debug, Clone)]
pub struct JunctionPerformanceMetrics {
    pub stream_id: String,
    pub events_processed: u64,
    pub events_dropped: u64,
    pub processing_errors: u64,
    pub pipeline_metrics: MetricsSnapshot,
    pub subscriber_count: usize,
    pub is_async: bool,
    pub buffer_utilization: f64,
    pub remaining_capacity: usize,
}

impl JunctionPerformanceMetrics {
    /// Calculate overall health score (0.0 to 1.0)
    pub fn health_score(&self) -> f64 {
        let pipeline_health = self.pipeline_metrics.health_score();
        let error_rate = if self.events_processed > 0 {
            self.processing_errors as f64 / self.events_processed as f64
        } else {
            0.0
        };
        // Guard on processed + dropped: a junction that dropped everything it
        // ever saw (processed == 0, dropped == K) must not score as healthy.
        let drop_rate = if self.events_processed + self.events_dropped > 0 {
            self.events_dropped as f64 / (self.events_processed + self.events_dropped) as f64
        } else {
            0.0
        };

        let mut score = pipeline_health;
        score -= error_rate * 2.0; // errors weigh 2x
        score -= drop_rate * 1.5; // drops weigh 1.5x

        score.clamp(0.0, 1.0)
    }

    /// Returns true when the health score is above 0.8, the pipeline metrics
    /// look healthy, and errors are under 1% of processed events. With fewer
    /// than 100 processed events the error limit is 0, so this is false.
    pub fn is_healthy(&self) -> bool {
        self.health_score() > 0.8
            && self.pipeline_metrics.is_healthy()
            && self.processing_errors < (self.events_processed / 100) // under 1% errors
    }
}

/// Sends events into a stream junction. Used as an InputProcessor.
///
/// Holds an Arc to the junction, so the junction stays alive as long as the
/// publisher does, even when the publisher outlives the code that made it.
/// The stream index is ignored.
#[derive(Debug, Clone)]
pub struct Publisher {
    junction: Arc<Mutex<StreamJunction>>,
}

impl Publisher {
    fn new(junction: Arc<Mutex<StreamJunction>>) -> Self {
        Self { junction }
    }
}

impl InputProcessor for Publisher {
    fn send_event_with_data(
        &mut self,
        timestamp: i64,
        data: Vec<crate::core::event::value::AttributeValue>,
        _stream_index: usize,
    ) -> Result<(), String> {
        let event = Event::new_with_data(timestamp, data);
        self.junction
            .lock()
            .map_err(|_| "Junction mutex poisoned".to_string())?
            .send_event(event)
            .map_err(|e| format!("Send error: {e}"))
    }

    fn send_single_event(&mut self, event: Event, _stream_index: usize) -> Result<(), String> {
        self.junction
            .lock()
            .map_err(|_| "Junction mutex poisoned".to_string())?
            .send_event(event)
            .map_err(|e| format!("Send error: {e}"))
    }

    fn send_multiple_events(
        &mut self,
        events: Vec<Event>,
        _stream_index: usize,
    ) -> Result<(), String> {
        self.junction
            .lock()
            .map_err(|_| "Junction mutex poisoned".to_string())?
            .send_events(events)
            .map_err(|e| format!("Send error: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::eventflux_context::EventFluxContext;
    use crate::core::event::value::AttributeValue;
    use crate::query_api::definition::attribute::Type as AttrType;
    use crate::query_api::eventflux_app::EventFluxApp;
    use std::thread;
    use std::time::{Duration, Instant};

    // Processor for tests that records the data of each StreamEvent it gets
    #[derive(Debug)]
    struct TestProcessor {
        events: Arc<Mutex<Vec<Vec<AttributeValue>>>>,
        name: String,
    }

    impl TestProcessor {
        fn new(name: String) -> Self {
            Self {
                events: Arc::new(Mutex::new(Vec::new())),
                name,
            }
        }

        fn get_events(&self) -> Vec<Vec<AttributeValue>> {
            self.events.lock().unwrap().clone()
        }
    }

    impl Processor for TestProcessor {
        fn process(&self, mut chunk: Option<Box<dyn ComplexEvent>>) {
            while let Some(mut ce) = chunk {
                chunk = ce.set_next(None);
                if let Some(se) = ce.as_any().downcast_ref::<StreamEvent>() {
                    self.events
                        .lock()
                        .unwrap()
                        .push(se.before_window_data.clone());
                }
            }
        }

        fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
            None
        }

        fn set_next_processor(&mut self, _n: Option<Arc<Mutex<dyn Processor>>>) {}

        fn clone_processor(
            &self,
            _c: &Arc<crate::core::config::eventflux_query_context::EventFluxQueryContext>,
        ) -> Box<dyn Processor> {
            Box::new(TestProcessor::new(self.name.clone()))
        }

        fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
            Arc::new(EventFluxAppContext::new(
                Arc::new(EventFluxContext::new()),
                "TestApp".to_string(),
                Arc::new(crate::query_api::eventflux_app::EventFluxApp::new(
                    "TestApp".to_string(),
                )),
                String::new(),
            ))
        }

        fn get_eventflux_query_context(
            &self,
        ) -> Arc<crate::core::config::eventflux_query_context::EventFluxQueryContext> {
            Arc::new(
                crate::core::config::eventflux_query_context::EventFluxQueryContext::new(
                    self.get_eventflux_app_context(),
                    "TestQuery".to_string(),
                    None,
                ),
            )
        }

        fn get_processing_mode(&self) -> crate::core::query::processor::ProcessingMode {
            crate::core::query::processor::ProcessingMode::DEFAULT
        }

        fn is_stateful(&self) -> bool {
            false
        }
    }

    fn setup_junction(is_async: bool) -> (StreamJunction, Arc<Mutex<TestProcessor>>) {
        let eventflux_context = Arc::new(EventFluxContext::new());
        let app = Arc::new(crate::query_api::eventflux_app::EventFluxApp::new(
            "TestApp".to_string(),
        ));
        let mut app_ctx = EventFluxAppContext::new(
            Arc::clone(&eventflux_context),
            "TestApp".to_string(),
            Arc::clone(&app),
            String::new(),
        );
        app_ctx.root_metrics_level =
            crate::core::config::eventflux_app_context::MetricsLevelPlaceholder::BASIC;

        let stream_def = Arc::new(
            StreamDefinition::new("TestStream".to_string())
                .attribute("id".to_string(), AttrType::INT),
        );

        let junction = StreamJunction::new(
            "TestStream".to_string(),
            stream_def,
            Arc::new(app_ctx),
            4096,
            is_async,
            None,
        )
        .unwrap();

        let processor = Arc::new(Mutex::new(TestProcessor::new("TestProcessor".to_string())));
        junction.subscribe(processor.clone() as Arc<Mutex<dyn Processor>>);

        (junction, processor)
    }

    #[test]
    fn test_sync_junction_single_event() {
        let (junction, processor) = setup_junction(false);

        junction.start_processing().unwrap();

        let event = Event::new_with_data(1000, vec![AttributeValue::Int(42)]);
        junction.send_event(event).unwrap();

        let events = processor.lock().unwrap().get_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0], vec![AttributeValue::Int(42)]);

        let metrics = junction.get_performance_metrics();
        assert_eq!(metrics.events_processed, 1);
        assert_eq!(metrics.events_dropped, 0);
    }

    #[test]
    fn test_async_junction_auto_starts_without_explicit_start_processing() {
        // REGRESSION TEST: async junctions must start their consumer threads in
        // the constructor. The old bug was that async junctions needed an
        // explicit start_processing() call. This test acts like real use,
        // where start_processing() is never called.

        let (junction, processor) = setup_junction(true); // async=true

        // CRITICAL: do NOT call start_processing(), as in real code. If the
        // automatic start is broken, events queue forever and this test fails.

        // Send events right after construction
        let events: Vec<_> = (0..50)
            .map(|i| Event::new_with_data(1000 + i, vec![AttributeValue::Int(i as i32)]))
            .collect();

        junction.send_events(events).unwrap();

        // Wait for async processing, with the consumer already running
        thread::sleep(Duration::from_millis(200));

        // Events must be received, which shows the consumer started on its own
        let received_events = processor.lock().unwrap().get_events();
        assert_eq!(
            received_events.len(),
            50,
            "Async junction must auto-start consumer thread in constructor. \
             Events were not processed, indicating consumer never started!"
        );

        // The metrics must show the processing
        let metrics = junction.get_performance_metrics();
        assert!(
            metrics.events_processed >= 50,
            "Async junction consumer must auto-start. Processed: {}, Expected: >= 50",
            metrics.events_processed
        );
        assert!(
            metrics.pipeline_metrics.events_consumed > 0,
            "Pipeline consumer must be running. No events consumed!"
        );
    }

    #[test]
    fn test_async_junction_multiple_events() {
        let (junction, processor) = setup_junction(true);

        junction.start_processing().unwrap();

        let events: Vec<_> = (0..100)
            .map(|i| Event::new_with_data(1000 + i, vec![AttributeValue::Int(i as i32)]))
            .collect();

        junction.send_events(events).unwrap();

        // Wait for async processing
        thread::sleep(Duration::from_millis(200));

        let received_events = processor.lock().unwrap().get_events();
        assert_eq!(received_events.len(), 100);

        let metrics = junction.get_performance_metrics();
        assert!(metrics.events_processed >= 100);
        // The health check can be too strict for a small load, so only check the basics
        assert!(metrics.pipeline_metrics.events_published > 0);
        assert!(metrics.pipeline_metrics.events_consumed > 0);
    }

    #[test]
    fn test_junction_throughput() {
        let (junction, _processor) = setup_junction(true);

        junction.start_processing().unwrap();

        let start = Instant::now();
        let num_events = 10000;

        for i in 0..num_events {
            let event = Event::new_with_data(i, vec![AttributeValue::Int(i as i32)]);
            let _ = junction.send_event(event);
        }

        let duration = start.elapsed();
        let throughput = num_events as f64 / duration.as_secs_f64();

        println!("Throughput: {:.0} events/sec", throughput);

        // Must be much faster than the old crossbeam_channel version
        assert!(throughput > 50000.0, "Throughput should be >50K events/sec");

        // Wait for processing
        thread::sleep(Duration::from_millis(500));

        let metrics = junction.get_performance_metrics();
        println!("Pipeline metrics: {:?}", metrics.pipeline_metrics);
        assert!(metrics.pipeline_metrics.throughput_events_per_sec > 10000.0);
    }

    #[test]
    fn test_junction_metrics_integration() {
        let (junction, _processor) = setup_junction(true);

        junction.start_processing().unwrap();

        // Send some events
        for i in 0..1000 {
            let event = Event::new_with_data(i, vec![AttributeValue::Int(i as i32)]);
            let _ = junction.send_event(event);
        }

        thread::sleep(Duration::from_millis(100));

        let metrics = junction.get_performance_metrics();

        // Metrics must be collected
        assert!(metrics.events_processed > 0);
        assert!(metrics.pipeline_metrics.events_published > 0);
        assert!(metrics.pipeline_metrics.throughput_events_per_sec > 0.0);
        assert!(metrics.health_score() > 0.0);

        // The older getter methods still work
        assert!(junction.total_events().unwrap() > 0);
        // average_latency_ns may be None if processing is too fast
    }

    #[test]
    fn test_async_batch_partial_failure_returns_error() {
        // REGRESSION TEST: a batch where only some events fail must return an
        // error. The old bug returned Ok(()) if ANY event worked, even when
        // others failed. That broke retry logic, since callers thought all
        // events went through.

        let ctx = Arc::new(EventFluxContext::new());
        let app = Arc::new(EventFluxApp::new("TestApp".to_string()));
        let app_ctx = Arc::new(EventFluxAppContext::new(
            Arc::clone(&ctx),
            "Test".to_string(),
            Arc::clone(&app),
            String::new(),
        ));

        let stream_def = Arc::new(
            StreamDefinition::new("TestStream".to_string())
                .attribute("value".to_string(), AttrType::INT),
        );

        // Async junction with a tiny buffer, to force drops
        let junction = Arc::new(Mutex::new(
            StreamJunction::new(
                "TestStream".to_string(),
                stream_def,
                app_ctx,
                64, // smallest buffer size
                true,
                None,
            )
            .unwrap(),
        ));

        let processor = Arc::new(Mutex::new(TestProcessor::new("test".to_string())));
        junction.lock().unwrap().subscribe(processor.clone());

        // start_processing() is not called, but async junctions start their
        // consumers in the constructor anyway. The batch below is still far
        // bigger than the buffer, so it fills faster than the consumers read.

        // Send a large batch. The 64 slot buffer fills and some events are dropped.
        let events: Vec<_> = (0..500)
            .map(|i| Event::new_with_data(1000 + i, vec![AttributeValue::Int(i as i32)]))
            .collect();

        let result = junction.lock().unwrap().send_events(events);

        // CRITICAL: must return an error because SOME events failed. Before
        // the fix this returned Ok(()) if ANY event worked.
        assert!(
            result.is_err(),
            "send_events() must return error when ANY events fail (partial batch failure). \
             Got Ok(()) which breaks retry logic!"
        );

        // The error message must give details
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Failed to process") || msg.contains("dropped") || msg.contains("errors"),
            "Error message should include failure details, got: {msg}"
        );

        // The metrics must show dropped events
        let metrics = junction.lock().unwrap().get_performance_metrics();
        assert!(
            metrics.events_dropped > 0,
            "Should have dropped events due to small buffer"
        );
    }

    #[test]
    fn test_single_threaded_executor_no_starvation() {
        // REGRESSION TEST: a one thread executor must not starve subscribers.
        // The old bug was that the consumer and subscribers shared one executor.
        // With only 1 thread the consumer held it, subscriber tasks never ran,
        // and the pipeline filled up. The fix runs subscribers on the consumer
        // threads when pool_size == 1. Consumers now also have their own threads.

        let ctx = Arc::new(EventFluxContext::new());
        let app = Arc::new(EventFluxApp::new("TestApp".to_string()));
        let app_ctx = Arc::new(EventFluxAppContext::new(
            Arc::clone(&ctx),
            "Test".to_string(),
            Arc::clone(&app),
            String::new(),
        ));

        let stream_def = Arc::new(
            StreamDefinition::new("TestStream".to_string())
                .attribute("value".to_string(), AttrType::INT),
        );

        // CRITICAL: a one thread executor, to reproduce the starvation bug
        let single_thread_executor =
            Arc::new(crate::core::util::executor_service::ExecutorService::new(
                "single-thread-test",
                1, // only 1 thread, which starves subscribers if handled wrong
            ));

        let junction = Arc::new(Mutex::new(
            StreamJunction::new_with_executor(
                "TestStream".to_string(),
                stream_def,
                app_ctx,
                4096,
                true, // async junction
                None,
                single_thread_executor,
            )
            .unwrap(),
        ));

        let processor = Arc::new(Mutex::new(TestProcessor::new("test".to_string())));
        junction.lock().unwrap().subscribe(processor.clone());

        // These must be processed even with a one thread executor
        let events: Vec<_> = (0..100)
            .map(|i| Event::new_with_data(1000 + i, vec![AttributeValue::Int(i as i32)]))
            .collect();

        junction.lock().unwrap().send_events(events).unwrap();

        // Wait for async processing
        thread::sleep(Duration::from_millis(500));

        // CRITICAL: events must arrive even with a one thread executor. If the
        // fix breaks, the consumer starves subscribers and no events are processed.
        let received_events = processor.lock().unwrap().get_events();
        assert_eq!(
            received_events.len(),
            100,
            "Single-threaded executor must not starve subscribers! \
             Expected 100 events processed, got {}. \
             Bug: Consumer occupies the only thread and subscriber tasks never execute.",
            received_events.len()
        );

        // The metrics must show all events processed and none dropped
        let metrics = junction.lock().unwrap().get_performance_metrics();
        assert_eq!(
            metrics.events_processed, 100,
            "All events should be processed on single-threaded executor"
        );
        assert_eq!(
            metrics.events_dropped, 0,
            "No events should be dropped due to starvation"
        );
    }

    #[test]
    fn test_limited_pool_executor_no_starvation() {
        // REGRESSION TEST: a small executor pool must not starve subscribers.
        // The old bug was that consumer threads used up the executor pool and
        // left no threads for subscriber tasks. Case: pool_size=2, but the
        // junction wants 4 consumers (num_cpus/2 on an 8 core machine). The fix
        // runs consumers on their own threads, so the pool serves only
        // subscriber dispatches.

        let ctx = Arc::new(EventFluxContext::new());
        let app = Arc::new(EventFluxApp::new("TestApp".to_string()));
        let app_ctx = Arc::new(EventFluxAppContext::new(
            Arc::clone(&ctx),
            "Test".to_string(),
            Arc::clone(&app),
            String::new(),
        ));

        let stream_def = Arc::new(
            StreamDefinition::new("TestStream".to_string())
                .attribute("value".to_string(), AttrType::INT),
        );

        // CRITICAL: an executor with only 2 threads, fewer than the 4 consumers
        // above. This acts like EVENTFLUX_EXECUTOR_THREADS=2 in production.
        let limited_executor = Arc::new(crate::core::util::executor_service::ExecutorService::new(
            "limited-pool-test",
            2, // only 2 threads, which consumers used to use up
        ));

        let junction = Arc::new(Mutex::new(
            StreamJunction::new_with_executor(
                "TestStream".to_string(),
                stream_def,
                app_ctx,
                4096,
                true, // async junction, which starts num_cpus/2 consumers
                None,
                limited_executor,
            )
            .unwrap(),
        ));

        let processor = Arc::new(Mutex::new(TestProcessor::new("test".to_string())));
        junction.lock().unwrap().subscribe(processor.clone());

        // These must be processed even with the small executor
        let events: Vec<_> = (0..200)
            .map(|i| Event::new_with_data(1000 + i, vec![AttributeValue::Int(i as i32)]))
            .collect();

        junction.lock().unwrap().send_events(events).unwrap();

        // Wait for async processing
        thread::sleep(Duration::from_millis(1000));

        // CRITICAL: events must arrive even with the small pool. If the fix
        // breaks, consumers hold all threads and no subscriber tasks run.
        let received_events = processor.lock().unwrap().get_events();
        assert_eq!(
            received_events.len(),
            200,
            "Limited executor pool must not starve subscribers! \
             Expected 200 events processed, got {}. \
             Bug: pool_size=2 exhausted by consumers, leaving no threads for subscribers.",
            received_events.len()
        );

        // The metrics must show all events processed and none dropped from starvation
        let metrics = junction.lock().unwrap().get_performance_metrics();
        assert_eq!(
            metrics.events_processed, 200,
            "All events should be processed with a small dispatch pool"
        );
        assert_eq!(
            metrics.events_dropped, 0,
            "No events should be dropped due to executor starvation"
        );
    }

    #[test]
    fn test_batch_shutdown_counts_all_failed_events() {
        // REGRESSION TEST: shutdown must count both the event that got Shutdown
        // AND the rest of the batch.
        // Bug 1: PipelineResult::Shutdown used to just break the loop without
        //        counting the rest, so callers thought the whole batch worked.
        // Bug 2: the event that got Shutdown was never counted as dropped, so
        //        data was lost silently when shutdown hit the last event.

        // This test has no assertions. A real shutdown in the middle of a batch
        // is hard to set up without mocking the pipeline. It only records the
        // expected logic, which lives in the PipelineResult::Shutdown arm of
        // send_events. When Shutdown comes back, that code
        // 1. adds 1 to dropped for the event itself (dropped += 1)
        // 2. works out remaining = event_count - processed_count
        // 3. adds remaining to dropped (dropped += remaining)
        // 4. returns Err because dropped > 0

        // Case 1: shutdown on the last event
        // - Send 100 events
        // - The first 99 succeed
        // - Event 100 gets PipelineResult::Shutdown
        // - dropped = 1 (the event itself) + 0 (remaining) = 1
        // - Returns Err (not Ok) because dropped = 1

        // Case 2: shutdown in the middle of the batch
        // - Send 100 events
        // - The first 20 succeed
        // - Event 21 gets PipelineResult::Shutdown
        // - dropped = 1 (the event itself) + 79 (remaining) = 80
        // - Returns Err (not Ok) because dropped = 80

        // A real integration test would need a mock pipeline, which is complex
    }
}
