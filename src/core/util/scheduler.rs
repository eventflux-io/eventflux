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

//! Scheduler for time based event triggering
//!
//! # Threading Model
//!
//! Each Scheduler has one timer thread and a thread pool (its ExecutorService).
//!
//! - The timer thread keeps every pending timer in a queue ordered by due time. It
//!   sleeps until the earliest one is due, so a waiting timer holds no pool thread, and
//!   any number of timers and triggers can wait at once.
//! - When a timer is due, the timer thread hands its callback to the pool. Callbacks
//!   can run at the same time as each other.
//! - A periodic or cron timer schedules its next run only after its callback returns,
//!   so the runs of one timer never overlap.
//!
//! The runtime pauses the scheduler on shutdown and resumes it on a restart, so window
//! timers armed before a shutdown still fire after the restart. Dropping the runtime
//! shuts the scheduler down for good, which drops every pending timer and stops the
//! timer thread.
//!
//! # Configuration
//!
//! Set the pool size with:
//! - the `EVENTFLUX_EXECUTOR_THREADS` environment variable (for the default executor)
//! - or a custom `ExecutorService` with the thread count you want
//!
//! Default: `num_cpus::get()` threads

use crate::core::util::executor_service::ExecutorService;
use chrono::{DateTime, Utc};
use cron::Schedule;
use std::cell::Cell;
use std::cmp::{Ordering as CmpOrdering, Reverse};
use std::collections::BinaryHeap;
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Something the Scheduler calls back when its time comes.
pub trait Schedulable: Send + Sync {
    /// Called with the scheduled time in epoch milliseconds, which can be slightly
    /// earlier than the real time of the call.
    fn on_time(&self, timestamp: i64);
}

/// Scheduler for time based callbacks. See the module docs for the threading model.
///
/// Dropping the last handle shuts it down, like `shutdown`.
pub struct Scheduler {
    shared: Arc<Shared>,
    timer_thread: Mutex<Option<JoinHandle<()>>>,
}

impl fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.shared.lock();
        f.debug_struct("Scheduler")
            .field("pending", &state.timers.len())
            .field("paused", &state.paused)
            .field("stopped", &state.stopped)
            .finish()
    }
}

impl Scheduler {
    /// A scheduler that runs callbacks on `executor`. Starts its timer thread.
    ///
    /// # Panics
    /// Panics if the OS cannot start the timer thread.
    pub fn new(executor: Arc<ExecutorService>) -> Self {
        let shared = Arc::new(Shared {
            executor,
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
            idle: Condvar::new(),
        });
        let timer_shared = Arc::clone(&shared);
        let timer_thread = thread::Builder::new()
            .name("eventflux-scheduler".to_string())
            .spawn(move || timer_shared.run_timer_thread())
            .expect("failed to start the scheduler timer thread");
        Self {
            shared,
            timer_thread: Mutex::new(Some(timer_thread)),
        }
    }

    /// Calls `target.on_time(timestamp)` once at `timestamp` (epoch milliseconds), or
    /// right away if that time has passed.
    pub fn notify_at(&self, timestamp: i64, target: Arc<dyn Schedulable>) {
        self.shared.add_timer(
            timestamp,
            Job {
                target,
                repeat: Repeat::Once,
                remaining: None,
                running: None,
            },
        );
    }

    /// Calls `target.on_time` every `period_ms` milliseconds, starting one period from
    /// now. Each run is due one period after the previous due time, so a late run does
    /// not shift the ones after it.
    ///
    /// Stops after `limit` calls if given, or once `running` is false. `running` is
    /// checked when a run is due, so a stopped timer makes no more calls.
    pub fn schedule_periodic(
        &self,
        period_ms: i64,
        target: Arc<dyn Schedulable>,
        limit: Option<usize>,
        running: Option<Arc<AtomicBool>>,
    ) {
        self.shared.add_timer(
            now_ms() + period_ms,
            Job {
                target,
                repeat: Repeat::Every(period_ms),
                remaining: limit,
                running,
            },
        );
    }

    /// Calls `target.on_time` at each upcoming time of `cron_expr`, in UTC.
    ///
    /// Stops after `limit` calls if given, or once `running` is false. `running` is
    /// checked when a run is due, so a stopped timer makes no more calls.
    ///
    /// # Errors
    /// Returns an error if `cron_expr` does not parse.
    pub fn schedule_cron(
        &self,
        cron_expr: &str,
        target: Arc<dyn Schedulable>,
        limit: Option<usize>,
        running: Option<Arc<AtomicBool>>,
    ) -> Result<(), String> {
        let schedule = Schedule::from_str(cron_expr).map_err(|e| e.to_string())?;
        if let Some(first) = schedule.upcoming(Utc).next() {
            self.shared.add_timer(
                first.timestamp_millis(),
                Job {
                    target,
                    repeat: Repeat::Cron(Box::new(schedule)),
                    remaining: limit,
                    running,
                },
            );
        }
        Ok(())
    }

    /// Stop firing timers until `resume`. Pending and new timers are kept, and timers
    /// that come due while paused fire on resume.
    ///
    /// Waits for callbacks that are already running to return, so nothing fires after
    /// this returns. Called from inside one of this scheduler's callbacks, as when a
    /// timer tick reaches a stream callback that shuts the runtime down, it waits for
    /// every other callback but not for the one it runs in.
    pub fn pause(&self) {
        let own_callback = usize::from(self.shared.is_running_callback_on_this_thread());
        let mut state = self.shared.lock();
        state.paused = true;
        self.shared.changed.notify_all();
        while state.in_flight > own_callback {
            state = self.shared.idle.wait(state).unwrap();
        }
    }

    /// Fire timers again after `pause`. Timers that came due while paused fire now.
    pub fn resume(&self) {
        self.shared.lock().paused = false;
        self.shared.changed.notify_all();
    }

    /// Stop for good. Drops every pending timer, ignores timers added later and stops
    /// the timer thread. Callbacks that are already running are not waited for.
    /// Calling it again does nothing.
    pub fn shutdown(&self) {
        let dropped_timers = {
            let mut state = self.shared.lock();
            state.stopped = true;
            std::mem::take(&mut state.timers)
        };
        self.shared.changed.notify_all();
        // Dropped outside the lock. A timer's target can hold the last handle to a
        // scheduler, and dropping that handle locks the state again.
        drop(dropped_timers);

        let timer_thread = self.timer_thread.lock().unwrap().take();
        if let Some(handle) = timer_thread {
            if handle.thread().id() != thread::current().id() {
                let _ = handle.join();
            }
        }
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// State shared by the scheduler handle, its timer thread and running callbacks.
struct Shared {
    executor: Arc<ExecutorService>,
    state: Mutex<State>,
    /// Wakes the timer thread when a timer is added or the scheduler is paused,
    /// resumed or stopped.
    changed: Condvar,
    /// Wakes `pause` each time a running callback returns.
    idle: Condvar,
}

#[derive(Default)]
struct State {
    /// Pending timers, earliest first.
    timers: BinaryHeap<Reverse<Timer>>,
    /// Breaks ties between timers with the same due time, so they fire in the order
    /// they were added.
    next_seq: u64,
    paused: bool,
    stopped: bool,
    /// Callbacks handed to the pool that have not returned yet.
    in_flight: usize,
}

thread_local! {
    /// The `Shared::id` of the scheduler whose callback this thread is running, or 0.
    static CALLBACK_SCHEDULER: Cell<usize> = const { Cell::new(0) };
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap()
    }

    /// Tells schedulers apart. It is the address of this `Shared`, which stays the same
    /// while any callback of it runs, because the callback holds an `Arc` to it.
    fn id(&self) -> usize {
        std::ptr::from_ref(self) as usize
    }

    fn is_running_callback_on_this_thread(&self) -> bool {
        CALLBACK_SCHEDULER.with(|current| current.get() == self.id())
    }

    /// Queue `job` to run at `due` (epoch milliseconds). Does nothing once stopped, or
    /// when `job` has no runs left.
    fn add_timer(&self, due: i64, job: Job) {
        if job.remaining == Some(0) {
            return;
        }
        let mut state = self.lock();
        if state.stopped {
            drop(state);
            drop(job); // outside the lock, see `Scheduler::shutdown`
            return;
        }
        let seq = state.next_seq;
        state.next_seq += 1;
        state.timers.push(Reverse(Timer { due, seq, job }));
        drop(state);
        self.changed.notify_all();
    }

    /// Body of the timer thread. Hands each due timer to the pool until stopped.
    fn run_timer_thread(self: Arc<Self>) {
        let mut state = self.lock();
        loop {
            if state.stopped {
                return;
            }
            let next_due = if state.paused {
                None
            } else {
                state.timers.peek().map(|Reverse(timer)| timer.due)
            };
            match next_due {
                None => state = self.changed.wait(state).unwrap(),
                Some(due) => {
                    let now = now_ms();
                    if due > now {
                        let wait = Duration::from_millis((due - now) as u64);
                        state = self.changed.wait_timeout(state, wait).unwrap().0;
                        continue;
                    }
                    let Reverse(timer) = state.timers.pop().expect("peeked timer");
                    if !timer.job.is_running() {
                        drop(state);
                        drop(timer); // outside the lock, see `Scheduler::shutdown`
                        state = self.lock();
                        continue;
                    }
                    state.in_flight += 1;
                    let shared = Arc::clone(&self);
                    self.executor.execute(move || shared.fire(timer));
                }
            }
        }
    }

    /// Run one due timer on a pool thread, then queue its next run if it repeats.
    fn fire(&self, timer: Timer) {
        // Marks this thread as running a callback of this scheduler, and counts the
        // callback as done even if it panics, so `pause` cannot hang.
        struct InFlight<'a> {
            shared: &'a Shared,
            outer_callback: usize,
        }
        impl Drop for InFlight<'_> {
            fn drop(&mut self) {
                CALLBACK_SCHEDULER.with(|current| current.set(self.outer_callback));
                let mut state = self.shared.lock();
                state.in_flight -= 1;
                // Every return wakes `pause`, which may be waiting for one callback
                // to be left, the one it was called from.
                self.shared.idle.notify_all();
            }
        }
        let _in_flight = InFlight {
            shared: self,
            outer_callback: CALLBACK_SCHEDULER.with(|current| current.replace(self.id())),
        };

        let Timer { due, mut job, .. } = timer;
        job.target.on_time(due);
        if let Some(next_due) = job.next_due_after(due) {
            self.add_timer(next_due, job);
        }
    }
}

struct Timer {
    /// Epoch milliseconds.
    due: i64,
    seq: u64,
    job: Job,
}

// Timers are ordered by due time, then by the order they were added.
impl Ord for Timer {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        (self.due, self.seq).cmp(&(other.due, other.seq))
    }
}
impl PartialOrd for Timer {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Timer {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == CmpOrdering::Equal
    }
}
impl Eq for Timer {}

/// What to call and when to call it again.
struct Job {
    target: Arc<dyn Schedulable>,
    repeat: Repeat,
    /// Calls left, or `None` for no limit.
    remaining: Option<usize>,
    /// The timer stops once this is false.
    running: Option<Arc<AtomicBool>>,
}

enum Repeat {
    Once,
    /// Period in milliseconds.
    Every(i64),
    Cron(Box<Schedule>),
}

impl Job {
    fn is_running(&self) -> bool {
        self.running
            .as_ref()
            .is_none_or(|flag| flag.load(Ordering::Acquire))
    }

    /// Count the run that was due at `due` and return when the next run is due, or
    /// `None` when the job is done.
    fn next_due_after(&mut self, due: i64) -> Option<i64> {
        if let Some(remaining) = self.remaining.as_mut() {
            *remaining = remaining.saturating_sub(1);
            if *remaining == 0 {
                return None;
            }
        }
        match &self.repeat {
            Repeat::Once => None,
            Repeat::Every(period_ms) => Some(due + period_ms),
            Repeat::Cron(schedule) => {
                let after = DateTime::<Utc>::from_timestamp_millis(due)?;
                schedule.after(&after).next().map(|t| t.timestamp_millis())
            }
        }
    }
}

/// Wall clock time in epoch milliseconds.
///
/// # Panics
/// Panics if the system clock is before the Unix epoch.
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System clock before Unix epoch - check system time configuration")
        .as_millis() as i64
}
