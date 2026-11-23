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
//! The runtime gives the Scheduler its **own thread pool**, apart from event processing.
//! Scheduled tasks wait with `std::thread::sleep`, which is fine because:
//!
//! - The Scheduler has its own ExecutorService (a rayon ThreadPool)
//! - A sleeping scheduler thread does not slow event processing
//! - Event processing uses lock free crossbeam queues and calls that do not block
//!
//! Each scheduled task holds one pool thread for as long as it runs, and periodic and
//! cron tasks run until stopped or their limit is reached. So more long running tasks
//! than pool threads will delay the extra ones.
//!
//! # Configuration
//!
//! Set the pool size with:
//! - the `EVENTFLUX_EXECUTOR_THREADS` environment variable (for the default executor)
//! - or a custom `ExecutorService` with the thread count you want
//!
//! Default: `num_cpus::get()` threads

use crate::core::util::executor_service::ExecutorService;
use chrono::Utc;
use cron::Schedule;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Something the Scheduler calls back when its time comes.
pub trait Schedulable: Send + Sync {
    /// Called with the scheduled time in epoch milliseconds, which can be slightly
    /// earlier than the real time of the call.
    fn on_time(&self, timestamp: i64);
}

/// Scheduler for time based event triggering on its own thread pool
///
/// **Important:** Give this scheduler its own ExecutorService so its sleeps never block
/// event processing threads. See the module docs for threading details.
#[derive(Debug, Clone)]
pub struct Scheduler {
    executor: Arc<ExecutorService>,
}

impl Scheduler {
    pub fn new(executor: Arc<ExecutorService>) -> Self {
        Self { executor }
    }

    /// Calls `target.on_time(timestamp)` once at `timestamp` (epoch milliseconds), or
    /// right away if that time has passed.
    ///
    /// # Panics
    /// The task panics if the system clock is before the Unix epoch.
    pub fn notify_at(&self, timestamp: i64, target: Arc<dyn Schedulable>) {
        self.executor.execute(move || {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("System clock before Unix epoch - check system time configuration")
                .as_millis() as i64;
            let delay = if timestamp > now { timestamp - now } else { 0 } as u64;
            std::thread::sleep(Duration::from_millis(delay));
            target.on_time(timestamp);
        });
    }

    /// Calls `target.on_time` every `period_ms` milliseconds, starting one period from
    /// now. Stops after `limit` calls if given, or once `running` is false. `running` is
    /// checked before each wait, so a call already waiting still runs after a stop.
    pub fn schedule_periodic(
        &self,
        period_ms: i64,
        target: Arc<dyn Schedulable>,
        limit: Option<usize>,
        running: Option<Arc<AtomicBool>>,
    ) {
        self.executor.execute(move || {
            let mut next = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("System clock before Unix epoch - check system time configuration")
                .as_millis() as i64
                + period_ms;
            let mut count = 0usize;
            loop {
                if let Some(ref flag) = running {
                    if !flag.load(Ordering::Acquire) {
                        break;
                    }
                }
                if let Some(lim) = limit {
                    if count >= lim {
                        break;
                    }
                }
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("System clock before Unix epoch - check system time configuration")
                    .as_millis() as i64;
                if next > now {
                    std::thread::sleep(Duration::from_millis((next - now) as u64));
                }
                target.on_time(next);
                count += 1;
                next += period_ms;
            }
        });
    }

    /// Calls `target.on_time` at each upcoming time of `cron_expr`, in UTC. Stops after
    /// `limit` calls if given, or once `running` is false. `running` is checked before
    /// each wait, so a call already waiting still runs after a stop.
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
        self.executor.execute(move || {
            let iter = schedule.upcoming(Utc);
            let it: Box<dyn Iterator<Item = chrono::DateTime<Utc>>> = match limit {
                Some(l) => Box::new(iter.take(l)),
                None => Box::new(iter),
            };
            for datetime in it {
                if let Some(ref flag) = running {
                    if !flag.load(Ordering::Acquire) {
                        break;
                    }
                }
                let ts = datetime.timestamp_millis();
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("System clock before Unix epoch - check system time configuration")
                    .as_millis() as i64;
                if ts > now {
                    std::thread::sleep(Duration::from_millis((ts - now) as u64));
                }
                target.on_time(ts);
            }
        });
        Ok(())
    }

    /// Calls `ExecutorService::wait_all`, which does nothing today. Tasks already
    /// scheduled keep running. Stop periodic and cron tasks through their `running` flag.
    pub fn shutdown(&self) {
        self.executor.wait_all();
    }
}
