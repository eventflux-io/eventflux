// SPDX-License-Identifier: MIT OR Apache-2.0

//! Scheduler for time-based event triggering
//!
//! # Threading Model
//!
//! The Scheduler uses a **dedicated thread pool** separate from event processing to avoid
//! blocking event processing threads. Scheduling tasks use `std::thread::sleep` for delays,
//! which is acceptable because:
//!
//! - Scheduler has its own isolated ExecutorService (rayon ThreadPool)
//! - Blocking sleep in scheduler threads doesn't affect event processing throughput
//! - Event processing uses lock-free crossbeam queues and non-blocking operations
//!
//! # Configuration
//!
//! The scheduler thread pool size can be configured via:
//! - `EVENTFLUX_EXECUTOR_THREADS` environment variable (for default executor)
//! - Or by providing a custom `ExecutorService` with desired thread count
//!
//! Default: Uses `num_cpus::get()` threads for maximum scheduling parallelism

use crate::core::util::executor_service::ExecutorService;
use chrono::Utc;
use cron::Schedule;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub trait Schedulable: Send + Sync {
    fn on_time(&self, timestamp: i64);
}

/// Scheduler for time-based event triggering with dedicated thread pool
///
/// **Important:** This scheduler uses a dedicated ExecutorService to prevent blocking
/// event processing threads. See module documentation for threading details.
#[derive(Debug, Clone)]
pub struct Scheduler {
    executor: Arc<ExecutorService>,
}

impl Scheduler {
    pub fn new(executor: Arc<ExecutorService>) -> Self {
        Self { executor }
    }

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

    pub fn shutdown(&self) {
        self.executor.wait_all();
    }
}
