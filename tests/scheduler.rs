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

use eventflux::core::util::{ExecutorService, Schedulable, Scheduler};
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct Counter {
    count: Arc<Mutex<i32>>,
}

impl Schedulable for Counter {
    fn on_time(&self, _timestamp: i64) {
        let mut c = self.count.lock().unwrap();
        *c += 1;
    }
}

#[test]
fn test_periodic_scheduler() {
    let exec = Arc::new(ExecutorService::new("test", 2));
    let scheduler = Scheduler::new(Arc::clone(&exec));
    let counter = Counter {
        count: Arc::new(Mutex::new(0)),
    };
    let count_arc = Arc::clone(&counter.count);
    scheduler.schedule_periodic(50, Arc::new(counter), Some(3), None);
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(*count_arc.lock().unwrap(), 3);
}

#[test]
fn test_cron_scheduler() {
    let exec = Arc::new(ExecutorService::new("cron", 1));
    let scheduler = Scheduler::new(Arc::clone(&exec));
    let counter = Counter {
        count: Arc::new(Mutex::new(0)),
    };
    let count_arc = Arc::clone(&counter.count);
    scheduler
        .schedule_cron("*/1 * * * * *", Arc::new(counter), Some(2), None)
        .unwrap();
    std::thread::sleep(Duration::from_millis(2500));
    assert_eq!(*count_arc.lock().unwrap(), 2);
}

/// Counts calls, and tracks how many calls run at once.
#[derive(Default)]
struct Probe {
    calls: std::sync::atomic::AtomicUsize,
    running: std::sync::atomic::AtomicUsize,
    max_running: std::sync::atomic::AtomicUsize,
    sleep_ms: u64,
}

impl Schedulable for Probe {
    fn on_time(&self, _timestamp: i64) {
        use std::sync::atomic::Ordering::SeqCst;
        let now_running = self.running.fetch_add(1, SeqCst) + 1;
        self.max_running.fetch_max(now_running, SeqCst);
        std::thread::sleep(Duration::from_millis(self.sleep_ms));
        self.running.fetch_sub(1, SeqCst);
        self.calls.fetch_add(1, SeqCst);
    }
}

fn calls(probe: &Probe) -> usize {
    probe.calls.load(std::sync::atomic::Ordering::SeqCst)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Waiting timers hold no pool thread, so periodic timers cannot starve each other or a
/// one shot timer, even with a single pool thread (#74).
#[test]
fn test_waiting_timers_do_not_starve_a_small_pool() {
    let scheduler = Scheduler::new(Arc::new(ExecutorService::new("one", 1)));
    let periodic_a = Arc::new(Probe::default());
    let periodic_b = Arc::new(Probe::default());
    let once = Arc::new(Probe::default());
    scheduler.schedule_periodic(20, periodic_a.clone(), None, None);
    scheduler.schedule_periodic(20, periodic_b.clone(), None, None);
    scheduler.notify_at(now_ms() + 50, once.clone());

    std::thread::sleep(Duration::from_millis(300));
    scheduler.shutdown();
    assert!(calls(&periodic_a) >= 5, "a: {}", calls(&periodic_a));
    assert!(calls(&periodic_b) >= 5, "b: {}", calls(&periodic_b));
    assert_eq!(calls(&once), 1);
}

/// The runs of one periodic timer never overlap, even when a run takes longer than the
/// period.
#[test]
fn test_periodic_runs_do_not_overlap() {
    let scheduler = Scheduler::new(Arc::new(ExecutorService::new("pool", 4)));
    let slow = Arc::new(Probe {
        sleep_ms: 30,
        ..Probe::default()
    });
    scheduler.schedule_periodic(10, slow.clone(), Some(4), None);

    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(calls(&slow), 4);
    assert_eq!(
        slow.max_running.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

/// Pause stops timers firing. Resume fires the ones that came due while paused.
#[test]
fn test_pause_holds_timers_until_resume() {
    let scheduler = Scheduler::new(Arc::new(ExecutorService::new("pool", 2)));
    let probe = Arc::new(Probe::default());
    scheduler.pause();
    scheduler.notify_at(now_ms() + 20, probe.clone());

    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(calls(&probe), 0, "a paused scheduler must not fire");

    scheduler.resume();
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(calls(&probe), 1, "resume fires the overdue timer");
}

/// Pause waits for a running callback, so nothing fires after it returns.
#[test]
fn test_pause_waits_for_running_callbacks() {
    let scheduler = Scheduler::new(Arc::new(ExecutorService::new("pool", 2)));
    let slow = Arc::new(Probe {
        sleep_ms: 100,
        ..Probe::default()
    });
    scheduler.notify_at(0, slow.clone());
    std::thread::sleep(Duration::from_millis(20)); // let the callback start
    scheduler.pause();
    assert_eq!(calls(&slow), 1, "pause must wait for the running callback");
}

/// Shutdown drops pending timers and ignores timers added after it, so their targets
/// are released and never called (#75).
#[test]
fn test_shutdown_drops_pending_and_later_timers() {
    let scheduler = Scheduler::new(Arc::new(ExecutorService::new("pool", 2)));
    let pending = Arc::new(Probe::default());
    let periodic_running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    scheduler.notify_at(now_ms() + 50, pending.clone());
    scheduler.schedule_periodic(10, pending.clone(), None, Some(periodic_running));

    scheduler.shutdown();
    let late = Arc::new(Probe::default());
    scheduler.notify_at(0, late.clone());

    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(calls(&pending), 0);
    assert_eq!(calls(&late), 0);
    assert_eq!(
        Arc::strong_count(&pending),
        1,
        "shutdown must release pending targets"
    );
    assert_eq!(
        Arc::strong_count(&late),
        1,
        "a timer added after shutdown is dropped"
    );
}

/// A timer whose running flag is turned off makes no more calls.
#[test]
fn test_running_flag_stops_a_periodic_timer() {
    let scheduler = Scheduler::new(Arc::new(ExecutorService::new("pool", 2)));
    let probe = Arc::new(Probe::default());
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    scheduler.schedule_periodic(10, probe.clone(), None, Some(running.clone()));

    std::thread::sleep(Duration::from_millis(60));
    running.store(false, std::sync::atomic::Ordering::SeqCst);
    std::thread::sleep(Duration::from_millis(20)); // let a run in progress finish
    let after_stop = calls(&probe);
    std::thread::sleep(Duration::from_millis(60));
    assert!(after_stop > 0);
    assert_eq!(calls(&probe), after_stop);
}
