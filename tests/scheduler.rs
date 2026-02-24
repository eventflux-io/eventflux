// SPDX-License-Identifier: MIT OR Apache-2.0

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
    scheduler.schedule_periodic(50, Arc::new(counter), Some(3));
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
        .schedule_cron("*/1 * * * * *", Arc::new(counter), Some(2))
        .unwrap();
    std::thread::sleep(Duration::from_millis(2500));
    assert_eq!(*count_arc.lock().unwrap(), 2);
}
