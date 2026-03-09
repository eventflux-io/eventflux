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
