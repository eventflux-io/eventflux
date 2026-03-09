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

use crate::core::config::{EventFluxAppContext, EventFluxContext};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicI64, AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

static LATENCY_BY_STREAM: Lazy<Mutex<HashMap<String, Arc<LatencyTracker>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static THROUGHPUT_BY_STREAM: Lazy<Mutex<HashMap<String, Arc<ThroughputTracker>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static COUNTERS: Lazy<Mutex<HashMap<String, Arc<Counter>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static TIMERS: Lazy<Mutex<HashMap<String, Arc<Timer>>>> = Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Default)]
pub struct LatencyTracker {
    total_ns: AtomicU64,
    count: AtomicU64,
}

impl Clone for LatencyTracker {
    fn clone(&self) -> Self {
        Self {
            total_ns: AtomicU64::new(self.total_ns.load(Ordering::Relaxed)),
            count: AtomicU64::new(self.count.load(Ordering::Relaxed)),
        }
    }
}

impl LatencyTracker {
    pub fn new(name: &str, _ctx: &EventFluxAppContext) -> Arc<Self> {
        let tracker = Arc::new(Self::default());
        LATENCY_BY_STREAM
            .lock()
            .unwrap()
            .insert(name.to_string(), Arc::clone(&tracker));
        tracker
    }
    pub fn add_latency(&self, dur: Duration) {
        self.total_ns
            .fetch_add(dur.as_nanos() as u64, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }
    pub fn average_latency_ns(&self) -> Option<u64> {
        let c = self.count.load(Ordering::Relaxed);
        if c == 0 {
            None
        } else {
            Some(self.total_ns.load(Ordering::Relaxed) / c)
        }
    }
}

#[derive(Debug, Default)]
pub struct ThroughputTracker {
    count: AtomicU64,
}

impl Clone for ThroughputTracker {
    fn clone(&self) -> Self {
        Self {
            count: AtomicU64::new(self.count.load(Ordering::Relaxed)),
        }
    }
}

impl ThroughputTracker {
    pub fn new(name: &str, _ctx: &EventFluxAppContext) -> Arc<Self> {
        let tracker = Arc::new(Self::default());
        THROUGHPUT_BY_STREAM
            .lock()
            .unwrap()
            .insert(name.to_string(), Arc::clone(&tracker));
        tracker
    }
    pub fn event_in(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }
    pub fn events_in(&self, n: u32) {
        self.count.fetch_add(n as u64, Ordering::Relaxed);
    }
    pub fn total_events(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Default)]
pub struct BufferedEventsTracker {
    count: AtomicI64,
}

#[derive(Debug, Default)]
pub struct Counter {
    count: AtomicU64,
}

impl Counter {
    pub fn new(name: &str, _ctx: &EventFluxContext) -> Arc<Self> {
        let c = Arc::new(Self::default());
        COUNTERS
            .lock()
            .unwrap()
            .insert(name.to_string(), Arc::clone(&c));
        c
    }

    pub fn inc(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(&self, v: u64) {
        self.count.fetch_add(v, Ordering::Relaxed);
    }

    pub fn value(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Default)]
pub struct Timer {
    total_ns: AtomicU64,
    count: AtomicU64,
}

impl Timer {
    pub fn new(name: &str, _ctx: &EventFluxContext) -> Arc<Self> {
        let t = Arc::new(Self::default());
        TIMERS
            .lock()
            .unwrap()
            .insert(name.to_string(), Arc::clone(&t));
        t
    }

    pub fn record(&self, dur: Duration) {
        self.total_ns
            .fetch_add(dur.as_nanos() as u64, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn average_ns(&self) -> Option<u64> {
        let c = self.count.load(Ordering::Relaxed);
        if c == 0 {
            None
        } else {
            Some(self.total_ns.load(Ordering::Relaxed) / c)
        }
    }
}

impl Clone for BufferedEventsTracker {
    fn clone(&self) -> Self {
        Self {
            count: AtomicI64::new(self.count.load(Ordering::Relaxed)),
        }
    }
}

impl BufferedEventsTracker {
    pub fn new(_ctx: &EventFluxAppContext, _name: &str, _is_snapshotable: bool) -> Self {
        Self::default()
    }
    pub fn increment_event_count_by(&self, n: i32) {
        self.count.fetch_add(n as i64, Ordering::Relaxed);
    }
    pub fn get_size(&self) -> i64 {
        self.count.load(Ordering::Relaxed)
    }
}

/// Retrieve the average latency in nanoseconds for a stream if metrics exist.
pub fn get_stream_latency(stream_id: &str) -> Option<u64> {
    LATENCY_BY_STREAM
        .lock()
        .unwrap()
        .get(stream_id)
        .and_then(|t| t.average_latency_ns())
}

/// Retrieve the total number of events processed for a stream if metrics exist.
pub fn get_stream_throughput(stream_id: &str) -> Option<u64> {
    THROUGHPUT_BY_STREAM
        .lock()
        .unwrap()
        .get(stream_id)
        .map(|t| t.total_events())
}
