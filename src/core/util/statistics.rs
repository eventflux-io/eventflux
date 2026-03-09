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

use std::sync::atomic::{AtomicBool, Ordering};

/// Basic trait matching the Java `StatisticsManager` interface.
pub trait StatisticsManager: Send + Sync {
    fn start_reporting(&self);
    fn stop_reporting(&self);
    fn cleanup(&self);
}

/// Simple in-memory statistics manager used for tests.
#[derive(Debug, Default)]
pub struct DefaultStatisticsManager {
    running: AtomicBool,
}

impl DefaultStatisticsManager {
    pub fn new() -> Self {
        Self {
            running: AtomicBool::new(false),
        }
    }
}

impl StatisticsManager for DefaultStatisticsManager {
    fn start_reporting(&self) {
        self.running.store(true, Ordering::Relaxed);
    }

    fn stop_reporting(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    fn cleanup(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}
