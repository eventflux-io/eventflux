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

use crate::core::util::executor_service::ExecutorService;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug)]
pub struct ScheduledExecutorService {
    pub executor: Arc<ExecutorService>,
}

impl ScheduledExecutorService {
    pub fn new(executor: Arc<ExecutorService>) -> Self {
        Self { executor }
    }

    pub fn schedule<F>(&self, delay_ms: u64, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let _exec = Arc::clone(&self.executor);
        self.executor.execute(move || {
            std::thread::sleep(Duration::from_millis(delay_ms));
            task();
        });
    }
}
