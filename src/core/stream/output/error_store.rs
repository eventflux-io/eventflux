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

use crate::core::exception::error::EventFluxError;
use std::sync::Mutex;

pub trait ErrorStore: Send + Sync + std::fmt::Debug {
    fn store(&self, stream_id: &str, error: EventFluxError);
}

#[derive(Default, Debug)]
pub struct InMemoryErrorStore {
    inner: Mutex<Vec<(String, String)>>, // Store error messages as strings
}

impl InMemoryErrorStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn errors(&self) -> Vec<(String, String)> {
        self.inner.lock().unwrap().clone()
    }

    pub fn clear(&mut self) {
        self.inner.lock().unwrap().clear();
    }
}

impl ErrorStore for InMemoryErrorStore {
    fn store(&self, stream_id: &str, error: EventFluxError) {
        self.inner
            .lock()
            .unwrap()
            .push((stream_id.to_string(), error.to_string()));
    }
}
