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

// src/core/window/window_runtime.rs

use crate::core::query::processor::Processor;
use crate::query_api::definition::WindowDefinition;
use std::sync::{Arc, Mutex};

/// Minimal runtime representation of a window.
#[derive(Debug)]
pub struct WindowRuntime {
    pub definition: Arc<WindowDefinition>,
    pub processor: Option<Arc<Mutex<dyn Processor>>>,
    initialized: bool,
}

impl WindowRuntime {
    pub fn new(definition: Arc<WindowDefinition>) -> Self {
        Self {
            definition,
            processor: None,
            initialized: false,
        }
    }

    pub fn set_processor(&mut self, processor: Arc<Mutex<dyn Processor>>) {
        self.processor = Some(processor);
    }

    /// Perform one-time initialization for the window processor if present.
    pub fn initialize(&mut self) {
        if self.initialized {
            return;
        }
        if let Some(proc) = &self.processor {
            // Placeholder for processor-specific initialization logic.
            let _guard = proc.lock().unwrap();
        }
        self.initialized = true;
    }
}
