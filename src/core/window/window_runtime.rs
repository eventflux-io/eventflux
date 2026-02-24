// SPDX-License-Identifier: MIT OR Apache-2.0

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
