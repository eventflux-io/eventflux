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

// src/core/query/input/stream/state/inner_state_runtime.rs
// InnerStateRuntime trait, which runs the setup, reset and update steps of a processor chain

use crate::core::query::input::stream::state::post_state_processor::PostStateProcessor;
use crate::core::query::input::stream::state::pre_state_processor::PreStateProcessor;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// Holds a chain of PreStateProcessor and PostStateProcessor steps for a pattern or
/// sequence, and runs its setup, reset and update steps.
///
/// **Lifecycle Methods**:
/// - `init()` sets up the chain. Called once at startup.
/// - `reset()` clears state by calling `reset_state()` on the first processor.
/// - `update()` runs after event processing and calls `update_state()` on the first
///   processor.
pub trait InnerStateRuntime: Debug + Send {
    /// First PreStateProcessor in the chain, if set
    fn get_first_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>>;

    fn set_first_processor(&mut self, first_processor: Arc<Mutex<dyn PreStateProcessor>>);

    /// Last PostStateProcessor in the chain, if set
    fn get_last_processor(&self) -> Option<Arc<Mutex<dyn PostStateProcessor>>>;

    fn set_last_processor(&mut self, last_processor: Arc<Mutex<dyn PostStateProcessor>>);

    /// Set up the chain. Called once at startup.
    fn init(&mut self);

    /// Clear state after a match or expiry
    fn reset(&mut self);

    /// Move newly added states into the pending list after event processing
    fn update(&mut self);

    /// Return self as Any so callers can downcast to the concrete type
    fn as_any(&self) -> &dyn std::any::Any;
}
