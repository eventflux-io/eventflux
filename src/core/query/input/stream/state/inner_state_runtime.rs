// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/input/stream/state/inner_state_runtime.rs
// InnerStateRuntime trait for managing PreStateProcessor lifecycle

use crate::core::query::input::stream::state::post_state_processor::PostStateProcessor;
use crate::core::query::input::stream::state::pre_state_processor::PreStateProcessor;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// InnerStateRuntime manages the lifecycle of PreStateProcessor and PostStateProcessor chains.
///
/// **Purpose**: Coordinates initialization, reset, and update operations for pattern/sequence processing.
///
/// **Lifecycle Methods**:
/// - `init()` - Initialize the processor chain (called once at startup)
/// - `reset()` - Reset all state (calls resetState() on first processor)
/// - `update()` - Update state after event processing (calls updateState() on first processor)
pub trait InnerStateRuntime: Debug + Send {
    /// Get the first PreStateProcessor in the chain
    fn get_first_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>>;

    /// Set the first PreStateProcessor in the chain
    fn set_first_processor(&mut self, first_processor: Arc<Mutex<dyn PreStateProcessor>>);

    /// Get the last PostStateProcessor in the chain
    fn get_last_processor(&self) -> Option<Arc<Mutex<dyn PostStateProcessor>>>;

    /// Set the last PostStateProcessor in the chain
    fn set_last_processor(&mut self, last_processor: Arc<Mutex<dyn PostStateProcessor>>);

    /// Initialize the runtime (called once at startup)
    fn init(&mut self);

    /// Reset all state (clears pending events, resets processors)
    fn reset(&mut self);

    /// Update state after event processing (processes pending events)
    fn update(&mut self);

    /// Get a reference to self as Any for downcasting
    fn as_any(&self) -> &dyn std::any::Any;
}
