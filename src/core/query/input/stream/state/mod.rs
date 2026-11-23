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

// Core pattern processors
pub mod logical_post_state_processor; // AND/OR PostStateProcessor
pub mod logical_pre_state_processor; // AND/OR PreStateProcessor
pub mod post_state_processor; // Trait for the step that handles a match
pub mod pre_state_processor; // Trait for the step that matches events
pub mod shared_processor_state; // Changed flag a pre and post processor share without locks
pub mod stream_post_state_processor; // Default PostStateProcessor
pub mod stream_pre_state; // The new, pending and every state lists of one step
pub mod stream_pre_state_processor; // Default PreStateProcessor

// Count quantifiers
pub mod count_post_state_processor; // Forwards count matches
pub mod count_pre_state_processor; // Count quantifiers (A{n}, A{m,n}, A+, A*, A?)
pub mod pattern_chain_builder; // Builds and wires multi step processor chains

// Runtime
pub mod inner_state_runtime; // InnerStateRuntime trait for the chain lifecycle
pub mod receiver; // Stream receivers for patterns and sequences
pub mod state_stream_runtime; // StateStreamRuntime, which wraps an InnerStateRuntime
pub mod stream_inner_state_runtime; // Default InnerStateRuntime

// Helpers not used by the engine yet
pub mod timers; // TimerWheel, meant for absent patterns (NOT operator)
pub mod util; // EventStore, meant to cut memory use. Not wired in yet.

// Re-export pattern processing types
pub use logical_post_state_processor::LogicalPostStateProcessor;
pub use logical_pre_state_processor::{LogicalPreStateProcessor, LogicalType};
pub use post_state_processor::PostStateProcessor;
pub use pre_state_processor::PreStateProcessor;
pub use shared_processor_state::ProcessorSharedState;
pub use stream_post_state_processor::StreamPostStateProcessor;
pub use stream_pre_state::StreamPreState;
pub use stream_pre_state_processor::{StateType, StreamPreStateProcessor};

// Re-export count quantifier and chain builder types
pub use count_post_state_processor::CountPostStateProcessor;
pub use count_pre_state_processor::CountPreStateProcessor;
pub use pattern_chain_builder::{PatternChainBuilder, PatternStepConfig, ProcessorChain};

// Re-export runtime types
pub use inner_state_runtime::InnerStateRuntime;
pub use state_stream_runtime::StateStreamRuntime;
pub use stream_inner_state_runtime::StreamInnerStateRuntime;

// Re-export utility types
pub use timers::TimerWheel;
pub use util::{EventId, EventStore};
