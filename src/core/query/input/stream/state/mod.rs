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

// Pattern processing foundation (Phase 1)
pub mod logical_post_state_processor;
pub mod logical_pre_state_processor; // AND/OR PreStateProcessor implementation
pub mod post_state_processor; // Pattern match handling interface (trait)
pub mod pre_state_processor; // Core pattern matching interface (trait)
pub mod shared_processor_state; // Lock-free shared state between Pre/Post processors
pub mod stream_post_state_processor; // Base implementation of PostStateProcessor
pub mod stream_pre_state; // Three-list state management for pattern processing
pub mod stream_pre_state_processor; // Base implementation of PreStateProcessor // AND/OR PostStateProcessor implementation

// Pattern processing Phase 2: Count Quantifiers
pub mod count_post_state_processor; // Count quantifier validation
pub mod count_pre_state_processor; // Count quantifier patterns (A{n}, A{m,n}, A+, A*)
pub mod pattern_chain_builder; // Pattern chain factory for multi-processor chains

// Runtime infrastructure (Week 6)
pub mod inner_state_runtime; // InnerStateRuntime trait for processor lifecycle
pub mod receiver;
pub mod state_stream_runtime; // StateStreamRuntime wrapper for InnerStateRuntime
pub mod stream_inner_state_runtime; // Basic InnerStateRuntime implementation // Stream receivers for Pattern/Sequence processing

// Utility components (preserved from cleanup)
pub mod timers; // timer_wheel for Phase 3 absent patterns (NOT operator)
pub mod util; // event_store for memory optimization (commit after profiling)

// Re-export pattern processing types
pub use logical_post_state_processor::LogicalPostStateProcessor;
pub use logical_pre_state_processor::{LogicalPreStateProcessor, LogicalType};
pub use post_state_processor::PostStateProcessor;
pub use pre_state_processor::PreStateProcessor;
pub use shared_processor_state::ProcessorSharedState;
pub use stream_post_state_processor::StreamPostStateProcessor;
pub use stream_pre_state::StreamPreState;
pub use stream_pre_state_processor::{StateType, StreamPreStateProcessor};

// Re-export Phase 2 types
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
