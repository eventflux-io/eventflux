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

//! Pattern Chain Builder - Factory for creating multi-processor pattern chains
//!
//! Creates and wires CountPreStateProcessor chains for patterns like A{2} -> B{2} -> C{2}.
//! Each step in the chain is a separate processor with unique state_id.
//!
//! Reference: feat/pattern_processing/STATE_MACHINE_DESIGN.md

use super::count_post_state_processor::CountPostStateProcessor;
use super::count_pre_state_processor::CountPreStateProcessor;
use super::logical_post_state_processor::LogicalPostStateProcessor;
use super::logical_pre_state_processor::LogicalPreStateProcessor;
use super::post_state_processor::PostStateProcessor;
use super::pre_state_processor::PreStateProcessor;
use super::stream_pre_state_processor::StateType;
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use std::sync::{Arc, Mutex};

// Re-export LogicalType for public use
pub use super::logical_pre_state_processor::LogicalType;

/// Configuration for a logical group (AND/OR between two pattern steps)
#[derive(Debug, Clone)]
pub struct LogicalGroupConfig {
    /// Type of logical operation (AND or OR)
    pub logical_type: LogicalType,
    /// Left side of the logical expression
    pub left: PatternStepConfig,
    /// Right side of the logical expression
    pub right: PatternStepConfig,
}

impl LogicalGroupConfig {
    pub fn new(
        logical_type: LogicalType,
        left: PatternStepConfig,
        right: PatternStepConfig,
    ) -> Self {
        Self {
            logical_type,
            left,
            right,
        }
    }

    /// Create an AND group
    pub fn and(left: PatternStepConfig, right: PatternStepConfig) -> Self {
        Self::new(LogicalType::And, left, right)
    }

    /// Create an OR group
    pub fn or(left: PatternStepConfig, right: PatternStepConfig) -> Self {
        Self::new(LogicalType::Or, left, right)
    }

    /// Validate this logical group's constraints
    pub fn validate(&self) -> Result<(), String> {
        self.left.validate()?;
        self.right.validate()?;
        Ok(())
    }
}

/// A pattern element is either a simple step or a logical group
#[derive(Debug, Clone)]
pub enum PatternElement {
    /// A single pattern step (e.g., e1=TempStream{1,3})
    Step(PatternStepConfig),
    /// A logical group (e.g., (e1=A AND e2=B) or (e1=A OR e2=B))
    LogicalGroup(LogicalGroupConfig),
}

impl PatternElement {
    /// Get the number of state positions this element consumes
    pub fn state_count(&self) -> usize {
        match self {
            PatternElement::Step(_) => 1,
            PatternElement::LogicalGroup(_) => 2, // Left and right each get a position
        }
    }

    /// Validate this element
    pub fn validate(&self) -> Result<(), String> {
        match self {
            PatternElement::Step(step) => step.validate(),
            PatternElement::LogicalGroup(group) => group.validate(),
        }
    }
}

/// Configuration for a single pattern step
#[derive(Debug, Clone)]
pub struct PatternStepConfig {
    /// Event alias (e1, e2, etc.)
    pub alias: String,
    /// Stream name to match
    pub stream_name: String,
    /// Minimum count required
    pub min_count: usize,
    /// Maximum count allowed
    pub max_count: usize,
}

/// Sentinel value representing an unbounded/unspecified max_count.
/// Used by parsers to indicate patterns like A+, A{1,}, A{n,} where max is not specified.
pub const UNBOUNDED_MAX_COUNT: usize = usize::MAX;

impl PatternStepConfig {
    pub fn new(alias: String, stream_name: String, min_count: usize, max_count: usize) -> Self {
        Self {
            alias,
            stream_name,
            min_count,
            max_count,
        }
    }

    /// Validate this step's constraints
    ///
    /// Rules:
    /// 1. min_count >= 1 (all steps must match at least one event)
    /// 2. min_count <= max_count (logical consistency)
    /// 3. max_count must be bounded (not UNBOUNDED_MAX_COUNT)
    ///
    /// Rejected patterns:
    /// - A{0}, A{0,n}, A*, A? (zero-count: min_count must be >= 1)
    /// - A+, A{1,}, A{n,} (unbounded: max_count must be explicitly specified)
    pub fn validate(&self) -> Result<(), String> {
        if self.min_count == 0 {
            return Err(format!(
                "Step '{}': min_count must be >= 1 (got 0). Zero-count patterns (A*, A?, A{{0,n}}) are not supported.",
                self.alias
            ));
        }
        if self.max_count == UNBOUNDED_MAX_COUNT {
            return Err(format!(
                "Step '{}': max_count must be explicitly specified. \
                Unbounded patterns (A+, A{{1,}}, A{{n,}}) are not supported. Use A{{min,max}} with explicit bounds.",
                self.alias
            ));
        }
        if self.min_count > self.max_count {
            return Err(format!(
                "Step '{}': min_count ({}) cannot be greater than max_count ({})",
                self.alias, self.min_count, self.max_count
            ));
        }
        Ok(())
    }
}

/// Pattern chain builder for creating multi-processor chains
pub struct PatternChainBuilder {
    elements: Vec<PatternElement>,
    state_type: StateType,
    within_duration_ms: Option<i64>,
    is_every: bool,
}

impl PatternChainBuilder {
    pub fn new(state_type: StateType) -> Self {
        Self {
            elements: Vec::new(),
            state_type,
            within_duration_ms: None,
            is_every: false,
        }
    }

    /// Add a simple pattern step (e.g., e1=TempStream{1,3})
    pub fn add_step(&mut self, step: PatternStepConfig) {
        self.elements.push(PatternElement::Step(step));
    }

    /// Add a logical group (AND/OR between two steps)
    ///
    /// Example: For pattern `(e1=A AND e2=B) -> e3=C`
    /// ```ignore
    /// builder.add_logical_group(LogicalGroupConfig::and(
    ///     PatternStepConfig::new("e1".into(), "A".into(), 1, 1),
    ///     PatternStepConfig::new("e2".into(), "B".into(), 1, 1),
    /// ));
    /// builder.add_step(PatternStepConfig::new("e3".into(), "C".into(), 1, 1));
    /// ```
    pub fn add_logical_group(&mut self, group: LogicalGroupConfig) {
        self.elements.push(PatternElement::LogicalGroup(group));
    }

    pub fn set_within(&mut self, duration_ms: i64) {
        self.within_duration_ms = Some(duration_ms);
    }

    /// Get the total number of state positions (for calculating StateEvent size)
    pub fn total_state_count(&self) -> usize {
        self.elements.iter().map(|e| e.state_count()).sum()
    }

    /// Helper to get all steps (flattened) for backward compatibility
    fn get_all_steps(&self) -> Vec<&PatternStepConfig> {
        let mut steps = Vec::new();
        for element in &self.elements {
            match element {
                PatternElement::Step(step) => steps.push(step),
                PatternElement::LogicalGroup(group) => {
                    steps.push(&group.left);
                    steps.push(&group.right);
                }
            }
        }
        steps
    }

    /// Enable EVERY pattern (multi-instance matching with pattern restart)
    ///
    /// When enabled, completed patterns restart from the beginning, allowing
    /// new pattern instances after each match. For example:
    /// - Pattern: EVERY (A -> B)
    /// - Events: A(1) → B(2) → A(3) → B(4)
    /// - Matches: A1-B2 (completes, restarts) AND A3-B4 (new instance)
    ///
    /// Note: This implements "pattern restart" semantics, not simultaneous
    /// overlapping instances. Each match triggers a restart for the next sequence.
    ///
    /// # Restrictions
    /// - Only valid in PATTERN mode (not SEQUENCE)
    /// - Should only be applied to top-level patterns (no nested EVERY)
    pub fn set_every(&mut self, is_every: bool) {
        self.is_every = is_every;
    }

    /// Validate pattern chain constraints
    pub fn validate(&self) -> Result<(), String> {
        if self.elements.is_empty() {
            return Err("Pattern chain must have at least one element".to_string());
        }

        // Validate each element individually
        for element in &self.elements {
            element.validate()?;
        }

        // Get all steps (flattened) for validation
        let all_steps = self.get_all_steps();

        // All steps: min >= 1 (no zero-count steps allowed, including first step)
        for step in &all_steps {
            if step.min_count == 0 {
                return Err(format!(
                    "Step '{}' must have min_count >= 1 (got 0)",
                    step.alias
                ));
            }
        }

        // Last element validation: min == max (exact count)
        // For logical groups, both sides must have exact counts
        let last_element = &self.elements[self.elements.len() - 1];
        match last_element {
            PatternElement::Step(step) => {
                if step.min_count != step.max_count {
                    return Err(format!(
                        "Last step '{}' must have exact count (min=max), got min={} max={}",
                        step.alias, step.min_count, step.max_count
                    ));
                }
            }
            PatternElement::LogicalGroup(group) => {
                if group.left.min_count != group.left.max_count {
                    return Err(format!(
                        "Last logical group left '{}' must have exact count (min=max), got min={} max={}",
                        group.left.alias, group.left.min_count, group.left.max_count
                    ));
                }
                if group.right.min_count != group.right.max_count {
                    return Err(format!(
                        "Last logical group right '{}' must have exact count (min=max), got min={} max={}",
                        group.right.alias, group.right.min_count, group.right.max_count
                    ));
                }
            }
        }

        // All steps: min <= max (already enforced by PatternStepConfig.validate)

        // EVERY validation: Only allowed in PATTERN mode, not SEQUENCE
        if self.is_every && !matches!(self.state_type, StateType::Pattern) {
            return Err(
                "EVERY patterns are only supported in PATTERN mode, not SEQUENCE mode".to_string(),
            );
        }

        Ok(())
    }

    /// Build the processor chain
    ///
    /// Creates processors for each element and wires them together:
    /// - Simple steps: CountPreStateProcessor → CountPostStateProcessor
    /// - Logical groups: LogicalPreStateProcessor pairs → LogicalPostStateProcessor pairs
    ///
    /// Chain wiring: element[0] → element[1] → ... → element[n]
    pub fn build(
        self,
        app_context: Arc<EventFluxAppContext>,
        query_context: Arc<EventFluxQueryContext>,
    ) -> Result<ProcessorChain, String> {
        self.validate()?;

        // Collect all pre/post processors (as trait objects)
        let mut pre_processors: Vec<Arc<Mutex<dyn PreStateProcessor>>> = Vec::new();
        let mut post_processors: Vec<Arc<Mutex<dyn PostStateProcessor>>> = Vec::new();

        // Keep concrete CountPreStateProcessor refs for backward compatibility
        let mut pre_processors_concrete: Vec<Arc<Mutex<CountPreStateProcessor>>> = Vec::new();

        // Track current state_id across all elements
        let mut current_state_id: usize = 0;

        // Process each element
        for (elem_idx, element) in self.elements.iter().enumerate() {
            let is_first_element = elem_idx == 0;

            match element {
                PatternElement::Step(step) => {
                    // Create CountPreStateProcessor
                    let pre = Arc::new(Mutex::new(CountPreStateProcessor::new(
                        step.min_count,
                        step.max_count,
                        current_state_id,
                        is_first_element, // is_start_state
                        self.state_type,
                        app_context.clone(),
                        query_context.clone(),
                    )));

                    // Set WITHIN on first processor
                    if is_first_element {
                        if let Some(within_ms) = self.within_duration_ms {
                            pre.lock().unwrap().set_within_time(within_ms);
                        }
                    }

                    // Create CountPostStateProcessor
                    let post = Arc::new(Mutex::new(CountPostStateProcessor::new(
                        step.min_count,
                        step.max_count,
                        current_state_id,
                    )));

                    // Wire Pre -> Post
                    pre.lock()
                        .unwrap()
                        .stream_processor
                        .set_this_state_post_processor(
                            post.clone() as Arc<Mutex<dyn PostStateProcessor>>
                        );

                    // Store processors
                    pre_processors.push(pre.clone() as Arc<Mutex<dyn PreStateProcessor>>);
                    post_processors.push(post.clone() as Arc<Mutex<dyn PostStateProcessor>>);
                    pre_processors_concrete.push(pre);

                    current_state_id += 1;
                }
                PatternElement::LogicalGroup(group) => {
                    // Create LogicalPreStateProcessor pair (left and right)
                    let left_state_id = current_state_id;
                    let right_state_id = current_state_id + 1;

                    let left_pre = Arc::new(Mutex::new(LogicalPreStateProcessor::new(
                        left_state_id,
                        is_first_element, // is_start_state (left is the "entry" for the group)
                        group.logical_type,
                        self.state_type,
                        app_context.clone(),
                        query_context.clone(),
                    )));

                    let right_pre = Arc::new(Mutex::new(LogicalPreStateProcessor::new(
                        right_state_id,
                        is_first_element, // Both are start states for logical groups
                        group.logical_type,
                        self.state_type,
                        app_context.clone(),
                        query_context.clone(),
                    )));

                    // Wire partners together (bidirectional)
                    left_pre
                        .lock()
                        .unwrap()
                        .set_partner_processor(right_pre.clone());
                    right_pre
                        .lock()
                        .unwrap()
                        .set_partner_processor(left_pre.clone());

                    // Set WITHIN on first processors
                    if is_first_element {
                        if let Some(within_ms) = self.within_duration_ms {
                            left_pre.lock().unwrap().set_within_time(within_ms);
                            right_pre.lock().unwrap().set_within_time(within_ms);
                        }
                    }

                    // Create LogicalPostStateProcessor pair
                    let left_post = Arc::new(Mutex::new(LogicalPostStateProcessor::new(
                        left_state_id,
                        group.logical_type,
                    )));

                    let right_post = Arc::new(Mutex::new(LogicalPostStateProcessor::new(
                        right_state_id,
                        group.logical_type,
                    )));

                    // Wire partner post processors (for OR coordination)
                    left_post
                        .lock()
                        .unwrap()
                        .set_partner_post_processor(right_post.clone());
                    right_post
                        .lock()
                        .unwrap()
                        .set_partner_post_processor(left_post.clone());

                    // Wire partner pre processors (for AND checking)
                    left_post
                        .lock()
                        .unwrap()
                        .set_partner_pre_processor(right_pre.clone());
                    right_post
                        .lock()
                        .unwrap()
                        .set_partner_pre_processor(left_pre.clone());

                    // Wire this_state_pre_processor for each post
                    left_post.lock().unwrap().set_this_state_pre_processor(
                        left_pre.clone() as Arc<Mutex<dyn PreStateProcessor>>
                    );
                    right_post.lock().unwrap().set_this_state_pre_processor(
                        right_pre.clone() as Arc<Mutex<dyn PreStateProcessor>>
                    );

                    // Store processors - left side is the "main" entry point
                    pre_processors.push(left_pre.clone() as Arc<Mutex<dyn PreStateProcessor>>);
                    pre_processors.push(right_pre.clone() as Arc<Mutex<dyn PreStateProcessor>>);
                    post_processors.push(left_post.clone() as Arc<Mutex<dyn PostStateProcessor>>);
                    post_processors.push(right_post.clone() as Arc<Mutex<dyn PostStateProcessor>>);

                    // Note: We don't add logical processors to pre_processors_concrete
                    // as they're a different type. Tests should use pre_processors directly.

                    current_state_id += 2;
                }
            }
        }

        // Wire chain: Post[n] -> Pre[n+1] (for pattern chain forwarding)
        // For logical groups, both posts should forward to the same next pre
        let mut post_idx = 0;
        for (elem_idx, element) in self.elements.iter().enumerate() {
            let posts_for_element = element.state_count();

            // Find the next element's first pre processor
            if elem_idx + 1 < self.elements.len() {
                let next_pre_idx = self.elements[..=elem_idx]
                    .iter()
                    .map(|e| e.state_count())
                    .sum::<usize>();
                let next_pre = &pre_processors[next_pre_idx];

                // Wire all posts from this element to the next pre
                for i in 0..posts_for_element {
                    post_processors[post_idx + i]
                        .lock()
                        .unwrap()
                        .set_next_state_pre_processor(next_pre.clone());
                }
            }

            post_idx += posts_for_element;
        }

        // Wire EVERY loopback if enabled
        if self.is_every {
            // Get all posts from the last element
            let last_element_start_idx: usize = self.elements[..self.elements.len() - 1]
                .iter()
                .map(|e| e.state_count())
                .sum();
            let last_element_count = self.elements.last().unwrap().state_count();

            let first_pre = &pre_processors[0];

            // Set loopback on ALL posts from the last element
            for i in 0..last_element_count {
                post_processors[last_element_start_idx + i]
                    .lock()
                    .unwrap()
                    .set_next_every_state_pre_processor(first_pre.clone());
            }

            // Set EVERY flag on concrete CountPreStateProcessors
            // (LogicalPreStateProcessors don't need this flag - they handle EVERY differently)
            for pre in &pre_processors_concrete {
                pre.lock()
                    .unwrap()
                    .stream_processor
                    .set_every_pattern_flag(true);
            }
        }

        // Clone first processor before moving the vector
        let first_processor = pre_processors[0].clone();

        Ok(ProcessorChain {
            pre_processors,
            post_processors,
            first_processor,
            pre_processors_concrete,
        })
    }
}

/// Processor chain holding all wired processors
pub struct ProcessorChain {
    pub pre_processors: Vec<Arc<Mutex<dyn PreStateProcessor>>>,
    pub post_processors: Vec<Arc<Mutex<dyn PostStateProcessor>>>,
    pub first_processor: Arc<Mutex<dyn PreStateProcessor>>,
    // Keep concrete types for setup and test access
    pub pre_processors_concrete: Vec<Arc<Mutex<CountPreStateProcessor>>>,
}

impl ProcessorChain {
    /// Initialize all processors
    pub fn init(&mut self) {
        for pre in &self.pre_processors {
            pre.lock().unwrap().init();
        }
    }

    /// Set up stream and state event cloners for all processors
    ///
    /// Must be called before using the chain to process events
    pub fn setup_cloners(
        &mut self,
        stream_defs: Vec<Arc<crate::query_api::definition::stream_definition::StreamDefinition>>,
    ) {
        use crate::core::event::state::meta_state_event::MetaStateEvent;
        use crate::core::event::state::state_event_cloner::StateEventCloner;
        use crate::core::event::state::state_event_factory::StateEventFactory;
        use crate::core::event::stream::stream_event_cloner::StreamEventCloner;
        use crate::core::event::stream::stream_event_factory::StreamEventFactory;

        let num_steps = self.pre_processors_concrete.len();

        for (i, pre) in self.pre_processors_concrete.iter().enumerate() {
            // Set up stream event cloner
            let stream_def = if i < stream_defs.len() {
                stream_defs[i].clone()
            } else {
                stream_defs[0].clone() // Fallback to first def
            };

            // Use attribute count from stream definition for before_window_data size
            // (new_for_single_input puts attrs in output_data but we need before_window_data)
            let attr_count = stream_def.abstract_definition.attribute_list.len();
            let stream_factory = StreamEventFactory::new(attr_count, 0, 0);
            let stream_cloner = StreamEventCloner::new_with_sizes(attr_count, 0, 0, stream_factory);

            // Set up state event cloner
            let meta_state = MetaStateEvent::new(num_steps);
            let state_factory = StateEventFactory::new(num_steps, 0);
            let state_cloner = StateEventCloner::new(&meta_state, state_factory);

            // Set cloners on the processor
            let mut pre_locked = pre.lock().unwrap();
            pre_locked
                .stream_processor
                .set_stream_event_cloner(stream_cloner);
            pre_locked
                .stream_processor
                .set_state_event_cloner(state_cloner);
        }
    }

    /// Expire events in all processors
    pub fn expire_events(&mut self, timestamp: i64) {
        for pre in &self.pre_processors {
            pre.lock().unwrap().expire_events(timestamp);
        }
    }

    /// Update state in all processors (moves new_list to pending_list)
    ///
    /// Must be called after events are processed to ensure forwarded states
    /// are moved from new_list to pending_list in all processors
    pub fn update_state(&mut self) {
        for pre in &self.pre_processors {
            pre.lock().unwrap().update_state();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_contexts() -> (Arc<EventFluxAppContext>, Arc<EventFluxQueryContext>) {
        let app_ctx = Arc::new(EventFluxAppContext::default_for_testing());
        let query_ctx = Arc::new(EventFluxQueryContext::new(
            app_ctx.clone(),
            "test_query".to_string(),
            None,
        ));
        (app_ctx, query_ctx)
    }

    #[test]
    fn test_pattern_step_config_creation() {
        let step = PatternStepConfig::new("e1".to_string(), "TempStream".to_string(), 1, 3);
        assert_eq!(step.alias, "e1");
        assert_eq!(step.stream_name, "TempStream");
        assert_eq!(step.min_count, 1);
        assert_eq!(step.max_count, 3);
    }

    #[test]
    fn test_pattern_step_config_validation_success() {
        let step = PatternStepConfig::new("e1".to_string(), "S".to_string(), 2, 5);
        assert!(step.validate().is_ok());
    }

    #[test]
    fn test_pattern_step_config_validation_fail_min_greater_than_max() {
        let step = PatternStepConfig::new("e1".to_string(), "S".to_string(), 5, 2);
        let result = step.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("min_count"));
    }

    #[test]
    fn test_pattern_step_config_validation_fail_min_zero() {
        let step = PatternStepConfig::new("e1".to_string(), "S".to_string(), 0, 2);
        let result = step.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("min_count must be >= 1"));
    }

    #[test]
    fn test_pattern_step_config_validation_fail_unbounded_max() {
        // Test A+ pattern (unbounded max_count using sentinel value)
        let step =
            PatternStepConfig::new("e1".to_string(), "S".to_string(), 1, UNBOUNDED_MAX_COUNT);
        let result = step.validate();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("must be explicitly specified"),
            "Error should mention explicit specification required: {}",
            err
        );
        assert!(
            err.contains("Unbounded patterns"),
            "Error should mention unbounded patterns: {}",
            err
        );
    }

    #[test]
    fn test_pattern_step_config_validation_success_large_bounded_max() {
        // Test large but explicitly bounded max_count (should succeed)
        let step = PatternStepConfig::new("e1".to_string(), "S".to_string(), 1, 100_000);
        assert!(
            step.validate().is_ok(),
            "Large but explicit max_count should be allowed"
        );
    }

    #[test]
    fn test_pattern_step_config_validation_success_explicit_bounds() {
        // Test explicitly bounded pattern A{1,10} (should succeed)
        let step = PatternStepConfig::new("e1".to_string(), "S".to_string(), 1, 10);
        assert!(step.validate().is_ok());
    }

    #[test]
    fn test_pattern_chain_builder_creation() {
        let builder = PatternChainBuilder::new(StateType::Sequence);
        assert_eq!(builder.elements.len(), 0);
    }

    #[test]
    fn test_pattern_chain_builder_add_step() {
        let mut builder = PatternChainBuilder::new(StateType::Sequence);
        builder.add_step(PatternStepConfig::new(
            "e1".to_string(),
            "A".to_string(),
            2,
            2,
        ));
        assert_eq!(builder.elements.len(), 1);
    }

    #[test]
    fn test_pattern_chain_validation_empty() {
        let builder = PatternChainBuilder::new(StateType::Sequence);
        let result = builder.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("at least one element"));
    }

    #[test]
    fn test_pattern_chain_validation_first_step_min_zero() {
        let mut builder = PatternChainBuilder::new(StateType::Sequence);
        builder.add_step(PatternStepConfig::new(
            "e1".to_string(),
            "A".to_string(),
            0,
            2,
        ));
        builder.add_step(PatternStepConfig::new(
            "e2".to_string(),
            "B".to_string(),
            2,
            2,
        ));
        let result = builder.validate();
        assert!(result.is_err());
        // Error is caught at PatternStepConfig.validate() level before chain-level validation
        assert!(result.unwrap_err().contains("min_count must be >= 1"));
    }

    #[test]
    fn test_pattern_chain_validation_last_step_not_exact() {
        let mut builder = PatternChainBuilder::new(StateType::Sequence);
        builder.add_step(PatternStepConfig::new(
            "e1".to_string(),
            "A".to_string(),
            2,
            2,
        ));
        builder.add_step(PatternStepConfig::new(
            "e2".to_string(),
            "B".to_string(),
            1,
            5,
        ));
        let result = builder.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Last step"));
    }

    #[test]
    fn test_pattern_chain_validation_success() {
        let mut builder = PatternChainBuilder::new(StateType::Sequence);
        builder.add_step(PatternStepConfig::new(
            "e1".to_string(),
            "A".to_string(),
            2,
            2,
        ));
        builder.add_step(PatternStepConfig::new(
            "e2".to_string(),
            "B".to_string(),
            0,
            2,
        ));
        builder.add_step(PatternStepConfig::new(
            "e3".to_string(),
            "C".to_string(),
            2,
            2,
        ));
        // This should fail because middle step has min=0
        let result = builder.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_pattern_chain_validation_success_all_exact() {
        let mut builder = PatternChainBuilder::new(StateType::Sequence);
        builder.add_step(PatternStepConfig::new(
            "e1".to_string(),
            "A".to_string(),
            2,
            2,
        ));
        builder.add_step(PatternStepConfig::new(
            "e2".to_string(),
            "B".to_string(),
            2,
            2,
        ));
        builder.add_step(PatternStepConfig::new(
            "e3".to_string(),
            "C".to_string(),
            2,
            2,
        ));
        let result = builder.validate();
        assert!(result.is_ok());
    }

    #[test]
    fn test_processor_chain_build_success() {
        let (app_ctx, query_ctx) = create_test_contexts();

        let mut builder = PatternChainBuilder::new(StateType::Sequence);
        builder.add_step(PatternStepConfig::new(
            "e1".to_string(),
            "A".to_string(),
            2,
            2,
        ));
        builder.add_step(PatternStepConfig::new(
            "e2".to_string(),
            "B".to_string(),
            2,
            2,
        ));

        let result = builder.build(app_ctx, query_ctx);
        assert!(result.is_ok());

        let chain = result.unwrap();
        assert_eq!(chain.pre_processors.len(), 2);
        assert_eq!(chain.post_processors.len(), 2);
    }

    #[test]
    fn test_processor_chain_build_fail_invalid() {
        let (app_ctx, query_ctx) = create_test_contexts();

        let mut builder = PatternChainBuilder::new(StateType::Sequence);
        builder.add_step(PatternStepConfig::new(
            "e1".to_string(),
            "A".to_string(),
            0,
            2,
        ));
        builder.add_step(PatternStepConfig::new(
            "e2".to_string(),
            "B".to_string(),
            2,
            2,
        ));

        let result = builder.build(app_ctx, query_ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_processor_chain_with_within() {
        let (app_ctx, query_ctx) = create_test_contexts();

        let mut builder = PatternChainBuilder::new(StateType::Sequence);
        builder.set_within(5000); // 5 seconds
        builder.add_step(PatternStepConfig::new(
            "e1".to_string(),
            "A".to_string(),
            2,
            2,
        ));
        builder.add_step(PatternStepConfig::new(
            "e2".to_string(),
            "B".to_string(),
            2,
            2,
        ));

        let result = builder.build(app_ctx, query_ctx);
        assert!(result.is_ok());
    }

    // ===== LogicalGroupConfig Tests =====

    #[test]
    fn test_logical_group_config_creation() {
        let left = PatternStepConfig::new("e1".to_string(), "A".to_string(), 1, 1);
        let right = PatternStepConfig::new("e2".to_string(), "B".to_string(), 1, 1);
        let group = LogicalGroupConfig::new(LogicalType::And, left.clone(), right.clone());

        assert_eq!(group.logical_type, LogicalType::And);
        assert_eq!(group.left.alias, "e1");
        assert_eq!(group.right.alias, "e2");
    }

    #[test]
    fn test_logical_group_config_and_helper() {
        let left = PatternStepConfig::new("e1".to_string(), "A".to_string(), 1, 1);
        let right = PatternStepConfig::new("e2".to_string(), "B".to_string(), 1, 1);
        let group = LogicalGroupConfig::and(left, right);

        assert_eq!(group.logical_type, LogicalType::And);
    }

    #[test]
    fn test_logical_group_config_or_helper() {
        let left = PatternStepConfig::new("e1".to_string(), "A".to_string(), 1, 1);
        let right = PatternStepConfig::new("e2".to_string(), "B".to_string(), 1, 1);
        let group = LogicalGroupConfig::or(left, right);

        assert_eq!(group.logical_type, LogicalType::Or);
    }

    #[test]
    fn test_logical_group_config_validation_success() {
        let left = PatternStepConfig::new("e1".to_string(), "A".to_string(), 1, 1);
        let right = PatternStepConfig::new("e2".to_string(), "B".to_string(), 1, 1);
        let group = LogicalGroupConfig::and(left, right);

        assert!(group.validate().is_ok());
    }

    #[test]
    fn test_logical_group_config_validation_fail_left_invalid() {
        let left = PatternStepConfig::new("e1".to_string(), "A".to_string(), 5, 2); // invalid: min > max
        let right = PatternStepConfig::new("e2".to_string(), "B".to_string(), 1, 1);
        let group = LogicalGroupConfig::and(left, right);

        let result = group.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("min_count"));
    }

    #[test]
    fn test_logical_group_config_validation_fail_right_invalid() {
        let left = PatternStepConfig::new("e1".to_string(), "A".to_string(), 1, 1);
        let right = PatternStepConfig::new("e2".to_string(), "B".to_string(), 0, 1); // invalid: min = 0
        let group = LogicalGroupConfig::and(left, right);

        let result = group.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("min_count must be >= 1"));
    }

    // ===== PatternElement Tests =====

    #[test]
    fn test_pattern_element_step_state_count() {
        let step = PatternStepConfig::new("e1".to_string(), "A".to_string(), 1, 1);
        let element = PatternElement::Step(step);

        assert_eq!(element.state_count(), 1);
    }

    #[test]
    fn test_pattern_element_logical_group_state_count() {
        let left = PatternStepConfig::new("e1".to_string(), "A".to_string(), 1, 1);
        let right = PatternStepConfig::new("e2".to_string(), "B".to_string(), 1, 1);
        let group = LogicalGroupConfig::and(left, right);
        let element = PatternElement::LogicalGroup(group);

        assert_eq!(element.state_count(), 2); // Left and right each get a position
    }

    // ===== Builder with LogicalGroup Tests =====

    #[test]
    fn test_add_logical_group() {
        let mut builder = PatternChainBuilder::new(StateType::Pattern);
        let group = LogicalGroupConfig::and(
            PatternStepConfig::new("e1".to_string(), "A".to_string(), 1, 1),
            PatternStepConfig::new("e2".to_string(), "B".to_string(), 1, 1),
        );
        builder.add_logical_group(group);

        assert_eq!(builder.elements.len(), 1);
        assert_eq!(builder.total_state_count(), 2);
    }

    #[test]
    fn test_total_state_count_mixed_elements() {
        let mut builder = PatternChainBuilder::new(StateType::Pattern);

        // Add a simple step (1 state)
        builder.add_step(PatternStepConfig::new(
            "e1".to_string(),
            "A".to_string(),
            1,
            1,
        ));

        // Add a logical group (2 states)
        builder.add_logical_group(LogicalGroupConfig::and(
            PatternStepConfig::new("e2".to_string(), "B".to_string(), 1, 1),
            PatternStepConfig::new("e3".to_string(), "C".to_string(), 1, 1),
        ));

        assert_eq!(builder.elements.len(), 2);
        assert_eq!(builder.total_state_count(), 3); // 1 + 2
    }

    #[test]
    fn test_build_chain_with_logical_group() {
        let (app_ctx, query_ctx) = create_test_contexts();

        let mut builder = PatternChainBuilder::new(StateType::Pattern);

        // Pattern: (A AND B)
        builder.add_logical_group(LogicalGroupConfig::and(
            PatternStepConfig::new("e1".to_string(), "A".to_string(), 1, 1),
            PatternStepConfig::new("e2".to_string(), "B".to_string(), 1, 1),
        ));

        let result = builder.build(app_ctx, query_ctx);
        assert!(result.is_ok());

        let chain = result.unwrap();
        // Logical group creates 2 pre and 2 post processors
        assert_eq!(chain.pre_processors.len(), 2);
        assert_eq!(chain.post_processors.len(), 2);
        // But no concrete CountPreStateProcessors (logical uses LogicalPreStateProcessor)
        assert_eq!(chain.pre_processors_concrete.len(), 0);
    }

    #[test]
    fn test_build_chain_with_step_then_logical_group() {
        let (app_ctx, query_ctx) = create_test_contexts();

        let mut builder = PatternChainBuilder::new(StateType::Pattern);

        // Pattern: A -> (B AND C)
        builder.add_step(PatternStepConfig::new(
            "e1".to_string(),
            "A".to_string(),
            1,
            1,
        ));
        builder.add_logical_group(LogicalGroupConfig::and(
            PatternStepConfig::new("e2".to_string(), "B".to_string(), 1, 1),
            PatternStepConfig::new("e3".to_string(), "C".to_string(), 1, 1),
        ));

        let result = builder.build(app_ctx, query_ctx);
        assert!(result.is_ok());

        let chain = result.unwrap();
        // 1 step + 2 from logical group = 3 pre and 3 post processors
        assert_eq!(chain.pre_processors.len(), 3);
        assert_eq!(chain.post_processors.len(), 3);
        // Only 1 concrete CountPreStateProcessor (from the step)
        assert_eq!(chain.pre_processors_concrete.len(), 1);
    }

    #[test]
    fn test_build_chain_with_logical_group_then_step() {
        let (app_ctx, query_ctx) = create_test_contexts();

        let mut builder = PatternChainBuilder::new(StateType::Pattern);

        // Pattern: (A AND B) -> C
        builder.add_logical_group(LogicalGroupConfig::and(
            PatternStepConfig::new("e1".to_string(), "A".to_string(), 1, 1),
            PatternStepConfig::new("e2".to_string(), "B".to_string(), 1, 1),
        ));
        builder.add_step(PatternStepConfig::new(
            "e3".to_string(),
            "C".to_string(),
            1,
            1,
        ));

        let result = builder.build(app_ctx, query_ctx);
        assert!(result.is_ok());

        let chain = result.unwrap();
        // 2 from logical group + 1 step = 3 pre and 3 post processors
        assert_eq!(chain.pre_processors.len(), 3);
        assert_eq!(chain.post_processors.len(), 3);
        // Only 1 concrete CountPreStateProcessor (from the step at the end)
        assert_eq!(chain.pre_processors_concrete.len(), 1);
    }

    #[test]
    fn test_build_chain_with_or_group() {
        let (app_ctx, query_ctx) = create_test_contexts();

        let mut builder = PatternChainBuilder::new(StateType::Pattern);

        // Pattern: (A OR B)
        builder.add_logical_group(LogicalGroupConfig::or(
            PatternStepConfig::new("e1".to_string(), "A".to_string(), 1, 1),
            PatternStepConfig::new("e2".to_string(), "B".to_string(), 1, 1),
        ));

        let result = builder.build(app_ctx, query_ctx);
        assert!(result.is_ok());

        let chain = result.unwrap();
        assert_eq!(chain.pre_processors.len(), 2);
        assert_eq!(chain.post_processors.len(), 2);
    }

    #[test]
    fn test_validation_logical_group_last_element_not_exact() {
        let mut builder = PatternChainBuilder::new(StateType::Pattern);

        // Last element is a logical group where left has min != max
        builder.add_logical_group(LogicalGroupConfig::and(
            PatternStepConfig::new("e1".to_string(), "A".to_string(), 1, 3), // not exact!
            PatternStepConfig::new("e2".to_string(), "B".to_string(), 1, 1),
        ));

        let result = builder.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must have exact count"));
    }

    #[test]
    fn test_validation_logical_group_middle_element_ok() {
        let mut builder = PatternChainBuilder::new(StateType::Pattern);

        // Middle element is a logical group with range counts (allowed)
        builder.add_logical_group(LogicalGroupConfig::and(
            PatternStepConfig::new("e1".to_string(), "A".to_string(), 1, 3),
            PatternStepConfig::new("e2".to_string(), "B".to_string(), 1, 3),
        ));
        // Last step must be exact
        builder.add_step(PatternStepConfig::new(
            "e3".to_string(),
            "C".to_string(),
            1,
            1,
        ));

        let result = builder.validate();
        assert!(result.is_ok());
    }
}
