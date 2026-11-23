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

//! Pattern chain builder, which builds and wires the processors for a multi step pattern.
//!
//! Handles patterns like A{2} -> B{2} -> C{2}, and AND/OR groups. Each simple step
//! gets its own CountPreStateProcessor and CountPostStateProcessor with a unique
//! `state_id`. Each logical group gets a pair of logical processors and two state ids.

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

pub use super::logical_pre_state_processor::LogicalType;

/// Settings for a logical group, an AND or OR between two pattern steps
#[derive(Debug, Clone)]
pub struct LogicalGroupConfig {
    pub logical_type: LogicalType,
    pub left: PatternStepConfig,
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

    pub fn and(left: PatternStepConfig, right: PatternStepConfig) -> Self {
        Self::new(LogicalType::And, left, right)
    }

    pub fn or(left: PatternStepConfig, right: PatternStepConfig) -> Self {
        Self::new(LogicalType::Or, left, right)
    }

    /// Validate both sides. Returns the first side's error message.
    pub fn validate(&self) -> Result<(), String> {
        self.left.validate()?;
        self.right.validate()?;
        Ok(())
    }
}

/// A pattern element is either a simple step or a logical group
#[derive(Debug, Clone)]
pub enum PatternElement {
    /// A single step, for example e1=TempStream{1,3}
    Step(PatternStepConfig),
    /// A logical group, for example (e1=A AND e2=B) or (e1=A OR e2=B)
    LogicalGroup(LogicalGroupConfig),
}

impl PatternElement {
    /// Number of StateEvent positions this element takes. A group takes one per side.
    pub fn state_count(&self) -> usize {
        match self {
            PatternElement::Step(_) => 1,
            PatternElement::LogicalGroup(_) => 2,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        match self {
            PatternElement::Step(step) => step.validate(),
            PatternElement::LogicalGroup(group) => group.validate(),
        }
    }
}

/// Settings for a single pattern step
#[derive(Debug, Clone)]
pub struct PatternStepConfig {
    /// Event alias such as e1 or e2
    pub alias: String,
    /// Stream this step matches
    pub stream_name: String,
    /// Fewest events this step needs
    pub min_count: usize,
    /// Most events this step accepts
    pub max_count: usize,
}

/// Marker for a max_count with no upper bound.
/// Parsers set it for patterns like A+, A{1,} and A{n,} that give no max.
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

    /// Check the step's counts. Returns an error message naming the step on failure.
    ///
    /// Rules:
    /// 1. min_count >= 1, so every step matches at least one event
    /// 2. max_count is bounded, so it is not UNBOUNDED_MAX_COUNT
    /// 3. min_count <= max_count
    ///
    /// Rejected patterns:
    /// - A{0}, A{0,n}, A*, A? (zero count, min_count must be >= 1)
    /// - A+, A{1,}, A{n,} (no upper bound, max_count must be given)
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

/// Collects pattern elements and builds the wired processor chain for them
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

    /// Add a simple step, for example e1=TempStream{1,3}
    pub fn add_step(&mut self, step: PatternStepConfig) {
        self.elements.push(PatternElement::Step(step));
    }

    /// Add a logical group, an AND or OR between two steps
    ///
    /// Example for the pattern `(e1=A AND e2=B) -> e3=C`
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

    /// Set the WITHIN limit in milliseconds. It is applied to the first element's
    /// processors.
    pub fn set_within(&mut self, duration_ms: i64) {
        self.within_duration_ms = Some(duration_ms);
    }

    /// Total StateEvent positions across all elements, which sizes the StateEvent
    pub fn total_state_count(&self) -> usize {
        self.elements.iter().map(|e| e.state_count()).sum()
    }

    /// All steps in order, with each logical group split into its left and right step
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

    /// Turn EVERY on or off, so the pattern starts again after each match.
    ///
    /// With EVERY on, a finished match loops back to the first step so a new instance
    /// can begin. For example:
    /// - Pattern: EVERY (A -> B)
    /// - Events: A(1) → B(2) → A(3) → B(4)
    /// - Matches: A1-B2 (finishes and restarts) and A3-B4 (new instance)
    ///
    /// Each match restarts the pattern. Whole patterns do not run as overlapping
    /// instances, though a count step at the start can still open overlapping windows
    /// (see CountPreStateProcessor).
    ///
    /// # Restrictions
    /// - Only valid in PATTERN mode, not SEQUENCE. `validate` rejects it otherwise.
    /// - Meant only for top level patterns, with no nested EVERY
    pub fn set_every(&mut self, is_every: bool) {
        self.is_every = is_every;
    }

    /// Check the whole chain. Returns an error message for the first rule broken.
    ///
    /// Rules:
    /// - At least one element
    /// - Every step passes PatternStepConfig::validate
    /// - The last element has exact counts (min == max). For a group, both sides do.
    /// - EVERY is only used in PATTERN mode
    pub fn validate(&self) -> Result<(), String> {
        if self.elements.is_empty() {
            return Err("Pattern chain must have at least one element".to_string());
        }

        for element in &self.elements {
            element.validate()?;
        }

        let all_steps = self.get_all_steps();

        // Already covered by element.validate above, kept as a second guard
        for step in &all_steps {
            if step.min_count == 0 {
                return Err(format!(
                    "Step '{}' must have min_count >= 1 (got 0)",
                    step.alias
                ));
            }
        }

        // The last element needs exact counts (min == max). For a group, both sides do.
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

        // min <= max for every step is already checked by PatternStepConfig::validate

        if self.is_every && !matches!(self.state_type, StateType::Pattern) {
            return Err(
                "EVERY patterns are only supported in PATTERN mode, not SEQUENCE mode".to_string(),
            );
        }

        Ok(())
    }

    /// Validate the chain, then build and wire its processors.
    ///
    /// - A simple step gets a CountPreStateProcessor and a CountPostStateProcessor
    /// - A logical group gets a pair of LogicalPreStateProcessors and a pair of
    ///   LogicalPostStateProcessors, linked as partners
    ///
    /// Elements are chained in order, element[0] → element[1] → ... → element[n]. With
    /// EVERY on, the last element's post processors loop back to the first pre processor.
    ///
    /// # Errors
    /// Returns the `validate` error message when the chain is not valid.
    pub fn build(
        self,
        app_context: Arc<EventFluxAppContext>,
        query_context: Arc<EventFluxQueryContext>,
    ) -> Result<ProcessorChain, String> {
        self.validate()?;

        let mut pre_processors: Vec<Arc<Mutex<dyn PreStateProcessor>>> = Vec::new();
        let mut post_processors: Vec<Arc<Mutex<dyn PostStateProcessor>>> = Vec::new();

        // Concrete handles to the count pre processors. setup_cloners and the EVERY flag
        // need the concrete type.
        let mut pre_processors_concrete: Vec<Arc<Mutex<CountPreStateProcessor>>> = Vec::new();

        let mut current_state_id: usize = 0;

        for (elem_idx, element) in self.elements.iter().enumerate() {
            let is_first_element = elem_idx == 0;

            match element {
                PatternElement::Step(step) => {
                    let pre = Arc::new(Mutex::new(CountPreStateProcessor::new(
                        step.min_count,
                        step.max_count,
                        current_state_id,
                        is_first_element, // is_start_state
                        self.state_type,
                        app_context.clone(),
                        query_context.clone(),
                    )));

                    // WITHIN applies only to the first element
                    if is_first_element {
                        if let Some(within_ms) = self.within_duration_ms {
                            pre.lock().unwrap().set_within_time(within_ms);
                        }
                    }

                    let post = Arc::new(Mutex::new(CountPostStateProcessor::new(
                        step.min_count,
                        step.max_count,
                        current_state_id,
                    )));

                    pre.lock()
                        .unwrap()
                        .stream_processor
                        .set_this_state_post_processor(
                            post.clone() as Arc<Mutex<dyn PostStateProcessor>>
                        );

                    pre_processors.push(pre.clone() as Arc<Mutex<dyn PreStateProcessor>>);
                    post_processors.push(post.clone() as Arc<Mutex<dyn PostStateProcessor>>);
                    pre_processors_concrete.push(pre);

                    current_state_id += 1;
                }
                PatternElement::LogicalGroup(group) => {
                    let left_state_id = current_state_id;
                    let right_state_id = current_state_id + 1;

                    // In a first element group, both sides are start states
                    let left_pre = Arc::new(Mutex::new(LogicalPreStateProcessor::new(
                        left_state_id,
                        is_first_element, // is_start_state
                        group.logical_type,
                        self.state_type,
                        app_context.clone(),
                        query_context.clone(),
                    )));

                    let right_pre = Arc::new(Mutex::new(LogicalPreStateProcessor::new(
                        right_state_id,
                        is_first_element, // is_start_state
                        group.logical_type,
                        self.state_type,
                        app_context.clone(),
                        query_context.clone(),
                    )));

                    // Link the two sides both ways
                    left_pre
                        .lock()
                        .unwrap()
                        .set_partner_processor(right_pre.clone());
                    right_pre
                        .lock()
                        .unwrap()
                        .set_partner_processor(left_pre.clone());

                    // WITHIN applies only to the first element, so both sides here
                    if is_first_element {
                        if let Some(within_ms) = self.within_duration_ms {
                            left_pre.lock().unwrap().set_within_time(within_ms);
                            right_pre.lock().unwrap().set_within_time(within_ms);
                        }
                    }

                    let left_post = Arc::new(Mutex::new(LogicalPostStateProcessor::new(
                        left_state_id,
                        group.logical_type,
                    )));

                    let right_post = Arc::new(Mutex::new(LogicalPostStateProcessor::new(
                        right_state_id,
                        group.logical_type,
                    )));

                    // OR uses the partner post processor to set its returned flag
                    left_post
                        .lock()
                        .unwrap()
                        .set_partner_post_processor(right_post.clone());
                    right_post
                        .lock()
                        .unwrap()
                        .set_partner_post_processor(left_post.clone());

                    // AND uses the partner pre processor to find the partner position
                    left_post
                        .lock()
                        .unwrap()
                        .set_partner_pre_processor(right_pre.clone());
                    right_post
                        .lock()
                        .unwrap()
                        .set_partner_pre_processor(left_pre.clone());

                    left_post.lock().unwrap().set_this_state_pre_processor(
                        left_pre.clone() as Arc<Mutex<dyn PreStateProcessor>>
                    );
                    right_post.lock().unwrap().set_this_state_pre_processor(
                        right_pre.clone() as Arc<Mutex<dyn PreStateProcessor>>
                    );

                    // Left goes first, so the left pre processor is the group's entry point
                    pre_processors.push(left_pre.clone() as Arc<Mutex<dyn PreStateProcessor>>);
                    pre_processors.push(right_pre.clone() as Arc<Mutex<dyn PreStateProcessor>>);
                    post_processors.push(left_post.clone() as Arc<Mutex<dyn PostStateProcessor>>);
                    post_processors.push(right_post.clone() as Arc<Mutex<dyn PostStateProcessor>>);

                    // Logical processors are a different type, so they are not added to
                    // pre_processors_concrete. Tests should use pre_processors.

                    current_state_id += 2;
                }
            }
        }

        // Chain each element's post processors to the next element's first pre processor.
        // Both posts of a logical group lead to the same next pre.
        let mut post_idx = 0;
        for (elem_idx, element) in self.elements.iter().enumerate() {
            let posts_for_element = element.state_count();

            if elem_idx + 1 < self.elements.len() {
                let next_pre_idx = self.elements[..=elem_idx]
                    .iter()
                    .map(|e| e.state_count())
                    .sum::<usize>();
                let next_pre = &pre_processors[next_pre_idx];

                for i in 0..posts_for_element {
                    post_processors[post_idx + i]
                        .lock()
                        .unwrap()
                        .set_next_state_pre_processor(next_pre.clone());
                }
            }

            post_idx += posts_for_element;
        }

        // With EVERY on, loop every post processor of the last element back to the first
        // pre processor
        if self.is_every {
            let last_element_start_idx: usize = self.elements[..self.elements.len() - 1]
                .iter()
                .map(|e| e.state_count())
                .sum();
            let last_element_count = self.elements.last().unwrap().state_count();

            let first_pre = &pre_processors[0];

            for i in 0..last_element_count {
                post_processors[last_element_start_idx + i]
                    .lock()
                    .unwrap()
                    .set_next_every_state_pre_processor(first_pre.clone());
            }

            // Only count pre processors get the EVERY flag. Logical pre processors handle
            // EVERY in their own add_every_state.
            for pre in &pre_processors_concrete {
                pre.lock()
                    .unwrap()
                    .stream_processor
                    .set_every_pattern_flag(true);
            }
        }

        let first_processor = pre_processors[0].clone();

        Ok(ProcessorChain {
            pre_processors,
            post_processors,
            first_processor,
            pre_processors_concrete,
        })
    }
}

/// All processors of a built and wired pattern chain
pub struct ProcessorChain {
    /// Pre processors in StateEvent position order
    pub pre_processors: Vec<Arc<Mutex<dyn PreStateProcessor>>>,
    /// Post processors in StateEvent position order
    pub post_processors: Vec<Arc<Mutex<dyn PostStateProcessor>>>,
    pub first_processor: Arc<Mutex<dyn PreStateProcessor>>,
    // Concrete count pre processors, kept for setup and test access. Logical processors
    // are not in this list.
    pub pre_processors_concrete: Vec<Arc<Mutex<CountPreStateProcessor>>>,
}

impl ProcessorChain {
    /// Call init on every pre processor
    pub fn init(&mut self) {
        for pre in &self.pre_processors {
            pre.lock().unwrap().init();
        }
    }

    /// Give each count pre processor its stream and state event cloners.
    ///
    /// Call it before the chain processes events. The count pre processor at index `i`
    /// uses `stream_defs[i]`, or `stream_defs[0]` when there are fewer definitions.
    /// Panics when `stream_defs` is empty and there is at least one count pre processor.
    /// Logical pre processors are skipped, since they are not in
    /// `pre_processors_concrete`.
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
            let stream_def = if i < stream_defs.len() {
                stream_defs[i].clone()
            } else {
                stream_defs[0].clone()
            };

            // Size before_window_data from the stream's attribute count.
            // new_for_single_input would put the attributes in output_data instead.
            let attr_count = stream_def.abstract_definition.attribute_list.len();
            let stream_factory = StreamEventFactory::new(attr_count, 0, 0);
            let stream_cloner = StreamEventCloner::new_with_sizes(attr_count, 0, 0, stream_factory);

            let meta_state = MetaStateEvent::new(num_steps);
            let state_factory = StateEventFactory::new(num_steps, 0);
            let state_cloner = StateEventCloner::new(&meta_state, state_factory);

            let mut pre_locked = pre.lock().unwrap();
            pre_locked
                .stream_processor
                .set_stream_event_cloner(stream_cloner);
            pre_locked
                .stream_processor
                .set_state_event_cloner(state_cloner);
        }
    }

    /// Call expire_events with `timestamp` on every pre processor
    pub fn expire_events(&mut self, timestamp: i64) {
        for pre in &self.pre_processors {
            pre.lock().unwrap().expire_events(timestamp);
        }
    }

    /// Call update_state on every pre processor, which moves its new list to pending.
    ///
    /// Call it after events are processed, so states forwarded to later steps become
    /// pending there and can match the next event.
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
        // PatternStepConfig::validate catches this before the chain checks run
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
