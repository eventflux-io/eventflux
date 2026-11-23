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

// count_post_state_processor.rs - post processor for count quantifiers like A{2,5}

use super::post_state_processor::PostStateProcessor;
use super::pre_state_processor::PreStateProcessor;
use super::stream_post_state_processor::StreamPostStateProcessor;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::state::state_event::StateEvent;
use std::sync::{Arc, Mutex};

/// Post processor for a count quantifier step such as `A{2,5}`.
///
/// CountPreStateProcessor checks the count range before it calls this processor. This
/// processor only forwards the matched StateEvent to the next processors through the
/// wrapped StreamPostStateProcessor. It keeps `min_count` and `max_count` so callers
/// can read the range back.
pub struct CountPostStateProcessor {
    /// Fewest events this step needs
    min_count: usize,

    /// Most events this step accepts
    max_count: usize,

    /// Wrapped stream post processor. Most trait methods delegate to it.
    stream_processor: StreamPostStateProcessor,
}

impl CountPostStateProcessor {
    /// Create a count post processor.
    ///
    /// **Parameters**:
    /// - `min_count`: Fewest events this step needs
    /// - `max_count`: Most events this step accepts
    /// - `state_id`: Position of this step in the pattern
    pub fn new(min_count: usize, max_count: usize, state_id: usize) -> Self {
        Self {
            min_count,
            max_count,
            stream_processor: StreamPostStateProcessor::new(state_id),
        }
    }

    pub fn get_min_count(&self) -> usize {
        self.min_count
    }

    pub fn get_max_count(&self) -> usize {
        self.max_count
    }

    /// Count the StreamEvents chained at this step's `state_id` position.
    ///
    /// Returns 0 when the position is empty. A link that is not a StreamEvent is still
    /// counted, but the walk stops there. Only used by tests today.
    ///
    /// **Example**:
    /// For pattern `A{2,3}` with events e1, e2, e3 at position 0:
    /// ```text
    /// stream_events[0] = e1 -> e2 -> e3
    /// count_events_in_chain() = 3
    /// ```
    #[allow(dead_code)]
    fn count_events_in_chain(&self, state_event: &StateEvent) -> usize {
        use crate::core::event::stream::stream_event::StreamEvent;

        let state_id = self.stream_processor.state_id();

        let mut current = match state_event.get_stream_event(state_id) {
            Some(se) => se,
            None => return 0,
        };

        let mut count = 1;

        while let Some(next) = current.get_next() {
            count += 1;
            // Count patterns only chain StreamEvents, so this branch should always match
            if let Some(se) = next.as_any().downcast_ref::<StreamEvent>() {
                current = se;
            } else {
                break;
            }
        }

        count
    }

    /// Forward a matched StateEvent to the next processors.
    ///
    /// No range check happens here. CountPreStateProcessor calls this only after it has
    /// checked that the count is in `[min, max]`.
    fn process_count_validation(&mut self, state_event: &StateEvent) {
        self.stream_processor.process_state_event(state_event);
    }
}

impl PostStateProcessor for CountPostStateProcessor {
    fn process(&mut self, chunk: Option<Box<dyn ComplexEvent>>) -> Option<Box<dyn ComplexEvent>> {
        if let Some(event) = chunk.as_ref() {
            if let Some(state_event) = event.as_any().downcast_ref::<StateEvent>() {
                self.process_count_validation(state_event);
            }
        }
        // Hand the chunk back so the caller can pass it on
        chunk
    }

    fn set_next_state_pre_processor(&mut self, processor: Arc<Mutex<dyn PreStateProcessor>>) {
        self.stream_processor
            .set_next_state_pre_processor(processor);
    }

    fn set_callback_pre_state_processor(&mut self, processor: Arc<Mutex<dyn PreStateProcessor>>) {
        self.stream_processor
            .set_callback_pre_state_processor(processor);
    }

    fn set_next_every_state_pre_processor(&mut self, processor: Arc<Mutex<dyn PreStateProcessor>>) {
        self.stream_processor
            .set_next_every_state_pre_processor(processor);
    }

    fn get_next_every_state_pre_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>> {
        self.stream_processor.get_next_every_state_pre_processor()
    }

    fn set_next_processor(&mut self, processor: Arc<Mutex<dyn PostStateProcessor>>) {
        self.stream_processor.set_next_processor(processor);
    }

    fn get_next_processor(&self) -> Option<Arc<Mutex<dyn PostStateProcessor>>> {
        self.stream_processor.get_next_processor()
    }

    fn state_id(&self) -> usize {
        self.stream_processor.state_id()
    }

    fn is_event_returned(&self) -> bool {
        self.stream_processor.is_event_returned()
    }

    fn clear_processed_event(&mut self) {
        self.stream_processor.clear_processed_event();
    }

    fn this_state_pre_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>> {
        self.stream_processor.this_state_pre_processor()
    }
}

impl std::fmt::Debug for CountPostStateProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CountPostStateProcessor")
            .field("min_count", &self.min_count)
            .field("max_count", &self.max_count)
            .field("state_id", &self.stream_processor.state_id())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_post_processor_creation() {
        let proc = CountPostStateProcessor::new(2, 5, 0);
        assert_eq!(proc.get_min_count(), 2);
        assert_eq!(proc.get_max_count(), 5);
        assert_eq!(proc.state_id(), 0);
    }

    #[test]
    fn test_exactly_n() {
        let proc = CountPostStateProcessor::new(3, 3, 0);
        assert_eq!(proc.get_min_count(), 3);
        assert_eq!(proc.get_max_count(), 3);
    }

    #[test]
    fn test_range() {
        let proc = CountPostStateProcessor::new(2, 5, 1);
        assert_eq!(proc.get_min_count(), 2);
        assert_eq!(proc.get_max_count(), 5);
        assert_eq!(proc.state_id(), 1);
    }

    #[test]
    fn test_one_or_more() {
        let proc = CountPostStateProcessor::new(1, i32::MAX as usize, 0);
        assert_eq!(proc.get_min_count(), 1);
        assert_eq!(proc.get_max_count(), i32::MAX as usize);
    }

    #[test]
    fn test_count_empty_chain() {
        let proc = CountPostStateProcessor::new(2, 5, 0);
        let state_event = StateEvent::new(1, 0); // Empty StateEvent
        let count = proc.count_events_in_chain(&state_event);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_debug_impl() {
        let proc = CountPostStateProcessor::new(2, 5, 0);
        let debug_str = format!("{:?}", proc);
        assert!(debug_str.contains("CountPostStateProcessor"));
        assert!(debug_str.contains("min_count: 2"));
        assert!(debug_str.contains("max_count: 5"));
    }
}
