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

// src/core/query/input/stream/state/stream_pre_state.rs
// The three state lists of one pattern step

use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::state::state_event::StateEvent;
use crate::core::util::snapshot::state::State;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};

/// Holds the three StateEvent lists of one pattern step:
/// 1. current_state_event_chunk: the chunk being worked on (Vec, for fast indexing)
/// 2. pending_state_event_list: states waiting for a matching event (VecDeque, first
///    in first out)
/// 3. new_and_every_state_event_list: new states and 'every' states not yet pending
///    (VecDeque, first in first out)
///
/// Keeping new states apart from pending ones means a state added while an event is
/// processed is only tried against later events. Used for patterns across streams,
/// count quantifiers (A{2,5}) and 'every' patterns.
#[derive(Debug, Default)]
pub struct StreamPreState {
    current_state_event_chunk: Vec<StateEvent>,

    /// States waiting for a matching event. Each batch moved in from the new list is
    /// sorted by timestamp.
    pending_state_event_list: VecDeque<StateEvent>,

    /// New states and 'every' states, moved to pending by move_new_to_pending
    new_and_every_state_event_list: VecDeque<StateEvent>,

    /// Set when the state changes
    state_changed: bool,

    /// Set once a start state has created its first StateEvent
    initialized: bool,
}

impl StreamPreState {
    /// Create a StreamPreState with empty lists and both flags cleared
    pub fn new() -> Self {
        Self {
            current_state_event_chunk: Vec::new(),
            pending_state_event_list: VecDeque::new(),
            new_and_every_state_event_list: VecDeque::new(),
            state_changed: false,
            initialized: false,
        }
    }

    // ===== List Operations =====

    /// Add a state to the end of the new list. Used for states passed on from the
    /// previous step and for 'every' restarts.
    pub fn add_to_new_list(&mut self, event: StateEvent) {
        self.new_and_every_state_event_list.push_back(event);
    }

    /// Add a state straight to the end of the pending list, skipping the new list
    pub fn add_to_pending_list(&mut self, event: StateEvent) {
        self.pending_state_event_list.push_back(event);
    }

    /// Move every state from the new list to the end of the pending list.
    ///
    /// The moved states are sorted by timestamp, oldest first, so matching follows
    /// time order. States already pending are not sorted again.
    pub fn move_new_to_pending(&mut self) {
        let mut new_events: Vec<_> = self.new_and_every_state_event_list.drain(..).collect();

        new_events.sort_by_key(|e| e.get_timestamp());

        for event in new_events {
            self.pending_state_event_list.push_back(event);
        }
    }

    pub fn clear_pending(&mut self) {
        self.pending_state_event_list.clear();
    }

    pub fn clear_new(&mut self) {
        self.new_and_every_state_event_list.clear();
    }

    pub fn clear_current(&mut self) {
        self.current_state_event_chunk.clear();
    }

    /// Empty all three lists. The flags are left as they are.
    pub fn clear_all(&mut self) {
        self.clear_current();
        self.clear_pending();
        self.clear_new();
    }

    // ===== Accessors =====

    pub fn get_pending_list(&self) -> &VecDeque<StateEvent> {
        &self.pending_state_event_list
    }

    /// Mutable pending list, so processors can edit it in place while they process
    pub fn get_pending_list_mut(&mut self) -> &mut VecDeque<StateEvent> {
        &mut self.pending_state_event_list
    }

    pub fn get_new_list(&self) -> &VecDeque<StateEvent> {
        &self.new_and_every_state_event_list
    }

    pub fn get_current_chunk(&self) -> &Vec<StateEvent> {
        &self.current_state_event_chunk
    }

    pub fn pending_count(&self) -> usize {
        self.pending_state_event_list.len()
    }

    pub fn new_count(&self) -> usize {
        self.new_and_every_state_event_list.len()
    }

    pub fn current_count(&self) -> usize {
        self.current_state_event_chunk.len()
    }

    // ===== State Flags =====

    pub fn mark_state_changed(&mut self) {
        self.state_changed = true;
    }

    pub fn is_state_changed(&self) -> bool {
        self.state_changed
    }

    pub fn reset_state_changed(&mut self) {
        self.state_changed = false;
    }

    pub fn mark_initialized(&mut self) {
        self.initialized = true;
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    pub fn reset_initialized(&mut self) {
        self.initialized = false;
    }
}

// ===== State trait implementation for snapshot/restore =====

impl State for StreamPreState {
    /// Snapshot the list sizes and the two flags. The StateEvents themselves are not
    /// saved.
    fn snapshot(&self) -> HashMap<String, Value> {
        let mut map = HashMap::new();

        map.insert(
            "pending_count".to_string(),
            Value::from(self.pending_state_event_list.len()),
        );
        map.insert(
            "new_count".to_string(),
            Value::from(self.new_and_every_state_event_list.len()),
        );
        map.insert(
            "current_count".to_string(),
            Value::from(self.current_state_event_chunk.len()),
        );

        map.insert("state_changed".to_string(), Value::from(self.state_changed));
        map.insert("initialized".to_string(), Value::from(self.initialized));

        map
    }

    /// Restore only the two flags. The lists are left as they are, since the snapshot
    /// holds no StateEvents. Restoring events would mean saving them in snapshot,
    /// putting them back in the right lists and keeping time order.
    fn restore(&mut self, state: HashMap<String, Value>) {
        if let Some(Value::Bool(b)) = state.get("state_changed") {
            self.state_changed = *b;
        }
        if let Some(Value::Bool(b)) = state.get("initialized") {
            self.initialized = *b;
        }
    }

    fn can_destroy(&self) -> bool {
        // Safe to drop only when all lists are empty and no matching has started
        self.current_state_event_chunk.is_empty()
            && self.pending_state_event_list.is_empty()
            && self.new_and_every_state_event_list.is_empty()
            && !self.initialized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===== StreamPreState Basic Operations =====

    #[test]
    fn test_new_state() {
        let state = StreamPreState::new();
        assert_eq!(state.pending_count(), 0);
        assert_eq!(state.new_count(), 0);
        assert_eq!(state.current_count(), 0);
        assert!(!state.is_state_changed());
        assert!(!state.is_initialized());
    }

    #[test]
    fn test_new_list_operations() {
        let mut state = StreamPreState::new();
        let event = StateEvent::new(1, 0);
        state.add_to_new_list(event);
        assert_eq!(state.new_count(), 1);
        assert_eq!(state.get_new_list().len(), 1);
    }

    #[test]
    fn test_pending_list_operations() {
        let mut state = StreamPreState::new();
        let event = StateEvent::new(1, 0);
        state.add_to_pending_list(event);
        assert_eq!(state.pending_count(), 1);
        assert_eq!(state.get_pending_list().len(), 1);
    }

    #[test]
    fn test_move_new_to_pending() {
        let mut state = StreamPreState::new();
        let mut event1 = StateEvent::new(1, 0);
        event1.set_timestamp(1000);
        let mut event2 = StateEvent::new(1, 0);
        event2.set_timestamp(2000);

        state.add_to_new_list(event1);
        state.add_to_new_list(event2);
        assert_eq!(state.new_count(), 2);

        state.move_new_to_pending();

        assert_eq!(state.new_count(), 0);
        assert_eq!(state.pending_count(), 2);
    }

    #[test]
    fn test_clear_pending() {
        let mut state = StreamPreState::new();
        state.add_to_pending_list(StateEvent::new(1, 0));
        state.add_to_pending_list(StateEvent::new(1, 0));
        assert_eq!(state.pending_count(), 2);

        state.clear_pending();
        assert_eq!(state.pending_count(), 0);
    }

    #[test]
    fn test_clear_new() {
        let mut state = StreamPreState::new();
        state.add_to_new_list(StateEvent::new(1, 0));
        state.add_to_new_list(StateEvent::new(1, 0));
        assert_eq!(state.new_count(), 2);

        state.clear_new();
        assert_eq!(state.new_count(), 0);
    }

    #[test]
    fn test_clear_all() {
        let mut state = StreamPreState::new();
        state.add_to_new_list(StateEvent::new(1, 0));
        state.add_to_pending_list(StateEvent::new(1, 0));
        assert_eq!(state.new_count(), 1);
        assert_eq!(state.pending_count(), 1);

        state.clear_all();
        assert_eq!(state.new_count(), 0);
        assert_eq!(state.pending_count(), 0);
        assert_eq!(state.current_count(), 0);
    }

    #[test]
    fn test_state_changed_flag() {
        let mut state = StreamPreState::new();
        assert!(!state.is_state_changed());

        state.mark_state_changed();
        assert!(state.is_state_changed());

        state.reset_state_changed();
        assert!(!state.is_state_changed());
    }

    #[test]
    fn test_initialized_flag() {
        let mut state = StreamPreState::new();
        assert!(!state.is_initialized());

        state.mark_initialized();
        assert!(state.is_initialized());

        state.reset_initialized();
        assert!(!state.is_initialized());
    }

    #[test]
    fn test_multiple_events_in_lists() {
        let mut state = StreamPreState::new();

        // Add 3 events to each list
        for i in 0..3 {
            let mut event = StateEvent::new(1, 0);
            event.set_timestamp(i * 1000);
            state.add_to_new_list(event.clone());
            state.add_to_pending_list(event);
        }

        assert_eq!(state.new_count(), 3);
        assert_eq!(state.pending_count(), 3);

        // Verify all events preserved
        assert_eq!(state.get_new_list().len(), 3);
        assert_eq!(state.get_pending_list().len(), 3);
    }

    #[test]
    fn test_list_ordering() {
        let mut state = StreamPreState::new();

        // Add events in reverse chronological order
        let mut event3 = StateEvent::new(1, 0);
        event3.set_timestamp(3000);
        let mut event1 = StateEvent::new(1, 0);
        event1.set_timestamp(1000);
        let mut event2 = StateEvent::new(1, 0);
        event2.set_timestamp(2000);

        state.add_to_new_list(event3);
        state.add_to_new_list(event1);
        state.add_to_new_list(event2);

        // Move to pending (should sort by timestamp)
        state.move_new_to_pending();

        // Verify sorted order (ascending)
        let pending = state.get_pending_list();
        assert_eq!(pending.len(), 3);
        assert_eq!(pending[0].get_timestamp(), 1000);
        assert_eq!(pending[1].get_timestamp(), 2000);
        assert_eq!(pending[2].get_timestamp(), 3000);
    }

    #[test]
    fn test_empty_move_new_to_pending() {
        let mut state = StreamPreState::new();
        state.move_new_to_pending();
        assert_eq!(state.pending_count(), 0);
    }

    #[test]
    fn test_independent_lists() {
        let mut state = StreamPreState::new();

        // Add to different lists
        state.add_to_new_list(StateEvent::new(1, 0));
        state.add_to_pending_list(StateEvent::new(1, 0));

        // Clear one shouldn't affect the other
        state.clear_new();
        assert_eq!(state.new_count(), 0);
        assert_eq!(state.pending_count(), 1);

        state.clear_pending();
        assert_eq!(state.pending_count(), 0);
    }

    // ===== State trait implementation (snapshot/restore) =====

    #[test]
    fn test_snapshot_restore() {
        let mut state = StreamPreState::new();
        state.mark_state_changed();
        state.mark_initialized();

        // Take snapshot
        let snapshot = state.snapshot();

        // Create new state and restore from snapshot
        let mut restored = StreamPreState::new();
        restored.restore(snapshot);

        // Verify flags were restored
        assert_eq!(restored.is_state_changed(), state.is_state_changed());
        assert_eq!(restored.is_initialized(), state.is_initialized());
    }

    #[test]
    fn test_can_destroy_empty() {
        let state = StreamPreState::new();
        // New empty state should be destroyable
        assert!(state.can_destroy());
    }

    #[test]
    fn test_can_destroy_with_pending() {
        let mut state = StreamPreState::new();
        state.add_to_pending_list(StateEvent::new(1, 0));
        // State with pending events cannot be destroyed
        assert!(!state.can_destroy());
    }

    #[test]
    fn test_can_destroy_with_new() {
        let mut state = StreamPreState::new();
        state.add_to_new_list(StateEvent::new(1, 0));
        // State with new events cannot be destroyed
        assert!(!state.can_destroy());
    }

    #[test]
    fn test_can_destroy_when_initialized() {
        let mut state = StreamPreState::new();
        state.mark_initialized();
        // Initialized state cannot be destroyed (active pattern matching)
        assert!(!state.can_destroy());
    }

    #[test]
    fn test_snapshot_preserves_all_lists() {
        let mut state = StreamPreState::new();

        // Add events to all lists
        state.add_to_pending_list(StateEvent::new(1, 0));
        state.add_to_pending_list(StateEvent::new(1, 0));
        state.add_to_new_list(StateEvent::new(1, 0));

        // Take snapshot
        let snapshot = state.snapshot();

        // Verify counts are captured
        assert_eq!(snapshot.get("pending_count"), Some(&Value::from(2)));
        assert_eq!(snapshot.get("new_count"), Some(&Value::from(1)));
        assert_eq!(snapshot.get("current_count"), Some(&Value::from(0)));
    }

    #[test]
    fn test_restore_full_state() {
        let mut state = StreamPreState::new();

        // Setup state with all flags
        state.mark_state_changed();
        state.mark_initialized();
        state.add_to_pending_list(StateEvent::new(1, 0));
        state.add_to_new_list(StateEvent::new(1, 0));

        // Take snapshot
        let snapshot = state.snapshot();

        // Create fresh state
        let mut restored = StreamPreState::new();
        assert!(!restored.is_state_changed());
        assert!(!restored.is_initialized());

        // Restore
        restored.restore(snapshot);

        // Verify all flags restored
        assert!(restored.is_state_changed());
        assert!(restored.is_initialized());
    }

    #[test]
    fn test_snapshot_with_flags() {
        let mut state = StreamPreState::new();
        state.mark_state_changed();
        state.mark_initialized();

        let snapshot = state.snapshot();

        assert_eq!(snapshot.get("state_changed"), Some(&Value::from(true)));
        assert_eq!(snapshot.get("initialized"), Some(&Value::from(true)));
    }

    #[test]
    fn test_can_destroy_after_clear() {
        let mut state = StreamPreState::new();
        state.mark_initialized();
        state.add_to_pending_list(StateEvent::new(1, 0));

        // Cannot destroy with data and initialized
        assert!(!state.can_destroy());

        // Clear all and reset initialized
        state.clear_all();
        state.reset_initialized();

        // Now can destroy
        assert!(state.can_destroy());
    }
}
