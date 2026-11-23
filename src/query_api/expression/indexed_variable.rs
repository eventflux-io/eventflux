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

//! Indexed access into pattern event lists, as in e[0].attr and e[last].attr.
//!
//! A count step in a pattern, such as `e1=FailedLogin{3,5}`, can match many events. These
//! types pick one event from that list.
//!
//! # Examples
//!
//! ```ignore
//! // Access first event's userId: e[0].userId
//! let var = IndexedVariable::new_with_index("userId".to_string(), 0)
//!     .of_stream_with_index("e1".to_string(), 0);
//!
//! // Access last event's timestamp: e[last].timestamp
//! let var = IndexedVariable::new_with_last("timestamp".to_string())
//!     .of_stream_with_index("e1".to_string(), 0);
//! ```

use crate::query_api::eventflux_element::EventFluxElement;

/// Which event to pick from a pattern event list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventIndex {
    /// Zero based position. e[0] is the first event, e[1] the second, and so on.
    Numeric(usize),

    /// The `last` keyword, as in e[last]. Picks the last event at the time of reading.
    Last,
}

impl EventIndex {
    /// Turn this into a position in a list of `collection_size` events.
    ///
    /// Returns `None` when the list is empty or a numeric index is past the end.
    pub fn resolve(&self, collection_size: usize) -> Option<usize> {
        if collection_size == 0 {
            return None;
        }

        match self {
            EventIndex::Numeric(idx) => {
                if *idx < collection_size {
                    Some(*idx)
                } else {
                    None
                }
            }
            EventIndex::Last => Some(collection_size - 1),
        }
    }
}

/// An attribute read from one event of a pattern event list, as in e[0].attr or e[last].attr.
///
/// For a pattern like `e1=FailedLogin{3,5}` you can read:
/// - `e1[0].userId`, the userId of the first failed login
/// - `e1[last].timestamp`, the timestamp of the last failed login
/// - `e1[2].ipAddress`, the IP address of the third failed login
///
/// # Grammar Mapping
///
/// ```text
/// e1[0].userId     → IndexedVariable { stream_index: 0, index: Numeric(0), attribute: "userId" }
/// e1[last].time    → IndexedVariable { stream_index: 0, index: Last, attribute: "time" }
/// e2[3].value      → IndexedVariable { stream_index: 1, index: Numeric(3), attribute: "value" }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct IndexedVariable {
    pub eventflux_element: EventFluxElement,

    /// Pattern alias such as "e1" or "e2", if given.
    pub stream_id: Option<String>,

    /// Position of the stream in StateEvent.stream_events, so e1 is 0, e2 is 1 and so on.
    /// The runtime needs it to know which event list to read.
    pub stream_index: Option<i32>,

    /// Which event to pick from that list.
    pub index: EventIndex,

    /// Attribute to read from the picked event.
    pub attribute_name: String,
}

impl IndexedVariable {
    /// Build an indexed variable with a numeric index. The stream starts unset.
    ///
    /// # Arguments
    /// * `attribute_name` - Attribute to read, such as "userId" or "timestamp"
    /// * `index` - Zero based position in the event list, so 0 is the first event
    ///
    /// # Example
    ///
    /// ```ignore
    /// let var = IndexedVariable::new_with_index("userId".to_string(), 0);
    /// // Represents: e[0].userId
    /// ```
    pub fn new_with_index(attribute_name: String, index: usize) -> Self {
        IndexedVariable {
            eventflux_element: EventFluxElement::default(),
            stream_id: None,
            stream_index: None,
            index: EventIndex::Numeric(index),
            attribute_name,
        }
    }

    /// Build an indexed variable that reads the last event. The stream starts unset.
    ///
    /// # Arguments
    /// * `attribute_name` - Attribute to read from the last event
    ///
    /// # Example
    ///
    /// ```ignore
    /// let var = IndexedVariable::new_with_last("timestamp".to_string());
    /// // Represents: e[last].timestamp
    /// ```
    pub fn new_with_last(attribute_name: String) -> Self {
        IndexedVariable {
            eventflux_element: EventFluxElement::default(),
            stream_id: None,
            stream_index: None,
            index: EventIndex::Last,
            attribute_name,
        }
    }

    /// Set the stream alias and its position, replacing any set before.
    ///
    /// # Arguments
    /// * `stream_id` - Pattern alias such as "e1" or "e2"
    /// * `stream_index` - Position in the StateEvent, so e1 is 0, e2 is 1 and so on
    ///
    /// # Example
    ///
    /// ```ignore
    /// let var = IndexedVariable::new_with_index("userId".to_string(), 0)
    ///     .of_stream_with_index("e1".to_string(), 0);
    /// // Represents: e1[0].userId where e1 is at position 0
    /// ```
    pub fn of_stream_with_index(mut self, stream_id: String, stream_index: i32) -> Self {
        self.stream_id = Some(stream_id);
        self.stream_index = Some(stream_index);
        self
    }

    pub fn get_stream_id(&self) -> Option<&String> {
        self.stream_id.as_ref()
    }

    pub fn get_stream_index(&self) -> Option<i32> {
        self.stream_index
    }

    pub fn get_index(&self) -> &EventIndex {
        &self.index
    }

    pub fn get_attribute_name(&self) -> &String {
        &self.attribute_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_index_resolve_numeric() {
        let index = EventIndex::Numeric(0);
        assert_eq!(index.resolve(3), Some(0));

        let index = EventIndex::Numeric(2);
        assert_eq!(index.resolve(5), Some(2));
    }

    #[test]
    fn test_event_index_resolve_last() {
        let index = EventIndex::Last;
        assert_eq!(index.resolve(1), Some(0)); // Only one element
        assert_eq!(index.resolve(5), Some(4)); // Last of 5 is index 4
        assert_eq!(index.resolve(10), Some(9)); // Last of 10 is index 9
    }

    #[test]
    fn test_event_index_resolve_out_of_bounds() {
        let index = EventIndex::Numeric(10);
        assert_eq!(index.resolve(3), None); // Index 10 with size 3 → out of bounds

        let index = EventIndex::Numeric(0);
        assert_eq!(index.resolve(0), None); // Empty collection
    }

    #[test]
    fn test_event_index_resolve_last_empty() {
        let index = EventIndex::Last;
        assert_eq!(index.resolve(0), None); // Empty collection
    }

    #[test]
    fn test_indexed_variable_creation_numeric() {
        let var = IndexedVariable::new_with_index("userId".to_string(), 0);

        assert_eq!(var.get_attribute_name(), "userId");
        assert_eq!(var.get_index(), &EventIndex::Numeric(0));
        assert_eq!(var.get_stream_id(), None);
        assert_eq!(var.get_stream_index(), None);
    }

    #[test]
    fn test_indexed_variable_creation_last() {
        let var = IndexedVariable::new_with_last("timestamp".to_string());

        assert_eq!(var.get_attribute_name(), "timestamp");
        assert_eq!(var.get_index(), &EventIndex::Last);
        assert_eq!(var.get_stream_id(), None);
        assert_eq!(var.get_stream_index(), None);
    }

    #[test]
    fn test_indexed_variable_with_stream() {
        let var = IndexedVariable::new_with_index("userId".to_string(), 0)
            .of_stream_with_index("e1".to_string(), 0);

        assert_eq!(var.get_attribute_name(), "userId");
        assert_eq!(var.get_index(), &EventIndex::Numeric(0));
        assert_eq!(var.get_stream_id(), Some(&"e1".to_string()));
        assert_eq!(var.get_stream_index(), Some(0));
    }

    #[test]
    fn test_indexed_variable_last_with_stream() {
        let var = IndexedVariable::new_with_last("price".to_string())
            .of_stream_with_index("e2".to_string(), 1);

        assert_eq!(var.get_attribute_name(), "price");
        assert_eq!(var.get_index(), &EventIndex::Last);
        assert_eq!(var.get_stream_id(), Some(&"e2".to_string()));
        assert_eq!(var.get_stream_index(), Some(1));
    }

    #[test]
    fn test_indexed_variable_multiple_indices() {
        let var1 = IndexedVariable::new_with_index("attr1".to_string(), 0);
        let var2 = IndexedVariable::new_with_index("attr2".to_string(), 1);
        let var3 = IndexedVariable::new_with_index("attr3".to_string(), 5);

        assert_eq!(var1.get_index(), &EventIndex::Numeric(0));
        assert_eq!(var2.get_index(), &EventIndex::Numeric(1));
        assert_eq!(var3.get_index(), &EventIndex::Numeric(5));
    }
}
