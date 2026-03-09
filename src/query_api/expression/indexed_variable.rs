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

//! Indexed variable access for pattern event collections: e[0].attr, e[last].attr
//!
//! This module provides support for accessing specific events within an event collection
//! matched by count quantifiers in pattern queries.
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

/// Index specification for array access in pattern event collections
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventIndex {
    /// Numeric index (0-based): e[0] (first), e[1] (second), e[2] (third), ...
    Numeric(usize),

    /// Special "last" keyword: e[last] (dynamically resolves to last event)
    Last,
}

impl EventIndex {
    /// Resolve index to concrete value given collection size
    /// Returns None if index is out of bounds
    pub fn resolve(&self, collection_size: usize) -> Option<usize> {
        if collection_size == 0 {
            return None;
        }

        match self {
            EventIndex::Numeric(idx) => {
                if *idx < collection_size {
                    Some(*idx)
                } else {
                    None // Out of bounds
                }
            }
            EventIndex::Last => Some(collection_size - 1),
        }
    }
}

/// Represents indexed access to events in a pattern collection: e[0].attr, e[last].attr
///
/// Used for accessing specific events from event collections matched by count quantifiers.
/// For example, in a pattern like `e1=FailedLogin{3,5}`, you can access:
/// - `e1[0].userId` - first failed login's userId
/// - `e1[last].timestamp` - last failed login's timestamp
/// - `e1[2].ipAddress` - third failed login's IP address
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

    /// Stream ID (e.g., "e1", "e2") if specified
    pub stream_id: Option<String>,

    /// Stream position in StateEvent.stream_events[] array (e1=0, e2=1, e3=2, ...)
    /// This is required for runtime evaluation to know which event collection to access
    pub stream_index: Option<i32>,

    /// Index into the event collection at the stream position
    pub index: EventIndex,

    /// Attribute name to access from the indexed event
    pub attribute_name: String,
}

impl IndexedVariable {
    /// Create new indexed variable with numeric index
    ///
    /// # Arguments
    /// * `attribute_name` - Attribute to access (e.g., "userId", "timestamp")
    /// * `index` - Zero-based index into event collection (e.g., 0 for first, 1 for second)
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

    /// Create new indexed variable with "last" keyword
    ///
    /// # Arguments
    /// * `attribute_name` - Attribute to access from last event
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

    /// Builder: Set stream ID and position index
    ///
    /// # Arguments
    /// * `stream_id` - Stream identifier (e.g., "e1", "e2")
    /// * `stream_index` - Position in StateEvent (e1=0, e2=1, ...)
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

    // Getter methods

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
