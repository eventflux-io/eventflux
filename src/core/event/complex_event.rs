// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/event/complex_event.rs
// Corresponds to io.eventflux.core.event.ComplexEvent (interface)
use super::value::AttributeValue;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::Debug; // For as_any_mut

// Moved from stream_event.rs (or defined here if it wasn't there)
// This enum corresponds to ComplexEvent.Type in Java
/// Type of complex event (CURRENT, EXPIRED, TIMER, RESET).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum ComplexEventType {
    #[default]
    Current,
    Expired,
    Timer,
    Reset,
}

/// Trait for complex events that can form a linked list (chunk) and carry various data arrays.
pub trait ComplexEvent: Debug + Send + Sync + 'static {
    fn get_next(&self) -> Option<&dyn ComplexEvent>;
    fn set_next(
        &mut self,
        next_event: Option<Box<dyn ComplexEvent>>,
    ) -> Option<Box<dyn ComplexEvent>>; // Return old next
                                        // Provides mutable access to the 'next' field's Option for easier list manipulation
    fn mut_next_ref_option(&mut self) -> &mut Option<Box<dyn ComplexEvent>>;

    fn get_output_data(&self) -> Option<&[AttributeValue]>; // Changed to Option<&[AttributeValue]>
    fn set_output_data(&mut self, data: Option<Vec<AttributeValue>>);
    // Optional: get_output_data_mut if direct mutation of Vec is needed
    // fn get_output_data_mut(&mut self) -> Option<&mut Vec<AttributeValue>>;

    fn get_timestamp(&self) -> i64;
    fn set_timestamp(&mut self, timestamp: i64);

    fn get_event_type(&self) -> ComplexEventType;
    fn set_event_type(&mut self, event_type: ComplexEventType);

    fn is_expired(&self) -> bool {
        // Default impl
        self.get_event_type() == ComplexEventType::Expired
    }
    fn set_expired(&mut self, expired: bool) {
        // Default impl
        if expired {
            self.set_event_type(ComplexEventType::Expired);
        } else if self.get_event_type() == ComplexEventType::Expired {
            // Only change from EXPIRED to CURRENT if explicitly set to not expired.
            // Other types (TIMER, RESET) should remain.
            self.set_event_type(ComplexEventType::Current);
        }
    }

    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;

    // Helper for cloning, if ComplexEvent needs to be clonable for some strategies
    // Each implementor would need to implement this.
    // fn clone_complex_event(&self) -> Box<dyn ComplexEvent>;
}

/// Deep clone a [`ComplexEvent`] chain.
pub fn clone_box_complex_event(event: &dyn ComplexEvent) -> Box<dyn ComplexEvent> {
    if let Some(se) = event
        .as_any()
        .downcast_ref::<crate::core::event::stream::StreamEvent>()
    {
        Box::new(se.clone())
    } else if let Some(se) = event
        .as_any()
        .downcast_ref::<crate::core::event::state::StateEvent>()
    {
        Box::new(se.clone())
    } else {
        panic!("Unknown ComplexEvent type for cloning");
    }
}

/// Clone a single [`ComplexEvent`] without cloning its `next` pointer.
pub fn clone_box_complex_event_without_next(event: &dyn ComplexEvent) -> Box<dyn ComplexEvent> {
    if let Some(se) = event
        .as_any()
        .downcast_ref::<crate::core::event::stream::StreamEvent>()
    {
        Box::new(se.clone_without_next())
    } else if let Some(se) = event
        .as_any()
        .downcast_ref::<crate::core::event::state::StateEvent>()
    {
        Box::new(se.clone_without_next())
    } else {
        panic!("Unknown ComplexEvent type for cloning");
    }
}

/// Clone an entire chain of [`ComplexEvent`]s starting at `head`.
pub fn clone_event_chain(head: &dyn ComplexEvent) -> Box<dyn ComplexEvent> {
    let mut new_head = clone_box_complex_event_without_next(head);
    if let Some(next) = head.get_next() {
        new_head.set_next(Some(clone_event_chain(next)));
    }
    new_head
}

// Example of how a concrete type might implement clone_box
// This is now part of the trait comment as it's specific to implementors.
// impl Clone for Box<dyn ComplexEvent> {
//     fn clone(&self) -> Self {
//         self.clone_complex_event()
//     }
// }
