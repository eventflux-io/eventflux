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

// Port of the Java ComplexEvent interface.
use super::value::AttributeValue;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::Debug;

// Port of ComplexEvent.Type in Java.
/// Type of complex event (CURRENT, EXPIRED, TIMER, RESET). Defaults to `Current`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum ComplexEventType {
    #[default]
    Current,
    Expired,
    Timer,
    Reset,
}

/// An event that can link to the next one to form a chunk and carries data arrays.
pub trait ComplexEvent: Debug + Send + Sync + 'static {
    fn get_next(&self) -> Option<&dyn ComplexEvent>;
    fn set_next(
        &mut self,
        next_event: Option<Box<dyn ComplexEvent>>,
    ) -> Option<Box<dyn ComplexEvent>>; // returns the old next
                                        // Mutable access to the `next` slot, for easier list edits.
    fn mut_next_ref_option(&mut self) -> &mut Option<Box<dyn ComplexEvent>>;

    fn get_output_data(&self) -> Option<&[AttributeValue]>;
    fn set_output_data(&mut self, data: Option<Vec<AttributeValue>>);
    // Optional: get_output_data_mut if direct mutation of Vec is needed
    // fn get_output_data_mut(&mut self) -> Option<&mut Vec<AttributeValue>>;

    fn get_timestamp(&self) -> i64;
    fn set_timestamp(&mut self, timestamp: i64);

    fn get_event_type(&self) -> ComplexEventType;
    fn set_event_type(&mut self, event_type: ComplexEventType);

    fn is_expired(&self) -> bool {
        self.get_event_type() == ComplexEventType::Expired
    }
    fn set_expired(&mut self, expired: bool) {
        if expired {
            self.set_event_type(ComplexEventType::Expired);
        } else if self.get_event_type() == ComplexEventType::Expired {
            // Clearing only turns EXPIRED back into CURRENT. TIMER and RESET stay as they are.
            self.set_event_type(ComplexEventType::Current);
        }
    }

    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;

    // Possible clone hook if some strategy needs cloneable events. Every
    // implementor would have to provide it.
    // fn clone_complex_event(&self) -> Box<dyn ComplexEvent>;
}

/// Deep clones a [`ComplexEvent`] and every event after it in the chain.
///
/// # Panics
///
/// Panics if the event is not a `StreamEvent` or `StateEvent`.
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

/// Clones a single [`ComplexEvent`]. The clone has no `next` event.
///
/// # Panics
///
/// Panics if the event is not a `StreamEvent` or `StateEvent`.
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

/// Clones the whole chain of [`ComplexEvent`]s starting at `head`.
///
/// Recurses once per event, so a very long chain can overflow the stack.
///
/// # Panics
///
/// Panics if any event is not a `StreamEvent` or `StateEvent`.
pub fn clone_event_chain(head: &dyn ComplexEvent) -> Box<dyn ComplexEvent> {
    let mut new_head = clone_box_complex_event_without_next(head);
    if let Some(next) = head.get_next() {
        new_head.set_next(Some(clone_event_chain(next)));
    }
    new_head
}

// Example of how `Box<dyn ComplexEvent>` could be made cloneable with the hook above.
// impl Clone for Box<dyn ComplexEvent> {
//     fn clone(&self) -> Self {
//         self.clone_complex_event()
//     }
// }
