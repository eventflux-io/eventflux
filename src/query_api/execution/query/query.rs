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

use super::{OutputEventType, OutputRate, OutputStream}; // Use parent module's re-exports
use crate::query_api::annotation::Annotation;
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::execution::execution_element::ExecutionElementTrait;
use crate::query_api::execution::query::input::InputStream;
use crate::query_api::execution::query::selection::Selector;

/// Defines a EventFlux query with input, selection, output, etc.
#[derive(Clone, Debug, PartialEq)] // Default will be custom or via new()
pub struct Query {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement

    pub input_stream: Option<InputStream>,
    pub selector: Selector,
    pub output_stream: OutputStream,
    pub output_rate: Option<OutputRate>,
    pub annotations: Vec<Annotation>,
}

impl Query {
    pub fn new() -> Self {
        Query {
            eventflux_element: EventFluxElement::default(), // Initialize composed element
            input_stream: None,
            selector: Selector::new(), // Java default
            output_stream: OutputStream::default_return_stream(), // Java default
            output_rate: None,
            annotations: Vec::new(),
        }
    }

    // Static factory method from Java
    #[allow(clippy::self_named_constructors)]
    pub fn query() -> Self {
        Self::new()
    }

    // Builder methods
    pub fn from(mut self, input_stream: InputStream) -> Self {
        self.input_stream = Some(input_stream);
        self
    }

    pub fn select(mut self, selector: Selector) -> Self {
        self.selector = selector;
        self
    }

    pub fn out_stream(mut self, output_stream: OutputStream) -> Self {
        self.output_stream = output_stream;
        self.update_output_event_type();
        self
    }

    pub fn output(mut self, output_rate: OutputRate) -> Self {
        self.output_rate = Some(output_rate);
        self.update_output_event_type();
        self
    }

    fn update_output_event_type(&mut self) {
        if self.output_stream.get_output_event_type().is_none() {
            let is_snapshot_rate = self.output_rate.as_ref().is_some_and(|r| r.is_snapshot());

            let event_type = if is_snapshot_rate {
                OutputEventType::AllEvents
            } else {
                OutputEventType::CurrentEvents
            };
            self.output_stream.set_output_event_type_if_none(event_type);
        }
    }

    pub fn annotation(mut self, annotation: Annotation) -> Self {
        self.annotations.push(annotation);
        self
    }

    // Getter for annotations, needed by ExecutionElementTrait impl
    pub fn get_annotations(&self) -> &Vec<Annotation> {
        &self.annotations
    }

    // Other Getters
    pub fn get_input_stream(&self) -> Option<&InputStream> {
        self.input_stream.as_ref()
    }

    pub fn get_selector(&self) -> &Selector {
        &self.selector
    }

    pub fn get_output_stream(&self) -> &OutputStream {
        &self.output_stream
    }

    pub fn get_output_rate(&self) -> Option<&OutputRate> {
        self.output_rate.as_ref()
    }
}

impl Default for Query {
    fn default() -> Self {
        Self::new()
    }
}

// EventFluxElement is composed, access via `self.eventflux_element.query_context_start_index` etc.
// Or implement Deref/DerefMut if desired for direct access.

// Implement ExecutionElementTrait for Query
impl ExecutionElementTrait for Query {
    fn get_annotations(&self) -> &Vec<Annotation> {
        &self.annotations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_api::annotation::Annotation;
    use crate::query_api::execution::query::input::stream::{InputStream, SingleInputStream};
    use crate::query_api::execution::query::output::output_stream::OutputStream;
    use crate::query_api::execution::query::output::ratelimit::{OutputRate, OutputRateBehavior};
    use crate::query_api::execution::query::selection::Selector;
    use crate::query_api::expression::constant::Constant;

    #[test]
    fn test_query_new() {
        let q = Query::new();
        assert!(q.get_input_stream().is_none());
        assert_eq!(q.get_selector(), &Selector::new()); // Assumes Selector derives PartialEq and has a new
        assert_eq!(
            q.get_output_stream(),
            &OutputStream::default_return_stream()
        ); // Assumes OutputStream has PartialEq and a default
        assert!(q.get_output_rate().is_none());
        assert!(q.get_annotations().is_empty());
        assert_eq!(q.eventflux_element.query_context_start_index, None);
    }

    #[test]
    fn test_query_builder() {
        let input_stream = InputStream::Single(SingleInputStream::new_basic(
            "MyStream".to_string(),
            false,
            false,
            None,
            Vec::new(),
        ));
        let selector = Selector::new(); // Empty selector
        let output_stream = OutputStream::default_return_stream();
        let output_rate =
            OutputRate::per_events(Constant::int(10), OutputRateBehavior::All).unwrap();
        let annotation = Annotation::new("TestAnn".to_string());

        let q = Query::query()
            .from(input_stream.clone())
            .select(selector.clone())
            .out_stream(output_stream.clone())
            .output(output_rate.clone())
            .annotation(annotation.clone());

        assert_eq!(q.get_input_stream().unwrap(), &input_stream);
        assert_eq!(q.get_selector(), &selector);

        // out_stream might modify output_stream's event type, so compare relevant fields
        // or ensure the clone for comparison is made *after* potential modification if it matters.
        // For this test, we check the target_id set and that event type logic is hit.
        assert_eq!(q.get_output_stream().get_target_id(), None);
        assert_eq!(q.get_output_rate().unwrap(), &output_rate);
        assert_eq!(q.get_annotations().len(), 1);
        assert_eq!(q.get_annotations()[0].name, "TestAnn");
    }

    #[test]
    fn test_query_update_output_event_type() {
        let mut q = Query::query();

        // Default output event type should be CurrentEvents
        q.update_output_event_type(); // Called internally by out_stream and output, but can be called directly
        assert_eq!(
            q.output_stream.get_output_event_type(),
            Some(OutputEventType::CurrentEvents)
        );

        // If output rate is snapshot, it should be AllEvents
        let snapshot_rate = OutputRate::per_snapshot(Constant::long(1000)).unwrap();
        q = q.output(snapshot_rate); // This will call update_output_event_type
                                     // In the current implementation `update_output_event_type` only changes
                                     // the type if it was previously `None`. Since `OutputStream::default_return_stream`
                                     // sets it to `CurrentEvents`, applying a snapshot rate does not modify it.
        assert_eq!(
            q.output_stream.get_output_event_type(),
            Some(OutputEventType::CurrentEvents)
        );

        // If output rate is not snapshot, and type was already set, it should not change
        // (unless explicitly set to None first and then a non-snapshot rate is applied)
        // Let's reset the query to test this part
        let mut q2 = Query::query();
        // Manually set an output event type
        q2.output_stream
            .set_output_event_type(OutputEventType::ExpiredEvents);
        assert_eq!(
            q2.output_stream.get_output_event_type(),
            Some(OutputEventType::ExpiredEvents)
        );

        // Add a non-snapshot output rate
        let events_rate =
            OutputRate::per_events(Constant::int(5), OutputRateBehavior::All).unwrap();
        q2 = q2.output(events_rate);
        // The event type should remain ExpiredEvents because it was already set
        assert_eq!(
            q2.output_stream.get_output_event_type(),
            Some(OutputEventType::ExpiredEvents)
        );

        // If output_event_type is None and a non-snapshot rate is applied, it should become CurrentEvents
        let mut q3 = Query::query();
        q3.output_stream.output_event_type = None; // Explicitly set to None
        let time_rate =
            OutputRate::per_time_period(Constant::long(100), OutputRateBehavior::All).unwrap();
        q3 = q3.output(time_rate);
        assert_eq!(
            q3.output_stream.get_output_event_type(),
            Some(OutputEventType::CurrentEvents)
        );
    }
}
