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

use super::{OutputEventType, OutputRate, OutputStream};
use crate::query_api::annotation::Annotation;
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::execution::execution_element::ExecutionElementTrait;
use crate::query_api::execution::query::input::InputStream;
use crate::query_api::execution::query::selection::Selector;

/// An EventFlux query. Input, selection, output, an optional output rate and annotations.
#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    pub eventflux_element: EventFluxElement,

    pub input_stream: Option<InputStream>,
    pub selector: Selector,
    pub output_stream: OutputStream,
    /// `None` means no output rate limit.
    pub output_rate: Option<OutputRate>,
    pub annotations: Vec<Annotation>,
}

impl Query {
    /// An empty query with no input, an empty selector and a return stream that emits
    /// `CurrentEvents`.
    pub fn new() -> Self {
        Query {
            eventflux_element: EventFluxElement::default(),
            input_stream: None,
            selector: Selector::new(),
            output_stream: OutputStream::default_return_stream(),
            output_rate: None,
            annotations: Vec::new(),
        }
    }

    /// Same as `new`. Starts a builder chain.
    #[allow(clippy::self_named_constructors)]
    pub fn query() -> Self {
        Self::new()
    }

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

    // Fill in the output event type only when it is unset. AllEvents for a snapshot rate,
    // else CurrentEvents. An event type that is already set is never changed.
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

    pub fn get_annotations(&self) -> &Vec<Annotation> {
        &self.annotations
    }

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

// Read the query positions through `self.eventflux_element`, as in
// `self.eventflux_element.query_context_start_index`.

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
        assert_eq!(q.get_selector(), &Selector::new());
        assert_eq!(
            q.get_output_stream(),
            &OutputStream::default_return_stream()
        );
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
        let selector = Selector::new();
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

        // out_stream() may fill in the event type, so check the target id instead of
        // comparing the whole output stream.
        assert_eq!(q.get_output_stream().get_target_id(), None);
        assert_eq!(q.get_output_rate().unwrap(), &output_rate);
        assert_eq!(q.get_annotations().len(), 1);
        assert_eq!(q.get_annotations()[0].name, "TestAnn");
    }

    #[test]
    fn test_query_update_output_event_type() {
        let mut q = Query::query();

        // A new query starts with CurrentEvents. out_stream() and output() call this
        // themselves. Here it is called directly.
        q.update_output_event_type();
        assert_eq!(
            q.output_stream.get_output_event_type(),
            Some(OutputEventType::CurrentEvents)
        );

        // The type is only filled in when unset. default_return_stream() already set
        // CurrentEvents, so a snapshot rate does not change it to AllEvents.
        let snapshot_rate = OutputRate::per_snapshot(Constant::long(1000)).unwrap();
        q = q.output(snapshot_rate);
        assert_eq!(
            q.output_stream.get_output_event_type(),
            Some(OutputEventType::CurrentEvents)
        );

        // A type that is already set stays as it is when a non snapshot rate is added.
        let mut q2 = Query::query();
        q2.output_stream
            .set_output_event_type(OutputEventType::ExpiredEvents);
        assert_eq!(
            q2.output_stream.get_output_event_type(),
            Some(OutputEventType::ExpiredEvents)
        );

        let events_rate =
            OutputRate::per_events(Constant::int(5), OutputRateBehavior::All).unwrap();
        q2 = q2.output(events_rate);
        assert_eq!(
            q2.output_stream.get_output_event_type(),
            Some(OutputEventType::ExpiredEvents)
        );

        // An unset type becomes CurrentEvents when a non snapshot rate is added.
        let mut q3 = Query::query();
        q3.output_stream.output_event_type = None;
        let time_rate =
            OutputRate::per_time_period(Constant::long(100), OutputRateBehavior::All).unwrap();
        q3 = q3.output(time_rate);
        assert_eq!(
            q3.output_stream.get_output_event_type(),
            Some(OutputEventType::CurrentEvents)
        );
    }
}
