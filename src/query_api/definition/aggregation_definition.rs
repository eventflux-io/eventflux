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

// Corresponds to io.eventflux.query.api.definition.AggregationDefinition
use crate::query_api::annotation::Annotation;
use crate::query_api::definition::abstract_definition::AbstractDefinition;
// BasicSingleInputStream was merged into SingleInputStream; AggregationDefinition should use SingleInputStream.
use crate::query_api::aggregation::TimePeriod;
use crate::query_api::execution::query::input::stream::SingleInputStream;
use crate::query_api::execution::query::selection::Selector;
use crate::query_api::expression::Variable; // Using actual TimePeriod

#[derive(Clone, Debug, PartialEq, Default)] // Added Default
pub struct AggregationDefinition {
    // Composition for inheritance from AbstractDefinition
    pub abstract_definition: AbstractDefinition,

    // Fields specific to AggregationDefinition
    pub basic_single_input_stream: Option<SingleInputStream>, // Changed from BasicSingleInputStream
    pub selector: Option<Selector>, // In Java, it's Selector, not BasicSelector for the field type
    pub aggregate_attribute: Option<Variable>, // This is 'aggregateBy' in Java
    pub time_period: Option<TimePeriod>, // This is 'every' in Java

                                    // annotations are in AbstractDefinition
}

impl AggregationDefinition {
    // Constructor that takes an id, as per Java's `AggregationDefinition.id(aggregationName)`
    pub fn new(id: String) -> Self {
        AggregationDefinition {
            abstract_definition: AbstractDefinition::new(id),
            basic_single_input_stream: None,
            selector: None, // Java initializes selector to null
            aggregate_attribute: None,
            time_period: None,
        }
    }

    // Static factory `id` from Java
    pub fn id(aggregation_name: String) -> Self {
        Self::new(aggregation_name)
    }

    // Builder-style methods from Java
    pub fn from(mut self, stream: SingleInputStream) -> Self {
        // Changed from BasicSingleInputStream
        self.basic_single_input_stream = Some(stream);
        self
    }

    // In Java, select takes BasicSelector, but field is Selector.
    // Using Selector type here.
    pub fn select(mut self, selector: Selector) -> Self {
        self.selector = Some(selector);
        self
    }

    pub fn aggregate_by(mut self, var: Variable) -> Self {
        self.aggregate_attribute = Some(var);
        self
    }

    pub fn every(mut self, period: TimePeriod) -> Self {
        self.time_period = Some(period);
        self
    }

    pub fn annotation(mut self, annotation: Annotation) -> Self {
        self.abstract_definition.annotations.push(annotation);
        self
    }
}

// Provide access to AbstractDefinition fields and EventFluxElement fields
impl AsRef<AbstractDefinition> for AggregationDefinition {
    fn as_ref(&self) -> &AbstractDefinition {
        &self.abstract_definition
    }
}

impl AsMut<AbstractDefinition> for AggregationDefinition {
    fn as_mut(&mut self) -> &mut AbstractDefinition {
        &mut self.abstract_definition
    }
}

use crate::query_api::eventflux_element::EventFluxElement;
impl AsRef<EventFluxElement> for AggregationDefinition {
    fn as_ref(&self) -> &EventFluxElement {
        self.abstract_definition.as_ref()
    }
}

impl AsMut<EventFluxElement> for AggregationDefinition {
    fn as_mut(&mut self) -> &mut EventFluxElement {
        self.abstract_definition.as_mut()
    }
}
