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

use crate::query_api::aggregation::TimePeriod;
use crate::query_api::annotation::Annotation;
use crate::query_api::definition::abstract_definition::AbstractDefinition;
use crate::query_api::execution::query::input::stream::SingleInputStream;
use crate::query_api::execution::query::selection::Selector;
use crate::query_api::expression::Variable;

/// An incremental aggregation definition. Reads one input stream, groups by a time
/// attribute and keeps results per time unit. Annotations live in `abstract_definition`.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct AggregationDefinition {
    pub abstract_definition: AbstractDefinition,

    /// The input stream. The name is historical. Any SingleInputStream is accepted.
    pub basic_single_input_stream: Option<SingleInputStream>,
    pub selector: Option<Selector>,
    /// The time attribute to aggregate by. Set with `aggregate_by`.
    pub aggregate_attribute: Option<Variable>,
    /// The time units to keep results for. Set with `every`.
    pub time_period: Option<TimePeriod>,
}

impl AggregationDefinition {
    /// An empty definition with just the id set.
    pub fn new(id: String) -> Self {
        AggregationDefinition {
            abstract_definition: AbstractDefinition::new(id),
            basic_single_input_stream: None,
            selector: None,
            aggregate_attribute: None,
            time_period: None,
        }
    }

    /// Same as `new`. Starts a builder chain.
    pub fn id(aggregation_name: String) -> Self {
        Self::new(aggregation_name)
    }

    pub fn from(mut self, stream: SingleInputStream) -> Self {
        self.basic_single_input_stream = Some(stream);
        self
    }

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

// Access to the shared definition fields and the query positions.
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
