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

use crate::query_api::annotation::Annotation;
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::expression::constant::{Constant, ConstantValueWithFloat};

/// A trigger that sends events into a stream on a schedule.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct TriggerDefinition {
    pub eventflux_element: EventFluxElement,

    pub id: String,
    /// Fire period in milliseconds. When set, the runtime uses it and ignores `at`.
    pub at_every: Option<i64>,
    /// `"start"` to fire once when the app starts, or else a cron expression.
    pub at: Option<String>,
    pub annotations: Vec<Annotation>,
}

impl TriggerDefinition {
    pub fn new(id: String) -> Self {
        TriggerDefinition {
            eventflux_element: EventFluxElement::default(),
            id,
            at_every: None,
            at: None,
            annotations: Vec::new(),
        }
    }

    /// Start a builder chain with only the id set.
    pub fn id(id: String) -> Self {
        TriggerDefinition {
            id,
            annotations: Vec::new(),
            ..Default::default()
        }
    }

    pub fn at_every(mut self, time_in_milliseconds: i64) -> Self {
        self.at_every = Some(time_in_milliseconds);
        self
    }

    /// Set the fire period from a constant holding milliseconds.
    ///
    /// # Errors
    ///
    /// Returns an error unless the constant is a Time or Long value.
    pub fn at_every_time_constant(mut self, time_constant: Constant) -> Result<Self, String> {
        match time_constant.value {
            ConstantValueWithFloat::Time(ms) => {
                self.at_every = Some(ms);
                Ok(self)
            }
            ConstantValueWithFloat::Long(ms) => {
                self.at_every = Some(ms);
                Ok(self)
            }
            _ => Err("at_every_time_constant expects a Time or Long constant".to_string()),
        }
    }

    /// Set `at`. Pass `"start"` or a cron expression.
    pub fn at(mut self, interval: String) -> Self {
        self.at = Some(interval);
        self
    }

    pub fn annotation(mut self, annotation: Annotation) -> Self {
        self.annotations.push(annotation);
        self
    }
}

// Possible Deref impls, if callers ever need direct access to the EventFluxElement fields.
// impl std::ops::Deref for TriggerDefinition {
//     type Target = EventFluxElement;
//     fn deref(&self) -> &Self::Target { &self.eventflux_element }
// }
// impl std::ops::DerefMut for TriggerDefinition {
//     fn deref_mut(&mut self) -> &mut Self::Target { &mut self.eventflux_element }
// }
