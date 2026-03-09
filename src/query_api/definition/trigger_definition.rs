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

// Corresponds to io.eventflux.query.api.definition.TriggerDefinition
use crate::query_api::annotation::Annotation;
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::expression::constant::{Constant, ConstantValueWithFloat};

#[derive(Clone, Debug, PartialEq, Default)] // Added Default
pub struct TriggerDefinition {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement

    // TriggerDefinition fields
    pub id: String,
    pub at_every: Option<i64>, // Java Long maps to i64 in Rust
    pub at: Option<String>,    // For cron expressions or similar string-based time definitions
    pub annotations: Vec<Annotation>,
}

impl TriggerDefinition {
    // Constructor that takes an id, as per Java's `TriggerDefinition.id(id)`
    pub fn new(id: String) -> Self {
        TriggerDefinition {
            eventflux_element: EventFluxElement::default(),
            id,
            at_every: None,
            at: None,
            annotations: Vec::new(),
        }
    }

    // Static factory method `id` from Java
    pub fn id(id: String) -> Self {
        // This creates a new TriggerDefinition with the given ID and defaults for other fields
        TriggerDefinition {
            id,
            annotations: Vec::new(),
            ..Default::default()
        }
    }

    // Builder-style methods
    pub fn at_every(mut self, time_in_milliseconds: i64) -> Self {
        self.at_every = Some(time_in_milliseconds);
        self
    }

    // Builder method for TimeConstant
    pub fn at_every_time_constant(mut self, time_constant: Constant) -> Result<Self, String> {
        match time_constant.value {
            ConstantValueWithFloat::Time(ms) => {
                self.at_every = Some(ms);
                Ok(self)
            }
            ConstantValueWithFloat::Long(ms) => {
                // Also allow Long for flexibility as in Java
                self.at_every = Some(ms);
                Ok(self)
            }
            _ => Err("at_every_time_constant expects a Time or Long constant".to_string()),
        }
    }

    pub fn at(mut self, interval: String) -> Self {
        self.at = Some(interval);
        self
    }

    pub fn annotation(mut self, annotation: Annotation) -> Self {
        self.annotations.push(annotation);
        self
    }
}

// Removed: impl From<EventFluxElement> for TriggerDefinition

// For direct field access via Deref, if desired:
// impl std::ops::Deref for TriggerDefinition {
//     type Target = EventFluxElement;
//     fn deref(&self) -> &Self::Target { &self.eventflux_element }
// }
// impl std::ops::DerefMut for TriggerDefinition {
//     fn deref_mut(&mut self) -> &mut Self::Target { &mut self.eventflux_element }
// }
