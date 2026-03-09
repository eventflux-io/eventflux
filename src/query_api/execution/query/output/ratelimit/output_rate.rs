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

use super::events_output_rate::EventsOutputRate;
use super::snapshot_output_rate::SnapshotOutputRate;
use super::time_output_rate::TimeOutputRate;
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::expression::constant::{Constant, ConstantValueWithFloat as ConstantValue}; // Use renamed ConstantValue

#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy, Default)] // Added Eq, Hash, Copy
pub enum OutputRateBehavior {
    #[default]
    All,
    First,
    Last,
}

#[derive(Clone, Debug, PartialEq)]
pub enum OutputRateVariant {
    Events(EventsOutputRate, OutputRateBehavior),
    Time(TimeOutputRate, OutputRateBehavior),
    Snapshot(SnapshotOutputRate), // Snapshot implies 'All' behavior for its interval
}

impl Default for OutputRateVariant {
    fn default() -> Self {
        // Default to "output every event" which is EventOutputRate with ALL behavior and a count of 1 (or similar)
        // Or, more simply, just "ALL events" without specific count/time, if that's a valid standalone concept in EventFlux.
        // Java's default Query has no OutputRate. If one is added, `output every events` is common.
        // Let's default to a conceptual "all events without specific rate control"
        // which could be represented by EventsOutputRate(count=some_very_high_number or special_value, OutputRateBehavior::All)
        // Or, if OutputRate itself is Option in Query, None implies no rate limiting.
        // For a concrete default here, let's pick "every event".
        OutputRateVariant::Events(EventsOutputRate::new(1), OutputRateBehavior::All)
        // Default: output every 1 event
    }
}

#[derive(Clone, Debug, PartialEq, Default)] // Added Default
pub struct OutputRate {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement
    pub variant: OutputRateVariant,
}

impl OutputRate {
    // Constructor for specific variant
    pub fn new(variant: OutputRateVariant) -> Self {
        OutputRate {
            eventflux_element: EventFluxElement::default(),
            variant,
        }
    }

    // Factory methods based on Java static methods in OutputRate.java
    pub fn per_events(
        events_constant: Constant,
        behavior: OutputRateBehavior,
    ) -> Result<Self, String> {
        let event_count = match events_constant.value {
            ConstantValue::Long(val) => val as i32,
            ConstantValue::Int(val) => val,
            _ => {
                return Err(
                    "Unsupported constant type for events count, expected Int or Long.".to_string(),
                )
            }
        };
        Ok(Self::new(OutputRateVariant::Events(
            EventsOutputRate::new(event_count),
            behavior,
        )))
    }

    pub fn per_time_period(
        time_constant: Constant,
        behavior: OutputRateBehavior,
    ) -> Result<Self, String> {
        let time_millis = match time_constant.value {
            ConstantValue::Time(ms) | ConstantValue::Long(ms) => ms,
            _ => {
                return Err(
                    "Unsupported constant type for time period, expected Time or Long.".to_string(),
                )
            }
        };
        Ok(Self::new(OutputRateVariant::Time(
            TimeOutputRate::new(time_millis),
            behavior,
        )))
    }

    pub fn per_snapshot(time_constant: Constant) -> Result<Self, String> {
        let time_millis = match time_constant.value {
            ConstantValue::Time(ms) | ConstantValue::Long(ms) => ms,
            _ => {
                return Err(
                    "Unsupported constant type for snapshot period, expected Time or Long."
                        .to_string(),
                )
            }
        };
        Ok(Self::new(OutputRateVariant::Snapshot(
            SnapshotOutputRate::new(time_millis),
        )))
    }

    pub fn is_snapshot(&self) -> bool {
        matches!(self.variant, OutputRateVariant::Snapshot(_))
    }
}
