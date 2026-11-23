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
use crate::query_api::expression::constant::{Constant, ConstantValueWithFloat as ConstantValue};

/// Which events to emit from each batch. Defaults to `All`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy, Default)]
pub enum OutputRateBehavior {
    #[default]
    All,
    First,
    Last,
}

/// The kind of output rate limit with its settings.
#[derive(Clone, Debug, PartialEq)]
pub enum OutputRateVariant {
    Events(EventsOutputRate, OutputRateBehavior),
    Time(TimeOutputRate, OutputRateBehavior),
    // Takes no behaviour. A snapshot always emits the whole state.
    Snapshot(SnapshotOutputRate),
}

// Emit every event, with All behaviour. A query with no rate limit at all uses
// `output_rate: None` instead.
impl Default for OutputRateVariant {
    fn default() -> Self {
        OutputRateVariant::Events(EventsOutputRate::new(1), OutputRateBehavior::All)
    }
}

/// Output rate limit of a query, as in the OUTPUT clause.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct OutputRate {
    pub eventflux_element: EventFluxElement,
    pub variant: OutputRateVariant,
}

impl OutputRate {
    pub fn new(variant: OutputRateVariant) -> Self {
        OutputRate {
            eventflux_element: EventFluxElement::default(),
            variant,
        }
    }

    /// Limit by event count.
    ///
    /// # Errors
    ///
    /// Returns an error unless the constant is an Int or Long. A Long is cast to i32
    /// without a range check, so a value above `i32::MAX` wraps.
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

    /// Limit by time. The constant holds milliseconds.
    ///
    /// # Errors
    ///
    /// Returns an error unless the constant is a Time or Long.
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

    /// Snapshot output every period. The constant holds milliseconds.
    ///
    /// # Errors
    ///
    /// Returns an error unless the constant is a Time or Long.
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
