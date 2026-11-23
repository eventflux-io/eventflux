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

use crate::query_api::eventflux_element::EventFluxElement;

/// Time unit used by incremental aggregation, from seconds up to years.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy, Default)]
pub enum Duration {
    #[default]
    Seconds,
    Minutes,
    Hours,
    Days,
    Months,
    Years,
}

impl Duration {
    /// Length of one unit in milliseconds.
    ///
    /// Months are a fixed 30 days and years a fixed 365 days, so this does not follow the
    /// calendar.
    pub fn to_millis(self) -> u64 {
        match self {
            Duration::Seconds => 1000,
            Duration::Minutes => 60_000,
            Duration::Hours => 3_600_000,
            Duration::Days => 86_400_000,
            Duration::Months => 2_592_000_000,
            Duration::Years => 31_536_000_000,
        }
    }

    /// The next larger unit, or `None` for `Years`.
    pub fn next(self) -> Option<Duration> {
        match self {
            Duration::Seconds => Some(Duration::Minutes),
            Duration::Minutes => Some(Duration::Hours),
            Duration::Hours => Some(Duration::Days),
            Duration::Days => Some(Duration::Months),
            Duration::Months => Some(Duration::Years),
            Duration::Years => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy, Default)]
pub enum Operator {
    Range,
    #[default]
    Interval,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct TimePeriod {
    pub eventflux_element: EventFluxElement,
    pub operator: Operator,
    pub durations: Vec<Duration>,
}

impl TimePeriod {
    // Private. Build through range() or interval().
    fn new(operator: Operator, durations: Vec<Duration>) -> Self {
        Self {
            eventflux_element: EventFluxElement::default(),
            operator,
            durations,
        }
    }

    /// A period covering every unit from `start` to `end`. Stores just the two ends.
    pub fn range(start: Duration, end: Duration) -> Self {
        Self::new(Operator::Range, vec![start, end])
    }

    /// A period made of the listed units.
    pub fn interval(durations: Vec<Duration>) -> Self {
        // An empty list is not rejected here.
        Self::new(Operator::Interval, durations)
    }
}
