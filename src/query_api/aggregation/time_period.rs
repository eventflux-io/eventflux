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
// Expression is not directly part of TimePeriod fields, but Duration might be associated with values if not just enum.
// However, Java's TimePeriod.Duration is just an enum.

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
    // Private constructor as in Java, use factory methods.
    fn new(operator: Operator, durations: Vec<Duration>) -> Self {
        Self {
            eventflux_element: EventFluxElement::default(),
            operator,
            durations,
        }
    }

    // Corresponds to TimePeriod.range(Duration begging, Duration end)
    pub fn range(start: Duration, end: Duration) -> Self {
        Self::new(Operator::Range, vec![start, end])
    }

    // Corresponds to TimePeriod.interval(Duration... durations)
    pub fn interval(durations: Vec<Duration>) -> Self {
        // Java uses varargs, which implies at least one.
        // An empty Vec<Duration> for interval might be valid or not depending on EventFlux logic.
        Self::new(Operator::Interval, durations)
    }
}
