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

// Corresponds to parts of io.eventflux.query.api.execution.query.output.ratelimit.TimeOutputRate

// This struct holds the specific data for time-based rate limiting.
// EventFluxElement context and OutputRateBehavior are in the main OutputRate struct.

#[derive(Clone, Debug, PartialEq, Default)] // Added Default
pub struct TimeOutputRate {
    pub time_value_millis: i64, // Defaults to 0
}

impl TimeOutputRate {
    pub fn new(time_value_millis: i64) -> Self {
        TimeOutputRate { time_value_millis }
    }
}
