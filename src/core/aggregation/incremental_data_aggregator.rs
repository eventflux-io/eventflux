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

use super::aggregation_runtime::AggregationRuntime;
use crate::core::event::value::AttributeValue;
use crate::query_api::aggregation::time_period::Duration as TimeDuration;

#[derive(Debug, Default)]
pub struct IncrementalDataAggregator;

impl IncrementalDataAggregator {
    pub fn new() -> Self {
        Self {}
    }

    pub fn aggregate(
        runtime: &AggregationRuntime,
        duration: TimeDuration,
    ) -> Vec<Vec<AttributeValue>> {
        runtime.query_all(duration)
    }
}
