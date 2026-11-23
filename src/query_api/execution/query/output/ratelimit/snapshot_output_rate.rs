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

/// Snapshot output rate, as in `OUTPUT SNAPSHOT EVERY 1 sec`. Emits the full current
/// state once per period.
///
/// Takes no All, First or Last behaviour. The query positions live on the wrapping
/// OutputRate.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct SnapshotOutputRate {
    /// Period in milliseconds. Defaults to 0.
    pub time_value_millis: i64,
}

impl SnapshotOutputRate {
    pub fn new(time_value_millis: i64) -> Self {
        SnapshotOutputRate { time_value_millis }
    }
}
