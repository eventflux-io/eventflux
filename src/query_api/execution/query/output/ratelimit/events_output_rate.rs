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

/// Output rate limit by event count, as in `OUTPUT EVERY 5 EVENTS`.
///
/// The query positions and the All, First or Last behaviour live on the wrapping
/// OutputRate and OutputRateVariant.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct EventsOutputRate {
    /// Number of events per output. Defaults to 0.
    pub event_count: i32,
}

impl EventsOutputRate {
    pub fn new(event_count: i32) -> Self {
        EventsOutputRate { event_count }
    }
}
