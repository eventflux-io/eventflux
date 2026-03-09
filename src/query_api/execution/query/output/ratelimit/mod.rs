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

pub mod events_output_rate;
pub mod output_rate;
pub mod snapshot_output_rate;
pub mod time_output_rate; // This is the main file with the OutputRate enum/struct

pub use self::events_output_rate::EventsOutputRate;
pub use self::output_rate::{OutputRate, OutputRateBehavior, OutputRateVariant};
pub use self::snapshot_output_rate::SnapshotOutputRate;
pub use self::time_output_rate::TimeOutputRate;
