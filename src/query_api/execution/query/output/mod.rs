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

pub mod output_stream; // Defines the main OutputStream enum (wrapping actions) and OutputEventType
pub mod ratelimit;
pub mod stream; // Added this module for specific stream actions like SetAttribute

pub use self::output_stream::{OutputEventType, OutputStream};
// SetAttributePlaceholder was removed from output_stream.rs. SetAttribute is now re-exported below.

pub use self::ratelimit::{
    EventsOutputRate, OutputRate, OutputRateBehavior, OutputRateVariant, SnapshotOutputRate,
    TimeOutputRate,
};
pub use self::stream::{SetAttribute, UpdateSet}; // Re-exporting SetAttribute and UpdateSet
