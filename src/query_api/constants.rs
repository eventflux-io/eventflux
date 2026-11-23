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

// Shared names and flags used across the query API.

pub const ANNOTATION_INFO: &str = "info";
pub const ANNOTATION_ELEMENT_NAME: &str = "name";

pub const FAULT_STREAM_FLAG: &str = "!";
pub const INNER_STREAM_FLAG: &str = "#";
pub const TRIGGERED_TIME: &str = "triggered_time";

// Index value that means the last event, as in e1[last]. Same value as
// expression::variable::LAST_INDEX.
pub const LAST_INDEX: i32 = -2;
