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

// Corresponds to io.eventflux.query.api.util.EventFluxConstants

pub const ANNOTATION_INFO: &str = "info";
pub const ANNOTATION_ELEMENT_NAME: &str = "name";

pub const FAULT_STREAM_FLAG: &str = "!";
pub const INNER_STREAM_FLAG: &str = "#";
pub const TRIGGERED_TIME: &str = "triggered_time";

pub const LAST_INDEX: i32 = -2; // Renamed from LAST to avoid keyword clash and be more descriptive
                                // Variable.java also has a LAST = -2, which was translated to LAST_INDEX there.
                                // This makes it consistent.
