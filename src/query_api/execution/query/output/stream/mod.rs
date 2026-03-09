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

pub mod set_attribute;
pub mod update_set; // Added this line

pub use self::set_attribute::SetAttribute;
pub use self::update_set::UpdateSet; // Added this line

// This module will also contain other output stream action structs:
// InsertIntoStreamAction, ReturnStreamAction, DeleteStreamAction, etc.
// Currently, these are defined directly in `output_stream.rs` (the file that defines the OutputStream enum).
// For better organization, those action structs (InsertIntoStreamAction, etc.) could be moved into this `stream` module
// as well, each in their own file (e.g., insert_into_stream_action.rs).
// And then `output_stream.rs` would `use super::stream::actions::*;` or similar.
// For now, keeping action structs in `output_stream.rs` as per current structure.
