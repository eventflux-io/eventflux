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
//
// Reference compatibility tests
// Many of these tests are ported from the reference CEP implementation to check
// that EventFlux has the same features and behaves the same way.
//
// Tests are named by category, test number and feature, for example
// filter_test1_basic_greater_than.
// Ported tests name their reference source in a "Reference:" doc line.

#[path = "../common/mod.rs"]
pub mod common;

pub mod aggregations;
pub mod filters;
pub mod functions;
pub mod joins;
pub mod partitions;
pub mod patterns;
pub mod tables;
pub mod triggers;
pub mod windows;
