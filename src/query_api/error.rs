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

// This file is for defining error types specific to the eventflux_query_api Rust representation,
// if any are needed for structural validation or construction that isn't covered by panics
// or standard Result types from method calls.

// Most exceptions from io.eventflux.query.api.exception are related to the validation
// of a EventFlux Application string during its parsing by the EventFlux compiler,
// rather than errors that would arise from building or using the query API objects directly
// in Rust once they are parsed.

// For example, if building an Expression requires a specific type of Constant:
// #[derive(Debug, Clone, PartialEq, Eq, Hash)]
// pub enum QueryApiConstructionError {
//     InvalidArgument(String),
//     IncompatibleTypes(String),
// }

// impl std::fmt::Display for QueryApiConstructionError {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             QueryApiConstructionError::InvalidArgument(msg) => write!(f, "Invalid argument: {}", msg),
//             QueryApiConstructionError::IncompatibleTypes(msg) => write!(f, "Incompatible types: {}", msg),
//         }
//     }
// }

// impl std::error::Error for QueryApiConstructionError {}

// For now, keeping it empty. Specific errors can be added as needed when implementing
// methods that might fail validation for the object model itself.
// Panics are used in some factory methods for direct translation of Java exceptions,
// but idiomatic Rust would use Result<T, QueryApiConstructionError>.
