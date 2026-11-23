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

// Home for errors raised while building query API objects. It defines none today.
//
// Most errors come from checking the EventFlux app text while it is parsed, not from
// building or using these objects after parsing. Those live in the sql_compiler errors.

// A possible shape, for when building an Expression can fail on a bad Constant type:
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

// Add errors here when a method needs to reject a bad object. Some factory methods panic
// instead today. Returning Result<T, QueryApiConstructionError> would be the better fit.
