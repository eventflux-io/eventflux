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

// Arithmetic expressions.
//
// Each math struct holds an `eventflux_element: EventFluxElement`, a `left_value` and a
// `right_value`, both Box<Expression>. The Expression enum in expression/expression.rs
// wraps them, as in Expression::Add(Box<Add>). The boxes break the type recursion through
// Expression.

pub mod add;
pub mod divide;
pub mod mod_op; // not named `mod` because that is a Rust keyword
pub mod multiply;
pub mod subtract;

pub use self::add::Add;
pub use self::divide::Divide;
pub use self::mod_op::ModOp;
pub use self::multiply::Multiply;
pub use self::subtract::Subtract;
