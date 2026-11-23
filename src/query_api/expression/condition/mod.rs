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

// Condition expressions. AND, OR, NOT, comparisons, IN and IS NULL.
//
// Each condition struct holds an `eventflux_element: EventFluxElement` and its operands,
// usually as Box<Expression>. The Expression enum in expression/expression.rs wraps them,
// as in Expression::And(Box<And>). IsNull uses Option fields because it can test either
// an expression or a stream.

pub mod and;
pub mod compare;
pub mod in_op; // not named `in` because that is a Rust keyword
pub mod is_null;
pub mod not;
pub mod or;

pub use self::and::And;
pub use self::compare::{Compare, Operator as CompareOperator};
pub use self::in_op::InOp;
pub use self::is_null::IsNull;
pub use self::not::Not;
pub use self::or::Or;
