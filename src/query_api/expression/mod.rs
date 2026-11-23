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

// Expressions used in queries. The Expression enum and every node type it holds.

pub mod case;
pub mod condition;
pub mod constant;
pub mod math;

pub mod attribute_function;
pub mod cast;
#[allow(clippy::module_inception)]
pub mod expression; // the Expression enum
pub mod indexed_variable;
pub mod variable;

pub use self::attribute_function::AttributeFunction;
pub use self::case::{Case, WhenClause};
pub use self::cast::Cast;
pub use self::constant::{Constant, ConstantValueWithFloat, TimeUtil as ConstantTimeUtil};
pub use self::expression::Expression;
pub use self::indexed_variable::{EventIndex, IndexedVariable};
pub use self::variable::Variable;

// All math and condition types, used by the Expression factory methods and elsewhere.
pub use self::condition::*;
pub use self::math::*;
// Not needed. condition/mod.rs already exports `Operator as CompareOperator`, so the glob
// above makes `expression::CompareOperator` available.
// pub use self::condition::Operator as CompareOperator; // Re-exporting CompareOperator as it's used in Expression factory methods.
