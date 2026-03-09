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

// This is the main mod.rs for the expression module (eventflux/src/query_api/expression/mod.rs)

// Declare sub-modules for different expression categories
pub mod case;
pub mod condition;
pub mod constant;
pub mod math;

// Declare modules for individual expression types at this level
pub mod attribute_function;
pub mod cast;
pub mod expression;
pub mod indexed_variable;
pub mod variable; // This is the main Expression enum

// Re-export the main Expression enum and key structs/enums for easier access
// from parent modules (e.g., query_api)
pub use self::attribute_function::AttributeFunction;
pub use self::case::{Case, WhenClause};
pub use self::cast::Cast;
pub use self::constant::{Constant, ConstantValueWithFloat, TimeUtil as ConstantTimeUtil}; // Updated ConstantValue to ConstantValueWithFloat
pub use self::expression::Expression;
pub use self::indexed_variable::{EventIndex, IndexedVariable};
pub use self::variable::Variable;

// Re-export all math and condition structs and enums for easier use in Expression factory methods and elsewhere.
pub use self::condition::*;
pub use self::math::*;
// This re-exports CompareOperator from condition/mod.rs, so the specific alias below is not strictly needed
// if condition/mod.rs already exports `Operator as CompareOperator` or just `Operator`.
// The condition/mod.rs has `pub use self::compare::{Compare, Operator as CompareOperator};`
// So `expression::CompareOperator` will be available.
// The line below is fine and explicit.
// pub use self::condition::Operator as CompareOperator; // Re-exporting CompareOperator as it's used in Expression factory methods.
