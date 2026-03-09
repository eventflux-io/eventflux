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

// Corresponds to the package io.eventflux.query.api.expression.condition

pub mod and;
pub mod compare;
pub mod in_op; // Renamed from in to in_op
pub mod is_null;
pub mod not;
pub mod or;

pub use self::and::And;
pub use self::compare::{Compare, Operator as CompareOperator}; // Re-export Operator enum too
pub use self::in_op::InOp;
pub use self::is_null::IsNull;
pub use self::not::Not;
pub use self::or::Or;

// Comments from before, updated for EventFluxElement composition:
// Each condition struct (And, Or, etc.) will contain:
// - eventflux_element: EventFluxElement
// - Other specific fields, often Box<Expression> for operands.
//
// The Expression enum (in expression/expression.rs) will have variants
// like Expression::And(Box<And>), etc.
//
// IsNull struct has Option fields to handle its different construction paths.
