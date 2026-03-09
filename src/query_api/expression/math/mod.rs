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

// Corresponds to the package io.eventflux.query.api.expression.math

pub mod add;
pub mod divide;
pub mod mod_op;
pub mod multiply;
pub mod subtract; // Renamed from mod to mod_op to avoid keyword clash

pub use self::add::Add;
pub use self::divide::Divide;
pub use self::mod_op::ModOp;
pub use self::multiply::Multiply;
pub use self::subtract::Subtract;

// Comments from before are still relevant:
// Each math operation struct (Add, Subtract, etc.) will contain:
// - eventflux_element: EventFluxElement (updated from direct fields)
// - left_value: Box<Expression>
// - right_value: Box<Expression>
//
// The Expression enum will be defined in the parent module (expression/expression.rs)
// and will have variants like Expression::Add(Box<Add>), etc.
// Box<T> is used because these are recursive types via the Expression enum.
