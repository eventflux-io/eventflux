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

// src/core/executor/math/mod.rs

pub mod add;
pub mod common; // Added common module for CoerceNumeric
pub mod divide;
pub mod mod_expression_executor;
pub mod multiply;
pub mod subtract; // Changed from mod_op to match filename

pub use self::add::AddExpressionExecutor;
pub use self::divide::DivideExpressionExecutor;
pub use self::mod_expression_executor::ModExpressionExecutor;
pub use self::multiply::MultiplyExpressionExecutor;
pub use self::subtract::SubtractExpressionExecutor;
// CoerceNumeric is pub(super) in common.rs, so not re-exported here.
