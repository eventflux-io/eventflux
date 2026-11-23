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

pub mod and_expression_executor;
pub mod bool_expression_executor;
pub mod case_expression_executor;
pub mod compare_expression_executor;
pub mod in_expression_executor;
pub mod is_null_expression_executor;
pub mod not_expression_executor;
pub mod or_expression_executor;

pub use self::and_expression_executor::AndExpressionExecutor;
pub use self::bool_expression_executor::BoolExpressionExecutor;
pub use self::case_expression_executor::CaseExpressionExecutor;
pub use self::compare_expression_executor::CompareExpressionExecutor;
pub use self::in_expression_executor::InExpressionExecutor;
pub use self::is_null_expression_executor::IsNullExpressionExecutor;
pub use self::not_expression_executor::NotExpressionExecutor;
pub use self::or_expression_executor::OrExpressionExecutor;
// The compare operator comes from query_api::expression::condition::CompareOperator, so it
// is not re-exported here.

// In Java these classes extend the abstract ConditionExpressionExecutor. Here they just
// implement the ExpressionExecutor trait, so no separate base type is needed. What they
// share is that they all return Attribute::Type::BOOL.
