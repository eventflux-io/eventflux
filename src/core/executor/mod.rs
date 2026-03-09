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

pub mod cast_executor;
pub mod collection_aggregation_executor;
pub mod condition;
pub mod constant_expression_executor;
pub mod event_variable_function_executor;
pub mod expression_executor;
pub mod function; // Added for function executors
pub mod incremental;
pub mod indexed_variable_executor;
pub mod math;
pub mod multi_value_variable_function_executor;
pub mod variable_expression_executor; // For incremental aggregation executors

pub use self::cast_executor::CastExecutor;
pub use self::collection_aggregation_executor::{
    CollectionAvgExecutor, CollectionCountExecutor, CollectionMinMaxExecutor,
    CollectionStdDevExecutor, CollectionSumExecutor, MinMaxType,
};
pub use self::condition::*;
pub use self::constant_expression_executor::ConstantExpressionExecutor;
pub use self::event_variable_function_executor::EventVariableFunctionExecutor;
pub use self::expression_executor::ExpressionExecutor;
pub use self::function::*; // Re-export function executors
pub use self::indexed_variable_executor::IndexedVariableExecutor;
pub use self::math::*;
pub use self::multi_value_variable_function_executor::MultiValueVariableFunctionExecutor;
pub use self::variable_expression_executor::VariableExpressionExecutor; // Removed VariablePosition, EventDataArrayType
                                                                        // AttributeDynamicResolveType was in the prompt for VariableExpressionExecutor, but not used in my simplified impl yet.
                                                                        // If it were, it would be exported:
pub use self::incremental::*;
