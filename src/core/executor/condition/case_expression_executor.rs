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

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use crate::query_api::expression::Case;
use std::fmt;
use std::sync::Arc;

/// Runs CASE expressions, both Searched and Simple CASE.
///
/// Searched CASE: CASE WHEN condition THEN result ... ELSE default END
/// Simple CASE: CASE operand WHEN value THEN result ... ELSE default END
///
/// SQL-92 rules:
/// - Types are checked when built. All results must share one type, with OBJECT
///   (a NULL literal) allowed anywhere.
/// - NULL never equals NULL, so CASE NULL WHEN NULL returns the ELSE result
/// - Stops at the first matching WHEN
/// - ELSE is always present. The parser adds ELSE NULL when it is missing.
///
/// If the operand, a WHEN value or a WHEN condition gives no value, the whole CASE gives
/// `None`. A Searched CASE condition that is not BOOL or NULL also gives `None`.
pub struct CaseExpressionExecutor {
    /// Operand for Simple CASE (CASE expr WHEN val...).
    /// None for Searched CASE (CASE WHEN condition...).
    operand_executor: Option<Arc<dyn ExpressionExecutor>>,

    /// WHEN clauses as (condition or value, result)
    when_executors: Vec<WhenClauseExecutor>,

    /// ELSE result. Always present, since the parser adds NULL if missing.
    else_executor: Arc<dyn ExpressionExecutor>,

    /// Result type, checked when built
    result_type: ApiAttributeType,
}

struct WhenClauseExecutor {
    condition_executor: Arc<dyn ExpressionExecutor>,
    result_executor: Arc<dyn ExpressionExecutor>,
}

impl CaseExpressionExecutor {
    /// Builds the executor and checks the types. `_case` is not used.
    ///
    /// # Errors
    ///
    /// Returns an error when there is no WHEN clause, when a WHEN or ELSE result type
    /// differs from the result type, when a Simple CASE operand cannot be compared with
    /// a WHEN value, or when a Searched CASE condition is not BOOL.
    pub fn new(
        _case: &Case,
        operand_executor: Option<Arc<dyn ExpressionExecutor>>,
        when_executors: Vec<(Arc<dyn ExpressionExecutor>, Arc<dyn ExpressionExecutor>)>,
        else_executor: Arc<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        if when_executors.is_empty() {
            return Err("CASE expression must have at least one WHEN clause".to_string());
        }

        // The result type is the first type that is not OBJECT (NULL), from the WHEN
        // results and then ELSE. It stays OBJECT if all are NULL.
        let mut result_type = ApiAttributeType::OBJECT;
        for (_, result_exec) in &when_executors {
            let when_type = result_exec.get_return_type();
            if when_type != ApiAttributeType::OBJECT {
                result_type = when_type;
                break;
            }
        }
        if result_type == ApiAttributeType::OBJECT {
            let else_type = else_executor.get_return_type();
            if else_type != ApiAttributeType::OBJECT {
                result_type = else_type;
            }
        }

        // Every WHEN result must match the result type, or be OBJECT (NULL)
        for (idx, (_, result_exec)) in when_executors.iter().enumerate() {
            let when_type = result_exec.get_return_type();
            if when_type != result_type && when_type != ApiAttributeType::OBJECT {
                return Err(format!(
                    "CASE expression type mismatch: WHEN clause {} returns {:?}, expected {:?}",
                    idx + 1,
                    when_type,
                    result_type
                ));
            }
        }

        // Same for ELSE. OBJECT covers the ELSE NULL the parser adds.
        let else_type = else_executor.get_return_type();
        if else_type != result_type && else_type != ApiAttributeType::OBJECT {
            return Err(format!(
                "CASE expression type mismatch: ELSE clause returns {:?}, expected {:?}",
                else_type, result_type
            ));
        }

        // Simple CASE needs WHEN values comparable with the operand. Searched CASE needs
        // BOOL conditions.
        if let Some(ref operand_exec) = operand_executor {
            let operand_type = operand_exec.get_return_type();
            for (idx, (when_exec, _)) in when_executors.iter().enumerate() {
                let when_type = when_exec.get_return_type();
                if !Self::are_comparable_types(&operand_type, &when_type) {
                    return Err(format!(
                        "CASE expression Simple CASE type mismatch: operand type {:?} not comparable with WHEN clause {} type {:?}",
                        operand_type,
                        idx + 1,
                        when_type
                    ));
                }
            }
        } else {
            for (idx, (condition_exec, _)) in when_executors.iter().enumerate() {
                let condition_type = condition_exec.get_return_type();
                if condition_type != ApiAttributeType::BOOL {
                    return Err(format!(
                        "CASE expression: WHEN clause {} condition must be BOOL, got {:?}",
                        idx + 1,
                        condition_type
                    ));
                }
            }
        }

        Ok(Self {
            operand_executor,
            when_executors: when_executors
                .into_iter()
                .map(|(cond, res)| WhenClauseExecutor {
                    condition_executor: cond,
                    result_executor: res,
                })
                .collect(),
            else_executor,
            result_type,
        })
    }

    /// True when a Simple CASE operand of one type can be compared with a WHEN value of the
    /// other. Same types, any pair with OBJECT, and any two numeric types are comparable.
    fn are_comparable_types(type1: &ApiAttributeType, type2: &ApiAttributeType) -> bool {
        if type1 == type2 {
            return true;
        }

        // OBJECT (NULL) is allowed with any type. A NULL never matches at runtime.
        if *type1 == ApiAttributeType::OBJECT || *type2 == ApiAttributeType::OBJECT {
            return true;
        }

        let numeric_types = [
            ApiAttributeType::INT,
            ApiAttributeType::LONG,
            ApiAttributeType::FLOAT,
            ApiAttributeType::DOUBLE,
        ];

        let is_type1_numeric = numeric_types.contains(type1);
        let is_type2_numeric = numeric_types.contains(type2);

        is_type1_numeric && is_type2_numeric
    }

    /// Compares two values for equality the SQL way, so NULL never equals anything, NULL
    /// included.
    ///
    /// Numbers of different types (Int, Long, Float, Double) compare by value. Int and
    /// Long against Float are compared as f32, so large values can lose precision.
    fn sql_equals(left: &AttributeValue, right: &AttributeValue) -> bool {
        if matches!(left, AttributeValue::Null) || matches!(right, AttributeValue::Null) {
            return false;
        }

        match (left, right) {
            // Same type comparisons
            (AttributeValue::Int(a), AttributeValue::Int(b)) => a == b,
            (AttributeValue::Long(a), AttributeValue::Long(b)) => a == b,
            (AttributeValue::Float(a), AttributeValue::Float(b)) => a == b,
            (AttributeValue::Double(a), AttributeValue::Double(b)) => a == b,

            // Cross-type integer comparisons (Int <-> Long)
            (AttributeValue::Int(a), AttributeValue::Long(b)) => (*a as i64) == *b,
            (AttributeValue::Long(a), AttributeValue::Int(b)) => *a == (*b as i64),

            // Cross-type float comparisons (Float <-> Double)
            (AttributeValue::Float(a), AttributeValue::Double(b)) => (*a as f64) == *b,
            (AttributeValue::Double(a), AttributeValue::Float(b)) => *a == (*b as f64),

            // Integer to float comparisons
            (AttributeValue::Int(a), AttributeValue::Float(b)) => (*a as f32) == *b,
            (AttributeValue::Float(a), AttributeValue::Int(b)) => *a == (*b as f32),
            (AttributeValue::Int(a), AttributeValue::Double(b)) => (*a as f64) == *b,
            (AttributeValue::Double(a), AttributeValue::Int(b)) => *a == (*b as f64),
            (AttributeValue::Long(a), AttributeValue::Float(b)) => (*a as f32) == *b,
            (AttributeValue::Float(a), AttributeValue::Long(b)) => *a == (*b as f32),
            (AttributeValue::Long(a), AttributeValue::Double(b)) => (*a as f64) == *b,
            (AttributeValue::Double(a), AttributeValue::Long(b)) => *a == (*b as f64),

            // Other types use plain equality
            _ => left == right,
        }
    }
}

impl ExpressionExecutor for CaseExpressionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        if let Some(ref operand_exec) = self.operand_executor {
            // Simple CASE: CASE operand WHEN value1 THEN result1...
            let operand_value = operand_exec.execute(event)?;

            // Return the result of the first matching WHEN
            for when_clause in &self.when_executors {
                let when_value = when_clause.condition_executor.execute(event)?;

                if Self::sql_equals(&operand_value, &when_value) {
                    return when_clause.result_executor.execute(event);
                }
            }

            self.else_executor.execute(event)
        } else {
            // Searched CASE: CASE WHEN condition1 THEN result1...
            for when_clause in &self.when_executors {
                let condition_value = when_clause.condition_executor.execute(event)?;

                match condition_value {
                    AttributeValue::Bool(true) => {
                        return when_clause.result_executor.execute(event);
                    }
                    AttributeValue::Bool(false) | AttributeValue::Null => {
                        continue;
                    }
                    _ => {
                        // Condition is not BOOL or NULL
                        return None;
                    }
                }
            }

            self.else_executor.execute(event)
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        self.result_type
    }

    fn clone_executor(
        &self,
        eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(Self {
            operand_executor: self
                .operand_executor
                .as_ref()
                .map(|exec| Arc::from(exec.clone_executor(eventflux_app_context))),
            when_executors: self
                .when_executors
                .iter()
                .map(|when_clause| WhenClauseExecutor {
                    condition_executor: Arc::from(
                        when_clause
                            .condition_executor
                            .clone_executor(eventflux_app_context),
                    ),
                    result_executor: Arc::from(
                        when_clause
                            .result_executor
                            .clone_executor(eventflux_app_context),
                    ),
                })
                .collect(),
            else_executor: Arc::from(self.else_executor.clone_executor(eventflux_app_context)),
            result_type: self.result_type,
        })
    }
}

impl fmt::Debug for CaseExpressionExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CaseExpressionExecutor")
            .field("is_simple_case", &self.operand_executor.is_some())
            .field("num_when_clauses", &self.when_executors.len())
            .field("result_type", &self.result_type)
            .finish()
    }
}

// Tests live in the integration tests at tests/app_runner_case_expression.rs
