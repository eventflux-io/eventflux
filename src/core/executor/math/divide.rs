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

use super::common::CoerceNumeric;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;

/// Divides two numbers with `/`. Always returns a DOUBLE, even for INT / INT.
///
/// NULL on either side, or dividing by zero, gives NULL. No value on either side gives
/// `None`.
#[derive(Debug)]
pub struct DivideExpressionExecutor {
    left_executor: Box<dyn ExpressionExecutor>,
    right_executor: Box<dyn ExpressionExecutor>,
    return_type: ApiAttributeType, // Always DOUBLE today
}

impl DivideExpressionExecutor {
    /// # Errors
    ///
    /// Returns an error if either operand is STRING, BOOL or OBJECT.
    pub fn new(
        left: Box<dyn ExpressionExecutor>,
        right: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        let left_type = left.get_return_type();
        let right_type = right.get_return_type();

        // The result is always DOUBLE, which avoids losing precision and matches common SQL
        // behaviour for `/`. The Java typed executors (such as DivideExpressionExecutorInt)
        // keep integer division instead. Integer division would need its own executor or
        // operator.
        let return_type = match (left_type, right_type) {
            (ApiAttributeType::STRING, _)
            | (_, ApiAttributeType::STRING)
            | (ApiAttributeType::BOOL, _)
            | (_, ApiAttributeType::BOOL)
            | (ApiAttributeType::OBJECT, _)
            | (_, ApiAttributeType::OBJECT) => {
                return Err(format!(
                    "Division not supported for input types {left_type:?} and {right_type:?}"
                ));
            }
            _ => ApiAttributeType::DOUBLE,
        };
        // To match the Java per type executors instead, with INT / INT giving INT and so on:
        // let return_type = match (left_type, right_type) {
        //     (ApiAttributeType::DOUBLE, _) | (_, ApiAttributeType::DOUBLE) => ApiAttributeType::DOUBLE,
        //     (ApiAttributeType::FLOAT, _) | (_, ApiAttributeType::FLOAT) => ApiAttributeType::FLOAT,
        //     (ApiAttributeType::LONG, _) | (_, ApiAttributeType::LONG) => ApiAttributeType::LONG, // if long div long -> long
        //     (ApiAttributeType::INT, _) | (_, ApiAttributeType::INT) => ApiAttributeType::INT, // if int div int -> int
        //     _ => return Err(...)
        // };

        Ok(Self {
            left_executor: left,
            right_executor: right,
            return_type,
        })
    }
}

impl ExpressionExecutor for DivideExpressionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let left_val_opt = self.left_executor.execute(event);
        let right_val_opt = self.right_executor.execute(event);

        match (left_val_opt, right_val_opt) {
            (Some(left_val), Some(right_val)) => {
                if matches!(left_val, AttributeValue::Null)
                    || matches!(right_val, AttributeValue::Null)
                {
                    return Some(AttributeValue::Null);
                }

                // Divide as f64, then cast to the return type
                let l = left_val.to_f64_or_err_str("Divide")?;
                let r = right_val.to_f64_or_err_str("Divide")?;

                if r == 0.0 {
                    // Dividing by zero gives NULL, as is common in SQL, instead of an error
                    // log_error!("Division by zero error");
                    return Some(AttributeValue::Null);
                }
                let result = l / r;

                match self.return_type {
                    // Only DOUBLE is reachable today. The other arms cover a future change
                    // to the return type.
                    ApiAttributeType::DOUBLE => Some(AttributeValue::Double(result)),
                    ApiAttributeType::FLOAT => Some(AttributeValue::Float(result as f32)),
                    ApiAttributeType::LONG => Some(AttributeValue::Long(result as i64)), // Truncating
                    ApiAttributeType::INT => Some(AttributeValue::Int(result as i32)), // Truncating
                    _ => {
                        // log_error!("Division resulted in unexpected return type: {:?}", self.return_type);
                        None
                    }
                }
            }
            _ => None,
        }
    }
    fn get_return_type(&self) -> ApiAttributeType {
        self.return_type
    }
    fn clone_executor(
        &self,
        eventflux_app_context: &std::sync::Arc<
            crate::core::config::eventflux_app_context::EventFluxAppContext,
        >,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(DivideExpressionExecutor {
            left_executor: self.left_executor.clone_executor(eventflux_app_context),
            right_executor: self.right_executor.clone_executor(eventflux_app_context),
            return_type: self.return_type,
        })
    }
}
