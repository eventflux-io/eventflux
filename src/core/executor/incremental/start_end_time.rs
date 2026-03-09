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

use super::unix_time::IncrementalUnixTimeFunctionExecutor;
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::Arc;

#[derive(Debug)]
pub struct IncrementalStartTimeEndTimeFunctionExecutor {
    start_executor: Box<dyn ExpressionExecutor>,
    end_executor: Box<dyn ExpressionExecutor>,
}

impl IncrementalStartTimeEndTimeFunctionExecutor {
    pub fn new(
        start_exec: Box<dyn ExpressionExecutor>,
        end_exec: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        Ok(Self {
            start_executor: start_exec,
            end_executor: end_exec,
        })
    }
}

impl ExpressionExecutor for IncrementalStartTimeEndTimeFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let s_val = self.start_executor.execute(event)?;
        let start = match s_val {
            AttributeValue::Long(v) => v,
            AttributeValue::String(ref st) => {
                IncrementalUnixTimeFunctionExecutor::parse_timestamp(st)?
            }
            _ => return None,
        };
        let e_val = self.end_executor.execute(event)?;
        let end = match e_val {
            AttributeValue::Long(v) => v,
            AttributeValue::String(ref st) => {
                IncrementalUnixTimeFunctionExecutor::parse_timestamp(st)?
            }
            _ => return None,
        };
        if start >= end {
            return None;
        }
        Some(AttributeValue::Object(Some(Box::new(vec![
            AttributeValue::Long(start),
            AttributeValue::Long(end),
        ]))))
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::OBJECT
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(Self {
            start_executor: self.start_executor.clone_executor(ctx),
            end_executor: self.end_executor.clone_executor(ctx),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::executor::constant_expression_executor::ConstantExpressionExecutor;

    #[test]
    fn test_start_end() {
        let s = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Long(1000),
            ApiAttributeType::LONG,
        ));
        let e = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Long(2000),
            ApiAttributeType::LONG,
        ));
        let exec = IncrementalStartTimeEndTimeFunctionExecutor::new(s, e).unwrap();
        let res = exec.execute(None).unwrap();
        if let AttributeValue::Object(Some(b)) = res {
            let v = b.downcast_ref::<Vec<AttributeValue>>().unwrap();
            assert_eq!(v[0], AttributeValue::Long(1000));
            assert_eq!(v[1], AttributeValue::Long(2000));
        } else {
            panic!("unexpected result");
        }
    }
}
