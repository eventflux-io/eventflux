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
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct IncrementalShouldUpdateFunctionExecutor {
    timestamp_executor: Box<dyn ExpressionExecutor>,
    last_timestamp: Mutex<i64>,
}

impl IncrementalShouldUpdateFunctionExecutor {
    pub fn new(arg: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        if arg.get_return_type() != ApiAttributeType::LONG {
            return Err("shouldUpdate() expects LONG argument".into());
        }
        Ok(Self {
            timestamp_executor: arg,
            last_timestamp: Mutex::new(0),
        })
    }
}

impl ExpressionExecutor for IncrementalShouldUpdateFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let val = self.timestamp_executor.execute(event)?;
        let ts = match val {
            AttributeValue::Long(v) => v,
            AttributeValue::Int(v) => v as i64,
            _ => return None,
        };
        let mut last = self.last_timestamp.lock().unwrap();
        if ts >= *last {
            *last = ts;
            Some(AttributeValue::Bool(true))
        } else {
            Some(AttributeValue::Bool(false))
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::BOOL
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(Self {
            timestamp_executor: self.timestamp_executor.clone_executor(ctx),
            last_timestamp: Mutex::new(0),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::stream::stream_event::StreamEvent;
    use crate::core::executor::constant_expression_executor::ConstantExpressionExecutor;
    use crate::core::executor::variable_expression_executor::VariableExpressionExecutor;
    use crate::core::util::eventflux_constants::{
        BEFORE_WINDOW_DATA_INDEX, STREAM_ATTRIBUTE_INDEX_IN_TYPE, STREAM_ATTRIBUTE_TYPE_INDEX,
    };

    #[test]
    fn test_should_update() {
        let var_exec = Box::new(VariableExpressionExecutor::new(
            [0, 0, BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::LONG,
            "ts".to_string(),
        ));
        let exec = IncrementalShouldUpdateFunctionExecutor::new(var_exec).unwrap();
        let mut e1 = StreamEvent::new(0, 1, 0, 0);
        e1.before_window_data[0] = AttributeValue::Long(100);
        assert_eq!(exec.execute(Some(&e1)), Some(AttributeValue::Bool(true)));
        let mut e2 = StreamEvent::new(0, 1, 0, 0);
        e2.before_window_data[0] = AttributeValue::Long(90);
        assert_eq!(exec.execute(Some(&e2)), Some(AttributeValue::Bool(false)));
    }
}
