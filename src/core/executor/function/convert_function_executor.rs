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
use crate::core::util::attribute_converter::get_property_value;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::Arc;

#[derive(Debug)]
pub struct ConvertFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
    target_type: ApiAttributeType,
}

impl ConvertFunctionExecutor {
    pub fn new(
        value_executor: Box<dyn ExpressionExecutor>,
        type_executor: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        if type_executor.get_return_type() != ApiAttributeType::STRING {
            return Err("convert() type argument must be STRING".to_string());
        }
        let type_val = match type_executor.execute(None) {
            Some(AttributeValue::String(s)) => s.to_lowercase(),
            _ => return Err("convert() requires constant type string".to_string()),
        };
        let target_type = match type_val.as_str() {
            "string" => ApiAttributeType::STRING,
            "int" => ApiAttributeType::INT,
            "long" => ApiAttributeType::LONG,
            "float" => ApiAttributeType::FLOAT,
            "double" => ApiAttributeType::DOUBLE,
            "bool" | "boolean" => ApiAttributeType::BOOL,
            _ => return Err(format!("Unsupported convert target type: {type_val}")),
        };
        Ok(Self {
            value_executor,
            target_type,
        })
    }
}

impl ExpressionExecutor for ConvertFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let value = self.value_executor.execute(event)?;
        get_property_value(value, self.target_type)
    }

    fn get_return_type(&self) -> ApiAttributeType {
        self.target_type
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(ConvertFunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
            target_type: self.target_type,
        })
    }
}
