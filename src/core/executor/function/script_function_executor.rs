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
use crate::core::executor::function::scalar_function_executor::ScalarFunctionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ScriptFunctionExecutor {
    name: String,
    return_type: ApiAttributeType,
}

impl ScriptFunctionExecutor {
    pub fn new(name: String, return_type: ApiAttributeType) -> Self {
        Self { name, return_type }
    }
}

impl ExpressionExecutor for ScriptFunctionExecutor {
    fn execute(&self, _event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        eprintln!("Script function '{}' not implemented", self.name);
        Some(AttributeValue::Null)
    }

    fn get_return_type(&self) -> ApiAttributeType {
        self.return_type
    }

    fn clone_executor(&self, _ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(self.clone())
    }
}

impl ScalarFunctionExecutor for ScriptFunctionExecutor {
    fn init(
        &mut self,
        _args: &[Box<dyn ExpressionExecutor>],
        _ctx: &Arc<EventFluxAppContext>,
    ) -> Result<(), String> {
        Ok(())
    }

    fn destroy(&mut self) {}

    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn clone_scalar_function(&self) -> Box<dyn ScalarFunctionExecutor> {
        Box::new(self.clone())
    }
}
