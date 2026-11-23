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

use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;

// Defines an InstanceOf*ExpressionExecutor struct that returns true when its argument
// value is the given AttributeValue variant
macro_rules! define_instance_of_executor {
    ($struct_name:ident, $variant_name:ident) => {
        #[derive(Debug)] // No Clone because of the Box<dyn ExpressionExecutor> field
        pub struct $struct_name {
            executor: Box<dyn ExpressionExecutor>,
        }

        impl $struct_name {
            // Takes exactly one argument, which Java checks at run time. The argument can
            // return any type. Never fails.
            pub fn new(executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
                Ok(Self { executor })
            }
        }

        impl ExpressionExecutor for $struct_name {
            fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
                // Same as Java `data instanceof <Type>`. NULL, any other type and no value
                // all give false.
                match self.executor.execute(event) {
                    Some(AttributeValue::$variant_name(_)) => Some(AttributeValue::Bool(true)),
                    Some(AttributeValue::Null) => Some(AttributeValue::Bool(false)),
                    Some(_) => Some(AttributeValue::Bool(false)),
                    None => Some(AttributeValue::Bool(false)),
                }
            }

            fn get_return_type(&self) -> ApiAttributeType {
                ApiAttributeType::BOOL
            }

            fn clone_executor(
                &self,
                eventflux_app_context: &std::sync::Arc<
                    crate::core::config::eventflux_app_context::EventFluxAppContext,
                >,
            ) -> Box<dyn ExpressionExecutor> {
                Box::new(Self {
                    executor: self.executor.clone_executor(eventflux_app_context),
                })
            }
        }
    };
}

define_instance_of_executor!(InstanceOfBooleanExpressionExecutor, Bool);
define_instance_of_executor!(InstanceOfStringExpressionExecutor, String);
define_instance_of_executor!(InstanceOfIntegerExpressionExecutor, Int); // Java: Integer
define_instance_of_executor!(InstanceOfLongExpressionExecutor, Long);
define_instance_of_executor!(InstanceOfFloatExpressionExecutor, Float);
define_instance_of_executor!(InstanceOfDoubleExpressionExecutor, Double);

// There is no InstanceOfObject executor. The Java one only checks `data != null`, and the
// meaning here is unclear. If it should mean "is an AttributeValue::Object variant":
// define_instance_of_executor!(InstanceOfObjectExpressionExecutor, AttributeValue::Object);
// If it should mean "is not AttributeValue::Null", that is a different function, such as
// `isNotNull`.
