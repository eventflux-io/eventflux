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

use eventflux::core::event::value::AttributeValue;
use eventflux::core::executor::constant_expression_executor::ConstantExpressionExecutor;
use eventflux::core::executor::expression_executor::ExpressionExecutor;
use eventflux::core::executor::function::{
    CoalesceFunctionExecutor, InstanceOfStringExpressionExecutor, UuidFunctionExecutor,
};
use eventflux::query_api::definition::attribute::Type as AttrType;

#[test]
fn test_coalesce_function() {
    let exec = CoalesceFunctionExecutor::new(vec![
        Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Null,
            AttrType::STRING,
        )),
        Box::new(ConstantExpressionExecutor::new(
            AttributeValue::String("x".into()),
            AttrType::STRING,
        )),
    ])
    .unwrap();
    assert_eq!(exec.execute(None), Some(AttributeValue::String("x".into())));
}

// Note: ifThenElse function was replaced by SQL standard CASE expression
// See tests/app_runner_case_expression.rs for comprehensive CASE expression tests

#[test]
fn test_uuid_function() {
    let exec = UuidFunctionExecutor::new();
    match exec.execute(None) {
        Some(AttributeValue::String(s)) => assert_eq!(s.len(), 36),
        _ => panic!("expected uuid string"),
    }
}

#[test]
fn test_instance_of_string() {
    let inner = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::String("hi".into()),
        AttrType::STRING,
    ));
    let exec = InstanceOfStringExpressionExecutor::new(inner).unwrap();
    assert_eq!(exec.execute(None), Some(AttributeValue::Bool(true)));
}
