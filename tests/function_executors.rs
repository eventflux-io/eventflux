// SPDX-License-Identifier: MIT OR Apache-2.0

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
