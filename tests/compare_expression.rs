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

use eventflux::core::config::eventflux_app_context::EventFluxAppContext;
use eventflux::core::config::eventflux_context::EventFluxContext;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::executor::condition::CompareExpressionExecutor;
use eventflux::core::executor::constant_expression_executor::ConstantExpressionExecutor;
use eventflux::core::executor::expression_executor::ExpressionExecutor;
use eventflux::query_api::definition::attribute::Type as AttrType;
use eventflux::query_api::eventflux_app::EventFluxApp;
use eventflux::query_api::expression::condition::compare::Operator as CompareOp;
use std::sync::Arc;

fn dummy_ctx() -> Arc<EventFluxAppContext> {
    Arc::new(EventFluxAppContext::new(
        Arc::new(EventFluxContext::default()),
        "cmp_test".to_string(),
        Arc::new(EventFluxApp::new("app".to_string())),
        String::new(),
    ))
}

#[test]
fn test_numeric_comparison() {
    let left = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Int(5),
        AttrType::INT,
    ));
    let right = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Int(3),
        AttrType::INT,
    ));
    let cmp = CompareExpressionExecutor::new(left, right, CompareOp::GreaterThan).unwrap();
    assert_eq!(cmp.execute(None), Some(AttributeValue::Bool(true)));
}

#[test]
fn test_string_equality() {
    let left = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::String("a".to_string()),
        AttrType::STRING,
    ));
    let right = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::String("a".to_string()),
        AttrType::STRING,
    ));
    let cmp = CompareExpressionExecutor::new(left, right, CompareOp::Equal).unwrap();
    assert_eq!(cmp.execute(None), Some(AttributeValue::Bool(true)));
}

#[test]
fn test_cross_type_compare() {
    let left = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Int(5),
        AttrType::INT,
    ));
    let right = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Double(4.5),
        AttrType::DOUBLE,
    ));
    let cmp = CompareExpressionExecutor::new(left, right, CompareOp::GreaterThan).unwrap();
    assert_eq!(cmp.execute(None), Some(AttributeValue::Bool(true)));
}

#[test]
fn test_clone_executor() {
    let ctx = dummy_ctx();
    let left = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::String("b".to_string()),
        AttrType::STRING,
    ));
    let right = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::String("a".to_string()),
        AttrType::STRING,
    ));
    let cmp = CompareExpressionExecutor::new(left, right, CompareOp::GreaterThan).unwrap();
    let cloned = cmp.clone_executor(&ctx);
    assert_eq!(cloned.execute(None), Some(AttributeValue::Bool(true)));
}

#[test]
fn test_invalid_type_error() {
    let left = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::String("a".to_string()),
        AttrType::STRING,
    ));
    let right = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Int(1),
        AttrType::INT,
    ));
    assert!(CompareExpressionExecutor::new(left, right, CompareOp::Equal).is_err());
}

#[test]
fn test_bool_operator_error() {
    let left = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Bool(true),
        AttrType::BOOL,
    ));
    let right = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Bool(false),
        AttrType::BOOL,
    ));
    assert!(CompareExpressionExecutor::new(left, right, CompareOp::LessThan).is_err());
}
