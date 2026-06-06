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
use eventflux::core::executor::condition::InExpressionExecutor;
use eventflux::core::executor::constant_expression_executor::ConstantExpressionExecutor;
use eventflux::core::executor::expression_executor::ExpressionExecutor;
use eventflux::core::table::{InMemoryTable, Table};
use eventflux::query_api::definition::attribute::Type as ApiAttributeType;
use eventflux::query_api::eventflux_app::EventFluxApp;
use std::sync::Arc;

fn make_context_with_table() -> Arc<EventFluxAppContext> {
    let ctx = Arc::new(EventFluxAppContext::new(
        Arc::new(EventFluxContext::default()),
        "test_app".to_string(),
        Arc::new(EventFluxApp::new("test".to_string())),
        String::new(),
    ));
    let table: Arc<dyn Table> = Arc::new(InMemoryTable::new());
    table
        .insert(&[AttributeValue::Int(1)])
        .expect("Failed to insert test data");
    ctx.get_eventflux_context()
        .add_table("MyTable".to_string(), table);
    ctx
}

#[test]
fn test_in_true() {
    let app_ctx = make_context_with_table();
    let const_exec = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Int(1),
        ApiAttributeType::INT,
    ));
    let in_exec =
        InExpressionExecutor::new(const_exec, "MyTable".to_string(), Arc::clone(&app_ctx));
    let result = in_exec.execute(None);
    assert_eq!(result, Some(AttributeValue::Bool(true)));
}

#[test]
fn test_in_false() {
    let app_ctx = make_context_with_table();
    let const_exec = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Int(5),
        ApiAttributeType::INT,
    ));
    let in_exec =
        InExpressionExecutor::new(const_exec, "MyTable".to_string(), Arc::clone(&app_ctx));
    let result = in_exec.execute(None);
    assert_eq!(result, Some(AttributeValue::Bool(false)));
}

#[test]
fn test_in_clone() {
    let app_ctx = make_context_with_table();
    let const_exec = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Int(1),
        ApiAttributeType::INT,
    ));
    let in_exec =
        InExpressionExecutor::new(const_exec, "MyTable".to_string(), Arc::clone(&app_ctx));
    let cloned = in_exec.clone_executor(&app_ctx);
    let result = cloned.execute(None);
    assert_eq!(result, Some(AttributeValue::Bool(true)));
}
