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

use std::sync::Arc;

use eventflux::core::config::eventflux_app_context::EventFluxAppContext;
use eventflux::core::config::eventflux_context::EventFluxContext;
use eventflux::core::config::types::EventFluxConfig;
use eventflux::query_api::definition::attribute::Type as ApiAttributeType;
use eventflux::query_api::eventflux_app::EventFluxApp;

fn test_app_context() -> EventFluxAppContext {
    EventFluxAppContext::new_with_config(
        Arc::new(EventFluxContext::default()),
        "test_app_ctx".to_string(),
        Arc::new(EventFluxApp::new("test_api_app".to_string())),
        String::new(),
        Arc::new(EventFluxConfig::default()),
        None,
        None,
    )
}

#[test]
fn collection_aggregation_functions_registered_and_work() {
    let app_ctx = test_app_context();
    let ctx = app_ctx.get_eventflux_context();

    // All built-in collection aggregation functions should be present
    let names = ctx.list_collection_aggregation_function_names();
    assert!(names.contains(&"sum".to_string()));
    assert!(names.contains(&"avg".to_string()));
    assert!(names.contains(&"min".to_string()));
    assert!(names.contains(&"max".to_string()));
    assert!(names.contains(&"count".to_string()));

    // Validate basic behavior and return types from registry instances
    let sum_fn = ctx.get_collection_aggregation_function("sum").unwrap();
    assert_eq!(sum_fn.aggregate(&[1.0, 2.0, 3.0]), Some(6.0));
    assert_eq!(
        sum_fn.return_type(ApiAttributeType::INT),
        ApiAttributeType::LONG
    );

    let avg_fn = ctx.get_collection_aggregation_function("avg").unwrap();
    assert_eq!(avg_fn.aggregate(&[10.0, 20.0, 30.0]), Some(20.0));
    assert_eq!(
        avg_fn.return_type(ApiAttributeType::DOUBLE),
        ApiAttributeType::DOUBLE
    );

    let count_fn = ctx.get_collection_aggregation_function("count").unwrap();
    assert_eq!(count_fn.aggregate(&[]), Some(0.0));
    assert!(count_fn.supports_count_only());
}
