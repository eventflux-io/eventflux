// SPDX-License-Identifier: MIT OR Apache-2.0

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
