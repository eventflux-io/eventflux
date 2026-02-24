// SPDX-License-Identifier: MIT OR Apache-2.0

use eventflux::core::config::eventflux_app_context::EventFluxAppContext;
use eventflux::core::config::eventflux_context::EventFluxContext;
use eventflux::core::config::eventflux_query_context::EventFluxQueryContext;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::util::parser::{parse_expression, ExpressionParserContext};
use eventflux::query_api::definition::attribute::Type as AttrType;
use eventflux::query_api::eventflux_app::EventFluxApp;
use eventflux::query_api::expression::Expression;
use std::collections::HashMap;
use std::sync::Arc;

fn make_app_ctx() -> Arc<EventFluxAppContext> {
    Arc::new(EventFluxAppContext::new(
        Arc::new(EventFluxContext::default()),
        "test".to_string(),
        Arc::new(EventFluxApp::new("app".to_string())),
        String::new(),
    ))
}

fn make_query_ctx(name: &str) -> Arc<EventFluxQueryContext> {
    Arc::new(EventFluxQueryContext::new(
        make_app_ctx(),
        name.to_string(),
        None,
    ))
}

fn empty_ctx(query: &str) -> ExpressionParserContext {
    ExpressionParserContext {
        eventflux_app_context: make_app_ctx(),
        eventflux_query_context: make_query_ctx(query),
        stream_meta_map: HashMap::new(),
        table_meta_map: HashMap::new(),
        window_meta_map: HashMap::new(),
        aggregation_meta_map: HashMap::new(),
        state_meta_map: HashMap::new(),
        stream_positions: HashMap::new(),
        default_source: "dummy".to_string(),
        query_name: query,
        is_mutation_context: false,
    }
}

#[test]
fn test_cast_function() {
    let ctx = empty_ctx("cast");
    let expr = Expression::function_no_ns(
        "cast".to_string(),
        vec![
            Expression::value_int(5),
            Expression::value_string("string".to_string()),
        ],
    );
    let exec = parse_expression(&expr, &ctx).expect("parse failed");
    assert_eq!(exec.get_return_type(), AttrType::STRING);
    let result = exec.execute(None);
    assert_eq!(result, Some(AttributeValue::String("5".to_string())));
}

#[test]
fn test_concat_and_length_functions() {
    let ctx = empty_ctx("concat_len");
    let concat_expr = Expression::function_no_ns(
        "concat".to_string(),
        vec![
            Expression::value_string("ab".to_string()),
            Expression::value_string("cd".to_string()),
        ],
    );
    let concat_exec = parse_expression(&concat_expr, &ctx).unwrap();
    let concat_res = concat_exec.execute(None);
    assert_eq!(concat_res, Some(AttributeValue::String("abcd".to_string())));

    let len_expr = Expression::function_no_ns(
        "length".to_string(),
        vec![Expression::value_string("hello".to_string())],
    );
    let len_exec = parse_expression(&len_expr, &ctx).unwrap();
    let len_res = len_exec.execute(None);
    assert_eq!(len_res, Some(AttributeValue::Int(5)));
}

#[test]
fn test_now_function() {
    let ctx = empty_ctx("current_ts");
    let expr = Expression::function_no_ns("now".to_string(), vec![]);
    let exec = parse_expression(&expr, &ctx).unwrap();
    assert_eq!(exec.get_return_type(), AttrType::LONG);
    let val = exec.execute(None);
    match val {
        Some(AttributeValue::Long(_)) => {}
        _ => panic!("expected long"),
    }
}

#[test]
fn test_format_date_function() {
    let ctx = empty_ctx("format_date");
    let expr = Expression::function_no_ns(
        "formatDate".to_string(),
        vec![
            Expression::value_long(0),
            Expression::value_string("%Y".to_string()),
        ],
    );
    let exec = parse_expression(&expr, &ctx).unwrap();
    let result = exec.execute(None);
    assert_eq!(result, Some(AttributeValue::String("1970".to_string())));
}

#[test]
fn test_round_and_sqrt_functions() {
    let ctx = empty_ctx("math");
    let round_expr =
        Expression::function_no_ns("round".to_string(), vec![Expression::value_double(3.7)]);
    let round_exec = parse_expression(&round_expr, &ctx).unwrap();
    assert_eq!(round_exec.execute(None), Some(AttributeValue::Double(4.0)));

    let sqrt_expr = Expression::function_no_ns("sqrt".to_string(), vec![Expression::value_int(16)]);
    let sqrt_exec = parse_expression(&sqrt_expr, &ctx).unwrap();
    assert_eq!(sqrt_exec.execute(None), Some(AttributeValue::Double(4.0)));
}

#[test]
fn test_additional_math_functions() {
    let ctx = empty_ctx("math_extra");
    let log_expr =
        Expression::function_no_ns("log".to_string(), vec![Expression::value_double(1.0)]);
    let log_exec = parse_expression(&log_expr, &ctx).unwrap();
    assert_eq!(log_exec.execute(None), Some(AttributeValue::Double(0.0)));

    let sin_expr = Expression::function_no_ns(
        "sin".to_string(),
        vec![Expression::value_double(std::f64::consts::PI / 2.0)],
    );
    let sin_exec = parse_expression(&sin_expr, &ctx).unwrap();
    assert!(
        matches!(sin_exec.execute(None), Some(AttributeValue::Double(v)) if (v-1.0).abs() < 1e-6)
    );

    let tan_expr =
        Expression::function_no_ns("tan".to_string(), vec![Expression::value_double(0.0)]);
    let tan_exec = parse_expression(&tan_expr, &ctx).unwrap();
    assert_eq!(tan_exec.execute(None), Some(AttributeValue::Double(0.0)));
}

#[test]
fn test_string_and_date_functions() {
    let ctx = empty_ctx("str_date");
    let lower_expr = Expression::function_no_ns(
        "lower".to_string(),
        vec![Expression::value_string("ABC".to_string())],
    );
    let lower_exec = parse_expression(&lower_expr, &ctx).unwrap();
    assert_eq!(
        lower_exec.execute(None),
        Some(AttributeValue::String("abc".to_string()))
    );

    let upper_expr = Expression::function_no_ns(
        "upper".to_string(),
        vec![Expression::value_string("abc".to_string())],
    );
    let upper_exec = parse_expression(&upper_expr, &ctx).unwrap();
    assert_eq!(
        upper_exec.execute(None),
        Some(AttributeValue::String("ABC".to_string()))
    );

    let substr_expr = Expression::function_no_ns(
        "substring".to_string(),
        vec![
            Expression::value_string("hello".to_string()),
            Expression::value_int(1),
            Expression::value_int(3),
        ],
    );
    let substr_exec = parse_expression(&substr_expr, &ctx).unwrap();
    assert_eq!(
        substr_exec.execute(None),
        Some(AttributeValue::String("ell".to_string()))
    );

    let parse_expr = Expression::function_no_ns(
        "parseDate".to_string(),
        vec![
            Expression::value_string("1970-01-02".to_string()),
            Expression::value_string("%Y-%m-%d".to_string()),
        ],
    );
    let parse_exec = parse_expression(&parse_expr, &ctx).unwrap();
    assert_eq!(
        parse_exec.execute(None),
        Some(AttributeValue::Long(86_400_000))
    );

    let add_expr = Expression::function_no_ns(
        "dateAdd".to_string(),
        vec![
            Expression::value_long(0),
            Expression::value_int(1),
            Expression::value_string("days".to_string()),
        ],
    );
    let add_exec = parse_expression(&add_expr, &ctx).unwrap();
    assert_eq!(
        add_exec.execute(None),
        Some(AttributeValue::Long(86_400_000))
    );
}
