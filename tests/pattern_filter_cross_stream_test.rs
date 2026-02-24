// SPDX-License-Identifier: MIT OR Apache-2.0

// tests/pattern_filter_cross_stream_test.rs
// Tests for pattern filter conditions with cross-stream references

use eventflux::core::config::eventflux_app_context::EventFluxAppContext;
use eventflux::core::config::eventflux_context::EventFluxContext;
use eventflux::core::config::eventflux_query_context::EventFluxQueryContext;
use eventflux::core::event::state::state_event::StateEvent;
use eventflux::core::event::stream::stream_event::StreamEvent;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::query::input::stream::state::pre_state_processor::PreStateProcessor;
use eventflux::core::query::input::stream::state::stream_pre_state_processor::StateType;
use eventflux::core::query::input::stream::state::stream_pre_state_processor::StreamPreStateProcessor;
use eventflux::query_api::eventflux_app::EventFluxApp;
use std::sync::Arc;

fn create_test_context() -> (Arc<EventFluxAppContext>, Arc<EventFluxQueryContext>) {
    let eventflux_context = Arc::new(EventFluxContext::new());
    let app = Arc::new(EventFluxApp::new("TestApp".to_string()));
    let app_ctx = Arc::new(EventFluxAppContext::new(
        eventflux_context,
        "TestApp".to_string(),
        app,
        String::new(),
    ));
    let query_ctx = Arc::new(EventFluxQueryContext::new(
        app_ctx.clone(),
        "test_query".to_string(),
        None,
    ));
    (app_ctx, query_ctx)
}

/// Test that filter conditions can access previous events in StateEvent
#[test]
fn test_filter_with_cross_stream_reference_simple() {
    // Pattern: e1=StockPrice -> e2=StockPrice[price > e1.price]
    // e1.price = 100.0, e2.price = 110.0 -> should match
    // e1.price = 100.0, e2.price = 90.0 -> should not match

    let (app_ctx, query_ctx) = create_test_context();

    // Create e1 processor (start state)
    let mut e1_processor = StreamPreStateProcessor::new(
        0,
        true,
        StateType::Sequence,
        Arc::clone(&app_ctx),
        Arc::clone(&query_ctx),
    );

    // Create e2 processor with filter: price > e1.price
    let mut e2_processor = StreamPreStateProcessor::new(
        1,
        false,
        StateType::Sequence,
        Arc::clone(&app_ctx),
        Arc::clone(&query_ctx),
    );

    e2_processor.set_condition(|state_event| {
        // Access e1 (position 0) from StateEvent
        if let Some(e1) = state_event.get_stream_event(0) {
            // Get e1.price (attribute index 0)
            if let Some(AttributeValue::Float(e1_price)) = e1.before_window_data.get(0) {
                // Access e2 (position 1) from StateEvent
                if let Some(e2) = state_event.get_stream_event(1) {
                    if let Some(AttributeValue::Float(e2_price)) = e2.before_window_data.get(0) {
                        return e2_price > e1_price;
                    }
                }
            }
        }
        false
    });

    // Initialize e1 processor and update state
    e1_processor.init();
    e1_processor.update_state();

    // Send e1 event with price = 100.0
    let mut e1_event = StreamEvent::new(1000, 1, 0, 0);
    e1_event.before_window_data = vec![AttributeValue::Float(100.0)];

    let e1_result = e1_processor.process_and_return(Some(Box::new(e1_event.clone())));
    assert!(e1_result.is_some());

    // Get the StateEvent from e1 result
    let state_event_e1 = e1_result.unwrap();
    let state_event_e1 = state_event_e1
        .as_any()
        .downcast_ref::<StateEvent>()
        .unwrap();

    // Add state to e2 processor
    e2_processor.add_state(state_event_e1.clone());
    e2_processor.update_state();

    // Test 1: e2.price = 110.0 (greater than e1.price = 100.0) -> should match
    let mut e2_event_match = StreamEvent::new(2000, 1, 0, 0);
    e2_event_match.before_window_data = vec![AttributeValue::Float(110.0)];

    let e2_result_match = e2_processor.process_and_return(Some(Box::new(e2_event_match)));
    assert!(
        e2_result_match.is_some(),
        "Filter should match when e2.price (110.0) > e1.price (100.0)"
    );

    // Add state again for next test
    e2_processor.add_state(state_event_e1.clone());
    e2_processor.update_state();

    // Test 2: e2.price = 90.0 (less than e1.price = 100.0) -> should not match
    let mut e2_event_no_match = StreamEvent::new(3000, 1, 0, 0);
    e2_event_no_match.before_window_data = vec![AttributeValue::Float(90.0)];

    let e2_result_no_match = e2_processor.process_and_return(Some(Box::new(e2_event_no_match)));
    assert!(
        e2_result_no_match.is_none(),
        "Filter should not match when e2.price (90.0) <= e1.price (100.0)"
    );
}

/// Test cross-stream reference with percentage calculation
#[test]
fn test_filter_cross_stream_percentage() {
    // Pattern: e1=StockPrice -> e2=StockPrice[price > e1.price * 1.1]
    // e1.price = 100.0, e2.price = 111.0 -> should match (111 > 110)
    // e1.price = 100.0, e2.price = 109.0 -> should not match (109 <= 110)

    let (app_ctx, query_ctx) = create_test_context();

    let mut e1_processor = StreamPreStateProcessor::new(
        0,
        true,
        StateType::Sequence,
        Arc::clone(&app_ctx),
        Arc::clone(&query_ctx),
    );

    let mut e2_processor = StreamPreStateProcessor::new(
        1,
        false,
        StateType::Sequence,
        Arc::clone(&app_ctx),
        Arc::clone(&query_ctx),
    );

    // Filter: price > e1.price * 1.1
    e2_processor.set_condition(|state_event| {
        if let Some(e1) = state_event.get_stream_event(0) {
            if let Some(AttributeValue::Float(e1_price)) = e1.before_window_data.get(0) {
                if let Some(e2) = state_event.get_stream_event(1) {
                    if let Some(AttributeValue::Float(e2_price)) = e2.before_window_data.get(0) {
                        let threshold = e1_price * 1.1;
                        return e2_price > &threshold;
                    }
                }
            }
        }
        false
    });

    e1_processor.init();
    e1_processor.update_state();

    // Send e1 event with price = 100.0
    let mut e1_event = StreamEvent::new(1000, 1, 0, 0);
    e1_event.before_window_data = vec![AttributeValue::Float(100.0)];

    let e1_result = e1_processor.process_and_return(Some(Box::new(e1_event)));
    let state_event_e1 = e1_result
        .unwrap()
        .as_any()
        .downcast_ref::<StateEvent>()
        .unwrap()
        .clone();

    // Test 1: e2.price = 111.0 (> 110.0) -> should match
    e2_processor.add_state(state_event_e1.clone());
    e2_processor.update_state();

    let mut e2_event_match = StreamEvent::new(2000, 1, 0, 0);
    e2_event_match.before_window_data = vec![AttributeValue::Float(111.0)];

    let e2_result_match = e2_processor.process_and_return(Some(Box::new(e2_event_match)));
    assert!(
        e2_result_match.is_some(),
        "Should match when e2.price (111.0) > e1.price * 1.1 (110.0)"
    );

    // Test 2: e2.price = 109.0 (<= 110.0) -> should not match
    e2_processor.add_state(state_event_e1.clone());
    e2_processor.update_state();

    let mut e2_event_no_match = StreamEvent::new(3000, 1, 0, 0);
    e2_event_no_match.before_window_data = vec![AttributeValue::Float(109.0)];

    let e2_result_no_match = e2_processor.process_and_return(Some(Box::new(e2_event_no_match)));
    assert!(
        e2_result_no_match.is_none(),
        "Should not match when e2.price (109.0) <= e1.price * 1.1 (110.0)"
    );
}

/// Test cross-stream reference with String equality
#[test]
fn test_filter_cross_stream_string_equality() {
    // Pattern: e1=Login -> e2=Activity[userId == e1.userId]
    // e1.userId = "user1", e2.userId = "user1" -> should match
    // e1.userId = "user1", e2.userId = "user2" -> should not match

    let (app_ctx, query_ctx) = create_test_context();

    let mut e1_processor = StreamPreStateProcessor::new(
        0,
        true,
        StateType::Sequence,
        Arc::clone(&app_ctx),
        Arc::clone(&query_ctx),
    );

    let mut e2_processor = StreamPreStateProcessor::new(
        1,
        false,
        StateType::Sequence,
        Arc::clone(&app_ctx),
        Arc::clone(&query_ctx),
    );

    // Filter: userId == e1.userId
    e2_processor.set_condition(|state_event| {
        if let Some(e1) = state_event.get_stream_event(0) {
            if let Some(AttributeValue::String(e1_user)) = e1.before_window_data.get(0) {
                if let Some(e2) = state_event.get_stream_event(1) {
                    if let Some(AttributeValue::String(e2_user)) = e2.before_window_data.get(0) {
                        return e1_user == e2_user;
                    }
                }
            }
        }
        false
    });

    e1_processor.init();
    e1_processor.update_state();

    // Send e1 event with userId = "user1"
    let mut e1_event = StreamEvent::new(1000, 1, 0, 0);
    e1_event.before_window_data = vec![AttributeValue::String("user1".to_string())];

    let e1_result = e1_processor.process_and_return(Some(Box::new(e1_event)));
    let state_event_e1 = e1_result
        .unwrap()
        .as_any()
        .downcast_ref::<StateEvent>()
        .unwrap()
        .clone();

    // Test 1: e2.userId = "user1" (same) -> should match
    e2_processor.add_state(state_event_e1.clone());
    e2_processor.update_state();

    let mut e2_event_match = StreamEvent::new(2000, 1, 0, 0);
    e2_event_match.before_window_data = vec![AttributeValue::String("user1".to_string())];

    let e2_result_match = e2_processor.process_and_return(Some(Box::new(e2_event_match)));
    assert!(
        e2_result_match.is_some(),
        "Should match when e2.userId == e1.userId"
    );

    // Test 2: e2.userId = "user2" (different) -> should not match
    e2_processor.add_state(state_event_e1.clone());
    e2_processor.update_state();

    let mut e2_event_no_match = StreamEvent::new(3000, 1, 0, 0);
    e2_event_no_match.before_window_data = vec![AttributeValue::String("user2".to_string())];

    let e2_result_no_match = e2_processor.process_and_return(Some(Box::new(e2_event_no_match)));
    assert!(
        e2_result_no_match.is_none(),
        "Should not match when e2.userId != e1.userId"
    );
}

/// Test three-stream pattern with cross-stream references
#[test]
fn test_filter_cross_stream_three_events() {
    // Pattern: e1=Event -> e2=Event -> e3=Event[value > e1.value AND value > e2.value]
    // e3 must be greater than both e1 and e2

    let (app_ctx, query_ctx) = create_test_context();

    let mut e1_processor = StreamPreStateProcessor::new(
        0,
        true,
        StateType::Sequence,
        Arc::clone(&app_ctx),
        Arc::clone(&query_ctx),
    );

    let mut e2_processor = StreamPreStateProcessor::new(
        1,
        false,
        StateType::Sequence,
        Arc::clone(&app_ctx),
        Arc::clone(&query_ctx),
    );

    let mut e3_processor = StreamPreStateProcessor::new(
        2,
        false,
        StateType::Sequence,
        Arc::clone(&app_ctx),
        Arc::clone(&query_ctx),
    );

    // Filter for e3: value > e1.value AND value > e2.value
    e3_processor.set_condition(|state_event| {
        // Access e3 (position 2) from StateEvent
        if let Some(e3) = state_event.get_stream_event(2) {
            if let Some(AttributeValue::Int(e3_value)) = e3.before_window_data.get(0) {
                // Check e1.value
                if let Some(e1) = state_event.get_stream_event(0) {
                    if let Some(AttributeValue::Int(e1_value)) = e1.before_window_data.get(0) {
                        if e3_value <= e1_value {
                            return false;
                        }
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }

                // Check e2.value
                if let Some(e2) = state_event.get_stream_event(1) {
                    if let Some(AttributeValue::Int(e2_value)) = e2.before_window_data.get(0) {
                        return e3_value > e2_value;
                    }
                }
            }
        }
        false
    });

    // Process e1
    e1_processor.init();
    e1_processor.update_state();

    let mut e1_event = StreamEvent::new(1000, 1, 0, 0);
    e1_event.before_window_data = vec![AttributeValue::Int(10)];

    let e1_result = e1_processor.process_and_return(Some(Box::new(e1_event)));
    let state_e1 = e1_result
        .unwrap()
        .as_any()
        .downcast_ref::<StateEvent>()
        .unwrap()
        .clone();

    // Process e2
    e2_processor.add_state(state_e1);
    e2_processor.update_state();

    let mut e2_event = StreamEvent::new(2000, 1, 0, 0);
    e2_event.before_window_data = vec![AttributeValue::Int(15)];

    let e2_result = e2_processor.process_and_return(Some(Box::new(e2_event)));
    let state_e1_e2 = e2_result
        .unwrap()
        .as_any()
        .downcast_ref::<StateEvent>()
        .unwrap()
        .clone();

    // Test 1: e3.value = 20 (> both 10 and 15) -> should match
    e3_processor.add_state(state_e1_e2.clone());
    e3_processor.update_state();

    let mut e3_event_match = StreamEvent::new(3000, 1, 0, 0);
    e3_event_match.before_window_data = vec![AttributeValue::Int(20)];

    let e3_result_match = e3_processor.process_and_return(Some(Box::new(e3_event_match)));
    assert!(
        e3_result_match.is_some(),
        "Should match when e3.value (20) > e1.value (10) AND e3.value (20) > e2.value (15)"
    );

    // Test 2: e3.value = 12 (> 10 but < 15) -> should not match
    e3_processor.add_state(state_e1_e2.clone());
    e3_processor.update_state();

    let mut e3_event_no_match = StreamEvent::new(4000, 1, 0, 0);
    e3_event_no_match.before_window_data = vec![AttributeValue::Int(12)];

    let e3_result_no_match = e3_processor.process_and_return(Some(Box::new(e3_event_no_match)));
    assert!(
        e3_result_no_match.is_none(),
        "Should not match when e3.value (12) > e1.value (10) but e3.value (12) <= e2.value (15)"
    );
}

/// Test cross-stream reference with NULL handling
#[test]
fn test_filter_cross_stream_null_handling() {
    // If e1 doesn't exist in StateEvent, filter should return false

    let (app_ctx, query_ctx) = create_test_context();

    let mut e2_processor = StreamPreStateProcessor::new(
        1,
        false,
        StateType::Sequence,
        Arc::clone(&app_ctx),
        Arc::clone(&query_ctx),
    );

    // Filter references e1, but we'll pass StateEvent without e1
    e2_processor.set_condition(|state_event| {
        if let Some(e1) = state_event.get_stream_event(0) {
            if let Some(AttributeValue::Float(e1_price)) = e1.before_window_data.get(0) {
                if let Some(e2) = state_event.get_stream_event(1) {
                    if let Some(AttributeValue::Float(e2_price)) = e2.before_window_data.get(0) {
                        return e2_price > e1_price;
                    }
                }
            }
        }
        false // e1 doesn't exist -> filter fails
    });

    // Create StateEvent without e1 (only has position 1)
    let state_event_no_e1 = StateEvent::new(2, 0); // 2 positions but e1 (position 0) is None

    e2_processor.add_state(state_event_no_e1);
    e2_processor.update_state();

    let mut e2_event = StreamEvent::new(2000, 1, 0, 0);
    e2_event.before_window_data = vec![AttributeValue::Float(110.0)];

    let result = e2_processor.process_and_return(Some(Box::new(e2_event)));
    assert!(
        result.is_none(),
        "Should not match when e1 doesn't exist in StateEvent"
    );
}

/// Test that simple filters without cross-stream references still work
#[test]
fn test_filter_without_cross_stream_reference() {
    // Pattern: e1=Event[value > 100]
    // Simple filter that doesn't reference other streams

    let (app_ctx, query_ctx) = create_test_context();

    let mut e1_processor = StreamPreStateProcessor::new(
        0,
        true,
        StateType::Pattern,
        Arc::clone(&app_ctx),
        Arc::clone(&query_ctx),
    );

    // Simple filter: value > 100 (accesses from position 0)
    e1_processor.set_condition(|state_event| {
        if let Some(e1) = state_event.get_stream_event(0) {
            if let Some(AttributeValue::Int(value)) = e1.before_window_data.get(0) {
                return *value > 100;
            }
        }
        false
    });

    e1_processor.init();
    e1_processor.update_state();

    // Test 1: value = 150 -> should match
    let mut event_match = StreamEvent::new(1000, 1, 0, 0);
    event_match.before_window_data = vec![AttributeValue::Int(150)];

    let result_match = e1_processor.process_and_return(Some(Box::new(event_match)));
    assert!(
        result_match.is_some(),
        "Should match when value (150) > 100"
    );

    e1_processor.update_state();

    // Test 2: value = 50 -> should not match
    let mut event_no_match = StreamEvent::new(2000, 1, 0, 0);
    event_no_match.before_window_data = vec![AttributeValue::Int(50)];

    let result_no_match = e1_processor.process_and_return(Some(Box::new(event_no_match)));
    assert!(
        result_no_match.is_none(),
        "Should not match when value (50) <= 100"
    );
}
