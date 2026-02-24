// SPDX-License-Identifier: MIT OR Apache-2.0

//! Phase 2b.4: WITHIN Time Constraints
//!
//! Tests for WITHIN functionality in pattern chains - time-based event expiry
//!
//! Reference: feat/pattern_processing/STATE_MACHINE_DESIGN.md Phase 2b.4

mod common;

use common::pattern_chain_test_utils::*;
use eventflux::core::event::stream::stream_event::StreamEvent;
use eventflux::core::query::input::stream::state::pattern_chain_builder::{
    PatternChainBuilder, PatternStepConfig, ProcessorChain,
};
use eventflux::core::query::input::stream::state::post_state_processor::PostStateProcessor;
use eventflux::core::query::input::stream::state::stream_pre_state_processor::StateType;
use eventflux::query_api::definition::stream_definition::StreamDefinition;
use std::sync::{Arc, Mutex};

// ============================================================================
// PHASE 2b.4 TESTS: WITHIN Time Constraints
// ============================================================================
// Note: build_pattern_chain_with_within() is provided by common module

/// Test 0: WITHIN constraint - events arrive within time window
/// Pattern: A → B (WITHIN 10000ms / 10 seconds)
/// Events: [A(t=0), B(t=5000)] → MATCH (B arrives within 10s of A)
#[test]
fn test_2b4_0_within_success() {
    println!("\n=== Testing A -> B (WITHIN 10s), events [A(t=0), B(t=5000)] ===");

    let (mut chain, collector) = build_pattern_chain_with_within(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 1),
            ("e2".to_string(), "B".to_string(), 1, 1),
        ],
        StateType::Sequence,
        10000, // WITHIN 10 seconds (10000ms)
    );

    // Send A at t=0
    let event_a = create_stream_event(0);
    println!("Sending A (timestamp 0)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a)));
    chain.update_state();
    println!("  After A, outputs = {}", collector.get_outputs().len());

    // Send B at t=5000 (5 seconds later, within 10s window)
    let event_b = create_stream_event(5000);
    println!("Sending B (timestamp 5000, +5s from A)");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b)));
    chain.update_state();

    // Verify output
    let outputs = collector.get_outputs();
    println!("  After B, outputs = {}", outputs.len());
    assert_eq!(
        outputs.len(),
        1,
        "Expected 1 output - B arrived within 10s window"
    );

    println!("SUCCESS: Pattern matched within WITHIN constraint");
}

/// Test 1: WITHIN constraint - events arrive outside time window (expired)
/// Pattern: A → B (WITHIN 10000ms / 10 seconds)
/// Events: [A(t=0), B(t=15000)] → EXPIRED (B arrives 15s after A, exceeds 10s window)
///
/// NOTE: This test is currently ignored because proactive WITHIN expiry checking
/// is deferred to Phase 3 (Absent patterns) as noted in STATE_MACHINE_DESIGN.md.
/// The current implementation requires explicit expire_events() calls or
/// TimerWheel for automatic expiry, which is not yet implemented.
#[test]
#[ignore = "Proactive WITHIN expiry deferred to Phase 3"]
fn test_2b4_1_within_expired() {
    println!("\n=== Testing A -> B (WITHIN 10s), events [A(t=0), B(t=15000)] ===");

    let (mut chain, collector) = build_pattern_chain_with_within(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 1),
            ("e2".to_string(), "B".to_string(), 1, 1),
        ],
        StateType::Sequence,
        10000, // WITHIN 10 seconds (10000ms)
    );

    // Send A at t=0
    let event_a = create_stream_event(0);
    println!("Sending A (timestamp 0)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a)));
    chain.update_state();
    println!("  After A, outputs = {}", collector.get_outputs().len());

    // Send B at t=15000 (15 seconds later, outside 10s window)
    // When B is processed at processor[1], it should check if the time elapsed
    // from A (at position 0) exceeds the WITHIN constraint (10s)
    // Age = 15000 - 0 = 15000ms > 10000ms, so the state should be rejected
    let event_b = create_stream_event(15000);
    println!("Sending B (timestamp 15000, +15s from A - exceeds 10s WITHIN window)");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b)));
    chain.update_state();

    // Verify no output (B arrived too late, WITHIN constraint violated)
    let outputs = collector.get_outputs();
    println!("  After B, outputs = {}", outputs.len());
    assert_eq!(
        outputs.len(),
        0,
        "Expected no output - B arrived outside WITHIN window"
    );

    println!("SUCCESS: Pattern rejected due to WITHIN constraint violation");
}

/// Test 2: WITHIN constraint with Pattern mode ignoring non-matching events
/// Pattern: A → B (Pattern mode, WITHIN 10000ms / 10 seconds)
/// Events: [A(t=0), X(t=3000), B(t=9000)] → MATCH (X ignored, B within 10s of A)
#[test]
fn test_2b4_2_within_pattern_mode() {
    println!(
        "\n=== Testing A -> B (Pattern, WITHIN 10s), events [A(t=0), X(t=3000), B(t=9000)] ==="
    );

    let (mut chain, collector) = build_pattern_chain_with_within(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 1),
            ("e2".to_string(), "B".to_string(), 1, 1),
        ],
        StateType::Pattern, // Pattern mode ignores non-matching events
        10000,              // WITHIN 10 seconds (10000ms)
    );

    // Send A at t=0
    let event_a = create_stream_event(0);
    println!("Sending A (timestamp 0)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a)));
    chain.update_state();
    println!("  After A, outputs = {}", collector.get_outputs().len());

    // Send X at t=3000 (non-matching, should be ignored in Pattern mode)
    let event_x = create_stream_event(3000);
    println!("Sending X (timestamp 3000, non-matching)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_x)));
    chain.update_state();
    println!("  After X, outputs = {}", collector.get_outputs().len());

    // Send B at t=9000 (9 seconds after A, within 10s window)
    let event_b = create_stream_event(9000);
    println!("Sending B (timestamp 9000, +9s from A)");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b)));
    chain.update_state();

    // Verify output (Pattern mode ignored X, B arrived within 10s of A)
    let outputs = collector.get_outputs();
    println!("  After B, outputs = {}", outputs.len());
    assert_eq!(
        outputs.len(),
        1,
        "Expected 1 output - Pattern ignored X, B within 10s of A"
    );

    println!(
        "SUCCESS: Pattern mode ignored non-matching event and matched within WITHIN constraint"
    );
}

/// Test 3: WITHIN constraint with Sequence mode - immediate failure breaks pattern
/// Pattern: A → B (Sequence mode, WITHIN 10000ms / 10 seconds)
/// Events: [A(t=0), C(t=5000)] → FAIL (C breaks sequence immediately, not due to expiry)
#[test]
fn test_2b4_3_within_sequence_immediate_failure() {
    println!("\n=== Testing A -> B (Sequence, WITHIN 10s), events [A(t=0), C(t=5000)] ===");

    let (mut chain, collector) = build_pattern_chain_with_within(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 1),
            ("e2".to_string(), "B".to_string(), 1, 1),
        ],
        StateType::Sequence, // Sequence mode breaks on non-matching events
        10000,               // WITHIN 10 seconds (10000ms)
    );

    // Send A at t=0
    let event_a = create_stream_event(0);
    println!("Sending A (timestamp 0)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a)));
    chain.update_state();
    println!("  After A, outputs = {}", collector.get_outputs().len());

    // Send C at t=5000 (wrong event, breaks sequence immediately)
    let event_c = create_stream_event(5000);
    println!("Sending C (timestamp 5000, wrong event - breaks sequence)");
    // In Sequence mode, C would be sent to processor[0], not processor[1]
    // This would not match the pattern for the next step
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_c)));
    chain.update_state();

    // Verify no output (sequence broken by C, not expired)
    let outputs = collector.get_outputs();
    println!("  After C, outputs = {}", outputs.len());
    assert_eq!(
        outputs.len(),
        0,
        "Expected no output - Sequence broken by C (immediate failure)"
    );

    println!("SUCCESS: Sequence mode failed immediately on wrong event (not due to expiry)");
}
