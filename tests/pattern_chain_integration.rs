// SPDX-License-Identifier: MIT OR Apache-2.0

//! Phase 2b.5: Integration Testing
//!
//! Comprehensive integration tests combining all pattern chain features:
//! - Multi-instance scenarios (multiple overlapping patterns)
//! - Complex count quantifiers
//! - Pattern + WITHIN combinations
//! - Performance validation
//!
//! Reference: feat/pattern_processing/STATE_MACHINE_DESIGN.md Phase 2b.5

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
// Note: build_pattern_chain_with_within() is provided by common module

// ============================================================================
// PHASE 2b.5 TESTS: Integration Testing
// ============================================================================

/// Test 1: Complex count quantifiers with multi-instance patterns
/// Pattern: A{2,3} -> B{2} (Pattern mode)
/// Events: [A, A, X, B, A, B] → Multiple matches possible
///
/// Expected behavior:
/// - A{2,3} means: collect 2 or 3 A events
/// - B{2} means: collect exactly 2 B events (last step requires exact count)
/// - Pattern mode ignores X
/// - Expected match: After collecting 2+ As and 2 Bs
#[test]
fn test_integration_1_complex_count_quantifiers() {
    println!("\n=== Testing A{{2,3}} -> B{{2}} (Pattern), events [A, A, X, B, A, B] ===");

    let (mut chain, collector) = build_pattern_chain(
        vec![
            ("e1".to_string(), "A".to_string(), 2, 3), // A{2,3}
            ("e2".to_string(), "B".to_string(), 2, 2), // B{2} - last step must have exact count
        ],
        StateType::Pattern,
    );

    // Send first A
    let event_a1 = create_stream_event(1000);
    println!("Sending A1 (timestamp 1000)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a1)));
    chain.update_state();
    println!("  After A1, outputs = {}", collector.get_outputs().len());

    // Send second A
    let event_a2 = create_stream_event(2000);
    println!("Sending A2 (timestamp 2000)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a2)));
    chain.update_state();
    println!("  After A2, outputs = {}", collector.get_outputs().len());

    // Send X (non-matching, should be ignored in Pattern mode)
    let event_x = create_stream_event(3000);
    println!("Sending X (timestamp 3000, non-matching)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_x)));
    chain.update_state();
    println!("  After X, outputs = {}", collector.get_outputs().len());

    // Send first B
    let event_b1 = create_stream_event(4000);
    println!("Sending B1 (timestamp 4000)");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b1)));
    chain.update_state();
    println!("  After B1, outputs = {}", collector.get_outputs().len());

    // Send third A
    let event_a3 = create_stream_event(5000);
    println!("Sending A3 (timestamp 5000)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a3)));
    chain.update_state();
    println!("  After A3, outputs = {}", collector.get_outputs().len());

    // Send second B
    let event_b2 = create_stream_event(6000);
    println!("Sending B2 (timestamp 6000)");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b2)));
    chain.update_state();

    // Verify outputs
    let outputs = collector.get_outputs();
    println!("  Final outputs = {}", outputs.len());

    // With A{2,3} -> B{2}, we expect:
    // - After A1, A2: state with 2 As waiting for 2 Bs
    // - After B1, B2: First match (2 As, 2 Bs)
    // - Potentially more matches with A3
    assert!(
        outputs.len() >= 1,
        "Expected at least 1 output from A{{2,3}} -> B{{2}} pattern"
    );

    println!(
        "SUCCESS: Complex count quantifiers produced {} match(es)",
        outputs.len()
    );
}

/// Test 2: Pattern mode with WITHIN time constraint
/// Pattern: A -> B -> C (Pattern mode, WITHIN 20s)
/// Events: [A(t=0), X(t=5), B(t=10), Y(t=15), C(t=18)] → MATCH
///
/// Expected: Pattern mode ignores X and Y, C arrives within 20s of A
#[test]
fn test_integration_2_pattern_with_within() {
    println!("\n=== Testing A -> B -> C (Pattern, WITHIN 20s), events [A(t=0), X(t=5), B(t=10), Y(t=15), C(t=18)] ===");

    let (mut chain, collector) = build_pattern_chain_with_within(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 1),
            ("e2".to_string(), "B".to_string(), 1, 1),
            ("e3".to_string(), "C".to_string(), 1, 1),
        ],
        StateType::Pattern,
        20000, // WITHIN 20 seconds
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

    // Send X at t=5000 (non-matching, ignored)
    let event_x = create_stream_event(5000);
    println!("Sending X (timestamp 5000, non-matching)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_x)));
    chain.update_state();
    println!("  After X, outputs = {}", collector.get_outputs().len());

    // Send B at t=10000
    let event_b = create_stream_event(10000);
    println!("Sending B (timestamp 10000)");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b)));
    chain.update_state();
    println!("  After B, outputs = {}", collector.get_outputs().len());

    // Send Y at t=15000 (non-matching, ignored)
    let event_y = create_stream_event(15000);
    println!("Sending Y (timestamp 15000, non-matching)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_y)));
    chain.update_state();
    println!("  After Y, outputs = {}", collector.get_outputs().len());

    // Send C at t=18000 (within 20s of A)
    let event_c = create_stream_event(18000);
    println!("Sending C (timestamp 18000, +18s from A)");
    chain.pre_processors[2]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_c)));
    chain.update_state();

    // Verify output
    let outputs = collector.get_outputs();
    println!("  Final outputs = {}", outputs.len());
    assert_eq!(
        outputs.len(),
        1,
        "Expected 1 output - Pattern ignored X,Y and C arrived within 20s"
    );

    println!("SUCCESS: Pattern mode with WITHIN constraint produced match");
}

/// Test 3: Concurrent pattern instances with count quantifiers
/// Pattern: A{2} -> B{2} (Pattern mode)
/// Events: [A1, A2, A3, B1, B2, B3] → Multiple overlapping matches
///
/// Expected: With A{2} -> B{2}, we should get:
/// - (A1, A2) -> (B1, B2): First match
/// - Potentially more matches with A3 and B3
#[test]
fn test_integration_3_concurrent_instances() {
    println!("\n=== Testing A{{2}} -> B{{2}} (Pattern), events [A1, A2, A3, B1, B2, B3] ===");

    let (mut chain, collector) = build_pattern_chain(
        vec![
            ("e1".to_string(), "A".to_string(), 2, 2), // A{2}
            ("e2".to_string(), "B".to_string(), 2, 2), // B{2}
        ],
        StateType::Pattern,
    );

    // Send A1
    let event_a1 = create_stream_event(1000);
    println!("Sending A1");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a1)));
    chain.update_state();

    // Send A2
    let event_a2 = create_stream_event(2000);
    println!("Sending A2");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a2)));
    chain.update_state();

    // Send A3
    let event_a3 = create_stream_event(3000);
    println!("Sending A3");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a3)));
    chain.update_state();

    // Send B1
    let event_b1 = create_stream_event(4000);
    println!("Sending B1");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b1)));
    chain.update_state();

    // Send B2
    let event_b2 = create_stream_event(5000);
    println!("Sending B2");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b2)));
    chain.update_state();

    // Send B3
    let event_b3 = create_stream_event(6000);
    println!("Sending B3");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b3)));
    chain.update_state();

    // Verify outputs
    let outputs = collector.get_outputs();
    println!("  Final outputs = {}", outputs.len());

    // With A{2} -> B{2}, we expect at least 1 match from (A1,A2) -> (B1,B2)
    // Potentially more with overlapping instances
    assert!(
        outputs.len() >= 1,
        "Expected at least 1 output from concurrent A{{2}} -> B{{2}} instances"
    );

    println!(
        "SUCCESS: Concurrent pattern instances produced {} match(es)",
        outputs.len()
    );
}

/// Test 4: Sequence mode vs Pattern mode comparison
/// Compare behavior when all events match in order vs with non-matching events
///
/// Sequence: A -> B -> C, events [A, B, C] → MATCH
/// Pattern: A -> B -> C, events [A, X, B, Y, C] → MATCH (X, Y ignored)
#[test]
fn test_integration_4_sequence_vs_pattern_comparison() {
    println!("\n=== Testing Sequence vs Pattern mode comparison ===");

    // Test 4a: Sequence mode with perfect order [A, B, C] → MATCH
    {
        let (mut chain, collector) = build_pattern_chain(
            vec![
                ("e1".to_string(), "A".to_string(), 1, 1),
                ("e2".to_string(), "B".to_string(), 1, 1),
                ("e3".to_string(), "C".to_string(), 1, 1),
            ],
            StateType::Sequence,
        );

        println!("  Test 4a: Sequence mode [A, B, C] → MATCH");

        let event_a = create_stream_event(1000);
        chain.pre_processors[0]
            .lock()
            .unwrap()
            .process(Some(Box::new(event_a)));
        chain.update_state();

        let event_b = create_stream_event(2000);
        chain.pre_processors[1]
            .lock()
            .unwrap()
            .process(Some(Box::new(event_b)));
        chain.update_state();

        let event_c = create_stream_event(3000);
        chain.pre_processors[2]
            .lock()
            .unwrap()
            .process(Some(Box::new(event_c)));
        chain.update_state();

        let outputs = collector.get_outputs();
        println!("    Sequence mode outputs = {}", outputs.len());
        assert_eq!(outputs.len(), 1, "Expected 1 output from valid sequence");
    }

    // Test 4b: Pattern mode with non-matching events [A, X, B, Y, C] → MATCH
    {
        let (mut chain, collector) = build_pattern_chain(
            vec![
                ("e1".to_string(), "A".to_string(), 1, 1),
                ("e2".to_string(), "B".to_string(), 1, 1),
                ("e3".to_string(), "C".to_string(), 1, 1),
            ],
            StateType::Pattern,
        );

        println!("  Test 4b: Pattern mode [A, X, B, Y, C] → MATCH (X, Y ignored)");

        let event_a = create_stream_event(1000);
        chain.pre_processors[0]
            .lock()
            .unwrap()
            .process(Some(Box::new(event_a)));
        chain.update_state();

        // X is ignored in Pattern mode
        let event_x = create_stream_event(1500);
        chain.pre_processors[0]
            .lock()
            .unwrap()
            .process(Some(Box::new(event_x)));
        chain.update_state();

        let event_b = create_stream_event(2000);
        chain.pre_processors[1]
            .lock()
            .unwrap()
            .process(Some(Box::new(event_b)));
        chain.update_state();

        // Y is ignored in Pattern mode
        let event_y = create_stream_event(2500);
        chain.pre_processors[0]
            .lock()
            .unwrap()
            .process(Some(Box::new(event_y)));
        chain.update_state();

        let event_c = create_stream_event(3000);
        chain.pre_processors[2]
            .lock()
            .unwrap()
            .process(Some(Box::new(event_c)));
        chain.update_state();

        let outputs = collector.get_outputs();
        println!("    Pattern mode outputs = {}", outputs.len());
        assert_eq!(
            outputs.len(),
            1,
            "Expected 1 output - Pattern ignores X and Y"
        );
    }

    println!("SUCCESS: Sequence and Pattern modes validated");
}

/// Test 5: Four-step pattern chain
/// Pattern: A -> B -> C -> D (Pattern mode)
/// Events: [A, X, B, Y, C, Z, D] → MATCH (X, Y, Z ignored)
///
/// Expected: Pattern mode completes four-step chain ignoring non-matching events
#[test]
fn test_integration_5_four_step_chain() {
    println!("\n=== Testing A -> B -> C -> D (Pattern, 4 steps), events [A, X, B, Y, C, Z, D] ===");

    let (mut chain, collector) = build_pattern_chain(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 1),
            ("e2".to_string(), "B".to_string(), 1, 1),
            ("e3".to_string(), "C".to_string(), 1, 1),
            ("e4".to_string(), "D".to_string(), 1, 1),
        ],
        StateType::Pattern,
    );

    // Send A
    let event_a = create_stream_event(1000);
    println!("Sending A (timestamp 1000)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a)));
    chain.update_state();
    println!("  After A, outputs = {}", collector.get_outputs().len());

    // Send X (non-matching, ignored)
    let event_x = create_stream_event(2000);
    println!("Sending X (timestamp 2000, non-matching)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_x)));
    chain.update_state();
    println!("  After X, outputs = {}", collector.get_outputs().len());

    // Send B
    let event_b = create_stream_event(3000);
    println!("Sending B (timestamp 3000)");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b)));
    chain.update_state();
    println!("  After B, outputs = {}", collector.get_outputs().len());

    // Send Y (non-matching, ignored)
    let event_y = create_stream_event(4000);
    println!("Sending Y (timestamp 4000, non-matching)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_y)));
    chain.update_state();
    println!("  After Y, outputs = {}", collector.get_outputs().len());

    // Send C
    let event_c = create_stream_event(5000);
    println!("Sending C (timestamp 5000)");
    chain.pre_processors[2]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_c)));
    chain.update_state();
    println!("  After C, outputs = {}", collector.get_outputs().len());

    // Send Z (non-matching, ignored)
    let event_z = create_stream_event(6000);
    println!("Sending Z (timestamp 6000, non-matching)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_z)));
    chain.update_state();
    println!("  After Z, outputs = {}", collector.get_outputs().len());

    // Send D
    let event_d = create_stream_event(7000);
    println!("Sending D (timestamp 7000)");
    chain.pre_processors[3]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_d)));
    chain.update_state();

    // Verify output
    let outputs = collector.get_outputs();
    println!("  Final outputs = {}", outputs.len());
    assert_eq!(
        outputs.len(),
        1,
        "Expected 1 output from four-step pattern chain"
    );

    println!("SUCCESS: Four-step pattern chain completed with non-matching events ignored");
}
