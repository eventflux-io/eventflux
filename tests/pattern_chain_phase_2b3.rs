// SPDX-License-Identifier: MIT OR Apache-2.0

//! Phase 2b.3: Pattern Mode (StateType::Pattern)
//!
//! Tests for pattern mode behavior in chains - non-matching events are ignored
//!
//! Reference: feat/pattern_processing/STATE_MACHINE_DESIGN.md Phase 2b.3

mod common;

use common::pattern_chain_test_utils::*;
use eventflux::core::event::stream::stream_event::StreamEvent;
use eventflux::core::query::input::stream::state::stream_pre_state_processor::StateType;

// ============================================================================
// PHASE 2b.3 TESTS: Pattern Mode (StateType::Pattern)
// ============================================================================

/// Test 0: Pattern mode ignores non-matching events
/// Pattern: A → B (Pattern mode)
/// Events: [A, X, Y, B] → MATCH [A, B]
/// Non-matching events X and Y are ignored
#[test]
fn test_2b3_0_pattern_ignores_non_matching() {
    println!("\n=== Testing A -> B (Pattern) with non-matching events [A, X, Y, B] ===");

    let (mut chain, collector) = build_pattern_chain(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 1),
            ("e2".to_string(), "B".to_string(), 1, 1),
        ],
        StateType::Pattern,
    );

    // Create events
    let event_a = create_stream_event(1000);
    let event_x = create_stream_event(2000);
    let event_y = create_stream_event(3000);
    let event_b = create_stream_event(4000);

    // Send A to processor[0]
    println!("Sending A to processor[0]");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a)));
    chain.update_state();
    println!("  After A, outputs = {}", collector.get_outputs().len());

    // Send X to processor[0] (non-matching, should be ignored in Pattern mode)
    println!("Sending X to processor[0] (non-matching, should be ignored)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_x)));
    chain.update_state();
    println!("  After X, outputs = {}", collector.get_outputs().len());

    // Send Y to processor[0] (non-matching, should be ignored in Pattern mode)
    println!("Sending Y to processor[0] (non-matching, should be ignored)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_y)));
    chain.update_state();
    println!("  After Y, outputs = {}", collector.get_outputs().len());

    // Send B to processor[1] (should match and complete the pattern)
    println!("Sending B to processor[1]");
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
        "Expected 1 output - Pattern mode ignores non-matching events"
    );

    // Verify StateEvent contains A and B
    let state_evt = &outputs[0];
    assert!(
        state_evt.stream_events[0].is_some(),
        "Position 0 should have event A"
    );
    assert!(
        state_evt.stream_events[1].is_some(),
        "Position 1 should have event B"
    );

    println!("SUCCESS: Pattern mode ignored non-matching events X and Y");
}

/// Test 1: Compare Sequence vs Pattern behavior
/// Sequence: A → B, events [A, X] → FAIL (X breaks sequence)
/// Pattern: A → B, events [A, X, B] → MATCH [A, B] (X is ignored)
#[test]
fn test_2b3_1_sequence_vs_pattern() {
    println!("\n=== Testing Sequence vs Pattern behavior ===");

    // PART 1: Sequence mode - non-matching event breaks the sequence
    println!("\nPART 1: Sequence mode [A, X] → FAIL");
    let (mut seq_chain, seq_collector) = build_pattern_chain(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 1),
            ("e2".to_string(), "B".to_string(), 1, 1),
        ],
        StateType::Sequence,
    );

    let event_a1 = create_stream_event(1000);
    let event_x1 = create_stream_event(2000);

    // Send A
    println!("  Sending A to processor[0]");
    seq_chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a1)));
    seq_chain.update_state();

    // Send X (non-matching - in Sequence mode, this would go to processor[0] and not match)
    println!("  Sending X to processor[0] (should break sequence)");
    seq_chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_x1)));
    seq_chain.update_state();

    let seq_outputs = seq_collector.get_outputs();
    println!("  Sequence mode outputs: {}", seq_outputs.len());
    assert_eq!(seq_outputs.len(), 0, "Sequence mode: X breaks the sequence");

    // PART 2: Pattern mode - non-matching event is ignored
    println!("\nPART 2: Pattern mode [A, X, B] → MATCH");
    let (mut pat_chain, pat_collector) = build_pattern_chain(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 1),
            ("e2".to_string(), "B".to_string(), 1, 1),
        ],
        StateType::Pattern,
    );

    let event_a2 = create_stream_event(1000);
    let event_x2 = create_stream_event(2000);
    let event_b2 = create_stream_event(3000);

    // Send A
    println!("  Sending A to processor[0]");
    pat_chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a2)));
    pat_chain.update_state();

    // Send X (non-matching - in Pattern mode, this is ignored)
    println!("  Sending X to processor[0] (should be ignored)");
    pat_chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_x2)));
    pat_chain.update_state();

    // Send B (should match and complete the pattern)
    println!("  Sending B to processor[1]");
    pat_chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b2)));
    pat_chain.update_state();

    let pat_outputs = pat_collector.get_outputs();
    println!("  Pattern mode outputs: {}", pat_outputs.len());
    assert_eq!(
        pat_outputs.len(),
        1,
        "Pattern mode: X is ignored, pattern matches"
    );

    println!("SUCCESS: Sequence breaks on X, Pattern ignores X and matches");
}

/// Test 2: Three-step pattern with non-matching events in between
/// Pattern: A → B → C (Pattern mode)
/// Events: [A, X, B, Y, C] → MATCH [A, B, C]
#[test]
fn test_2b3_2_three_step_pattern_with_noise() {
    println!("\n=== Testing A -> B -> C (Pattern) with events [A, X, B, Y, C] ===");

    let (mut chain, collector) = build_pattern_chain(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 1),
            ("e2".to_string(), "B".to_string(), 1, 1),
            ("e3".to_string(), "C".to_string(), 1, 1),
        ],
        StateType::Pattern,
    );

    // Create events
    let event_a = create_stream_event(1000);
    let event_x = create_stream_event(2000);
    let event_b = create_stream_event(3000);
    let event_y = create_stream_event(4000);
    let event_c = create_stream_event(5000);

    // Send A to processor[0]
    println!("Sending A to processor[0]");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a)));
    chain.update_state();
    println!("  After A, outputs = {}", collector.get_outputs().len());

    // Send X (non-matching, should be ignored)
    println!("Sending X to processor[0] (non-matching)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_x)));
    chain.update_state();
    println!("  After X, outputs = {}", collector.get_outputs().len());

    // Send B to processor[1]
    println!("Sending B to processor[1]");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b)));
    chain.update_state();
    println!("  After B, outputs = {}", collector.get_outputs().len());

    // Send Y (non-matching, should be ignored)
    println!("Sending Y to processor[1] (non-matching)");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_y)));
    chain.update_state();
    println!("  After Y, outputs = {}", collector.get_outputs().len());

    // Send C to processor[2]
    println!("Sending C to processor[2]");
    chain.pre_processors[2]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_c)));
    chain.update_state();

    // Verify output
    let outputs = collector.get_outputs();
    println!("  After C, outputs = {}", outputs.len());
    assert_eq!(
        outputs.len(),
        1,
        "Expected 1 output - Pattern completed despite noise"
    );

    // Verify StateEvent contains all three steps
    let state_evt = &outputs[0];
    assert!(
        state_evt.stream_events[0].is_some(),
        "Position 0 should have event A"
    );
    assert!(
        state_evt.stream_events[1].is_some(),
        "Position 1 should have event B"
    );
    assert!(
        state_evt.stream_events[2].is_some(),
        "Position 2 should have event C"
    );

    println!("SUCCESS: Three-step pattern ignored non-matching events X and Y");
}

/// Test 3: Pattern with many non-matching events (stress test)
/// Pattern: A → B (Pattern mode)
/// Events: [A, X×1000, B] → MATCH [A, B]
/// Tests memory efficiency of ignoring many non-matching events
#[test]
fn test_2b3_3_pattern_with_many_non_matching() {
    println!("\n=== Testing A -> B (Pattern) with 1000 non-matching events ===");

    let (mut chain, collector) = build_pattern_chain(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 1),
            ("e2".to_string(), "B".to_string(), 1, 1),
        ],
        StateType::Pattern,
    );

    // Send A to processor[0]
    let event_a = create_stream_event(1000);
    println!("Sending A to processor[0]");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a)));
    chain.update_state();
    println!("  After A, outputs = {}", collector.get_outputs().len());

    // Send 1000 non-matching X events (should all be ignored)
    println!("Sending 1000 non-matching X events...");
    for i in 0..1000 {
        let event_x = create_stream_event(2000 + i);
        chain.pre_processors[0]
            .lock()
            .unwrap()
            .process(Some(Box::new(event_x)));
        chain.update_state();

        // Log progress every 100 events
        if (i + 1) % 100 == 0 {
            println!(
                "  Sent {} non-matching events, outputs = {}",
                i + 1,
                collector.get_outputs().len()
            );
        }
    }
    println!(
        "  After 1000 X events, outputs = {}",
        collector.get_outputs().len()
    );

    // Send B to processor[1] (should match and complete the pattern)
    let event_b = create_stream_event(3000);
    println!("Sending B to processor[1]");
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
        "Expected 1 output - Pattern matched despite 1000 non-matching events"
    );

    // Verify StateEvent contains A and B
    let state_evt = &outputs[0];
    assert!(
        state_evt.stream_events[0].is_some(),
        "Position 0 should have event A"
    );
    assert!(
        state_evt.stream_events[1].is_some(),
        "Position 1 should have event B"
    );

    println!("SUCCESS: Pattern handled 1000 non-matching events efficiently");
}
