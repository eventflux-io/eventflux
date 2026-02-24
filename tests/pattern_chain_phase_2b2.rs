// SPDX-License-Identifier: MIT OR Apache-2.0

//! Phase 2b.2: Three-Step Pattern Chains (A → B → C)
//!
//! Tests for three-step pattern chaining: A -> B -> C
//!
//! Reference: feat/pattern_processing/STATE_MACHINE_DESIGN.md Phase 2b.2

mod common;

use common::pattern_chain_test_utils::*;
use eventflux::core::event::stream::stream_event::StreamEvent;
use eventflux::core::query::input::stream::state::stream_pre_state_processor::StateType;

// Alias for three-step tests (same as build_pattern_chain)
use common::pattern_chain_test_utils::build_pattern_chain as build_three_step_chain;

// ============================================================================
// PHASE 2b.2 TESTS: Three-Step Chains (A → B → C)
// ============================================================================

/// Test 0: Simple three-step chain A → B → C
/// Pattern: A{1} → B{1} → C{1}
/// Events: [A, B, C] → MATCH
#[test]
fn test_2b2_0_simple_three_step() {
    println!("\n=== Testing A -> B -> C pattern ===");

    let (mut chain, collector) = build_three_step_chain(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 1),
            ("e2".to_string(), "B".to_string(), 1, 1),
            ("e3".to_string(), "C".to_string(), 1, 1),
        ],
        StateType::Sequence,
    );

    // Create events
    let event_a = create_stream_event(1000);
    let event_b = create_stream_event(2000);
    let event_c = create_stream_event(3000);

    // Send A to processor[0]
    println!("Sending A (timestamp 1000) to processor[0]");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a)));
    chain.update_state();

    let outputs = collector.get_outputs();
    println!("  After A, outputs = {}", outputs.len());
    assert_eq!(outputs.len(), 0, "No output yet after A");

    // Send B to processor[1]
    println!("Sending B (timestamp 2000) to processor[1]");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b)));
    chain.update_state();

    let outputs = collector.get_outputs();
    println!("  After B, outputs = {}", outputs.len());
    assert_eq!(outputs.len(), 0, "No output yet after B");

    // Send C to processor[2]
    println!("Sending C (timestamp 3000) to processor[2]");
    chain.pre_processors[2]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_c)));
    chain.update_state();

    // Verify output
    let outputs = collector.get_outputs();
    println!("  After C, outputs = {}", outputs.len());
    assert_eq!(outputs.len(), 1, "Expected 1 output after complete pattern");

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

    println!("Final outputs count: {}", outputs.len());
}

/// Test 1: Three-step chain with count quantifiers A{2} → B{2} → C{2}
/// Pattern: A{2} → B{2} → C{2}
/// Events: [A, A, B, B, C, C] → MATCH
#[test]
fn test_2b2_1_count_quantifiers() {
    println!("\n=== Testing A{{2}} -> B{{2}} -> C{{2}} pattern ===");

    let (mut chain, collector) = build_three_step_chain(
        vec![
            ("e1".to_string(), "A".to_string(), 2, 2),
            ("e2".to_string(), "B".to_string(), 2, 2),
            ("e3".to_string(), "C".to_string(), 2, 2),
        ],
        StateType::Sequence,
    );

    // Create events
    let events_a: Vec<StreamEvent> = (1..=2).map(|i| create_stream_event(i * 1000)).collect();
    let events_b: Vec<StreamEvent> = (3..=4).map(|i| create_stream_event(i * 1000)).collect();
    let events_c: Vec<StreamEvent> = (5..=6).map(|i| create_stream_event(i * 1000)).collect();

    // Send two A events to processor[0]
    for (i, event_a) in events_a.into_iter().enumerate() {
        println!("Sending A{} to processor[0]", i + 1);
        chain.pre_processors[0]
            .lock()
            .unwrap()
            .process(Some(Box::new(event_a)));
        chain.update_state();
        println!(
            "  After A{}, outputs = {}",
            i + 1,
            collector.get_outputs().len()
        );
    }

    // Send two B events to processor[1]
    for (i, event_b) in events_b.into_iter().enumerate() {
        println!("Sending B{} to processor[1]", i + 1);
        chain.pre_processors[1]
            .lock()
            .unwrap()
            .process(Some(Box::new(event_b)));
        chain.update_state();
        println!(
            "  After B{}, outputs = {}",
            i + 1,
            collector.get_outputs().len()
        );
    }

    // Send two C events to processor[2]
    for (i, event_c) in events_c.into_iter().enumerate() {
        println!("Sending C{} to processor[2]", i + 1);
        chain.pre_processors[2]
            .lock()
            .unwrap()
            .process(Some(Box::new(event_c)));
        chain.update_state();
        println!(
            "  After C{}, outputs = {}",
            i + 1,
            collector.get_outputs().len()
        );
    }

    // Verify output
    let outputs = collector.get_outputs();
    assert_eq!(outputs.len(), 1, "Expected 1 output after complete pattern");

    // Verify StateEvent contains all three steps with chained events
    let state_evt = &outputs[0];

    // Check position 0 has A events
    assert!(
        state_evt.stream_events[0].is_some(),
        "Position 0 should have event A"
    );
    let event_a_chain = state_evt.stream_events[0].as_ref().unwrap();
    assert!(
        event_a_chain.next.is_some(),
        "A should be chained (2 events)"
    );

    // Check position 1 has B events
    assert!(
        state_evt.stream_events[1].is_some(),
        "Position 1 should have event B"
    );
    let event_b_chain = state_evt.stream_events[1].as_ref().unwrap();
    assert!(
        event_b_chain.next.is_some(),
        "B should be chained (2 events)"
    );

    // Check position 2 has C events
    assert!(
        state_evt.stream_events[2].is_some(),
        "Position 2 should have event C"
    );
    let event_c_chain = state_evt.stream_events[2].as_ref().unwrap();
    assert!(
        event_c_chain.next.is_some(),
        "C should be chained (2 events)"
    );

    println!("Final outputs count: {}", outputs.len());
}

/// Test 2: Three-step chain failure - expecting C, got B
/// Pattern: A → B → C
/// Events: [A, B, B] → FAIL (expecting C at processor[2], but got B at processor[1])
#[test]
fn test_2b2_2_expecting_c_got_b() {
    println!("\n=== Testing expecting C, got B ===");

    let (mut chain, collector) = build_three_step_chain(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 1),
            ("e2".to_string(), "B".to_string(), 1, 1),
            ("e3".to_string(), "C".to_string(), 1, 1),
        ],
        StateType::Sequence,
    );

    // Create events
    let event_a = create_stream_event(1000);
    let event_b1 = create_stream_event(2000);
    let event_b2 = create_stream_event(3000);

    // Send A to processor[0]
    println!("Sending A to processor[0]");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a)));
    chain.update_state();

    // Send first B to processor[1]
    println!("Sending B1 to processor[1]");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b1)));
    chain.update_state();

    // Send second B to processor[1] (wrong - should send C to processor[2])
    println!("Sending B2 to processor[1] (wrong - expecting C at processor[2])");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b2)));
    chain.update_state();

    // Verify no output (sequence broken)
    let outputs = collector.get_outputs();
    println!("  After B2, outputs = {}", outputs.len());
    assert_eq!(
        outputs.len(),
        0,
        "Expected no output - PreB already satisfied, additional B ignored in Sequence mode"
    );

    println!("Final outputs count: {}", outputs.len());
}

/// Test 3: Three-step chain failure - skipped B
/// Pattern: A → B → C
/// Events: [A, C] → FAIL (skipped B - C arrives but PreC has no pending state)
#[test]
fn test_2b2_3_skipped_b() {
    println!("\n=== Testing skipped B ===");

    let (mut chain, collector) = build_three_step_chain(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 1),
            ("e2".to_string(), "B".to_string(), 1, 1),
            ("e3".to_string(), "C".to_string(), 1, 1),
        ],
        StateType::Sequence,
    );

    // Create events
    let event_a = create_stream_event(1000);
    let event_c = create_stream_event(3000);

    // Send A to processor[0]
    println!("Sending A to processor[0]");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a)));
    chain.update_state();

    let outputs = collector.get_outputs();
    println!("  After A, outputs = {}", outputs.len());
    assert_eq!(outputs.len(), 0, "No output yet after A");

    // Send C to processor[2] (skipping B - PreC should have no pending state)
    println!("Sending C to processor[2] (skipped B)");
    chain.pre_processors[2]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_c)));
    chain.update_state();

    // Verify no output (B was skipped, so PreC has no pending states)
    let outputs = collector.get_outputs();
    println!("  After C, outputs = {}", outputs.len());
    assert_eq!(outputs.len(), 0, "Expected no output - B was skipped");

    println!("Final outputs count: {}", outputs.len());
}

/// Test 4: Three-step chain with range quantifiers
/// Pattern: A{1,2} → B{1} → C{2}
/// Events: [A, A, B, C, C] → MATCH
/// Note: Last step must have exact count (min=max)
#[test]
fn test_2b2_4_range_quantifiers() {
    println!("\n=== Testing A{{1,2}} -> B -> C{{2}} pattern ===");

    let (mut chain, collector) = build_three_step_chain(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 2),
            ("e2".to_string(), "B".to_string(), 1, 1),
            ("e3".to_string(), "C".to_string(), 2, 2),
        ],
        StateType::Sequence,
    );

    // Create events
    let events_a: Vec<StreamEvent> = (1..=2).map(|i| create_stream_event(i * 1000)).collect();
    let event_b = create_stream_event(3000);
    let events_c: Vec<StreamEvent> = (4..=5).map(|i| create_stream_event(i * 1000)).collect();

    // Send two A events to processor[0]
    for (i, event_a) in events_a.into_iter().enumerate() {
        println!("Sending A{} to processor[0]", i + 1);
        chain.pre_processors[0]
            .lock()
            .unwrap()
            .process(Some(Box::new(event_a)));
        chain.update_state();
        println!(
            "  After A{}, outputs = {}",
            i + 1,
            collector.get_outputs().len()
        );
    }

    // Send B to processor[1]
    println!("Sending B to processor[1]");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b)));
    chain.update_state();
    println!("  After B, outputs = {}", collector.get_outputs().len());

    // Send two C events to processor[2]
    for (i, event_c) in events_c.into_iter().enumerate() {
        println!("Sending C{} to processor[2]", i + 1);
        chain.pre_processors[2]
            .lock()
            .unwrap()
            .process(Some(Box::new(event_c)));
        chain.update_state();
        println!(
            "  After C{}, outputs = {}",
            i + 1,
            collector.get_outputs().len()
        );
    }

    // Verify output
    // Range quantifiers A{1,2} should produce multiple matches (A{1} and A{2})
    // Each match flows through the chain and may produce outputs
    let outputs = collector.get_outputs();
    assert!(
        outputs.len() >= 1,
        "Expected at least 1 output with range quantifiers"
    );
    assert!(
        outputs.len() <= 2,
        "Expected at most 2 outputs for A{{1,2}} -> B -> C{{2}}"
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

    println!("Final outputs count: {}", outputs.len());
}
