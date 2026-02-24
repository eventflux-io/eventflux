// SPDX-License-Identifier: MIT OR Apache-2.0

//! Phase 2b.1: Basic Two-Step Pattern Chains
//!
//! Tests for basic pattern chaining: A -> B
//!
//! Reference: feat/pattern_processing/STATE_MACHINE_DESIGN.md Phase 2b.1

mod common;

use common::pattern_chain_test_utils::*;
use eventflux::core::event::stream::stream_event::StreamEvent;
use eventflux::core::query::input::stream::state::stream_pre_state_processor::StateType;

// Test 1: Simple single-step pattern first (A{1})
#[test]
fn test_2b1_0_single_step() {
    let (mut chain, collector) = build_pattern_chain(
        vec![("e1".to_string(), "A".to_string(), 1, 1)],
        StateType::Sequence,
    );

    println!("\n=== Testing single step A{{1}} ===");
    let event = create_stream_event(1000);

    println!("Sending event (timestamp 1000)");
    chain
        .first_processor
        .lock()
        .unwrap()
        .process(Some(Box::new(event)));
    chain.update_state();

    println!("Outputs: {}", collector.get_outputs().len());

    // For a single step pattern, output should be generated immediately
    let outputs = collector.get_outputs();
    assert_eq!(
        outputs.len(),
        1,
        "Expected 1 output for single-step pattern"
    );
}

// Test 2: A -> B, events [A, B] → MATCH
#[test]
fn test_2b1_1_simple_two_step() {
    let (mut chain, collector) = build_pattern_chain(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 1),
            ("e2".to_string(), "B".to_string(), 1, 1),
        ],
        StateType::Sequence,
    );

    // For a two-step sequence A -> B:
    // - Event A goes to processor[0] (PreA)
    // - Event B goes to processor[1] (PreB)
    // This mimics Java's ProcessStreamReceiver routing

    // Send event A to processor 0
    let event_a = create_stream_event(1000);
    println!("\n=== Sending event A (timestamp 1000) to processor[0] ===");
    println!(
        "  PreA has_state_changed before: {}",
        chain.pre_processors[0].lock().unwrap().has_state_changed()
    );
    let result_a = chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a)));
    println!("  Result A: {:?}", result_a.is_some());
    println!(
        "  PreA has_state_changed after: {}",
        chain.pre_processors[0].lock().unwrap().has_state_changed()
    );

    // CRITICAL: After PreA processes, update all processors to propagate forwarded states
    // PostA will have called add_state() on PreB, adding to PreB's new_list
    // We need to move that from new_list -> pending_list
    chain.update_state();
    println!(
        "  After update_state (propagates forwarded states), outputs = {}",
        collector.get_outputs().len()
    );

    // Check if PreB received forwarded state from PostA
    println!("\n=== Checking PreB state ===");
    // PreB should now have the forwarded StateEvent in its pending_list

    // Send event B to processor 1
    let event_b = create_stream_event(2000);
    println!("\n=== Sending event B (timestamp 2000) to processor[1] ===");
    println!(
        "  PreB has_state_changed before: {}",
        chain.pre_processors[1].lock().unwrap().has_state_changed()
    );
    let result_b = chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b)));
    println!("  Result B: {:?}", result_b.is_some());
    println!(
        "  PreB has_state_changed after: {}",
        chain.pre_processors[1].lock().unwrap().has_state_changed()
    );
    chain.update_state();
    println!(
        "  After update_state, outputs = {}",
        collector.get_outputs().len()
    );

    // Verify output: Should have exactly 1 StateEvent with both A and B
    let outputs = collector.get_outputs();
    println!("Final outputs count: {}", outputs.len());
    assert_eq!(outputs.len(), 1, "Expected exactly 1 output event");

    let state_event = &outputs[0];

    // Verify we have 2 stream positions (one for each step)
    assert_eq!(
        state_event.stream_events.len(),
        2,
        "Expected 2 stream event positions"
    );

    // Verify first position has event A
    assert!(
        state_event.stream_events[0].is_some(),
        "Expected event at position 0 (step A)"
    );
    let event_a_result = state_event.stream_events[0].as_ref().unwrap();
    assert_eq!(
        event_a_result.timestamp, 1000,
        "Expected timestamp 1000 for event A"
    );

    // Verify second position has event B
    assert!(
        state_event.stream_events[1].is_some(),
        "Expected event at position 1 (step B)"
    );
    let event_b_result = state_event.stream_events[1].as_ref().unwrap();
    assert_eq!(
        event_b_result.timestamp, 2000,
        "Expected timestamp 2000 for event B"
    );
}

// Test 3: A{2} -> B{2}, events [A, A, B, B] → MATCH
#[test]
fn test_2b1_2_count_quantifiers() {
    let (mut chain, collector) = build_pattern_chain(
        vec![
            ("e1".to_string(), "A".to_string(), 2, 2),
            ("e2".to_string(), "B".to_string(), 2, 2),
        ],
        StateType::Sequence,
    );

    println!("\n=== Testing A{{2}} -> B{{2}} pattern ===");

    // Send first A event to processor[0]
    let event_a1 = create_stream_event(1000);
    println!("Sending A1 (timestamp 1000) to processor[0]");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a1)));
    chain.update_state();
    println!("  After A1, outputs = {}", collector.get_outputs().len());

    // Send second A event to processor[0]
    let event_a2 = create_stream_event(2000);
    println!("Sending A2 (timestamp 2000) to processor[0]");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a2)));
    chain.update_state();
    println!("  After A2, outputs = {}", collector.get_outputs().len());

    // Send first B event to processor[1]
    let event_b1 = create_stream_event(3000);
    println!("Sending B1 (timestamp 3000) to processor[1]");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b1)));
    chain.update_state();
    println!("  After B1, outputs = {}", collector.get_outputs().len());

    // Send second B event to processor[1]
    let event_b2 = create_stream_event(4000);
    println!("Sending B2 (timestamp 4000) to processor[1]");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b2)));
    chain.update_state();
    println!("  After B2, outputs = {}", collector.get_outputs().len());

    // Verify output: Should have exactly 1 StateEvent with both A and B chains
    let outputs = collector.get_outputs();
    println!("Final outputs count: {}", outputs.len());
    assert_eq!(outputs.len(), 1, "Expected exactly 1 output event");

    let state_event = &outputs[0];

    // Verify we have 2 stream positions (one for each step)
    assert_eq!(
        state_event.stream_events.len(),
        2,
        "Expected 2 stream event positions"
    );

    // Verify first position has chained A events (A1->A2)
    assert!(
        state_event.stream_events[0].is_some(),
        "Expected event at position 0 (step A)"
    );
    let event_a_chain = state_event.stream_events[0].as_ref().unwrap();
    assert_eq!(
        event_a_chain.timestamp, 1000,
        "Expected first A timestamp 1000"
    );

    // Check that A2 is chained after A1
    assert!(event_a_chain.next.is_some(), "Expected A2 chained after A1");
    let event_a2_box = event_a_chain.next.as_ref().unwrap();
    let event_a2 = event_a2_box.as_any().downcast_ref::<StreamEvent>().unwrap();
    assert_eq!(event_a2.timestamp, 2000, "Expected second A timestamp 2000");

    // Verify second position has chained B events (B1->B2)
    assert!(
        state_event.stream_events[1].is_some(),
        "Expected event at position 1 (step B)"
    );
    let event_b_chain = state_event.stream_events[1].as_ref().unwrap();
    assert_eq!(
        event_b_chain.timestamp, 3000,
        "Expected first B timestamp 3000"
    );

    // Check that B2 is chained after B1
    assert!(event_b_chain.next.is_some(), "Expected B2 chained after B1");
    let event_b2_box = event_b_chain.next.as_ref().unwrap();
    let event_b2 = event_b2_box.as_any().downcast_ref::<StreamEvent>().unwrap();
    assert_eq!(event_b2.timestamp, 4000, "Expected second B timestamp 4000");
}

// Test 4: A -> B, events [B, A] → FAIL (wrong order)
#[test]
fn test_2b1_3_wrong_order_fails() {
    let (mut chain, collector) = build_pattern_chain(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 1),
            ("e2".to_string(), "B".to_string(), 1, 1),
        ],
        StateType::Sequence,
    );

    println!("\n=== Testing wrong order: B before A ===");

    // Send B first (should fail - no pending state in PreB)
    let event_b = create_stream_event(1000);
    println!("Sending B (timestamp 1000) to processor[1]");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b)));
    chain.update_state();
    println!("  After B, outputs = {}", collector.get_outputs().len());

    // Send A second (too late - sequence is broken)
    let event_a = create_stream_event(2000);
    println!("Sending A (timestamp 2000) to processor[0]");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a)));
    chain.update_state();
    println!("  After A, outputs = {}", collector.get_outputs().len());

    // Verify no output (wrong order should not match in sequence)
    let outputs = collector.get_outputs();
    println!("Final outputs count: {}", outputs.len());
    assert_eq!(outputs.len(), 0, "Expected no output for wrong order");
}

// Test 5: A -> B, events [A, C] → FAIL (wrong stream)
// NOTE: In our test setup, we manually route events to processors,
// so this test validates that sending to wrong processor produces no output
#[test]
fn test_2b1_4_wrong_stream_fails() {
    let (mut chain, collector) = build_pattern_chain(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 1),
            ("e2".to_string(), "B".to_string(), 1, 1),
        ],
        StateType::Sequence,
    );

    println!("\n=== Testing wrong stream: A then C (not B) ===");

    // Send A to processor[0]
    let event_a = create_stream_event(1000);
    println!("Sending A (timestamp 1000) to processor[0]");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a)));
    chain.update_state();
    println!("  After A, outputs = {}", collector.get_outputs().len());

    // Send C to processor[0] instead of B to processor[1]
    // This simulates wrong stream routing
    let event_c = create_stream_event(2000);
    println!("Sending C (timestamp 2000) to processor[0] (wrong - should go to processor[1])");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_c)));
    chain.update_state();
    println!("  After C, outputs = {}", collector.get_outputs().len());

    // Verify no output (C routed to wrong processor)
    let outputs = collector.get_outputs();
    println!("Final outputs count: {}", outputs.len());
    assert_eq!(outputs.len(), 0, "Expected no output for wrong stream");
}

// Test 6: A -> B, events [A, A] → FAIL (expecting B, got A in Sequence mode)
#[test]
fn test_2b1_5_expecting_b_got_a() {
    let (mut chain, collector) = build_pattern_chain(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 1),
            ("e2".to_string(), "B".to_string(), 1, 1),
        ],
        StateType::Sequence,
    );

    println!("\n=== Testing expecting B, got A ===");

    // Send first A to processor[0]
    let event_a1 = create_stream_event(1000);
    println!("Sending A1 (timestamp 1000) to processor[0]");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a1)));
    chain.update_state();
    println!("  After A1, outputs = {}", collector.get_outputs().len());

    // Send second A to processor[0] (instead of B to processor[1])
    let event_a2 = create_stream_event(2000);
    println!("Sending A2 (timestamp 2000) to processor[0] (should be B to processor[1])");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a2)));
    chain.update_state();
    println!("  After A2, outputs = {}", collector.get_outputs().len());

    // Verify no output (second A doesn't match B requirement)
    let outputs = collector.get_outputs();
    println!("Final outputs count: {}", outputs.len());
    assert_eq!(
        outputs.len(),
        0,
        "Expected no output when expecting B but got A"
    );
}

// Test 7: A{1,3} -> B{2}, events [A, A, B, B] → MATCH
#[test]
fn test_2b1_6_range_quantifier() {
    let (mut chain, collector) = build_pattern_chain(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 3),
            ("e2".to_string(), "B".to_string(), 2, 2),
        ],
        StateType::Sequence,
    );

    println!("\n=== Testing A{{1,3}} -> B{{2}} pattern ===");

    // Send two A events to processor[0]
    let event_a1 = create_stream_event(1000);
    println!("Sending A1 (timestamp 1000) to processor[0]");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a1)));
    chain.update_state();

    let event_a2 = create_stream_event(2000);
    println!("Sending A2 (timestamp 2000) to processor[0]");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_a2)));
    chain.update_state();

    // Send two B events to processor[1]
    let event_b1 = create_stream_event(3000);
    println!("Sending B1 (timestamp 3000) to processor[1]");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b1)));
    chain.update_state();

    let event_b2 = create_stream_event(4000);
    println!("Sending B2 (timestamp 4000) to processor[1]");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b2)));
    chain.update_state();

    // Verify output: Range quantifiers produce multiple matches
    // A{1,3} forwards states for A{1} and A{2} (both within 1-3 range)
    // So we get 2 outputs: one for A{1}->B{2} and one for A{2}->B{2}
    let outputs = collector.get_outputs();
    println!("Final outputs count: {}", outputs.len());
    assert!(outputs.len() >= 1, "Expected at least 1 output event");
    assert!(
        outputs.len() <= 2,
        "Expected at most 2 output events for A{{1,3}}"
    );

    // Verify first output (A{1} -> B{2})
    let state_event = &outputs[0];
    assert_eq!(
        state_event.stream_events.len(),
        2,
        "Expected 2 stream event positions"
    );

    // Verify first position has A event(s)
    assert!(
        state_event.stream_events[0].is_some(),
        "Expected event at position 0 (step A)"
    );
    let event_a_chain = state_event.stream_events[0].as_ref().unwrap();
    assert_eq!(
        event_a_chain.timestamp, 1000,
        "Expected first A timestamp 1000"
    );

    // Verify second position has B events (B1->B2)
    assert!(
        state_event.stream_events[1].is_some(),
        "Expected event at position 1 (step B)"
    );
    let event_b_chain = state_event.stream_events[1].as_ref().unwrap();
    assert_eq!(
        event_b_chain.timestamp, 3000,
        "Expected first B timestamp 3000"
    );
}
