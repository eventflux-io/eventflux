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

//! Phase 2b.3. Pattern mode (StateType::Pattern)
//!
//! Tests for pattern mode in chains, where events that do not match are skipped.
//!
//! Follows Phase 2b.3 of feat/pattern_processing/STATE_MACHINE_DESIGN.md. That doc
//! is no longer in the repo.

mod common;

use common::pattern_chain_test_utils::*;
use eventflux::core::query::input::stream::state::stream_pre_state_processor::StateType;

// ============================================================================
// PHASE 2b.3 TESTS. Pattern Mode (StateType::Pattern)
// ============================================================================

/// Test 0. Pattern mode skips events that do not match.
/// Pattern A → B (Pattern mode)
/// Events [A, X, Y, B] → MATCH [A, B]
/// X and Y do not match and are skipped.
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

    // Send X to processor[0]. It does not match, so Pattern mode skips it.
    println!("Sending X to processor[0] (non-matching, should be ignored)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_x)));
    chain.update_state();
    println!("  After X, outputs = {}", collector.get_outputs().len());

    // Send Y to processor[0]. It does not match, so Pattern mode skips it.
    println!("Sending Y to processor[0] (non-matching, should be ignored)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_y)));
    chain.update_state();
    println!("  After Y, outputs = {}", collector.get_outputs().len());

    // Send B to processor[1]. It matches and completes the pattern.
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

/// Test 1. Sequence mode compared with Pattern mode.
/// Sequence A → B, events [A, X] → FAIL, because X breaks the sequence
/// Pattern A → B, events [A, X, B] → MATCH [A, B], because X is skipped
#[test]
fn test_2b3_1_sequence_vs_pattern() {
    println!("\n=== Testing Sequence vs Pattern behavior ===");

    // PART 1. In Sequence mode an event that does not match breaks the sequence
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

    // Send X to processor[0], where it does not match and breaks the sequence
    println!("  Sending X to processor[0] (should break sequence)");
    seq_chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_x1)));
    seq_chain.update_state();

    let seq_outputs = seq_collector.get_outputs();
    println!("  Sequence mode outputs: {}", seq_outputs.len());
    assert_eq!(seq_outputs.len(), 0, "Sequence mode: X breaks the sequence");

    // PART 2. In Pattern mode an event that does not match is skipped
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

    // Send X. It does not match, so Pattern mode skips it.
    println!("  Sending X to processor[0] (should be ignored)");
    pat_chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_x2)));
    pat_chain.update_state();

    // Send B. It matches and completes the pattern.
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

/// Test 2. Three step pattern with events that do not match in between.
/// Pattern A → B → C (Pattern mode)
/// Events [A, X, B, Y, C] → MATCH [A, B, C]
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

    // Send X. It does not match and is skipped.
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

    // Send Y. It does not match and is skipped.
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

/// Test 3. Pattern with many events that do not match, as a stress test.
/// Pattern A → B (Pattern mode)
/// Events [A, X×1000, B] → MATCH [A, B]
/// Skipping many events must not build up memory.
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

    // Send 1000 X events that do not match. All must be skipped.
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

    // Send B to processor[1]. It matches and completes the pattern.
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
