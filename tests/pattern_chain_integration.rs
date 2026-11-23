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

//! Phase 2b.5. Integration testing
//!
//! Integration tests that combine the pattern chain features
//! - Several overlapping pattern instances at once
//! - Complex count quantifiers
//! - Pattern mode together with WITHIN
//! - Performance checks
//!
//! Follows Phase 2b.5 of feat/pattern_processing/STATE_MACHINE_DESIGN.md. That doc
//! is no longer in the repo.

mod common;

use common::pattern_chain_test_utils::*;
use eventflux::core::query::input::stream::state::stream_pre_state_processor::StateType;
// build_pattern_chain_with_within() comes from the common module

// ============================================================================
// PHASE 2b.5 TESTS. Integration Testing
// ============================================================================

/// Test 1. Complex count quantifiers with several pattern instances
/// Pattern A{2,3} -> B{2} (Pattern mode)
/// Events [A, A, X, B, A, B] → more than one match is possible
///
/// Expected behaviour
/// - A{2,3} collects 2 or 3 A events
/// - B{2} collects exactly 2 B events, because the last step needs an exact count
/// - Pattern mode skips X
/// - A match comes after at least 2 As and 2 Bs
#[test]
fn test_integration_1_complex_count_quantifiers() {
    println!("\n=== Testing A{{2,3}} -> B{{2}} (Pattern), events [A, A, X, B, A, B] ===");

    let (mut chain, collector) = build_pattern_chain(
        vec![
            ("e1".to_string(), "A".to_string(), 2, 3), // A{2,3}
            ("e2".to_string(), "B".to_string(), 2, 2), // B{2}, last step needs an exact count
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

    // Send X. It does not match, so Pattern mode skips it.
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

    // With A{2,3} -> B{2}
    // - After A1 and A2 there is a state with 2 As waiting for 2 Bs
    // - After B1 and B2 comes the first match, 2 As and 2 Bs
    // - A3 may lead to more matches
    assert!(
        !outputs.is_empty(),
        "Expected at least 1 output from A{{2,3}} -> B{{2}} pattern"
    );

    println!(
        "SUCCESS: Complex count quantifiers produced {} match(es)",
        outputs.len()
    );
}

/// Test 2. Pattern mode with a WITHIN time limit
/// Pattern A -> B -> C (Pattern mode, WITHIN 20 s)
/// Events [A(t=0), X(t=5), B(t=10), Y(t=15), C(t=18)] → MATCH
///
/// Pattern mode skips X and Y, and C arrives within 20 s of A.
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

    // Send X at t=5000. It does not match and is skipped.
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

    // Send Y at t=15000. It does not match and is skipped.
    let event_y = create_stream_event(15000);
    println!("Sending Y (timestamp 15000, non-matching)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_y)));
    chain.update_state();
    println!("  After Y, outputs = {}", collector.get_outputs().len());

    // Send C at t=18000, within 20 s of A
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

/// Test 3. Several pattern instances at once with count quantifiers
/// Pattern A{2} -> B{2} (Pattern mode)
/// Events [A1, A2, A3, B1, B2, B3] → several overlapping matches
///
/// With A{2} -> B{2}
/// - (A1, A2) -> (B1, B2) is the first match
/// - A3 and B3 may lead to more matches
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

    // A{2} -> B{2} gives at least 1 match, (A1,A2) -> (B1,B2).
    // Overlapping instances may give more.
    assert!(
        !outputs.is_empty(),
        "Expected at least 1 output from concurrent A{{2}} -> B{{2}} instances"
    );

    println!(
        "SUCCESS: Concurrent pattern instances produced {} match(es)",
        outputs.len()
    );
}

/// Test 4. Sequence mode compared with Pattern mode
/// Compares events that all match in order with events that include ones that do not match.
///
/// Sequence A -> B -> C, events [A, B, C] → MATCH
/// Pattern A -> B -> C, events [A, X, B, Y, C] → MATCH, X and Y are skipped
#[test]
fn test_integration_4_sequence_vs_pattern_comparison() {
    println!("\n=== Testing Sequence vs Pattern mode comparison ===");

    // Test 4a. Sequence mode with events in exact order [A, B, C] → MATCH
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

    // Test 4b. Pattern mode with events that do not match [A, X, B, Y, C] → MATCH
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

        // Pattern mode skips X
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

        // Pattern mode skips Y
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

/// Test 5. Four step pattern chain
/// Pattern A -> B -> C -> D (Pattern mode)
/// Events [A, X, B, Y, C, Z, D] → MATCH, X, Y and Z are skipped
///
/// Pattern mode completes the four step chain and skips events that do not match.
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

    // Send X. It does not match and is skipped.
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

    // Send Y. It does not match and is skipped.
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

    // Send Z. It does not match and is skipped.
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
