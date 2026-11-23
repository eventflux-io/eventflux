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

//! Phase 2b.4. WITHIN time constraints
//!
//! Tests for WITHIN in pattern chains, where partial matches expire after a time limit.
//!
//! Follows Phase 2b.4 of feat/pattern_processing/STATE_MACHINE_DESIGN.md. That doc
//! is no longer in the repo.

mod common;

use common::pattern_chain_test_utils::*;
use eventflux::core::query::input::stream::state::stream_pre_state_processor::StateType;

// ============================================================================
// PHASE 2b.4 TESTS. WITHIN Time Constraints
// ============================================================================
// build_pattern_chain_with_within() comes from the common module

/// Test 0. WITHIN constraint where the events arrive inside the time window.
/// Pattern A → B (WITHIN 10000 ms, which is 10 seconds)
/// Events [A(t=0), B(t=5000)] → MATCH, because B arrives within 10 s of A
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

    // Send B at t=5000, 5 seconds later and inside the 10 s window
    let event_b = create_stream_event(5000);
    println!("Sending B (timestamp 5000, +5s from A)");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b)));
    chain.update_state();

    let outputs = collector.get_outputs();
    println!("  After B, outputs = {}", outputs.len());
    assert_eq!(
        outputs.len(),
        1,
        "Expected 1 output - B arrived within 10s window"
    );

    println!("SUCCESS: Pattern matched within WITHIN constraint");
}

/// Test 1. WITHIN constraint where the events arrive outside the time window.
/// Pattern A → B (WITHIN 10000 ms, which is 10 seconds)
/// Events [A(t=0), B(t=15000)] → EXPIRED, because B arrives 15 s after A
///
/// Ignored because checking WITHIN expiry ahead of time was put off to Phase 3
/// (absent patterns). Expiry currently needs explicit expire_events() calls, or a
/// TimerWheel for automatic expiry, which does not exist yet.
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

    // Send B at t=15000, 15 seconds later and outside the 10 s window.
    // When processor[1] handles B, it should check whether the time since A
    // (at position 0) exceeds the 10 s WITHIN limit.
    // Age = 15000 - 0 = 15000 ms > 10000 ms, so the state should be rejected.
    let event_b = create_stream_event(15000);
    println!("Sending B (timestamp 15000, +15s from A - exceeds 10s WITHIN window)");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b)));
    chain.update_state();

    // No output, because B arrived too late for the WITHIN limit
    let outputs = collector.get_outputs();
    println!("  After B, outputs = {}", outputs.len());
    assert_eq!(
        outputs.len(),
        0,
        "Expected no output - B arrived outside WITHIN window"
    );

    println!("SUCCESS: Pattern rejected due to WITHIN constraint violation");
}

/// Test 2. WITHIN constraint in Pattern mode, which skips events that do not match.
/// Pattern A → B (Pattern mode, WITHIN 10000 ms, which is 10 seconds)
/// Events [A(t=0), X(t=3000), B(t=9000)] → MATCH, X is skipped and B is within 10 s of A
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
        StateType::Pattern, // Pattern mode skips events that do not match
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

    // Send X at t=3000. It does not match, so Pattern mode skips it.
    let event_x = create_stream_event(3000);
    println!("Sending X (timestamp 3000, non-matching)");
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_x)));
    chain.update_state();
    println!("  After X, outputs = {}", collector.get_outputs().len());

    // Send B at t=9000, 9 seconds after A and inside the 10 s window
    let event_b = create_stream_event(9000);
    println!("Sending B (timestamp 9000, +9s from A)");
    chain.pre_processors[1]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_b)));
    chain.update_state();

    // One match. Pattern mode skipped X and B arrived within 10 s of A.
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

/// Test 3. WITHIN constraint in Sequence mode, where a wrong event breaks the pattern at once.
/// Pattern A → B (Sequence mode, WITHIN 10000 ms, which is 10 seconds)
/// Events [A(t=0), C(t=5000)] → FAIL, C breaks the sequence right away, not by expiry
#[test]
fn test_2b4_3_within_sequence_immediate_failure() {
    println!("\n=== Testing A -> B (Sequence, WITHIN 10s), events [A(t=0), C(t=5000)] ===");

    let (mut chain, collector) = build_pattern_chain_with_within(
        vec![
            ("e1".to_string(), "A".to_string(), 1, 1),
            ("e2".to_string(), "B".to_string(), 1, 1),
        ],
        StateType::Sequence, // Sequence mode breaks on any event that does not match
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

    // Send C at t=5000. It is the wrong event, so the sequence breaks right away.
    let event_c = create_stream_event(5000);
    println!("Sending C (timestamp 5000, wrong event - breaks sequence)");
    // C goes to processor[0], not processor[1], so it cannot complete the next step
    chain.pre_processors[0]
        .lock()
        .unwrap()
        .process(Some(Box::new(event_c)));
    chain.update_state();

    // No output, because C broke the sequence. Nothing expired.
    let outputs = collector.get_outputs();
    println!("  After C, outputs = {}", outputs.len());
    assert_eq!(
        outputs.len(),
        0,
        "Expected no output - Sequence broken by C (immediate failure)"
    );

    println!("SUCCESS: Sequence mode failed immediately on wrong event (not due to expiry)");
}
