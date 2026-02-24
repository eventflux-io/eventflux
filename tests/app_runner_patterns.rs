// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;

// TODO: NOT PART OF M1 - Pattern/Sequence matching not in M1
// This test uses pattern sequence syntax ("from A -> B") which is an advanced CEP feature.
// M1 covers: Basic queries, Windows, Joins, GROUP BY, HAVING, ORDER BY, LIMIT
// Pattern matching and sequence processing will be implemented in Phase 2.
// See feat/grammar/GRAMMAR_STATUS.md for M1 feature list.
// NOTE: Tests using programmatic API (not parser) like kleene_star_pattern still work.
#[tokio::test]
#[ignore = "Requires PATTERN/SEQUENCE syntax - Not part of M1"]
async fn sequence_basic() {
    let app = "\
        define stream AStream (val int);\n\
        define stream BStream (val int);\n\
        define stream OutStream (aval int, bval int);\n\
        from AStream -> BStream select AStream.val as aval, BStream.val as bval insert into OutStream;\n";
    let runner = AppRunner::new(app, "OutStream").await;
    runner.send("AStream", vec![AttributeValue::Int(1)]);
    runner.send("BStream", vec![AttributeValue::Int(2)]);
    runner.send("AStream", vec![AttributeValue::Int(3)]);
    runner.send("BStream", vec![AttributeValue::Int(4)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![
            vec![AttributeValue::Int(1), AttributeValue::Int(2)],
            vec![AttributeValue::Int(3), AttributeValue::Int(4)],
        ]
    );
}

// TODO: NOT PART OF M1 - Pattern/Sequence matching not in M1
// This test uses pattern sequence syntax ("from every A -> B") which is an advanced CEP feature.
// M1 covers: Basic queries, Windows, Joins, GROUP BY, HAVING, ORDER BY, LIMIT
// Pattern matching and sequence processing will be implemented in Phase 2.
// See feat/grammar/GRAMMAR_STATUS.md for M1 feature list.
// NOTE: Tests using programmatic API (not parser) like kleene_star_pattern still work.
#[tokio::test]
#[ignore = "Requires PATTERN/SEQUENCE syntax - Not part of M1"]
async fn every_sequence() {
    let app = "\
        define stream A (val int);\n\
        define stream B (val int);\n\
        define stream Out (aval int, bval int);\n\
        from every A -> B select A.val as aval, B.val as bval insert into Out;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("A", vec![AttributeValue::Int(1)]);
    runner.send("B", vec![AttributeValue::Int(2)]);
    runner.send("B", vec![AttributeValue::Int(3)]);
    runner.send("A", vec![AttributeValue::Int(4)]);
    runner.send("B", vec![AttributeValue::Int(5)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![
            vec![AttributeValue::Int(1), AttributeValue::Int(2)],
            vec![AttributeValue::Int(1), AttributeValue::Int(3)],
            vec![AttributeValue::Int(4), AttributeValue::Int(5)],
        ]
    );
}

use eventflux::query_api::definition::{attribute::Type as AttrType, StreamDefinition};
use eventflux::query_api::execution::query::input::state::State;
use eventflux::query_api::execution::query::input::stream::input_stream::InputStream;
use eventflux::query_api::execution::query::input::stream::single_input_stream::SingleInputStream;
use eventflux::query_api::execution::query::input::stream::state_input_stream::StateInputStream;
use eventflux::query_api::execution::query::output::output_stream::{
    InsertIntoStreamAction, OutputStream, OutputStreamAction,
};
use eventflux::query_api::execution::query::selection::{OutputAttribute, Selector};
use eventflux::query_api::execution::query::Query;
use eventflux::query_api::execution::ExecutionElement;
use eventflux::query_api::expression::{constant::TimeUtil, variable::Variable, Expression};
use std::sync::Arc;

/// Test that zero-count patterns (A*, A?, A{0,n}) are rejected with a clear error.
/// These patterns are explicitly not supported because they can match zero events,
/// which creates ambiguity in pattern semantics.
#[tokio::test]
#[should_panic(expected = "Zero-count patterns (A*, A?, A{0,n}) are not supported")]
async fn kleene_star_pattern_rejected() {
    let mut app = eventflux::query_api::eventflux_app::EventFluxApp::new("Kleene".to_string());
    let a_def = StreamDefinition::new("A".to_string()).attribute("val".to_string(), AttrType::INT);
    let b_def = StreamDefinition::new("B".to_string()).attribute("val".to_string(), AttrType::INT);
    let out_def = StreamDefinition::new("Out".to_string())
        .attribute("aval".to_string(), AttrType::INT)
        .attribute("bval".to_string(), AttrType::INT);
    app.stream_definition_map
        .insert("A".to_string(), Arc::new(a_def));
    app.stream_definition_map
        .insert("B".to_string(), Arc::new(b_def));
    app.stream_definition_map
        .insert("Out".to_string(), Arc::new(out_def));

    let a_si = SingleInputStream::new_basic("A".to_string(), false, false, None, Vec::new());
    let b_si = SingleInputStream::new_basic("B".to_string(), false, false, None, Vec::new());
    // State::zero_or_many creates A* pattern (min_count=0)
    let pattern = State::next(
        State::zero_or_many(State::stream(a_si)),
        State::stream_element(b_si),
    );
    let state_stream = StateInputStream::sequence_stream(pattern, None);
    let input = InputStream::State(Box::new(state_stream));

    let mut selector = Selector::new();
    selector.selection_list = vec![
        OutputAttribute::new(
            Some("aval".to_string()),
            Expression::Variable(Variable::new("val".to_string()).of_stream("A".to_string())),
        ),
        OutputAttribute::new(
            Some("bval".to_string()),
            Expression::Variable(Variable::new("val".to_string()).of_stream("B".to_string())),
        ),
    ];
    let insert_action = InsertIntoStreamAction {
        target_id: "Out".to_string(),
        is_inner_stream: false,
        is_fault_stream: false,
    };
    let out_stream = OutputStream::new(OutputStreamAction::InsertInto(insert_action), None);
    let query = Query::query()
        .from(input)
        .select(selector)
        .out_stream(out_stream);
    app.execution_element_list
        .push(ExecutionElement::Query(query));

    // This should panic with the zero-count pattern error
    let _runner = AppRunner::new_from_api(app, "Out").await;
}

#[tokio::test]
async fn sequence_with_timeout() {
    let mut app =
        eventflux::query_api::eventflux_app::EventFluxApp::new("Timeout".to_string());
    let a_def = StreamDefinition::new("A".to_string()).attribute("val".to_string(), AttrType::INT);
    let b_def = StreamDefinition::new("B".to_string()).attribute("val".to_string(), AttrType::INT);
    let out_def = StreamDefinition::new("Out".to_string())
        .attribute("aval".to_string(), AttrType::INT)
        .attribute("bval".to_string(), AttrType::INT);
    app.stream_definition_map
        .insert("A".to_string(), Arc::new(a_def));
    app.stream_definition_map
        .insert("B".to_string(), Arc::new(b_def));
    app.stream_definition_map
        .insert("Out".to_string(), Arc::new(out_def));

    let a_si = SingleInputStream::new_basic("A".to_string(), false, false, None, Vec::new());
    let b_si = SingleInputStream::new_basic("B".to_string(), false, false, None, Vec::new());
    let pattern = State::next(State::stream_element(a_si), State::stream_element(b_si));
    let state_stream = StateInputStream::sequence_stream(pattern, Some(TimeUtil::sec(1)));
    let input = InputStream::State(Box::new(state_stream));

    let mut selector = Selector::new();
    selector.selection_list = vec![
        OutputAttribute::new(
            Some("aval".to_string()),
            Expression::Variable(Variable::new("val".to_string()).of_stream("A".to_string())),
        ),
        OutputAttribute::new(
            Some("bval".to_string()),
            Expression::Variable(Variable::new("val".to_string()).of_stream("B".to_string())),
        ),
    ];
    let insert_action = InsertIntoStreamAction {
        target_id: "Out".to_string(),
        is_inner_stream: false,
        is_fault_stream: false,
    };
    let out_stream = OutputStream::new(OutputStreamAction::InsertInto(insert_action), None);
    let query = Query::query()
        .from(input)
        .select(selector)
        .out_stream(out_stream);
    app.execution_element_list
        .push(ExecutionElement::Query(query));

    let runner = AppRunner::new_from_api(app, "Out").await;
    runner.send_with_ts("A", 0, vec![AttributeValue::Int(1)]);
    runner.send_with_ts("B", 500, vec![AttributeValue::Int(2)]);
    runner.send_with_ts("A", 2000, vec![AttributeValue::Int(3)]);
    runner.send_with_ts("B", 3500, vec![AttributeValue::Int(4)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![AttributeValue::Int(1), AttributeValue::Int(2)]]
    );
}

#[tokio::test]
async fn sequence_api() {
    use eventflux::query_api::definition::{attribute::Type as AttrType, StreamDefinition};
    use eventflux::query_api::execution::query::input::state::State;
    use eventflux::query_api::execution::query::input::stream::input_stream::InputStream;
    use eventflux::query_api::execution::query::input::stream::single_input_stream::SingleInputStream;
    use eventflux::query_api::execution::query::input::stream::state_input_stream::StateInputStream;
    use eventflux::query_api::execution::query::output::output_stream::{
        InsertIntoStreamAction, OutputStream, OutputStreamAction,
    };
    use eventflux::query_api::execution::query::selection::{OutputAttribute, Selector};
    use eventflux::query_api::execution::query::Query;
    use eventflux::query_api::execution::ExecutionElement;
    let mut app = eventflux::query_api::eventflux_app::EventFluxApp::new("SeqAPI".to_string());
    let a_def = StreamDefinition::new("A".to_string()).attribute("val".to_string(), AttrType::INT);
    let b_def = StreamDefinition::new("B".to_string()).attribute("val".to_string(), AttrType::INT);
    let out_def = StreamDefinition::new("Out".to_string())
        .attribute("aval".to_string(), AttrType::INT)
        .attribute("bval".to_string(), AttrType::INT);
    app.stream_definition_map
        .insert("A".to_string(), Arc::new(a_def));
    app.stream_definition_map
        .insert("B".to_string(), Arc::new(b_def));
    app.stream_definition_map
        .insert("Out".to_string(), Arc::new(out_def));

    let a_si = SingleInputStream::new_basic("A".to_string(), false, false, None, Vec::new());
    let b_si = SingleInputStream::new_basic("B".to_string(), false, false, None, Vec::new());
    let pattern = State::next(State::stream_element(a_si), State::stream_element(b_si));
    let state_stream = StateInputStream::sequence_stream(pattern, None);
    let input = InputStream::State(Box::new(state_stream));

    let mut selector = Selector::new();
    selector.selection_list = vec![
        OutputAttribute::new(
            Some("aval".to_string()),
            Expression::Variable(Variable::new("val".to_string()).of_stream("A".to_string())),
        ),
        OutputAttribute::new(
            Some("bval".to_string()),
            Expression::Variable(Variable::new("val".to_string()).of_stream("B".to_string())),
        ),
    ];
    let insert_action = InsertIntoStreamAction {
        target_id: "Out".to_string(),
        is_inner_stream: false,
        is_fault_stream: false,
    };
    let out_stream = OutputStream::new(OutputStreamAction::InsertInto(insert_action), None);
    let query = Query::query()
        .from(input)
        .select(selector)
        .out_stream(out_stream);
    app.execution_element_list
        .push(ExecutionElement::Query(query));

    let runner = AppRunner::new_from_api(app, "Out").await;
    runner.send("A", vec![AttributeValue::Int(1)]);
    runner.send("B", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![AttributeValue::Int(1), AttributeValue::Int(2)]]
    );
}

/// Test pattern alias resolution for two different streams
/// Pattern: e1=StreamA -> e2=StreamB
/// SELECT e1.val, e2.val
#[tokio::test]
async fn pattern_alias_two_streams() {
    use eventflux::query_api::definition::{attribute::Type as AttrType, StreamDefinition};
    use eventflux::query_api::execution::query::input::state::State;
    use eventflux::query_api::execution::query::input::stream::input_stream::InputStream;
    use eventflux::query_api::execution::query::input::stream::single_input_stream::SingleInputStream;
    use eventflux::query_api::execution::query::input::stream::state_input_stream::StateInputStream;
    use eventflux::query_api::execution::query::output::output_stream::{
        InsertIntoStreamAction, OutputStream, OutputStreamAction,
    };
    use eventflux::query_api::execution::query::selection::{OutputAttribute, Selector};
    use eventflux::query_api::execution::query::Query;
    use eventflux::query_api::execution::ExecutionElement;

    let mut app =
        eventflux::query_api::eventflux_app::EventFluxApp::new("AliasTest".to_string());
    let a_def = StreamDefinition::new("StreamA".to_string())
        .attribute("price".to_string(), AttrType::DOUBLE)
        .attribute("symbol".to_string(), AttrType::STRING);
    let b_def = StreamDefinition::new("StreamB".to_string())
        .attribute("price".to_string(), AttrType::DOUBLE)
        .attribute("symbol".to_string(), AttrType::STRING);
    let out_def = StreamDefinition::new("Out".to_string())
        .attribute("first_price".to_string(), AttrType::DOUBLE)
        .attribute("second_price".to_string(), AttrType::DOUBLE)
        .attribute("change".to_string(), AttrType::DOUBLE);
    app.stream_definition_map
        .insert("StreamA".to_string(), Arc::new(a_def));
    app.stream_definition_map
        .insert("StreamB".to_string(), Arc::new(b_def));
    app.stream_definition_map
        .insert("Out".to_string(), Arc::new(out_def));

    // Create pattern elements with aliases: e1=StreamA -> e2=StreamB
    let a_si = SingleInputStream::new_basic("StreamA".to_string(), false, false, None, Vec::new())
        .as_ref("e1".to_string());
    let b_si = SingleInputStream::new_basic("StreamB".to_string(), false, false, None, Vec::new())
        .as_ref("e2".to_string());
    let pattern = State::next(State::stream_element(a_si), State::stream_element(b_si));
    let state_stream = StateInputStream::sequence_stream(pattern, None);
    let input = InputStream::State(Box::new(state_stream));

    // SELECT using aliases e1.price, e2.price, e2.price - e1.price
    let mut selector = Selector::new();
    selector.selection_list = vec![
        OutputAttribute::new(
            Some("first_price".to_string()),
            Expression::Variable(Variable::new("price".to_string()).of_stream("e1".to_string())),
        ),
        OutputAttribute::new(
            Some("second_price".to_string()),
            Expression::Variable(Variable::new("price".to_string()).of_stream("e2".to_string())),
        ),
        OutputAttribute::new(
            Some("change".to_string()),
            Expression::Subtract(Box::new(
                eventflux::query_api::expression::math::Subtract::new(
                    Expression::Variable(
                        Variable::new("price".to_string()).of_stream("e2".to_string()),
                    ),
                    Expression::Variable(
                        Variable::new("price".to_string()).of_stream("e1".to_string()),
                    ),
                ),
            )),
        ),
    ];
    let insert_action = InsertIntoStreamAction {
        target_id: "Out".to_string(),
        is_inner_stream: false,
        is_fault_stream: false,
    };
    let out_stream = OutputStream::new(OutputStreamAction::InsertInto(insert_action), None);
    let query = Query::query()
        .from(input)
        .select(selector)
        .out_stream(out_stream);
    app.execution_element_list
        .push(ExecutionElement::Query(query));

    let runner = AppRunner::new_from_api(app, "Out").await;
    runner.send(
        "StreamA",
        vec![
            AttributeValue::Double(100.0),
            AttributeValue::String("AAPL".to_string()),
        ],
    );
    runner.send(
        "StreamB",
        vec![
            AttributeValue::Double(110.0),
            AttributeValue::String("AAPL".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![
            AttributeValue::Double(100.0),
            AttributeValue::Double(110.0),
            AttributeValue::Double(10.0), // change = 110 - 100
        ]]
    );
}

/// Test pattern alias resolution for same stream (self-join pattern)
/// Pattern: e1=RawTrades -> e2=RawTrades
/// SELECT e1.price, e2.price
#[tokio::test]
async fn pattern_alias_same_stream() {
    use eventflux::query_api::definition::{attribute::Type as AttrType, StreamDefinition};
    use eventflux::query_api::execution::query::input::state::State;
    use eventflux::query_api::execution::query::input::stream::input_stream::InputStream;
    use eventflux::query_api::execution::query::input::stream::single_input_stream::SingleInputStream;
    use eventflux::query_api::execution::query::input::stream::state_input_stream::StateInputStream;
    use eventflux::query_api::execution::query::output::output_stream::{
        InsertIntoStreamAction, OutputStream, OutputStreamAction,
    };
    use eventflux::query_api::execution::query::selection::{OutputAttribute, Selector};
    use eventflux::query_api::execution::query::Query;
    use eventflux::query_api::execution::ExecutionElement;

    let mut app =
        eventflux::query_api::eventflux_app::EventFluxApp::new("SameStreamAlias".to_string());
    let trades_def = StreamDefinition::new("RawTrades".to_string())
        .attribute("price".to_string(), AttrType::DOUBLE)
        .attribute("symbol".to_string(), AttrType::STRING);
    let out_def = StreamDefinition::new("Out".to_string())
        .attribute("first_price".to_string(), AttrType::DOUBLE)
        .attribute("second_price".to_string(), AttrType::DOUBLE);
    app.stream_definition_map
        .insert("RawTrades".to_string(), Arc::new(trades_def));
    app.stream_definition_map
        .insert("Out".to_string(), Arc::new(out_def));

    // Create pattern: e1=RawTrades -> e2=RawTrades (same stream, different aliases)
    let si1 = SingleInputStream::new_basic("RawTrades".to_string(), false, false, None, Vec::new())
        .as_ref("e1".to_string());
    let si2 = SingleInputStream::new_basic("RawTrades".to_string(), false, false, None, Vec::new())
        .as_ref("e2".to_string());
    let pattern = State::next(State::stream_element(si1), State::stream_element(si2));
    let state_stream = StateInputStream::sequence_stream(pattern, None);
    let input = InputStream::State(Box::new(state_stream));

    // SELECT using aliases
    let mut selector = Selector::new();
    selector.selection_list = vec![
        OutputAttribute::new(
            Some("first_price".to_string()),
            Expression::Variable(Variable::new("price".to_string()).of_stream("e1".to_string())),
        ),
        OutputAttribute::new(
            Some("second_price".to_string()),
            Expression::Variable(Variable::new("price".to_string()).of_stream("e2".to_string())),
        ),
    ];
    let insert_action = InsertIntoStreamAction {
        target_id: "Out".to_string(),
        is_inner_stream: false,
        is_fault_stream: false,
    };
    let out_stream = OutputStream::new(OutputStreamAction::InsertInto(insert_action), None);
    let query = Query::query()
        .from(input)
        .select(selector)
        .out_stream(out_stream);
    app.execution_element_list
        .push(ExecutionElement::Query(query));

    let runner = AppRunner::new_from_api(app, "Out").await;
    // Send first trade
    runner.send(
        "RawTrades",
        vec![
            AttributeValue::Double(100.0),
            AttributeValue::String("AAPL".to_string()),
        ],
    );
    // Send second trade - this should complete the pattern
    runner.send(
        "RawTrades",
        vec![
            AttributeValue::Double(110.0),
            AttributeValue::String("AAPL".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![
            AttributeValue::Double(100.0),
            AttributeValue::Double(110.0),
        ]]
    );
}

/// Test N-element patterns (3+ elements like A -> B -> C)
/// Pattern: e1=A -> e2=B -> e3=C
/// SELECT e1.val, e2.val, e3.val
///
/// NOTE: N-element pattern support foundation is in place (TerminalPostStateProcessor,
/// PatternChainBuilder integration, PreStateProcessorAdapters). Full pattern matching
/// requires additional PatternChainBuilder debugging - the chain is wired but pattern
/// completion logic needs work. The 2-element patterns work correctly.
#[tokio::test]
async fn n_element_pattern_three_streams() {
    use eventflux::query_api::definition::{attribute::Type as AttrType, StreamDefinition};
    use eventflux::query_api::execution::query::input::state::State;
    use eventflux::query_api::execution::query::input::stream::input_stream::InputStream;
    use eventflux::query_api::execution::query::input::stream::single_input_stream::SingleInputStream;
    use eventflux::query_api::execution::query::input::stream::state_input_stream::StateInputStream;
    use eventflux::query_api::execution::query::output::output_stream::{
        InsertIntoStreamAction, OutputStream, OutputStreamAction,
    };
    use eventflux::query_api::execution::query::selection::{OutputAttribute, Selector};
    use eventflux::query_api::execution::query::Query;
    use eventflux::query_api::execution::ExecutionElement;

    let mut app =
        eventflux::query_api::eventflux_app::EventFluxApp::new("NElementTest".to_string());
    let a_def = StreamDefinition::new("A".to_string()).attribute("val".to_string(), AttrType::INT);
    let b_def = StreamDefinition::new("B".to_string()).attribute("val".to_string(), AttrType::INT);
    let c_def = StreamDefinition::new("C".to_string()).attribute("val".to_string(), AttrType::INT);
    let out_def = StreamDefinition::new("Out".to_string())
        .attribute("aval".to_string(), AttrType::INT)
        .attribute("bval".to_string(), AttrType::INT)
        .attribute("cval".to_string(), AttrType::INT);
    app.stream_definition_map
        .insert("A".to_string(), Arc::new(a_def));
    app.stream_definition_map
        .insert("B".to_string(), Arc::new(b_def));
    app.stream_definition_map
        .insert("C".to_string(), Arc::new(c_def));
    app.stream_definition_map
        .insert("Out".to_string(), Arc::new(out_def));

    // Create 3-element pattern: A -> B -> C (N-element pattern)
    let a_si = SingleInputStream::new_basic("A".to_string(), false, false, None, Vec::new())
        .as_ref("e1".to_string());
    let b_si = SingleInputStream::new_basic("B".to_string(), false, false, None, Vec::new())
        .as_ref("e2".to_string());
    let c_si = SingleInputStream::new_basic("C".to_string(), false, false, None, Vec::new())
        .as_ref("e3".to_string());

    // Build nested Next structure: A -> (B -> C)
    let bc_pattern = State::next(State::stream_element(b_si), State::stream_element(c_si));
    let abc_pattern = State::next(State::stream_element(a_si), bc_pattern);
    let state_stream = StateInputStream::sequence_stream(abc_pattern, None);
    let input = InputStream::State(Box::new(state_stream));

    let mut selector = Selector::new();
    selector.selection_list = vec![
        OutputAttribute::new(
            Some("aval".to_string()),
            Expression::Variable(Variable::new("val".to_string()).of_stream("e1".to_string())),
        ),
        OutputAttribute::new(
            Some("bval".to_string()),
            Expression::Variable(Variable::new("val".to_string()).of_stream("e2".to_string())),
        ),
        OutputAttribute::new(
            Some("cval".to_string()),
            Expression::Variable(Variable::new("val".to_string()).of_stream("e3".to_string())),
        ),
    ];
    let insert_action = InsertIntoStreamAction {
        target_id: "Out".to_string(),
        is_inner_stream: false,
        is_fault_stream: false,
    };
    let out_stream = OutputStream::new(OutputStreamAction::InsertInto(insert_action), None);
    let query = Query::query()
        .from(input)
        .select(selector)
        .out_stream(out_stream);
    app.execution_element_list
        .push(ExecutionElement::Query(query));

    // Use AppRunner to test the N-element pattern
    let runner = AppRunner::new_from_api(app, "Out").await;

    // Send events in sequence: A, B, C
    runner.send("A", vec![AttributeValue::Int(1)]);
    runner.send("B", vec![AttributeValue::Int(2)]);
    runner.send("C", vec![AttributeValue::Int(3)]);

    let out = runner.shutdown();

    // The pattern A -> B -> C should produce output [1, 2, 3]
    assert_eq!(
        out,
        vec![vec![
            AttributeValue::Int(1),
            AttributeValue::Int(2),
            AttributeValue::Int(3),
        ]]
    );
}

/// Test 4-element N-element pattern: A -> B -> C -> D
#[tokio::test]
async fn n_element_pattern_four_streams() {
    use eventflux::query_api::definition::{attribute::Type as AttrType, StreamDefinition};
    use eventflux::query_api::execution::query::input::state::State;
    use eventflux::query_api::execution::query::input::stream::input_stream::InputStream;
    use eventflux::query_api::execution::query::input::stream::single_input_stream::SingleInputStream;
    use eventflux::query_api::execution::query::input::stream::state_input_stream::StateInputStream;
    use eventflux::query_api::execution::query::output::output_stream::{
        InsertIntoStreamAction, OutputStream, OutputStreamAction,
    };
    use eventflux::query_api::execution::query::selection::{OutputAttribute, Selector};
    use eventflux::query_api::execution::query::Query;
    use eventflux::query_api::execution::ExecutionElement;

    let mut app =
        eventflux::query_api::eventflux_app::EventFluxApp::new("FourElementTest".to_string());
    let a_def = StreamDefinition::new("A".to_string()).attribute("val".to_string(), AttrType::INT);
    let b_def = StreamDefinition::new("B".to_string()).attribute("val".to_string(), AttrType::INT);
    let c_def = StreamDefinition::new("C".to_string()).attribute("val".to_string(), AttrType::INT);
    let d_def = StreamDefinition::new("D".to_string()).attribute("val".to_string(), AttrType::INT);
    let out_def = StreamDefinition::new("Out".to_string())
        .attribute("aval".to_string(), AttrType::INT)
        .attribute("bval".to_string(), AttrType::INT)
        .attribute("cval".to_string(), AttrType::INT)
        .attribute("dval".to_string(), AttrType::INT);
    app.stream_definition_map
        .insert("A".to_string(), Arc::new(a_def));
    app.stream_definition_map
        .insert("B".to_string(), Arc::new(b_def));
    app.stream_definition_map
        .insert("C".to_string(), Arc::new(c_def));
    app.stream_definition_map
        .insert("D".to_string(), Arc::new(d_def));
    app.stream_definition_map
        .insert("Out".to_string(), Arc::new(out_def));

    // Create 4-element pattern: A -> B -> C -> D
    let a_si = SingleInputStream::new_basic("A".to_string(), false, false, None, Vec::new())
        .as_ref("e1".to_string());
    let b_si = SingleInputStream::new_basic("B".to_string(), false, false, None, Vec::new())
        .as_ref("e2".to_string());
    let c_si = SingleInputStream::new_basic("C".to_string(), false, false, None, Vec::new())
        .as_ref("e3".to_string());
    let d_si = SingleInputStream::new_basic("D".to_string(), false, false, None, Vec::new())
        .as_ref("e4".to_string());

    // Build nested Next structure: A -> (B -> (C -> D))
    let cd_pattern = State::next(State::stream_element(c_si), State::stream_element(d_si));
    let bcd_pattern = State::next(State::stream_element(b_si), cd_pattern);
    let abcd_pattern = State::next(State::stream_element(a_si), bcd_pattern);
    let state_stream = StateInputStream::sequence_stream(abcd_pattern, None);
    let input = InputStream::State(Box::new(state_stream));

    let mut selector = Selector::new();
    selector.selection_list = vec![
        OutputAttribute::new(
            Some("aval".to_string()),
            Expression::Variable(Variable::new("val".to_string()).of_stream("e1".to_string())),
        ),
        OutputAttribute::new(
            Some("bval".to_string()),
            Expression::Variable(Variable::new("val".to_string()).of_stream("e2".to_string())),
        ),
        OutputAttribute::new(
            Some("cval".to_string()),
            Expression::Variable(Variable::new("val".to_string()).of_stream("e3".to_string())),
        ),
        OutputAttribute::new(
            Some("dval".to_string()),
            Expression::Variable(Variable::new("val".to_string()).of_stream("e4".to_string())),
        ),
    ];
    let insert_action = InsertIntoStreamAction {
        target_id: "Out".to_string(),
        is_inner_stream: false,
        is_fault_stream: false,
    };
    let out_stream = OutputStream::new(OutputStreamAction::InsertInto(insert_action), None);
    let query = Query::query()
        .from(input)
        .select(selector)
        .out_stream(out_stream);
    app.execution_element_list
        .push(ExecutionElement::Query(query));

    let runner = AppRunner::new_from_api(app, "Out").await;

    // Send events in sequence: A, B, C, D
    runner.send("A", vec![AttributeValue::Int(10)]);
    runner.send("B", vec![AttributeValue::Int(20)]);
    runner.send("C", vec![AttributeValue::Int(30)]);
    runner.send("D", vec![AttributeValue::Int(40)]);

    let out = runner.shutdown();

    assert_eq!(
        out,
        vec![vec![
            AttributeValue::Int(10),
            AttributeValue::Int(20),
            AttributeValue::Int(30),
            AttributeValue::Int(40),
        ]]
    );
}

/// Test 5-element N-element pattern: A -> B -> C -> D -> E
#[tokio::test]
async fn n_element_pattern_five_streams() {
    use eventflux::query_api::definition::{attribute::Type as AttrType, StreamDefinition};
    use eventflux::query_api::execution::query::input::state::State;
    use eventflux::query_api::execution::query::input::stream::input_stream::InputStream;
    use eventflux::query_api::execution::query::input::stream::single_input_stream::SingleInputStream;
    use eventflux::query_api::execution::query::input::stream::state_input_stream::StateInputStream;
    use eventflux::query_api::execution::query::output::output_stream::{
        InsertIntoStreamAction, OutputStream, OutputStreamAction,
    };
    use eventflux::query_api::execution::query::selection::{OutputAttribute, Selector};
    use eventflux::query_api::execution::query::Query;
    use eventflux::query_api::execution::ExecutionElement;

    let mut app =
        eventflux::query_api::eventflux_app::EventFluxApp::new("FiveElementTest".to_string());
    let a_def = StreamDefinition::new("A".to_string()).attribute("val".to_string(), AttrType::INT);
    let b_def = StreamDefinition::new("B".to_string()).attribute("val".to_string(), AttrType::INT);
    let c_def = StreamDefinition::new("C".to_string()).attribute("val".to_string(), AttrType::INT);
    let d_def = StreamDefinition::new("D".to_string()).attribute("val".to_string(), AttrType::INT);
    let e_def = StreamDefinition::new("E".to_string()).attribute("val".to_string(), AttrType::INT);
    let out_def = StreamDefinition::new("Out".to_string())
        .attribute("aval".to_string(), AttrType::INT)
        .attribute("bval".to_string(), AttrType::INT)
        .attribute("cval".to_string(), AttrType::INT)
        .attribute("dval".to_string(), AttrType::INT)
        .attribute("eval".to_string(), AttrType::INT);
    app.stream_definition_map
        .insert("A".to_string(), Arc::new(a_def));
    app.stream_definition_map
        .insert("B".to_string(), Arc::new(b_def));
    app.stream_definition_map
        .insert("C".to_string(), Arc::new(c_def));
    app.stream_definition_map
        .insert("D".to_string(), Arc::new(d_def));
    app.stream_definition_map
        .insert("E".to_string(), Arc::new(e_def));
    app.stream_definition_map
        .insert("Out".to_string(), Arc::new(out_def));

    // Create 5-element pattern: A -> B -> C -> D -> E
    let a_si = SingleInputStream::new_basic("A".to_string(), false, false, None, Vec::new())
        .as_ref("e1".to_string());
    let b_si = SingleInputStream::new_basic("B".to_string(), false, false, None, Vec::new())
        .as_ref("e2".to_string());
    let c_si = SingleInputStream::new_basic("C".to_string(), false, false, None, Vec::new())
        .as_ref("e3".to_string());
    let d_si = SingleInputStream::new_basic("D".to_string(), false, false, None, Vec::new())
        .as_ref("e4".to_string());
    let e_si = SingleInputStream::new_basic("E".to_string(), false, false, None, Vec::new())
        .as_ref("e5".to_string());

    // Build nested Next structure: A -> (B -> (C -> (D -> E)))
    let de_pattern = State::next(State::stream_element(d_si), State::stream_element(e_si));
    let cde_pattern = State::next(State::stream_element(c_si), de_pattern);
    let bcde_pattern = State::next(State::stream_element(b_si), cde_pattern);
    let abcde_pattern = State::next(State::stream_element(a_si), bcde_pattern);
    let state_stream = StateInputStream::sequence_stream(abcde_pattern, None);
    let input = InputStream::State(Box::new(state_stream));

    let mut selector = Selector::new();
    selector.selection_list = vec![
        OutputAttribute::new(
            Some("aval".to_string()),
            Expression::Variable(Variable::new("val".to_string()).of_stream("e1".to_string())),
        ),
        OutputAttribute::new(
            Some("bval".to_string()),
            Expression::Variable(Variable::new("val".to_string()).of_stream("e2".to_string())),
        ),
        OutputAttribute::new(
            Some("cval".to_string()),
            Expression::Variable(Variable::new("val".to_string()).of_stream("e3".to_string())),
        ),
        OutputAttribute::new(
            Some("dval".to_string()),
            Expression::Variable(Variable::new("val".to_string()).of_stream("e4".to_string())),
        ),
        OutputAttribute::new(
            Some("eval".to_string()),
            Expression::Variable(Variable::new("val".to_string()).of_stream("e5".to_string())),
        ),
    ];
    let insert_action = InsertIntoStreamAction {
        target_id: "Out".to_string(),
        is_inner_stream: false,
        is_fault_stream: false,
    };
    let out_stream = OutputStream::new(OutputStreamAction::InsertInto(insert_action), None);
    let query = Query::query()
        .from(input)
        .select(selector)
        .out_stream(out_stream);
    app.execution_element_list
        .push(ExecutionElement::Query(query));

    let runner = AppRunner::new_from_api(app, "Out").await;

    // Send events in sequence: A, B, C, D, E
    runner.send("A", vec![AttributeValue::Int(100)]);
    runner.send("B", vec![AttributeValue::Int(200)]);
    runner.send("C", vec![AttributeValue::Int(300)]);
    runner.send("D", vec![AttributeValue::Int(400)]);
    runner.send("E", vec![AttributeValue::Int(500)]);

    let out = runner.shutdown();

    assert_eq!(
        out,
        vec![vec![
            AttributeValue::Int(100),
            AttributeValue::Int(200),
            AttributeValue::Int(300),
            AttributeValue::Int(400),
            AttributeValue::Int(500),
        ]]
    );
}

// ============================================================================
// SQL-Based Pattern Tests (using EventFlux SQL syntax via FROM PATTERN)
// ============================================================================

/// Test 2-element pattern with SQL syntax: e1=A -> e2=B
/// Uses EventFlux SQL parser, not programmatic API
///
/// NOTE: Currently uses stream names (A.val, B.val) in SELECT due to
/// pattern alias resolution not yet implemented in SQL type inference.
/// Pattern aliases (e1, e2) are set on SingleInputStream but not registered
/// with the SQL catalog for column resolution.
#[tokio::test]
async fn pattern_two_streams_sql() {
    let app = "\
        CREATE STREAM A (val INT);\n\
        CREATE STREAM B (val INT);\n\
        CREATE STREAM Out (aval INT, bval INT);\n\
        INSERT INTO Out\n\
        SELECT A.val AS aval, B.val AS bval\n\
        FROM PATTERN (e1=A -> e2=B);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("A", vec![AttributeValue::Int(10)]);
    runner.send("B", vec![AttributeValue::Int(20)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![AttributeValue::Int(10), AttributeValue::Int(20)]]
    );
}

/// Test 3-element N-element pattern with SQL syntax: e1=A -> e2=B -> e3=C
/// This tests that the SQL parser correctly handles N-element patterns
#[tokio::test]
async fn pattern_three_streams_sql() {
    let app = "\
        CREATE STREAM A (val INT);\n\
        CREATE STREAM B (val INT);\n\
        CREATE STREAM C (val INT);\n\
        CREATE STREAM Out (aval INT, bval INT, cval INT);\n\
        INSERT INTO Out\n\
        SELECT A.val AS aval, B.val AS bval, C.val AS cval\n\
        FROM PATTERN (e1=A -> e2=B -> e3=C);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("A", vec![AttributeValue::Int(1)]);
    runner.send("B", vec![AttributeValue::Int(2)]);
    runner.send("C", vec![AttributeValue::Int(3)]);
    let out = runner.shutdown();
    // Pattern completes when all three events arrive in sequence
    assert_eq!(
        out,
        vec![vec![
            AttributeValue::Int(1),
            AttributeValue::Int(2),
            AttributeValue::Int(3),
        ]]
    );
}

/// Test 4-element N-element pattern with SQL syntax: e1=A -> e2=B -> e3=C -> e4=D
#[tokio::test]
async fn pattern_four_streams_sql() {
    let app = "\
        CREATE STREAM A (val INT);\n\
        CREATE STREAM B (val INT);\n\
        CREATE STREAM C (val INT);\n\
        CREATE STREAM D (val INT);\n\
        CREATE STREAM Out (aval INT, bval INT, cval INT, dval INT);\n\
        INSERT INTO Out\n\
        SELECT A.val AS aval, B.val AS bval, C.val AS cval, D.val AS dval\n\
        FROM PATTERN (e1=A -> e2=B -> e3=C -> e4=D);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("A", vec![AttributeValue::Int(10)]);
    runner.send("B", vec![AttributeValue::Int(20)]);
    runner.send("C", vec![AttributeValue::Int(30)]);
    runner.send("D", vec![AttributeValue::Int(40)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![
            AttributeValue::Int(10),
            AttributeValue::Int(20),
            AttributeValue::Int(30),
            AttributeValue::Int(40),
        ]]
    );
}

/// Test 5-element N-element pattern with SQL syntax: e1=A -> e2=B -> e3=C -> e4=D -> e5=E
#[tokio::test]
async fn pattern_five_streams_sql() {
    let app = "\
        CREATE STREAM A (val INT);\n\
        CREATE STREAM B (val INT);\n\
        CREATE STREAM C (val INT);\n\
        CREATE STREAM D (val INT);\n\
        CREATE STREAM E (val INT);\n\
        CREATE STREAM Out (aval INT, bval INT, cval INT, dval INT, eval INT);\n\
        INSERT INTO Out\n\
        SELECT A.val AS aval, B.val AS bval, C.val AS cval, D.val AS dval, E.val AS eval\n\
        FROM PATTERN (e1=A -> e2=B -> e3=C -> e4=D -> e5=E);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("A", vec![AttributeValue::Int(100)]);
    runner.send("B", vec![AttributeValue::Int(200)]);
    runner.send("C", vec![AttributeValue::Int(300)]);
    runner.send("D", vec![AttributeValue::Int(400)]);
    runner.send("E", vec![AttributeValue::Int(500)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![
            AttributeValue::Int(100),
            AttributeValue::Int(200),
            AttributeValue::Int(300),
            AttributeValue::Int(400),
            AttributeValue::Int(500),
        ]]
    );
}

/// Test pattern with same stream appearing multiple times (self-join pattern)
/// Pattern: e1=Trades -> e2=Trades (two consecutive events from same stream)
/// Uses pattern aliases (e1, e2) to disambiguate and AS to avoid duplicate output columns
#[tokio::test]
async fn pattern_same_stream_sql() {
    let app = "\
        CREATE STREAM Trades (price DOUBLE, symbol STRING);\n\
        CREATE STREAM Out (first_price DOUBLE, second_price DOUBLE);\n\
        INSERT INTO Out\n\
        SELECT e1.price AS first_price, e2.price AS second_price\n\
        FROM PATTERN (e1=Trades -> e2=Trades);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "Trades",
        vec![
            AttributeValue::Double(100.0),
            AttributeValue::String("AAPL".to_string()),
        ],
    );
    runner.send(
        "Trades",
        vec![
            AttributeValue::Double(110.0),
            AttributeValue::String("AAPL".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![
            AttributeValue::Double(100.0),
            AttributeValue::Double(110.0),
        ]]
    );
}

/// Test 3-element pattern with same stream (triple self-join)
/// Pattern: e1=Trades -> e2=Trades -> e3=Trades
/// Uses pattern aliases to disambiguate and AS to create unique output columns
#[tokio::test]
async fn pattern_same_stream_three_elements_sql() {
    let app = "\
        CREATE STREAM Trades (price DOUBLE);\n\
        CREATE STREAM Out (p1 DOUBLE, p2 DOUBLE, p3 DOUBLE);\n\
        INSERT INTO Out\n\
        SELECT e1.price AS p1, e2.price AS p2, e3.price AS p3\n\
        FROM PATTERN (e1=Trades -> e2=Trades -> e3=Trades);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("Trades", vec![AttributeValue::Double(100.0)]);
    runner.send("Trades", vec![AttributeValue::Double(110.0)]);
    runner.send("Trades", vec![AttributeValue::Double(120.0)]);
    let out = runner.shutdown();
    // Three consecutive trades form a complete pattern
    assert_eq!(
        out,
        vec![vec![
            AttributeValue::Double(100.0),
            AttributeValue::Double(110.0),
            AttributeValue::Double(120.0),
        ]]
    );
}

/// Test pattern with interleaved events from other streams
/// The pattern should only match on A, B, C events in sequence
#[tokio::test]
async fn pattern_three_streams_with_noise_sql() {
    let app = "\
        CREATE STREAM A (val INT);\n\
        CREATE STREAM B (val INT);\n\
        CREATE STREAM C (val INT);\n\
        CREATE STREAM Noise (val INT);\n\
        CREATE STREAM Out (aval INT, bval INT, cval INT);\n\
        INSERT INTO Out\n\
        SELECT A.val AS aval, B.val AS bval, C.val AS cval\n\
        FROM PATTERN (e1=A -> e2=B -> e3=C);\n";
    let runner = AppRunner::new(app, "Out").await;
    // Send events with "noise" interleaved - pattern should still match
    runner.send("A", vec![AttributeValue::Int(1)]);
    runner.send("Noise", vec![AttributeValue::Int(999)]);
    runner.send("B", vec![AttributeValue::Int(2)]);
    runner.send("Noise", vec![AttributeValue::Int(888)]);
    runner.send("C", vec![AttributeValue::Int(3)]);
    let out = runner.shutdown();
    // PATTERN mode ignores non-matching events
    assert_eq!(
        out,
        vec![vec![
            AttributeValue::Int(1),
            AttributeValue::Int(2),
            AttributeValue::Int(3),
        ]]
    );
}

/// Test that incomplete pattern produces no output
/// Send A -> B but no C - pattern should not complete
#[tokio::test]
async fn pattern_incomplete_no_output_sql() {
    let app = "\
        CREATE STREAM A (val INT);\n\
        CREATE STREAM B (val INT);\n\
        CREATE STREAM C (val INT);\n\
        CREATE STREAM Out (aval INT, bval INT, cval INT);\n\
        INSERT INTO Out\n\
        SELECT A.val AS aval, B.val AS bval, C.val AS cval\n\
        FROM PATTERN (e1=A -> e2=B -> e3=C);\n";
    let runner = AppRunner::new(app, "Out").await;
    // Only send A and B - C never arrives
    runner.send("A", vec![AttributeValue::Int(1)]);
    runner.send("B", vec![AttributeValue::Int(2)]);
    // No C sent - pattern never completes
    let out = runner.shutdown();
    // Expect no output since pattern didn't complete
    assert_eq!(out, Vec::<Vec<AttributeValue>>::new());
}

/// Test pattern behavior without EVERY
/// Without EVERY, the pattern matches only once and then stops
#[tokio::test]
async fn pattern_single_match_without_every_sql() {
    let app = "\
        CREATE STREAM A (val INT);\n\
        CREATE STREAM B (val INT);\n\
        CREATE STREAM C (val INT);\n\
        CREATE STREAM Out (aval INT, bval INT, cval INT);\n\
        INSERT INTO Out\n\
        SELECT A.val AS aval, B.val AS bval, C.val AS cval\n\
        FROM PATTERN (e1=A -> e2=B -> e3=C);\n";
    let runner = AppRunner::new(app, "Out").await;
    // First complete pattern
    runner.send("A", vec![AttributeValue::Int(1)]);
    runner.send("B", vec![AttributeValue::Int(2)]);
    runner.send("C", vec![AttributeValue::Int(3)]);
    // Second complete sequence - will NOT match without EVERY
    runner.send("A", vec![AttributeValue::Int(10)]);
    runner.send("B", vec![AttributeValue::Int(20)]);
    runner.send("C", vec![AttributeValue::Int(30)]);
    let out = runner.shutdown();
    // Without EVERY, pattern only matches once (first complete sequence)
    // This is standard CEP pattern behavior - EVERY is needed for continuous matching
    assert_eq!(
        out,
        vec![vec![
            AttributeValue::Int(1),
            AttributeValue::Int(2),
            AttributeValue::Int(3),
        ]]
    );
}

// ============================================================================
// Logical Pattern Tests (AND/OR)
// ============================================================================

/// Test top-level logical AND pattern (no sequence): e1=A AND e2=B
/// Pattern fires when both A and B have arrived
#[tokio::test]
async fn pattern_logical_and_toplevel_sql() {
    let app = "\
        CREATE STREAM A (val INT);\n\
        CREATE STREAM B (val INT);\n\
        CREATE STREAM Out (aval INT, bval INT);\n\
        INSERT INTO Out\n\
        SELECT e1.val AS aval, e2.val AS bval\n\
        FROM PATTERN (e1=A AND e2=B);\n";
    let runner = AppRunner::new(app, "Out").await;
    // For AND, both A and B must arrive (order doesn't matter for AND)
    runner.send("A", vec![AttributeValue::Int(1)]);
    runner.send("B", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![AttributeValue::Int(1), AttributeValue::Int(2),]]
    );
}

/// Test top-level logical OR pattern (no sequence): e1=A OR e2=B
/// Pattern fires when either A or B arrives
#[tokio::test]
async fn pattern_logical_or_toplevel_sql() {
    let app = "\
        CREATE STREAM A (val INT);\n\
        CREATE STREAM B (val INT);\n\
        CREATE STREAM Out (aval INT);\n\
        INSERT INTO Out\n\
        SELECT e1.val AS aval\n\
        FROM PATTERN (e1=A OR e2=B);\n";
    let runner = AppRunner::new(app, "Out").await;
    // For OR, either A or B arriving fires the pattern
    runner.send("A", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Int(1),]]);
}

/// Test logical AND pattern with sequence: (e1=A AND e2=B) -> e3=C
/// NOTE: This is currently not supported - the runtime needs to be extended
/// to handle logical groups within sequence patterns.
#[tokio::test]
#[ignore = "Requires runtime extension for logical groups in sequences - See ROADMAP.md"]
async fn pattern_logical_and_sequence_sql() {
    let app = "\
        CREATE STREAM A (val INT);\n\
        CREATE STREAM B (val INT);\n\
        CREATE STREAM C (val INT);\n\
        CREATE STREAM Out (aval INT, bval INT, cval INT);\n\
        INSERT INTO Out\n\
        SELECT e1.val AS aval, e2.val AS bval, e3.val AS cval\n\
        FROM PATTERN ((e1=A AND e2=B) -> e3=C);\n";
    let runner = AppRunner::new(app, "Out").await;
    // For AND, both A and B must arrive (order doesn't matter for AND), then C
    runner.send("A", vec![AttributeValue::Int(1)]);
    runner.send("B", vec![AttributeValue::Int(2)]);
    runner.send("C", vec![AttributeValue::Int(3)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![
            AttributeValue::Int(1),
            AttributeValue::Int(2),
            AttributeValue::Int(3),
        ]]
    );
}

/// Test logical OR pattern with sequence: (e1=A OR e2=B) -> e3=C
/// NOTE: This is currently not supported - the runtime needs to be extended
/// to handle logical groups within sequence patterns.
#[tokio::test]
#[ignore = "Requires runtime extension for logical groups in sequences - See ROADMAP.md"]
async fn pattern_logical_or_sequence_sql() {
    let app = "\
        CREATE STREAM A (val INT);\n\
        CREATE STREAM B (val INT);\n\
        CREATE STREAM C (val INT);\n\
        CREATE STREAM Out (aval INT, cval INT);\n\
        INSERT INTO Out\n\
        SELECT e1.val AS aval, e3.val AS cval\n\
        FROM PATTERN ((e1=A OR e2=B) -> e3=C);\n";
    let runner = AppRunner::new(app, "Out").await;
    // For OR, either A or B arriving triggers the pattern, then C completes it
    runner.send("A", vec![AttributeValue::Int(1)]);
    runner.send("C", vec![AttributeValue::Int(3)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![AttributeValue::Int(1), AttributeValue::Int(3),]]
    );
}

/// Test same-stream logical AND pattern: e1=Trades AND e2=Trades
/// Tests both logical patterns AND same-stream alias resolution together
#[tokio::test]
async fn pattern_logical_and_same_stream_sql() {
    let app = "\
        CREATE STREAM Trades (price DOUBLE);\n\
        CREATE STREAM Out (p1 DOUBLE, p2 DOUBLE);\n\
        INSERT INTO Out\n\
        SELECT e1.price AS p1, e2.price AS p2\n\
        FROM PATTERN (e1=Trades AND e2=Trades);\n";
    let runner = AppRunner::new(app, "Out").await;
    // For AND pattern with same stream: first two trades satisfy AND
    runner.send("Trades", vec![AttributeValue::Double(100.0)]);
    runner.send("Trades", vec![AttributeValue::Double(110.0)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![
            AttributeValue::Double(100.0),
            AttributeValue::Double(110.0),
        ]]
    );
}
