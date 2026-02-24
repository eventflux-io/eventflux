// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::complex_event::{clone_event_chain, ComplexEvent};
use eventflux::core::event::event::Event;
use eventflux::core::event::state::state_event::StateEvent;
use eventflux::core::event::stream::stream_event::StreamEvent;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::util::{from_bytes, to_bytes};

// TODO: NOT PART OF M1 SQL TESTING - Event serialization is core functionality but test uses old syntax
// This test is for event serialization/deserialization which is core runtime functionality.
// However, it currently uses old EventFluxQL syntax ("define stream") which is no longer supported by AppRunner.
// The test should be rewritten to either:
// 1. Use SQL syntax (CREATE STREAM), or
// 2. Test event serialization without going through the parser (direct Event construction)
// Event serialization IS important for M1 (needed for state management), but the test itself
// is not about SQL parsing, so it's disabled pending refactoring.
#[tokio::test]
#[ignore = "Event serialization test - needs refactoring for SQL syntax"]
async fn test_clone_and_serialize_stream_event() {
    let app = "define stream InStream (a int); define stream OutStream (a int); from InStream select a as a insert into OutStream;";
    let runner = AppRunner::new(app, "OutStream").await;
    runner.send("InStream", vec![AttributeValue::Int(1)]);
    runner.send("InStream", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();

    let mut se1 = StreamEvent::new(0, 0, 0, 1);
    se1.output_data.as_mut().unwrap()[0] = out[0][0].clone();
    let mut se2 = StreamEvent::new(0, 0, 0, 1);
    se2.output_data.as_mut().unwrap()[0] = out[1][0].clone();
    se1.next = Some(Box::new(se2));

    let cloned = clone_event_chain(&se1);
    if let Some(c1) = cloned.as_any().downcast_ref::<StreamEvent>() {
        assert_eq!(c1.output_data.as_ref().unwrap()[0], AttributeValue::Int(1));
        if let Some(n) = c1.get_next() {
            let c2 = n.as_any().downcast_ref::<StreamEvent>().unwrap();
            assert_eq!(c2.output_data.as_ref().unwrap()[0], AttributeValue::Int(2));
        } else {
            panic!("no next");
        }
    } else {
        panic!("not stream event");
    }

    let bytes = bincode::serialize(&se1).unwrap();
    let de: StreamEvent = bincode::deserialize(&bytes).unwrap();
    assert_eq!(de.output_data.as_ref().unwrap()[0], AttributeValue::Int(1));
}

#[tokio::test]
async fn test_serialize_state_event() {
    let mut se = StreamEvent::new(0, 0, 0, 1);
    se.output_data.as_mut().unwrap()[0] = AttributeValue::Int(3);
    let mut state = StateEvent::new(1, 1);
    state.stream_events[0] = Some(se);
    let bytes = bincode::serialize(&state).unwrap();
    let de: StateEvent = bincode::deserialize(&bytes).unwrap();
    assert_eq!(
        de.stream_events[0]
            .as_ref()
            .unwrap()
            .output_data
            .as_ref()
            .unwrap()[0],
        AttributeValue::Int(3)
    );
}

// TODO: NOT PART OF M1 SQL TESTING - Event serialization is core functionality but test uses old syntax
// This test is for event serialization/deserialization which is core runtime functionality.
// However, it currently uses old EventFluxQL syntax ("define stream") which is no longer supported by AppRunner.
// The test should be rewritten to either:
// 1. Use SQL syntax (CREATE STREAM), or
// 2. Test event serialization without going through the parser (direct Event construction)
// Event serialization IS important for M1 (needed for state management), but the test itself
// is not about SQL parsing, so it's disabled pending refactoring.
#[tokio::test]
#[ignore = "Event serialization test - needs refactoring for SQL syntax"]
async fn clone_and_serialize_events() {
    let app = "\
        define stream In (a int);\n\
        define stream Out (a int);\n\
        from In select a insert into Out;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(42)]);
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Int(42)]]);

    let ev = Event::new_with_data(0, out[0].clone());
    let bytes = to_bytes(&ev).unwrap();
    let deser: Event = from_bytes(&bytes).unwrap();
    assert_eq!(deser, ev);

    let mut se1 = StreamEvent::new(0, 0, 0, 1);
    se1.output_data = Some(vec![AttributeValue::Int(1)]);
    let mut se2 = StreamEvent::new(0, 0, 0, 1);
    se2.output_data = Some(vec![AttributeValue::Int(2)]);
    se1.next = Some(Box::new(se2));
    let cloned = clone_event_chain(&se1);
    let se1c = cloned.as_any().downcast_ref::<StreamEvent>().unwrap();
    assert_eq!(
        se1c.output_data.as_ref().unwrap()[0],
        AttributeValue::Int(1)
    );
    if let Some(n) = se1c.get_next() {
        let se2c = n.as_any().downcast_ref::<StreamEvent>().unwrap();
        assert_eq!(
            se2c.output_data.as_ref().unwrap()[0],
            AttributeValue::Int(2)
        );
    } else {
        panic!("missing next");
    }
    let bytes = to_bytes(se1c).unwrap();
    let deser_se: StreamEvent = from_bytes(&bytes).unwrap();
    assert_eq!(
        deser_se.output_data.as_ref().unwrap()[0],
        AttributeValue::Int(1)
    );

    let mut state = StateEvent::new(1, 1);
    state.stream_events[0] = Some(se1.clone_without_next());
    state.output_data = Some(vec![AttributeValue::Int(3)]);
    let bytes = to_bytes(&state).unwrap();
    let deser_state: StateEvent = from_bytes(&bytes).unwrap();
    assert_eq!(
        deser_state.output_data.as_ref().unwrap()[0],
        AttributeValue::Int(3)
    );
    assert!(deser_state.stream_events[0].is_some());
}
