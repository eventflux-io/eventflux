// SPDX-License-Identifier: MIT OR Apache-2.0

use eventflux::core::event::complex_event::{clone_event_chain, ComplexEvent};
use eventflux::core::event::stream::{
    stream_event::StreamEvent, stream_event_cloner::StreamEventCloner,
    stream_event_factory::StreamEventFactory,
};
use eventflux::core::event::value::AttributeValue;

#[test]
fn test_clone_event_chain() {
    let mut se1 = StreamEvent::new(1, 1, 0, 0);
    se1.before_window_data[0] = AttributeValue::Int(10);
    let mut se2 = StreamEvent::new(2, 1, 0, 0);
    se2.before_window_data[0] = AttributeValue::Int(20);
    se1.next = Some(Box::new(se2));

    let cloned = clone_event_chain(&se1);
    // verify values
    if let Some(c1) = cloned.as_any().downcast_ref::<StreamEvent>() {
        assert_eq!(c1.before_window_data[0], AttributeValue::Int(10));
        if let Some(n) = c1.get_next() {
            if let Some(c2) = n.as_any().downcast_ref::<StreamEvent>() {
                assert_eq!(c2.before_window_data[0], AttributeValue::Int(20));
                assert!(c2.get_next().is_none());
            } else {
                panic!("second not stream event")
            }
        } else {
            panic!("no next")
        }
    } else {
        panic!("first not stream event");
    }

    // modify original second event and ensure clone unaffected
    if let Some(ref mut orig2) = se1.next {
        if let Some(se2_mut) = orig2.as_any_mut().downcast_mut::<StreamEvent>() {
            se2_mut.before_window_data[0] = AttributeValue::Int(99);
        }
    }
    if let Some(c1) = cloned.as_any().downcast_ref::<StreamEvent>() {
        if let Some(n) = c1.get_next() {
            if let Some(c2) = n.as_any().downcast_ref::<StreamEvent>() {
                assert_eq!(c2.before_window_data[0], AttributeValue::Int(20));
            }
        }
    }
}

#[test]
fn test_stream_event_pool_and_cloner() {
    let factory = StreamEventFactory::new(1, 0, 0);
    assert_eq!(factory.pool_size(), 0);
    let mut ev = factory.new_instance();
    ev.before_window_data[0] = AttributeValue::Int(5);
    let cloner = StreamEventCloner::from_event(&ev);
    let copy = cloner.copy_stream_event(&ev);
    assert_eq!(copy.before_window_data[0], AttributeValue::Int(5));
    factory.release(ev);
    factory.release(copy);
    assert_eq!(factory.pool_size(), 2);
    let reused = factory.new_instance();
    assert_eq!(factory.pool_size(), 1);
    assert_eq!(reused.before_window_data[0], AttributeValue::Null);
}
