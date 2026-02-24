// SPDX-License-Identifier: MIT OR Apache-2.0

use eventflux::core::event::{value::AttributeValue, Event};
use eventflux::core::eventflux_manager::EventFluxManager;
use eventflux::core::persistence::{
    InMemoryPersistenceStore, PersistenceStore, SnapshotService,
};
use eventflux::core::stream::output::stream_callback::StreamCallback;
use eventflux::core::util::{event_from_bytes, event_to_bytes};
use eventflux::query_api::definition::{attribute::Type as AttrType, StreamDefinition};
use eventflux::query_api::eventflux_app::EventFluxApp;
use eventflux::query_api::execution::{
    query::{
        input::stream::{single_input_stream::SingleInputStream, InputStream},
        output::output_stream::{InsertIntoStreamAction, OutputStream, OutputStreamAction},
        selection::Selector,
        Query,
    },
    ExecutionElement,
};
use std::sync::{Arc, Mutex};

#[test]
fn test_event_serialization_roundtrip() {
    let e = Event {
        id: 1,
        timestamp: 123,
        data: vec![AttributeValue::Int(5), AttributeValue::String("hi".into())],
        is_expired: false,
    };
    let bytes = event_to_bytes(&e).unwrap();
    let de = event_from_bytes(&bytes).unwrap();
    assert_eq!(e.id, de.id);
    assert_eq!(e.timestamp, de.timestamp);
    assert_eq!(e.data, de.data);
    assert_eq!(e.is_expired, de.is_expired);
}

#[test]
fn test_snapshot_service_persist_restore() {
    let store = Arc::new(InMemoryPersistenceStore::new());
    let mut service = SnapshotService::new("app1".to_string());
    service.persistence_store = Some(store.clone());
    service.set_state(b"state1".to_vec());
    let rev = service.persist().unwrap().revision;
    service.set_state(b"changed".to_vec());
    service.restore_revision(&rev).unwrap();
    assert_eq!(service.snapshot(), b"state1".to_vec());
    // ensure store recorded revision
    assert_eq!(store.get_last_revision("app1").unwrap(), rev);
}

#[derive(Debug)]
struct CountingCallback {
    count: Arc<Mutex<u32>>,
    service: Arc<SnapshotService>,
}

impl StreamCallback for CountingCallback {
    fn receive_events(&self, events: &[Event]) {
        let mut c = self.count.lock().unwrap();
        *c += events.len() as u32;
        self.service.set_state(c.to_be_bytes().to_vec());
    }
}

#[tokio::test]
async fn test_runtime_persist_restore() {
    let store = Arc::new(InMemoryPersistenceStore::new());
    let manager = EventFluxManager::new();
    let store_arc: Arc<dyn PersistenceStore> = store.clone();
    manager.set_persistence_store(store_arc).unwrap();

    let mut app = EventFluxApp::new("PersistApp".to_string());
    let in_def =
        StreamDefinition::new("InStream".to_string()).attribute("v".to_string(), AttrType::INT);
    let out_def =
        StreamDefinition::new("OutStream".to_string()).attribute("v".to_string(), AttrType::INT);
    app.stream_definition_map
        .insert("InStream".to_string(), Arc::new(in_def));
    app.stream_definition_map
        .insert("OutStream".to_string(), Arc::new(out_def));

    let si = SingleInputStream::new_basic("InStream".to_string(), false, false, None, Vec::new())
        .window(
            None,
            "length".to_string(),
            vec![eventflux::query_api::expression::Expression::value_int(2)],
        );
    let input = InputStream::Single(si);
    let selector = Selector::new();
    let insert = InsertIntoStreamAction {
        target_id: "OutStream".to_string(),
        is_inner_stream: false,
        is_fault_stream: false,
    };
    let out_stream = OutputStream::new(OutputStreamAction::InsertInto(insert), None);
    let query = Query::query()
        .from(input)
        .select(selector)
        .out_stream(out_stream);
    app.execution_element_list
        .push(ExecutionElement::Query(query));

    let app = Arc::new(app);
    let runtime = manager
        .create_eventflux_app_runtime_from_api(Arc::clone(&app), None)
        .await
        .unwrap();
    let svc = runtime
        .eventflux_app_context
        .get_snapshot_service()
        .unwrap();
    let count = Arc::new(Mutex::new(0u32));
    runtime
        .add_callback(
            "OutStream",
            Box::new(CountingCallback {
                count: Arc::clone(&count),
                service: Arc::clone(&svc),
            }),
        )
        .unwrap();
    runtime.start().expect("Failed to start runtime");

    let handler = runtime.get_input_handler("InStream").unwrap();
    handler
        .lock()
        .unwrap()
        .send_event_with_timestamp(0, vec![AttributeValue::Int(1)])
        .unwrap();
    handler
        .lock()
        .unwrap()
        .send_event_with_timestamp(1, vec![AttributeValue::Int(2)])
        .unwrap();
    assert_eq!(*count.lock().unwrap(), 2);

    let key = manager.get_eventflux_app_runtimes_keys()[0].clone();
    let rev = manager.persist_app(&key).unwrap();
    handler
        .lock()
        .unwrap()
        .send_event_with_timestamp(2, vec![AttributeValue::Int(3)])
        .unwrap();
    assert_eq!(*count.lock().unwrap(), 4);

    manager.restore_app_revision(&key, &rev).unwrap();
    let restored = svc.snapshot();
    let restored_val = u32::from_be_bytes([restored[0], restored[1], restored[2], restored[3]]);
    *count.lock().unwrap() = restored_val;

    handler
        .lock()
        .unwrap()
        .send_event_with_timestamp(3, vec![AttributeValue::Int(4)])
        .unwrap();
    assert_eq!(*count.lock().unwrap(), 4);
}
