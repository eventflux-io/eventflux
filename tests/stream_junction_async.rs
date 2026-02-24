// SPDX-License-Identifier: MIT OR Apache-2.0

use eventflux::core::config::eventflux_query_context::EventFluxQueryContext;
use eventflux::core::config::{
    eventflux_app_context::EventFluxAppContext, eventflux_context::EventFluxContext,
};
use eventflux::core::event::complex_event::ComplexEvent;
use eventflux::core::event::event::Event;
use eventflux::core::event::stream::StreamEvent;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::query::processor::Processor;
use eventflux::core::stream::stream_junction::StreamJunction;
use eventflux::query_api::definition::attribute::Type as AttrType;
use eventflux::query_api::definition::StreamDefinition;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[derive(Debug)]
struct RecordingProcessor {
    events: Arc<Mutex<Vec<Vec<AttributeValue>>>>,
}

impl Processor for RecordingProcessor {
    fn process(&self, mut chunk: Option<Box<dyn ComplexEvent>>) {
        while let Some(mut ce) = chunk {
            chunk = ce.set_next(None);
            if let Some(se) = ce.as_any().downcast_ref::<StreamEvent>() {
                self.events
                    .lock()
                    .unwrap()
                    .push(se.before_window_data.clone());
            }
        }
    }
    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        None
    }
    fn set_next_processor(&mut self, _n: Option<Arc<Mutex<dyn Processor>>>) {}
    fn clone_processor(
        &self,
        _c: &Arc<eventflux::core::config::eventflux_query_context::EventFluxQueryContext>,
    ) -> Box<dyn Processor> {
        Box::new(RecordingProcessor {
            events: Arc::clone(&self.events),
        })
    }
    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::new(EventFluxAppContext::new(
            Arc::new(EventFluxContext::new()),
            "T".to_string(),
            Arc::new(eventflux::query_api::eventflux_app::EventFluxApp::new(
                "T".to_string(),
            )),
            String::new(),
        ))
    }

    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        Arc::new(EventFluxQueryContext::new(
            Arc::new(EventFluxAppContext::new(
                Arc::new(EventFluxContext::new()),
                "T".to_string(),
                Arc::new(eventflux::query_api::eventflux_app::EventFluxApp::new(
                    "T".to_string(),
                )),
                String::new(),
            )),
            "q".to_string(),
            None,
        ))
    }
    fn get_processing_mode(&self) -> eventflux::core::query::processor::ProcessingMode {
        eventflux::core::query::processor::ProcessingMode::DEFAULT
    }
    fn is_stateful(&self) -> bool {
        false
    }
}

fn setup_junction(
    async_mode: bool,
) -> (
    Arc<Mutex<StreamJunction>>,
    Arc<Mutex<Vec<Vec<AttributeValue>>>>,
    Arc<Mutex<Vec<Vec<AttributeValue>>>>,
) {
    let eventflux_context = Arc::new(EventFluxContext::new());
    let app = Arc::new(eventflux::query_api::eventflux_app::EventFluxApp::new(
        "App".to_string(),
    ));
    let app_ctx = Arc::new(EventFluxAppContext::new(
        Arc::clone(&eventflux_context),
        "App".to_string(),
        Arc::clone(&app),
        String::new(),
    ));
    let def = Arc::new(
        StreamDefinition::new("InputStream".to_string()).attribute("a".to_string(), AttrType::INT),
    );
    let junction = Arc::new(Mutex::new(
        StreamJunction::new(
            "InputStream".to_string(),
            Arc::clone(&def),
            Arc::clone(&app_ctx),
            1024,
            async_mode,
            None,
        )
        .unwrap(),
    ));
    let rec1 = Arc::new(Mutex::new(Vec::new()));
    let rec2 = Arc::new(Mutex::new(Vec::new()));
    let p1 = Arc::new(Mutex::new(RecordingProcessor {
        events: Arc::clone(&rec1),
    }));
    let p2 = Arc::new(Mutex::new(RecordingProcessor {
        events: Arc::clone(&rec2),
    }));
    junction.lock().unwrap().subscribe(p1);
    junction.lock().unwrap().subscribe(p2);
    if async_mode {
        junction
            .lock()
            .unwrap()
            .start_processing()
            .expect("Failed to start async processing");
    }
    (junction, rec1, rec2)
}

#[test]
fn test_sync_cloning() {
    let (junction, r1, r2) = setup_junction(false);
    junction
        .lock()
        .unwrap()
        .send_event(Event::new_with_data(0, vec![AttributeValue::Int(1)]));
    let v1 = r1.lock().unwrap().clone();
    let v2 = r2.lock().unwrap().clone();
    assert_eq!(v1, vec![vec![AttributeValue::Int(1)]]);
    assert_eq!(v2, vec![vec![AttributeValue::Int(1)]]);
}

#[test]
fn test_async_processing() {
    let (junction, r1, r2) = setup_junction(true);
    junction
        .lock()
        .unwrap()
        .send_event(Event::new_with_data(0, vec![AttributeValue::Int(2)]));
    // wait for async tasks
    thread::sleep(Duration::from_millis(200));
    let v1 = r1.lock().unwrap().clone();
    let v2 = r2.lock().unwrap().clone();
    assert_eq!(v1, vec![vec![AttributeValue::Int(2)]]);
    assert_eq!(v2, vec![vec![AttributeValue::Int(2)]]);
}
