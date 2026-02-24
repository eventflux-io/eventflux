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
) {
    let eventflux_context = Arc::new(EventFluxContext::new());
    let app = Arc::new(eventflux::query_api::eventflux_app::EventFluxApp::new(
        "App".to_string(),
    ));
    let mut ctx = EventFluxAppContext::new(
        Arc::clone(&eventflux_context),
        "App".to_string(),
        Arc::clone(&app),
        String::new(),
    );
    ctx.root_metrics_level =
        eventflux::core::config::eventflux_app_context::MetricsLevelPlaceholder::BASIC;
    let app_ctx = Arc::new(ctx);
    let def = Arc::new(
        StreamDefinition::new("InputStream".to_string()).attribute("a".to_string(), AttrType::INT),
    );
    let junction = Arc::new(Mutex::new(
        StreamJunction::new(
            "InputStream".to_string(),
            Arc::clone(&def),
            Arc::clone(&app_ctx),
            4096,
            async_mode,
            None,
        )
        .unwrap(),
    ));
    let rec = Arc::new(Mutex::new(Vec::new()));
    let p = Arc::new(Mutex::new(RecordingProcessor {
        events: Arc::clone(&rec),
    }));
    junction.lock().unwrap().subscribe(p);
    if async_mode {
        junction
            .lock()
            .unwrap()
            .start_processing()
            .expect("Failed to start async processing");
    }
    (junction, rec)
}

#[test]
fn stress_async_concurrent_publish() {
    let (junction, rec) = setup_junction(true);
    let mut handles = Vec::new();
    for i in 0..4 {
        let j = junction.clone();
        handles.push(thread::spawn(move || {
            for k in 0..500 {
                j.lock().unwrap().send_event(Event::new_with_data(
                    0,
                    vec![AttributeValue::Int(i * 500 + k)],
                ));
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    // Give the async executor sufficient time to process all events.
    thread::sleep(Duration::from_millis(2000));
    let data = rec.lock().unwrap();
    assert_eq!(data.len(), 2000);
    // metrics
    let count = junction.lock().unwrap().total_events().unwrap_or(0);
    assert_eq!(count, 2000);
}
