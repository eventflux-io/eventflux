// SPDX-License-Identifier: MIT OR Apache-2.0

use eventflux::core::config::eventflux_app_context::EventFluxAppContext;
use eventflux::core::config::eventflux_context::EventFluxContext;
use eventflux::core::config::eventflux_query_context::EventFluxQueryContext;
use eventflux::core::query::output::callback_processor::CallbackProcessor;
use eventflux::core::stream::input::input_handler::InputHandler;
use eventflux::core::stream::input::mapper::PassthroughMapper as SourcePassthroughMapper;
use eventflux::core::stream::input::source::SourceCallbackAdapter;
use eventflux::core::stream::output::mapper::PassthroughMapper as SinkPassthroughMapper;
use eventflux::core::stream::output::sink::{LogSink, SinkCallbackAdapter};
use eventflux::core::stream::output::StreamCallback;
use eventflux::core::stream::{Source, StreamJunction, TimerSource};
use eventflux::query_api::definition::attribute::Type as AttrType;
use eventflux::query_api::definition::stream_definition::StreamDefinition;
use eventflux::query_api::eventflux_app::EventFluxApp;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[test]
fn timer_source_to_log_sink() {
    let ctx = Arc::new(EventFluxContext::new());
    let app = Arc::new(EventFluxApp::new("TestApp".to_string()));
    let app_ctx = Arc::new(EventFluxAppContext::new(
        Arc::clone(&ctx),
        "Test".to_string(),
        Arc::clone(&app),
        String::new(),
    ));

    let stream_def = Arc::new(
        StreamDefinition::new("FooStream".to_string())
            .attribute("message".to_string(), AttrType::STRING),
    );
    let junction = Arc::new(Mutex::new(
        StreamJunction::new(
            "FooStream".to_string(),
            Arc::clone(&stream_def),
            Arc::clone(&app_ctx),
            64,
            false,
            None,
        )
        .unwrap(),
    ));

    let publisher = StreamJunction::construct_publisher(Arc::clone(&junction));
    let input_handler = Arc::new(Mutex::new(InputHandler::new(
        "FooStream".to_string(),
        0,
        Arc::new(Mutex::new(publisher)),
        Arc::clone(&app_ctx),
    )));

    // Create sink with adapter (Events → mapper → bytes → sink)
    let sink = LogSink::new();
    let collected = sink.events.clone();
    let sink_adapter = SinkCallbackAdapter {
        sink: Arc::new(Mutex::new(Box::new(sink))),
        mapper: Arc::new(Mutex::new(Box::new(SinkPassthroughMapper::new()))),
    };
    let callback = Arc::new(Mutex::new(Box::new(sink_adapter) as Box<dyn StreamCallback>));
    let cb_processor = Arc::new(Mutex::new(CallbackProcessor::new(
        callback,
        Arc::clone(&app_ctx),
        Arc::new(EventFluxQueryContext::new(
            Arc::clone(&app_ctx),
            "cb".to_string(),
            None,
        )),
    )));
    junction.lock().unwrap().subscribe(cb_processor);

    // Create source with callback adapter (source → bytes → mapper → Events → InputHandler)
    let source_callback = Arc::new(SourceCallbackAdapter::new(
        Arc::new(Mutex::new(Box::new(SourcePassthroughMapper::new()))),
        Arc::clone(&input_handler),
    ));
    let mut source = TimerSource::new(10);
    source.start(source_callback);
    std::thread::sleep(Duration::from_millis(50));
    source.stop();
    std::thread::sleep(Duration::from_millis(20));

    let events = collected.lock().unwrap();
    assert!(events.len() > 0);
}
