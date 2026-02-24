// SPDX-License-Identifier: MIT OR Apache-2.0

use std::sync::{Arc, Mutex};
use eventflux::core::config::{eventflux_app_context::EventFluxAppContext, eventflux_query_context::EventFluxQueryContext};
use eventflux::core::event::complex_event::ComplexEvent;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::extension::WindowProcessorFactory;
use eventflux::core::executor::expression_executor::ExpressionExecutor;
use eventflux::core::executor::function::scalar_function_executor::ScalarFunctionExecutor;
use eventflux::core::query::processor::{CommonProcessorMeta, ProcessingMode, Processor};
use eventflux::core::query::processor::stream::window::WindowProcessor;
use eventflux::core::eventflux_manager::EventFluxManager;
use eventflux::query_api::definition::attribute::Type as AttrType;
use eventflux::query_api::execution::query::input::handler::WindowHandler;

#[derive(Debug)]
struct DynWindowProcessor {
    meta: CommonProcessorMeta,
}

impl Processor for DynWindowProcessor {
    fn process(&self, chunk: Option<Box<dyn ComplexEvent>>) {
        if let Some(ref next) = self.meta.next_processor {
            next.lock().unwrap().process(chunk);
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.meta.next_processor.as_ref().map(Arc::clone)
    }

    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.meta.next_processor = next;
    }

    fn clone_processor(&self, q: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        Box::new(DynWindowProcessor { meta: CommonProcessorMeta::new(Arc::clone(&self.meta.eventflux_app_context), Arc::clone(q)) })
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }

    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        Arc::clone(&self.meta.eventflux_query_context)
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::BATCH
    }

    fn is_stateful(&self) -> bool {
        false
    }
}
impl WindowProcessor for DynWindowProcessor {}

#[derive(Debug, Clone)]
struct DynWindowFactory;
impl WindowProcessorFactory for DynWindowFactory {
    fn name(&self) -> &'static str { "dynWindow" }
    fn create(&self, _h: &WindowHandler, app: Arc<EventFluxAppContext>, q: Arc<EventFluxQueryContext>, _parse_ctx: &eventflux::core::util::parser::expression_parser::ExpressionParserContext) -> Result<Arc<Mutex<dyn Processor>>, String> {
        Ok(Arc::new(Mutex::new(DynWindowProcessor { meta: CommonProcessorMeta::new(app, q) })))
    }
    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> { Box::new(self.clone()) }
}

#[derive(Debug, Default)]
struct DynPlusOne {
    arg: Option<Box<dyn ExpressionExecutor>>,
}
impl Clone for DynPlusOne {
    fn clone(&self) -> Self { Self { arg: None } }
}
impl ExpressionExecutor for DynPlusOne {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let v = self.arg.as_ref()?.execute(event)?;
        match v {
            AttributeValue::Int(i) => Some(AttributeValue::Int(i + 1)),
            _ => None,
        }
    }
    fn get_return_type(&self) -> AttrType { AttrType::INT }
    fn clone_executor(&self, _c: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(self.clone())
    }
}
impl ScalarFunctionExecutor for DynPlusOne {
    fn init(&mut self, args: &Vec<Box<dyn ExpressionExecutor>>, ctx: &Arc<EventFluxAppContext>) -> Result<(), String> {
        if args.len() != 1 { return Err("dynPlusOne expects one argument".to_string()); }
        self.arg = Some(args[0].clone_executor(ctx));
        Ok(())
    }
    fn destroy(&mut self) {}
    fn get_name(&self) -> String { "dynPlusOne".to_string() }
    fn clone_scalar_function(&self) -> Box<dyn ScalarFunctionExecutor> { Box::new(self.clone()) }
}

#[no_mangle]
pub extern "C" fn register_windows(manager: &EventFluxManager) {
    println!("[dyn_ext] register_windows called");
    manager.add_window_factory("dynWindow".to_string(), Box::new(DynWindowFactory));
}

#[no_mangle]
pub extern "C" fn register_functions(manager: &EventFluxManager) {
    println!("[dyn_ext] register_functions called");
    manager.add_scalar_function_factory("dynPlusOne".to_string(), Box::new(DynPlusOne::default()));
}

/// Returns the path to the compiled dynamic library for this crate.
pub fn library_path() -> std::path::PathBuf {
    use std::path::PathBuf;
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("../../target/debug/deps");
    p = p.canonicalize().unwrap_or(p);
    p.push(format!("{}custom_dyn_ext.{}", std::env::consts::DLL_PREFIX, std::env::consts::DLL_EXTENSION));
    p
}
