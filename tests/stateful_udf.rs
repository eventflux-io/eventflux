// SPDX-License-Identifier: MIT OR Apache-2.0

use eventflux::core::config::eventflux_app_context::EventFluxAppContext;
use eventflux::core::config::eventflux_query_context::EventFluxQueryContext;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::eventflux_manager::EventFluxManager;
use eventflux::core::util::parser::{parse_expression, ExpressionParserContext};
use eventflux::query_api::definition::attribute::Type as ApiAttributeType;
use eventflux::query_api::eventflux_app::EventFluxApp;
use eventflux::query_api::expression::Expression;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use eventflux::core::event::complex_event::ComplexEvent;
use eventflux::core::executor::expression_executor::ExpressionExecutor;
use eventflux::core::executor::function::scalar_function_executor::ScalarFunctionExecutor;

#[derive(Debug)]
struct StatefulCountFunction {
    state: Box<dyn Any + Send + Sync>,
}

impl StatefulCountFunction {
    fn new() -> Self {
        StatefulCountFunction {
            state: Box::new(Mutex::new(0)),
        }
    }

    fn counter(&self) -> &Mutex<i32> {
        self.state.downcast_ref::<Mutex<i32>>().unwrap()
    }
}

impl Clone for StatefulCountFunction {
    fn clone(&self) -> Self {
        StatefulCountFunction::new()
    }
}

impl ExpressionExecutor for StatefulCountFunction {
    fn execute(&self, _event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let mut guard = self.counter().lock().unwrap();
        *guard += 1;
        Some(AttributeValue::Int(*guard))
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::INT
    }

    fn clone_executor(&self, _ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(self.clone())
    }
}

impl ScalarFunctionExecutor for StatefulCountFunction {
    fn init(
        &mut self,
        _arg_execs: &Vec<Box<dyn ExpressionExecutor>>,
        _ctx: &Arc<EventFluxAppContext>,
    ) -> Result<(), String> {
        Ok(())
    }

    fn destroy(&mut self) {
        if let Some(c) = self.state.downcast_ref::<Mutex<i32>>() {
            *c.lock().unwrap() = 0;
        }
    }

    fn get_name(&self) -> String {
        "statefulCount".to_string()
    }

    fn clone_scalar_function(&self) -> Box<dyn ScalarFunctionExecutor> {
        Box::new(self.clone())
    }
}

fn parser_ctx(manager: &EventFluxManager) -> ExpressionParserContext<'static> {
    let app_ctx = Arc::new(EventFluxAppContext::new(
        manager.eventflux_context(),
        "test_app".to_string(),
        Arc::new(EventFluxApp::new("test_app".to_string())),
        String::new(),
    ));

    let query_ctx = Arc::new(EventFluxQueryContext::new(
        Arc::clone(&app_ctx),
        "q".to_string(),
        None,
    ));

    ExpressionParserContext {
        eventflux_app_context: app_ctx,
        eventflux_query_context: query_ctx,
        stream_meta_map: HashMap::new(),
        table_meta_map: HashMap::new(),
        window_meta_map: HashMap::new(),
        aggregation_meta_map: HashMap::new(),
        state_meta_map: HashMap::new(),
        stream_positions: HashMap::new(),
        default_source: "default".to_string(),
        query_name: "q",
        is_mutation_context: false,
    }
}

#[test]
fn test_stateful_udf() {
    let manager = EventFluxManager::new();
    manager.add_scalar_function_factory(
        "statefulCount".to_string(),
        Box::new(StatefulCountFunction::new()),
    );

    let ctx = parser_ctx(&manager);
    let expr = Expression::function_no_ns("statefulCount".to_string(), vec![]);
    let exec = parse_expression(&expr, &ctx).unwrap();

    assert_eq!(exec.execute(None), Some(AttributeValue::Int(1)));
    assert_eq!(exec.execute(None), Some(AttributeValue::Int(2)));
}
