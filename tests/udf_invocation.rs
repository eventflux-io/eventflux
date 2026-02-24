// SPDX-License-Identifier: MIT OR Apache-2.0

// TODO: NOT PART OF M1 - UDF invocation test uses old EventFluxQL syntax
// Test uses "define stream" which is not supported by SQL parser.
// See feat/grammar/GRAMMAR_STATUS.md for M1 feature list.

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::config::eventflux_app_context::EventFluxAppContext;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::eventflux_manager::EventFluxManager;
use eventflux::core::executor::expression_executor::ExpressionExecutor;
use eventflux::core::executor::function::scalar_function_executor::ScalarFunctionExecutor;
use eventflux::query_api::definition::attribute::Type as AttrType;
use eventflux::query_api::eventflux_app::EventFluxApp;
use std::sync::Arc;

#[derive(Debug, Default)]
struct PlusOneFn {
    arg: Option<Box<dyn ExpressionExecutor>>,
}

impl Clone for PlusOneFn {
    fn clone(&self) -> Self {
        Self { arg: None }
    }
}

impl ExpressionExecutor for PlusOneFn {
    fn execute(
        &self,
        event: Option<&dyn eventflux::core::event::complex_event::ComplexEvent>,
    ) -> Option<AttributeValue> {
        let v = self.arg.as_ref()?.execute(event)?;
        match v {
            AttributeValue::Int(i) => Some(AttributeValue::Int(i + 1)),
            _ => None,
        }
    }

    fn get_return_type(&self) -> AttrType {
        AttrType::INT
    }

    fn clone_executor(&self, _ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(self.clone())
    }
}

impl ScalarFunctionExecutor for PlusOneFn {
    fn init(
        &mut self,
        args: &Vec<Box<dyn ExpressionExecutor>>,
        ctx: &Arc<EventFluxAppContext>,
    ) -> Result<(), String> {
        if args.len() != 1 {
            return Err("plusOne expects one argument".to_string());
        }
        self.arg = Some(args[0].clone_executor(ctx));
        Ok(())
    }
    fn destroy(&mut self) {}
    fn get_name(&self) -> String {
        "plusOne".to_string()
    }
    fn clone_scalar_function(&self) -> Box<dyn ScalarFunctionExecutor> {
        Box::new(self.clone())
    }
}

#[tokio::test]
#[ignore = "Old EventFluxQL syntax not part of M1"]
async fn udf_invoked_in_query() {
    let mut manager = EventFluxManager::new();
    manager.add_scalar_function_factory("plusOne".to_string(), Box::new(PlusOneFn::default()));

    let app = "\
        define stream In (v int);\n\
        define stream Out (v int);\n\
        from In select plusOne(v) as v insert into Out;\n";
    let runner = AppRunner::new_with_manager(manager, app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Int(2)]]);
}
