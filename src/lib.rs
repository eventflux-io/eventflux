/*
 * Copyright 2025-2026 EventFlux.io
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

pub mod core;
pub mod query_api;
pub mod sql_compiler;

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    // query_api module items
    use crate::query_api::definition::{
        attribute::Type as ApiAttributeType, StreamDefinition as ApiStreamDefinition,
    };
    use crate::query_api::eventflux_app::EventFluxApp as ApiEventFluxApp;
    use crate::query_api::execution::{
        query::input::stream::{
            InputStream as ApiInputStream, SingleInputStream as ApiSingleInputStream,
        },
        query::output::OutputStream as ApiOutputStream,
        query::selection::{OutputAttribute as ApiOutputAttribute, Selector as ApiSelector},
        query::Query as ApiQuery,
        ExecutionElement as ApiExecutionElement,
    };
    use crate::query_api::expression::condition::compare::{
        Compare as ApiCompare, Operator as ApiCompareOperator,
    };
    use crate::query_api::expression::{
        constant::{Constant as ApiConstant, ConstantValueWithFloat as ApiConstantValue},
        variable::Variable as ApiVariable,
        Expression as ApiExpression,
    };

    // core module items
    use crate::core::config::eventflux_context::EventFluxContext;
    // Not needed. EventFluxAppRuntime::new builds the EventFluxAppContext itself.
    // use crate::core::config::eventflux_app_context::EventFluxAppContext;

    use crate::core::event::value::AttributeValue as CoreAttributeValue;
    use crate::core::eventflux_app_runtime::EventFluxAppRuntime;
    use crate::core::stream::output::stream_callback::LogStreamCallback;

    #[test]
    fn test_simple_filter_projection_query() {
        println!("Starting test_simple_filter_projection_query...");

        // a. Create the EventFluxContext. EventFluxAppRuntime::new builds the app context.
        let eventflux_context = Arc::new(EventFluxContext::new());

        // b. Build the query_api::EventFluxApp by hand
        let mut app_to_run = ApiEventFluxApp::new("TestApp".to_string());

        // Input stream. define stream InputStream (attribute1 int, attribute2 string);
        let input_stream_def = ApiStreamDefinition::new("InputStream".to_string())
            .attribute("attribute1".to_string(), ApiAttributeType::INT)
            .attribute("attribute2".to_string(), ApiAttributeType::STRING);
        app_to_run
            .stream_definition_map
            .insert("InputStream".to_string(), Arc::new(input_stream_def));

        // Output stream.
        // define stream OutputStream (projected_attr1 int, renamed_attr2 string);
        let output_stream_def = ApiStreamDefinition::new("OutputStream".to_string())
            .attribute("projected_attr1".to_string(), ApiAttributeType::INT)
            .attribute("renamed_attr2".to_string(), ApiAttributeType::STRING);
        app_to_run
            .stream_definition_map
            .insert("OutputStream".to_string(), Arc::new(output_stream_def));

        // The query being built:
        // FROM InputStream[attribute1 > 10]
        // SELECT attribute1 as projected_attr1, attribute2 as renamed_attr2
        // INSERT INTO OutputStream;

        // Filter attribute1 > 10
        let filter_condition = ApiExpression::Compare(Box::new(ApiCompare::new(
            ApiExpression::Variable(ApiVariable::new("attribute1".to_string())),
            ApiCompareOperator::GreaterThan,
            ApiExpression::Constant(ApiConstant::new(ApiConstantValue::Int(10))),
        )));

        // FROM InputStream[attribute1 > 10]
        let api_single_input_stream =
            ApiSingleInputStream::new_basic("InputStream".to_string(), false, false, None, {
                vec![
                    crate::query_api::execution::query::input::handler::StreamHandler::Filter(
                        crate::query_api::execution::query::input::handler::Filter::new(
                            filter_condition.clone(),
                        ),
                    ),
                ]
            });
        // The filter is passed as a StreamHandler::Filter in the stream handler list.
        // QueryParser reads it from there when it builds the runtime.
        let input_s = ApiInputStream::Single(api_single_input_stream);

        // SELECT attribute1 as projected_attr1, attribute2 as renamed_attr2
        let mut selector = ApiSelector::new();
        selector.selection_list = vec![
            ApiOutputAttribute::new(
                Some("projected_attr1".to_string()),
                ApiExpression::Variable(ApiVariable::new("attribute1".to_string())),
            ),
            ApiOutputAttribute::new(
                Some("renamed_attr2".to_string()),
                ApiExpression::Variable(ApiVariable::new("attribute2".to_string())),
            ),
        ];

        // INSERT INTO OutputStream
        // ApiOutputStream::new takes an OutputStreamAction and an Option<OutputEventType>.
        let insert_action =
            crate::query_api::execution::query::output::output_stream::InsertIntoStreamAction {
                target_id: "OutputStream".to_string(),
                is_inner_stream: false,
                is_fault_stream: false,
            };
        let output_s = ApiOutputStream::new(
            crate::query_api::execution::query::output::output_stream::OutputStreamAction::InsertInto(insert_action),
            None // let the runtime builder pick the default OutputEventType
        );

        let query = ApiQuery::query()
            .from(input_s)
            .select(selector)
            .out_stream(output_s);
        app_to_run
            .execution_element_list
            .push(ApiExecutionElement::Query(query));

        let runnable_api_app = Arc::new(app_to_run);

        // c. Create the EventFluxAppRuntime
        println!("Test: Creating EventFluxAppRuntime...");
        let runtime = EventFluxAppRuntime::new(runnable_api_app, eventflux_context, None)
            .expect("Test: Failed to create EventFluxAppRuntime");
        println!("Test: EventFluxAppRuntime created successfully.");

        // d. Print OutputStream events with a LogStreamCallback
        let callback = Box::new(LogStreamCallback::new("OutputStream".to_string()));
        runtime
            .add_callback("OutputStream", callback)
            .expect("Test: Failed to add callback to OutputStream");
        println!("Test: Callback added to OutputStream.");

        // e. Start the runtime
        runtime
            .start()
            .expect("Test: Failed to start EventFluxAppRuntime");
        println!("Test: EventFluxAppRuntime started.");

        // f. Get the InputHandler
        let input_handler = runtime
            .get_input_handler("InputStream")
            .expect("Test: Failed to get InputHandler for InputStream");
        println!("Test: InputHandler for InputStream obtained.");

        // g. Send three events. Only attribute1 values above 10 pass the filter.
        println!("Test: Sending events...");
        let event1_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let event1_data = vec![
            CoreAttributeValue::Int(20),
            CoreAttributeValue::String("event_one_val2".to_string()),
        ];
        input_handler
            .lock()
            .expect("input handler mutex")
            .send_event_with_timestamp(event1_ts, event1_data)
            .expect("Test: Failed to send event1");
        println!("Test: Sent event1 (attribute1=20, should pass filter)");

        let event2_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let event2_data = vec![
            CoreAttributeValue::Int(5),
            CoreAttributeValue::String("event_two_val2".to_string()),
        ];
        input_handler
            .lock()
            .expect("input handler mutex")
            .send_event_with_timestamp(event2_ts, event2_data)
            .expect("Test: Failed to send event2");
        println!("Test: Sent event2 (attribute1=5, should be filtered out)");

        let event3_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let event3_data = vec![
            CoreAttributeValue::Int(30),
            CoreAttributeValue::String("event_three_val2".to_string()),
        ];
        input_handler
            .lock()
            .expect("input handler mutex")
            .send_event_with_timestamp(event3_ts, event3_data)
            .expect("Test: Failed to send event3");
        println!("Test: Sent event3 (attribute1=30, should pass filter)");

        // Wait for async processing. Left off because this path runs synchronously.
        // std::thread::sleep(std::time::Duration::from_millis(100));

        // h. Shut down the runtime
        runtime.shutdown();
        println!("Test: EventFluxAppRuntime shutdown.");
        println!("Test: test_simple_filter_projection_query finished. Check console output for LogStreamCallback.");

        // i. Nothing is asserted. Check the console by eye.
        // Event 1 and event 3 should be printed and event 2 should not.
        // Each output event has projected_attr1 (int) and renamed_attr2 (string).
    }
}
