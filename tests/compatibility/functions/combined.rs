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
//
// Combined Function Tests
// Tests that combine multiple function types

use crate::compatibility::common::AppRunner;
use eventflux::core::event::value::AttributeValue;

// ============================================================================
// FUNCTION COMBINATIONS
// ============================================================================

/// Combine multiple string functions
#[tokio::test]
async fn function_test_string_chain() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT upper(concat(text, '_suffix')) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("hello".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0][0],
        AttributeValue::String("HELLO_SUFFIX".to_string())
    );
}

/// Combine math and string functions
#[tokio::test]
async fn function_test_mixed_functions() {
    let app = "\
        CREATE STREAM inputStream (name STRING, score DOUBLE);\n\
        CREATE STREAM outputStream (report STRING, rounded DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT upper(name) AS report, round(score) AS rounded FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("alice".to_string()),
            AttributeValue::Double(85.6),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("ALICE".to_string()));
    assert_eq!(out[0][1], AttributeValue::Double(86.0));
}

/// Nested function calls
#[tokio::test]
async fn function_test_nested_calls() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT round(sqrt(value)) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(17.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // sqrt(17) ≈ 4.12, round = 4.0
    assert_eq!(out[0][0], AttributeValue::Double(4.0));
}
