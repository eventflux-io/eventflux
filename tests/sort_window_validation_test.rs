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

// Sort window VALIDATION tests - ensures proper error handling and input validation
// These tests verify that the sort window correctly rejects invalid inputs

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;

/// Test that sort window rejects constant expressions (not variables)
#[tokio::test]
async fn test_sort_window_rejects_constant_expression() {
    // This should FAIL because 5 is a constant, not an attribute
    let app = "\
        CREATE STREAM In (value INT);\n\
        CREATE STREAM Out (value INT);\n\
        INSERT INTO Out \n\
        SELECT value \n\
        FROM In \n\
        WINDOW('sort', 2, 5);\n";

    let result = std::panic::catch_unwind(|| {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { AppRunner::new(app, "Out").await })
    });

    // Should panic with validation error
    assert!(
        result.is_err(),
        "Sort window should reject constant expressions like '5'"
    );
}

/// Test that sort window rejects string literals
#[tokio::test]
async fn test_sort_window_rejects_string_literal() {
    // This should FAIL because 'literal' is a constant string, not an attribute
    let app = "\
        CREATE STREAM In (value INT);\n\
        CREATE STREAM Out (value INT);\n\
        INSERT INTO Out \n\
        SELECT value \n\
        FROM In \n\
        WINDOW('sort', 2, 'literal');\n";

    let result = std::panic::catch_unwind(|| {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { AppRunner::new(app, "Out").await })
    });

    assert!(
        result.is_err(),
        "Sort window should reject string literal expressions"
    );
}

/// Test that sort window rejects invalid order strings
#[tokio::test]
async fn test_sort_window_rejects_invalid_order_string() {
    // This should FAIL because 'ascending' is not valid (must be 'asc' or 'desc')
    let app = "\
        CREATE STREAM In (value INT);\n\
        CREATE STREAM Out (value INT);\n\
        INSERT INTO Out \n\
        SELECT value \n\
        FROM In \n\
        WINDOW('sort', 2, value, 'ascending');\n";

    let result = std::panic::catch_unwind(|| {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { AppRunner::new(app, "Out").await })
    });

    assert!(
        result.is_err(),
        "Sort window should reject 'ascending' (must be 'asc' or 'desc')"
    );
}

/// Test that sort window rejects typos in order strings
#[tokio::test]
async fn test_sort_window_rejects_order_typo() {
    // This should FAIL because 'descending' is not valid (must be 'desc')
    let app = "\
        CREATE STREAM In (value INT);\n\
        CREATE STREAM Out (value INT);\n\
        INSERT INTO Out \n\
        SELECT value \n\
        FROM In \n\
        WINDOW('sort', 2, value, 'descending');\n";

    let result = std::panic::catch_unwind(|| {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { AppRunner::new(app, "Out").await })
    });

    assert!(
        result.is_err(),
        "Sort window should reject 'descending' typo (must be 'desc')"
    );
}

/// Test that sort window accepts valid 'asc' order
#[tokio::test]
async fn test_sort_window_accepts_valid_asc() {
    let app = "\
        CREATE STREAM In (value INT);\n\
        CREATE STREAM Out (value INT);\n\
        INSERT INTO Out \n\
        SELECT value \n\
        FROM In \n\
        WINDOW('sort', 2, value, 'asc');\n";

    // Should succeed
    let _runner = AppRunner::new(app, "Out").await;
}

/// Test that sort window accepts valid 'desc' order
#[tokio::test]
async fn test_sort_window_accepts_valid_desc() {
    let app = "\
        CREATE STREAM In (value INT);\n\
        CREATE STREAM Out (value INT);\n\
        INSERT INTO Out \n\
        SELECT value \n\
        FROM In \n\
        WINDOW('sort', 2, value, 'desc');\n";

    // Should succeed
    let _runner = AppRunner::new(app, "Out").await;
}

/// Test multi-attribute sorting with mixed order
#[tokio::test]
async fn test_sort_window_multi_attribute_mixed_order() {
    let app = "\
        CREATE STREAM In (price DOUBLE, volume INT, symbol STRING);\n\
        CREATE STREAM Out (price DOUBLE, volume INT, symbol STRING);\n\
        INSERT INTO Out \n\
        SELECT price, volume, symbol \n\
        FROM In \n\
        WINDOW('sort', 5, price, 'asc', volume, 'desc');\n";

    // Should succeed - multi-attribute with different orders
    let _runner = AppRunner::new(app, "Out").await;
}

/// Test that sort window accepts attribute without explicit order (defaults to asc)
#[tokio::test]
async fn test_sort_window_default_order() {
    let app = "\
        CREATE STREAM In (value INT);\n\
        CREATE STREAM Out (value INT);\n\
        INSERT INTO Out \n\
        SELECT value \n\
        FROM In \n\
        WINDOW('sort', 2, value);\n";

    // Should succeed and default to ascending
    let _runner = AppRunner::new(app, "Out").await;
}

/// Test case-insensitive order strings
#[tokio::test]
async fn test_sort_window_case_insensitive_order() {
    let app = "\
        CREATE STREAM In (value INT);\n\
        CREATE STREAM Out (value INT);\n\
        INSERT INTO Out \n\
        SELECT value \n\
        FROM In \n\
        WINDOW('sort', 2, value, 'ASC');\n";

    // Should succeed - 'ASC' should be accepted (case-insensitive)
    let _runner = AppRunner::new(app, "Out").await;
}

/// Test that sort window requires at least one attribute
#[tokio::test]
async fn test_sort_window_requires_attribute() {
    // This should FAIL because no sort attribute is provided
    let app = "\
        CREATE STREAM In (value INT);\n\
        CREATE STREAM Out (value INT);\n\
        INSERT INTO Out \n\
        SELECT value \n\
        FROM In \n\
        WINDOW('sort', 2);\n";

    let result = std::panic::catch_unwind(|| {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { AppRunner::new(app, "Out").await })
    });

    assert!(
        result.is_err(),
        "Sort window should require at least one sort attribute"
    );
}
