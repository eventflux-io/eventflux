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

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;
use std::thread;
use std::time::Duration;

// TODO: NOT PART OF M1. Partitions and the async pool are not in M1.
// This test is meant to check ordering in the async partition pool, which needs
// PARTITION syntax. M1 covers basic queries, windows, joins, GROUP BY, HAVING,
// ORDER BY and LIMIT. Partition support is planned for Phase 2.
// See feat/grammar/GRAMMAR.md for the grammar status.
#[tokio::test]
#[ignore = "Requires PARTITION support - Not part of M1"]
async fn async_partition_pool_order() {
    // Simplified for now. No partition, and it uses the old "define stream" syntax.
    let app = "\
        define stream In (v int, p string);\n\
        define stream Out (v int, p string);\n\
        from In select v, p insert into Out;\n";
    let runner = AppRunner::new(app, "Out").await;
    for i in 0..10 {
        // Only 10 events while the test is simplified
        let part = match i % 4 {
            0 => "a",
            1 => "b",
            2 => "c",
            _ => "d",
        };
        runner.send(
            "In",
            vec![
                AttributeValue::Int(i),
                AttributeValue::String(part.to_string()),
            ],
        );
    }
    thread::sleep(Duration::from_millis(100));
    let out = runner.shutdown();
    assert_eq!(out.len(), 10);
    // Order is not checked yet, only that the first event comes out first
    assert!(out[0][0] == AttributeValue::Int(0));
}
