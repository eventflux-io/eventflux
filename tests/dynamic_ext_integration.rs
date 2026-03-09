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

// TODO: NOT PART OF M1 - Dynamic extension loading test
// This test uses old EventFluxQL syntax and tests dynamic extension loading.
// While extension system exists, SQL syntax for custom extensions is not in M1.
// M1 focuses on: Basic queries, Windows, Joins, GROUP BY, HAVING, ORDER BY, LIMIT
// Dynamic extension SQL syntax will be implemented in Phase 2.
// See feat/grammar/GRAMMAR_STATUS.md for M1 feature list.

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::eventflux_manager::EventFluxManager;

#[tokio::test]
#[ignore = "Dynamic extension SQL syntax not part of M1"]
async fn test_dynamic_extension_loading() {
    let manager = EventFluxManager::new();
    let lib_path = custom_dyn_ext::library_path();
    manager
        .set_extension("dynlib", lib_path.to_str().unwrap().to_string())
        .unwrap();

    let ctx = manager.eventflux_context();
    assert!(ctx.get_window_factory("dynWindow").is_some());
    assert!(ctx.get_scalar_function_factory("dynPlusOne").is_some());

    let app = "\
        define stream In (v int);\n\
        define stream Out (v int);\n\
        from In#window:dynWindow() select dynPlusOne(v) as v insert into Out;\n";
    let runner = AppRunner::new_with_manager(manager, app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Int(2)]]);
}
