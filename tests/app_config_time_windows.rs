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

//! Time based windows in an app that has a YAML `applications.<name>` entry (#223).
//!
//! Such an app is built through a different constructor than a plain one. That path
//! used to leave the app context without a scheduler, so time windows quietly
//! stopped doing time work.

use eventflux::core::event::value::AttributeValue;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;

/// Wall clock time in milliseconds. `AppRunner::send` uses timestamp 0, which puts a
/// delay window's due time in the past, so these tests send real timestamps.
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as i64
}

const SUM_OVER_TIME: &str = "\
    CREATE STREAM In (v INT);\n\
    CREATE STREAM Out (s LONG);\n\
    INSERT INTO Out SELECT sum(v) AS s FROM In WINDOW('time', 200 MILLISECONDS);\n";

/// The app context and the runtime share one scheduler.
#[tokio::test]
async fn app_config_runtime_has_context_scheduler() {
    let manager = common::manager_with_app_config("SchedApp", "{}");
    let runner = AppRunner::new_with_manager(manager, SUM_OVER_TIME, "Out").await;
    let runtime = runner.runtime();

    let ctx_scheduler = runtime
        .eventflux_app_context
        .get_scheduler()
        .expect("context must have a scheduler");
    let runtime_scheduler = runtime
        .scheduler
        .as_ref()
        .expect("runtime must have a scheduler");
    assert!(Arc::ptr_eq(&ctx_scheduler, runtime_scheduler));

    runner.shutdown();
}

/// The issue's repro. The 5 expires before the 7 arrives, so the sum goes 5, 0, 7 and
/// never reaches 12. Shutdown may also expire the 7 and add a trailing 0.
#[tokio::test]
async fn app_config_time_window_expires_events() {
    let manager = common::manager_with_app_config("TimeApp", "{}");
    let runner = AppRunner::new_with_manager(manager, SUM_OVER_TIME, "Out").await;

    runner.send_with_ts("In", now_ms(), vec![AttributeValue::Int(5)]);
    sleep(Duration::from_millis(500)).await;
    runner.send_with_ts("In", now_ms(), vec![AttributeValue::Int(7)]);
    let out = runner.shutdown();

    let expected = [5, 0, 7].map(|s| vec![AttributeValue::Long(s)]);
    assert!(out.starts_with(&expected), "got {out:?}");
}

/// A delay window holds events back instead of passing them on at once.
#[tokio::test]
async fn app_config_delay_window_delays_events() {
    let sql = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out SELECT v FROM In WINDOW('delay', 200);\n";
    let manager = common::manager_with_app_config("DelayApp", "{}");
    let runner = AppRunner::new_with_manager(manager, sql, "Out").await;

    runner.send_with_ts("In", now_ms(), vec![AttributeValue::Int(1)]);
    sleep(Duration::from_millis(50)).await;
    assert!(
        runner.collected.lock().unwrap().is_empty(),
        "the event must not come out before the delay"
    );

    sleep(Duration::from_millis(400)).await;
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Int(1)]]);
}
