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
// Trigger compatibility tests
// Reference: TriggerTestCase.java from the reference implementation

use super::common::AppRunner;
use eventflux::query_api::definition::TriggerDefinition;
use eventflux::query_api::eventflux_app::EventFluxApp;
use eventflux::query_api::expression::constant::TimeUtil;
use std::thread::sleep;
use std::time::Duration;

// ============================================================================
// TRIGGERS BUILT WITH THE API
// ============================================================================

/// Start trigger fires once when the app starts
/// Reference: TriggerTestCase.java testStartTrigger
#[tokio::test]
async fn trigger_test1_start() {
    let mut app = EventFluxApp::new("TriggerApp".to_string());
    app.add_trigger_definition(
        TriggerDefinition::id("StartTriggerStream".to_string()).at("start".to_string()),
    );
    let runner = AppRunner::new_from_api(app, "StartTriggerStream").await;
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
}

/// Periodic trigger fires every 50 ms
/// Reference: TriggerTestCase.java testPeriodicTrigger
#[tokio::test]
async fn trigger_test2_periodic() {
    let mut app = EventFluxApp::new("PeriodicApp".to_string());
    let trig = TriggerDefinition::id("PeriodicStream".to_string())
        .at_every_time_constant(TimeUtil::millisec(50))
        .unwrap();
    app.add_trigger_definition(trig);
    let runner = AppRunner::new_from_api(app, "PeriodicStream").await;
    sleep(Duration::from_millis(130));
    let out = runner.shutdown();
    // At least 2 events in 130 ms, at 50 ms and 100 ms
    assert!(out.len() >= 2);
}

/// Cron trigger fires every second
/// Reference: TriggerTestCase.java testCronTrigger
#[tokio::test]
async fn trigger_test3_cron() {
    let mut app = EventFluxApp::new("CronApp".to_string());
    app.add_trigger_definition(
        TriggerDefinition::id("CronStream".to_string()).at("*/1 * * * * *".to_string()),
    );
    let runner = AppRunner::new_from_api(app, "CronStream").await;
    sleep(Duration::from_millis(2200));
    let out = runner.shutdown();
    // At least 2 events in 2.2 seconds, one per second
    assert!(out.len() >= 2);
}

/// A start trigger and a periodic trigger in one app. Only the start trigger
/// stream is collected.
#[tokio::test]
async fn trigger_test4_multiple() {
    let mut app = EventFluxApp::new("MultiTriggerApp".to_string());
    app.add_trigger_definition(
        TriggerDefinition::id("StartStream".to_string()).at("start".to_string()),
    );
    let trig = TriggerDefinition::id("PeriodicStream".to_string())
        .at_every_time_constant(TimeUtil::millisec(40))
        .unwrap();
    app.add_trigger_definition(trig);
    let runner = AppRunner::new_from_api(app, "StartStream").await;
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();
    // The periodic trigger must not add events to the start trigger stream
    assert_eq!(out.len(), 1);
}

/// Periodic trigger with a 100 ms interval
#[tokio::test]
async fn trigger_test5_long_interval() {
    let mut app = EventFluxApp::new("LongIntervalApp".to_string());
    let trig = TriggerDefinition::id("LongIntervalStream".to_string())
        .at_every_time_constant(TimeUtil::millisec(100))
        .unwrap();
    app.add_trigger_definition(trig);
    let runner = AppRunner::new_from_api(app, "LongIntervalStream").await;
    sleep(Duration::from_millis(350));
    let out = runner.shutdown();
    // At least 3 events in 350 ms, at 100 ms, 200 ms and 300 ms
    assert!(out.len() >= 3);
}

// ============================================================================
// TRIGGERS DEFINED IN SQL
// ============================================================================

/// Start trigger defined in SQL
#[tokio::test]
async fn trigger_test6_sql_start() {
    let app = "CREATE TRIGGER StartTrigger AT START;";
    let runner = AppRunner::new(app, "StartTrigger").await;
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
}

/// Periodic trigger defined in SQL
#[tokio::test]
async fn trigger_test7_sql_periodic() {
    let app = "CREATE TRIGGER PeriodicTrigger AT EVERY 50 MILLISECONDS;";
    let runner = AppRunner::new(app, "PeriodicTrigger").await;
    sleep(Duration::from_millis(130));
    let out = runner.shutdown();
    assert!(out.len() >= 2);
}

/// Cron trigger defined in SQL
#[tokio::test]
async fn trigger_test8_sql_cron() {
    let app = "CREATE TRIGGER CronTrigger AT CRON '*/1 * * * * *';";
    let runner = AppRunner::new(app, "CronTrigger").await;
    sleep(Duration::from_millis(2200));
    let out = runner.shutdown();
    assert!(out.len() >= 2);
}

/// A query reads from a trigger with `SELECT ... FROM HeartbeatTrigger`, so trigger
/// events flow through the query into outputStream.
#[tokio::test]
async fn trigger_test9_with_query() {
    let app = "\
        CREATE TRIGGER HeartbeatTrigger AT EVERY 50 MILLISECONDS;\n\
        CREATE STREAM outputStream (timestamp BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT now() AS timestamp FROM HeartbeatTrigger;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    sleep(Duration::from_millis(130));
    let out = runner.shutdown();
    assert!(out.len() >= 2);
}

/// A periodic trigger next to a windowed SUM query. The query reads only from
/// inputStream, not from the trigger.
#[tokio::test]
async fn trigger_test10_batch_processing() {
    let app = "\
        CREATE STREAM inputStream (value INT);\n\
        CREATE TRIGGER BatchTrigger AT EVERY 100 MILLISECONDS;\n\
        CREATE STREAM outputStream (total BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT SUM(value) AS total FROM inputStream WINDOW('time', 100 MILLISECONDS);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();
    // No data was sent to inputStream, so the sliding time window gets no events
    // and must produce no output.
    // The 50 ms sleep is shorter than the 100 ms trigger interval to lower the
    // chance of a race.
    assert!(
        out.is_empty(),
        "Expected no output since no data was sent to inputStream, got {} events",
        out.len()
    );
}
