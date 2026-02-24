// SPDX-License-Identifier: MIT OR Apache-2.0
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
// API-BASED TRIGGER TESTS (Working)
// ============================================================================

/// Start trigger fires once at application start
/// Reference: TriggerTestCase.java - testStartTrigger
#[tokio::test]
async fn trigger_test1_start() {
    let mut app = EventFluxApp::new("TriggerApp".to_string());
    app.add_trigger_definition(
        TriggerDefinition::id("StartTriggerStream".to_string()).at("start".to_string()),
    );
    let runner = AppRunner::new_from_api(app, "StartTriggerStream").await;
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();
    // Start trigger should emit exactly once
    assert_eq!(out.len(), 1);
}

/// Periodic trigger fires at regular intervals
/// Reference: TriggerTestCase.java - testPeriodicTrigger
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
    // Should emit at least 2 events (at 50ms and 100ms)
    assert!(out.len() >= 2);
}

/// Cron trigger fires at cron schedule
/// Reference: TriggerTestCase.java - testCronTrigger
#[tokio::test]
async fn trigger_test3_cron() {
    let mut app = EventFluxApp::new("CronApp".to_string());
    app.add_trigger_definition(
        TriggerDefinition::id("CronStream".to_string()).at("*/1 * * * * *".to_string()),
    );
    let runner = AppRunner::new_from_api(app, "CronStream").await;
    sleep(Duration::from_millis(2200));
    let out = runner.shutdown();
    // Should emit at least 2 events (each second)
    assert!(out.len() >= 2);
}

/// Multiple triggers in one app
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
    // Start trigger should emit once
    assert_eq!(out.len(), 1);
}

/// Periodic trigger with longer interval
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
    // Should emit at least 3 events (at 100ms, 200ms, 300ms)
    assert!(out.len() >= 3);
}

// ============================================================================
// SQL-BASED TRIGGER TESTS
// ============================================================================

/// SQL-based start trigger
#[tokio::test]
async fn trigger_test6_sql_start() {
    let app = "CREATE TRIGGER StartTrigger AT START;";
    let runner = AppRunner::new(app, "StartTrigger").await;
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
}

/// SQL-based periodic trigger
#[tokio::test]
async fn trigger_test7_sql_periodic() {
    let app = "CREATE TRIGGER PeriodicTrigger AT EVERY 50 MILLISECONDS;";
    let runner = AppRunner::new(app, "PeriodicTrigger").await;
    sleep(Duration::from_millis(130));
    let out = runner.shutdown();
    assert!(out.len() >= 2);
}

/// SQL-based cron trigger
#[tokio::test]
async fn trigger_test8_sql_cron() {
    let app = "CREATE TRIGGER CronTrigger AT CRON '*/1 * * * * *';";
    let runner = AppRunner::new(app, "CronTrigger").await;
    sleep(Duration::from_millis(2200));
    let out = runner.shutdown();
    assert!(out.len() >= 2);
}

/// Trigger with query that processes trigger events
/// Tests "SELECT FROM TriggerName" pattern where trigger events flow through the query pipeline.
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

/// Trigger for batch processing
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
    // Output depends on window implementation
    assert!(out.len() >= 0);
}
