// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::query_api::definition::TriggerDefinition;
use eventflux::query_api::eventflux_app::EventFluxApp;
use eventflux::query_api::expression::constant::TimeUtil;
use std::thread::sleep;
use std::time::Duration;

#[tokio::test]
async fn start_trigger_emits_once() {
    let mut app = EventFluxApp::new("T1".to_string());
    app.add_trigger_definition(
        TriggerDefinition::id("TrigStream".to_string()).at("start".to_string()),
    );
    let runner = AppRunner::new_from_api(app, "TrigStream").await;
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
}

#[tokio::test]
async fn periodic_trigger_emits() {
    let mut app = EventFluxApp::new("T2".to_string());
    let trig = TriggerDefinition::id("PTStream".to_string())
        .at_every_time_constant(TimeUtil::millisec(50))
        .unwrap();
    app.add_trigger_definition(trig);
    let runner = AppRunner::new_from_api(app, "PTStream").await;
    sleep(Duration::from_millis(130));
    let out = runner.shutdown();
    assert!(out.len() >= 2);
}

#[tokio::test]
async fn cron_trigger_emits() {
    let mut app = EventFluxApp::new("T3".to_string());
    app.add_trigger_definition(
        TriggerDefinition::id("CronStream".to_string()).at("*/1 * * * * *".to_string()),
    );
    let runner = AppRunner::new_from_api(app, "CronStream").await;
    sleep(Duration::from_millis(2200));
    let out = runner.shutdown();
    assert!(out.len() >= 2);
}

/// Periodic trigger using SQL syntax
#[tokio::test]
async fn parse_periodic_trigger_emits() {
    let app = "CREATE TRIGGER PT AT EVERY 50 MILLISECONDS;";
    let runner = AppRunner::new(app, "PT").await;
    sleep(Duration::from_millis(130));
    let out = runner.shutdown();
    assert!(out.len() >= 2);
}

/// Cron trigger using SQL syntax
#[tokio::test]
async fn parse_cron_trigger_emits() {
    let app = "CREATE TRIGGER CronStr AT CRON '*/1 * * * * *';";
    let runner = AppRunner::new(app, "CronStr").await;
    sleep(Duration::from_millis(2200));
    let out = runner.shutdown();
    assert!(out.len() >= 2);
}
