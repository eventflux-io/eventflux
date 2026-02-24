// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[tokio::test]
async fn concurrent_sends() {
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out\n\
        SELECT v FROM In;\n";
    let runner = Arc::new(AppRunner::new(app, "Out").await);
    let mut handles = Vec::new();
    for i in 0..4 {
        let r = Arc::clone(&runner);
        handles.push(thread::spawn(move || {
            for k in 0..500 {
                r.send("In", vec![AttributeValue::Int(i * 500 + k)]);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    thread::sleep(Duration::from_millis(200));
    let runner = Arc::try_unwrap(runner).expect("arc");
    let out = runner.shutdown();
    assert_eq!(out.len(), 2000);
}
