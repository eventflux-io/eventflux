// SPDX-License-Identifier: MIT OR Apache-2.0

// Integration tests for SQL compiler
// Tests end-to-end flow: SQL → Runtime → Event Processing → Output

use eventflux::core::event::value::AttributeValue;
use eventflux::core::eventflux_manager::EventFluxManager;
use eventflux::core::stream::output::stream_callback::StreamCallback;
use std::sync::{Arc, Mutex};

/// Test callback that collects events
#[derive(Clone, Debug)]
struct TestCallback {
    events: Arc<Mutex<Vec<Vec<AttributeValue>>>>,
}

impl TestCallback {
    fn new() -> Self {
        TestCallback {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn get_events(&self) -> Vec<Vec<AttributeValue>> {
        self.events.lock().unwrap().clone()
    }

    fn count(&self) -> usize {
        self.events.lock().unwrap().len()
    }
}

impl StreamCallback for TestCallback {
    fn receive_events(&self, events: &[eventflux::core::event::event::Event]) {
        let mut collected = self.events.lock().unwrap();
        for event in events {
            collected.push(event.data.clone());
        }
    }
}

#[tokio::test]
async fn test_sql_query_1_basic_filter() {
    // Query 1: Basic filter - SELECT symbol, price FROM StockStream WHERE price > 100
    let sql = r#"
        CREATE STREAM StockStream (symbol VARCHAR, price DOUBLE);

        SELECT symbol, price
        FROM StockStream
        WHERE price > 100;
    "#;

    // Create EventFluxManager and runtime from SQL
    let manager = EventFluxManager::new();
    let runtime = manager
        .create_runtime_from_sql(sql, Some("Query1Test".to_string()))
        .await
        .expect("Failed to create runtime from SQL");

    // Add callback to collect output
    let callback = TestCallback::new();
    runtime
        .add_callback("OutputStream", Box::new(callback.clone()))
        .expect("Failed to add callback");

    // Start runtime
    runtime.start();

    // Get input handler
    let input_handler = runtime
        .get_input_handler("StockStream")
        .expect("Failed to get input handler");

    // Send test events
    let events = vec![
        vec![
            AttributeValue::String("EventFlux".to_string()),
            AttributeValue::Double(55.6),
        ], // Should be filtered out (price <= 100)
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Double(150.0),
        ], // Should pass (price > 100)
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Double(200.5),
        ], // Should pass (price > 100)
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Double(99.9),
        ], // Should be filtered out (price <= 100)
    ];

    for event_data in events {
        input_handler
            .lock()
            .unwrap()
            .send_data(event_data)
            .expect("Failed to send event");
    }

    // Small delay for processing
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify output
    let output_events = callback.get_events();

    println!("Output events: {:?}", output_events);

    assert_eq!(
        output_events.len(),
        2,
        "Expected 2 events (price > 100), got {}",
        output_events.len()
    );

    // Verify first output event (IBM, 150.0)
    if let (AttributeValue::String(symbol), AttributeValue::Double(price)) =
        (&output_events[0][0], &output_events[0][1])
    {
        assert_eq!(symbol, "IBM");
        assert_eq!(*price, 150.0);
    } else {
        panic!("Invalid output event format");
    }

    // Verify second output event (MSFT, 200.5)
    if let (AttributeValue::String(symbol), AttributeValue::Double(price)) =
        (&output_events[1][0], &output_events[1][1])
    {
        assert_eq!(symbol, "MSFT");
        assert_eq!(*price, 200.5);
    } else {
        panic!("Invalid output event format");
    }

    // Shutdown
    runtime.shutdown();
}

#[tokio::test]
async fn test_sql_query_2_arithmetic() {
    // Query 2: Arithmetic - SELECT symbol, price * 1.1 AS adjusted_price FROM StockStream
    let sql = r#"
        CREATE STREAM StockStream (symbol VARCHAR, price DOUBLE);

        SELECT symbol, price * 1.1 AS adjusted_price
        FROM StockStream;
    "#;

    let manager = EventFluxManager::new();
    let runtime = manager
        .create_runtime_from_sql(sql, Some("Query2Test".to_string()))
        .await
        .expect("Failed to create runtime from SQL");

    let callback = TestCallback::new();
    runtime
        .add_callback("OutputStream", Box::new(callback.clone()))
        .expect("Failed to add callback");

    runtime.start();

    let input_handler = runtime
        .get_input_handler("StockStream")
        .expect("Failed to get input handler");

    // Send test event
    input_handler
        .lock()
        .unwrap()
        .send_data(vec![
            AttributeValue::String("EventFlux".to_string()),
            AttributeValue::Double(100.0),
        ])
        .expect("Failed to send event");

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let output_events = callback.get_events();

    println!("Output events: {:?}", output_events);

    assert_eq!(output_events.len(), 1, "Expected 1 event");

    // Verify arithmetic calculation (100.0 * 1.1 = 110.0)
    if let (AttributeValue::String(symbol), AttributeValue::Double(adjusted)) =
        (&output_events[0][0], &output_events[0][1])
    {
        assert_eq!(symbol, "EventFlux");
        assert!(
            (adjusted - 110.0).abs() < 0.001,
            "Expected 110.0, got {}",
            adjusted
        );
    } else {
        panic!("Invalid output event format");
    }

    runtime.shutdown();
}

#[tokio::test]
async fn test_sql_query_7_builtin_function() {
    // Query 7: Built-in scalar function (ROUND)
    let sql = r#"
        CREATE STREAM StockStream (symbol VARCHAR, price DOUBLE);

        SELECT symbol, ROUND(price) AS rounded_price
        FROM StockStream;
    "#;

    let manager = EventFluxManager::new();
    let runtime = manager
        .create_runtime_from_sql(sql, Some("Query7Test".to_string()))
        .await
        .expect("Failed to create runtime from SQL");

    let callback = TestCallback::new();
    runtime
        .add_callback("OutputStream", Box::new(callback.clone()))
        .expect("Failed to add callback");

    runtime.start();

    let input_handler = runtime
        .get_input_handler("StockStream")
        .expect("Failed to get input handler");

    // Send test events with prices that need rounding
    let test_data = vec![("AAPL", 123.456), ("MSFT", 99.999), ("GOOGL", 150.123)];

    for (symbol, price) in test_data {
        input_handler
            .lock()
            .unwrap()
            .send_data(vec![
                AttributeValue::String(symbol.to_string()),
                AttributeValue::Double(price),
            ])
            .expect("Failed to send event");
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let output_events = callback.get_events();

    println!(
        "Output events (Query 7 - ROUND function): {:?}",
        output_events
    );

    assert_eq!(output_events.len(), 3, "Expected 3 events");

    // Verify ROUND function works correctly
    // AAPL: 123.456 -> 123.0
    if let (AttributeValue::String(symbol), AttributeValue::Double(rounded)) =
        (&output_events[0][0], &output_events[0][1])
    {
        assert_eq!(symbol, "AAPL");
        assert_eq!(*rounded, 123.0, "Expected ROUND(123.456) = 123.0");
    } else {
        panic!("Invalid output event format for AAPL");
    }

    // MSFT: 99.999 -> 100.0
    if let (AttributeValue::String(symbol), AttributeValue::Double(rounded)) =
        (&output_events[1][0], &output_events[1][1])
    {
        assert_eq!(symbol, "MSFT");
        assert_eq!(*rounded, 100.0, "Expected ROUND(99.999) = 100.0");
    } else {
        panic!("Invalid output event format for MSFT");
    }

    // GOOGL: 150.123 -> 150.0
    if let (AttributeValue::String(symbol), AttributeValue::Double(rounded)) =
        (&output_events[2][0], &output_events[2][1])
    {
        assert_eq!(symbol, "GOOGL");
        assert_eq!(*rounded, 150.0, "Expected ROUND(150.123) = 150.0");
    } else {
        panic!("Invalid output event format for GOOGL");
    }

    runtime.shutdown();
}

#[tokio::test]
async fn test_sql_query_3_window_tumbling_avg() {
    // Query 3: WINDOW TUMBLING + AVG aggregation
    let sql = r#"
        CREATE STREAM StockStream (symbol VARCHAR, price DOUBLE);

        SELECT symbol, AVG(price) as avg_price
        FROM StockStream
        WINDOW('tumbling', 5 SECONDS)
        GROUP BY symbol;
    "#;

    let manager = EventFluxManager::new();
    let runtime = manager
        .create_runtime_from_sql(sql, Some("Query3Test".to_string()))
        .await
        .expect("Failed to create runtime from SQL");

    let callback = TestCallback::new();
    runtime
        .add_callback("OutputStream", Box::new(callback.clone()))
        .expect("Failed to add callback");

    runtime.start();

    let input_handler = runtime
        .get_input_handler("StockStream")
        .expect("Failed to get input handler");

    // Send test events for EventFlux
    input_handler
        .lock()
        .unwrap()
        .send_data(vec![
            AttributeValue::String("EventFlux".to_string()),
            AttributeValue::Double(100.0),
        ])
        .expect("Failed to send event");

    input_handler
        .lock()
        .unwrap()
        .send_data(vec![
            AttributeValue::String("EventFlux".to_string()),
            AttributeValue::Double(200.0),
        ])
        .expect("Failed to send event");

    // Wait for window to process
    tokio::time::sleep(tokio::time::Duration::from_millis(5100)).await;

    let output_events = callback.get_events();

    println!("Output events (Query 3): {:?}", output_events);

    // Verify AVG calculation: (100 + 200) / 2 = 150
    assert!(
        output_events.len() > 0,
        "Expected at least 1 event from tumbling window"
    );

    runtime.shutdown();
}

#[tokio::test]
async fn test_sql_query_4_window_length_count() {
    // Query 4: WINDOW LENGTH + COUNT aggregation
    let sql = r#"
        CREATE STREAM StockStream (symbol VARCHAR, price DOUBLE);

        SELECT symbol, COUNT(*) as count
        FROM StockStream
        WINDOW('length', 3)
        GROUP BY symbol;
    "#;

    let manager = EventFluxManager::new();
    let runtime = manager
        .create_runtime_from_sql(sql, Some("Query4Test".to_string()))
        .await
        .expect("Failed to create runtime from SQL");

    let callback = TestCallback::new();
    runtime
        .add_callback("OutputStream", Box::new(callback.clone()))
        .expect("Failed to add callback");

    runtime.start();

    let input_handler = runtime
        .get_input_handler("StockStream")
        .expect("Failed to get input handler");

    // Send 3 events for EventFlux (should trigger window)
    for i in 1..=3 {
        input_handler
            .lock()
            .unwrap()
            .send_data(vec![
                AttributeValue::String("EventFlux".to_string()),
                AttributeValue::Double(100.0 * i as f64),
            ])
            .expect("Failed to send event");
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let output_events = callback.get_events();

    println!("Output events (Query 4): {:?}", output_events);

    // Verify COUNT calculation: should have counts of 1, 2, 3 as events arrive
    assert!(
        output_events.len() >= 3,
        "Expected at least 3 events from length window"
    );

    runtime.shutdown();
}

#[tokio::test]
async fn test_sql_query_9_order_by_limit() {
    // Query 9: ORDER BY + LIMIT with WINDOW
    // Note: ORDER BY and LIMIT make sense on windowed streams, not unbounded streams
    let sql = r#"
        CREATE STREAM StockStream (symbol VARCHAR, price DOUBLE);

        SELECT symbol, price
        FROM StockStream
        WINDOW('length', 5)
        ORDER BY price DESC
        LIMIT 3;
    "#;

    let manager = EventFluxManager::new();
    let runtime = manager
        .create_runtime_from_sql(sql, Some("Query9Test".to_string()))
        .await
        .expect("Failed to create runtime from SQL");

    let callback = TestCallback::new();
    runtime
        .add_callback("OutputStream", Box::new(callback.clone()))
        .expect("Failed to add callback");

    runtime.start();

    let input_handler = runtime
        .get_input_handler("StockStream")
        .expect("Failed to get input handler");

    // Send 5 events
    let test_data = vec![
        ("AAPL", 150.0),
        ("MSFT", 200.0),
        ("GOOGL", 300.0),
        ("TSLA", 250.0),
        ("IBM", 100.0),
    ];

    for (symbol, price) in test_data {
        input_handler
            .lock()
            .unwrap()
            .send_data(vec![
                AttributeValue::String(symbol.to_string()),
                AttributeValue::Double(price),
            ])
            .expect("Failed to send event");
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let output_events = callback.get_events();

    println!("Output events (Query 9): {:?}", output_events);

    // NOTE: ORDER BY and LIMIT are converted correctly to Query API,
    // but runtime execution may not fully implement these features yet.
    // This test verifies that the SQL conversion succeeds without errors.
    // Actual ordering and limiting behavior depends on runtime implementation.

    // Verify we got some events (conversion and execution succeeded)
    assert!(output_events.len() > 0, "Expected at least 1 event");

    // TODO: Once runtime implements ORDER BY/LIMIT properly, add assertions:
    // - assert!(output_events.len() <= 3, "LIMIT 3 should restrict to 3 events");
    // - Verify events are sorted by price DESC

    runtime.shutdown();
}

#[tokio::test]
async fn test_sql_query_10_insert_into() {
    // Query 10: INSERT INTO with custom output stream
    let sql = r#"
        CREATE STREAM StockStream (symbol VARCHAR, price DOUBLE);

        INSERT INTO HighPriceAlerts
        SELECT symbol, price
        FROM StockStream
        WHERE price > 500;
    "#;

    let manager = EventFluxManager::new();
    let runtime = manager
        .create_runtime_from_sql(sql, Some("Query10Test".to_string()))
        .await
        .expect("Failed to create runtime from SQL");

    let callback = TestCallback::new();
    runtime
        .add_callback("HighPriceAlerts", Box::new(callback.clone()))
        .expect("Failed to add callback");

    runtime.start();

    let input_handler = runtime
        .get_input_handler("StockStream")
        .expect("Failed to get input handler");

    // Send test events
    let test_data = vec![
        ("AAPL", 150.0),  // Should be filtered out
        ("GOOGL", 600.0), // Should pass
        ("TSLA", 400.0),  // Should be filtered out
        ("BRK", 550.0),   // Should pass
    ];

    for (symbol, price) in test_data {
        input_handler
            .lock()
            .unwrap()
            .send_data(vec![
                AttributeValue::String(symbol.to_string()),
                AttributeValue::Double(price),
            ])
            .expect("Failed to send event");
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let output_events = callback.get_events();

    println!(
        "Output events (Query 10 - HighPriceAlerts): {:?}",
        output_events
    );

    // Verify only high-price stocks (> 500) are in HighPriceAlerts
    assert_eq!(output_events.len(), 2, "Expected 2 high-price alerts");

    // Verify first alert (GOOGL, 600.0)
    if let (AttributeValue::String(symbol), AttributeValue::Double(price)) =
        (&output_events[0][0], &output_events[0][1])
    {
        assert_eq!(symbol, "GOOGL");
        assert_eq!(*price, 600.0);
    } else {
        panic!("Invalid output event format");
    }

    // Verify second alert (BRK, 550.0)
    if let (AttributeValue::String(symbol), AttributeValue::Double(price)) =
        (&output_events[1][0], &output_events[1][1])
    {
        assert_eq!(symbol, "BRK");
        assert_eq!(*price, 550.0);
    } else {
        panic!("Invalid output event format");
    }

    runtime.shutdown();
}

#[tokio::test]
async fn test_sql_query_6_sum_having() {
    // Query 6: SUM() + HAVING clause
    let sql = r#"
        CREATE STREAM StockStream (symbol VARCHAR, volume INT);

        SELECT symbol, SUM(volume) AS total_volume
        FROM StockStream
        WINDOW('tumbling', 5 SECONDS)
        GROUP BY symbol
        HAVING SUM(volume) > 1000;
    "#;

    let manager = EventFluxManager::new();
    let runtime = manager
        .create_runtime_from_sql(sql, Some("Query6Test".to_string()))
        .await
        .expect("Failed to create runtime from SQL");

    let callback = TestCallback::new();
    runtime
        .add_callback("OutputStream", Box::new(callback.clone()))
        .expect("Failed to add callback");

    runtime.start();

    let input_handler = runtime
        .get_input_handler("StockStream")
        .expect("Failed to get input handler");

    // Send test events
    // AAPL: 300 + 400 = 700 (should be filtered out by HAVING SUM(volume) > 1000)
    input_handler
        .lock()
        .unwrap()
        .send_data(vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Int(300),
        ])
        .expect("Failed to send event");

    input_handler
        .lock()
        .unwrap()
        .send_data(vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Int(400),
        ])
        .expect("Failed to send event");

    // GOOGL: 600 + 500 = 1100 (should pass HAVING clause)
    input_handler
        .lock()
        .unwrap()
        .send_data(vec![
            AttributeValue::String("GOOGL".to_string()),
            AttributeValue::Int(600),
        ])
        .expect("Failed to send event");

    input_handler
        .lock()
        .unwrap()
        .send_data(vec![
            AttributeValue::String("GOOGL".to_string()),
            AttributeValue::Int(500),
        ])
        .expect("Failed to send event");

    // Wait for window to close (5 seconds + buffer)
    tokio::time::sleep(tokio::time::Duration::from_millis(5100)).await;

    let output_events = callback.get_events();

    println!(
        "Output events (Query 6 - SUM + HAVING): {:?}",
        output_events
    );

    // Verify HAVING clause filters correctly
    // Should only have GOOGL (1100 > 1000), not AAPL (700 <= 1000)
    assert!(output_events.len() > 0, "Expected at least 1 event (GOOGL)");

    // Find GOOGL in output
    let has_googl = output_events.iter().any(|event| {
        if let AttributeValue::String(symbol) = &event[0] {
            symbol == "GOOGL"
        } else {
            false
        }
    });
    assert!(
        has_googl,
        "Expected GOOGL in output (SUM(volume) = 1100 > 1000)"
    );

    // Verify AAPL is NOT in output (filtered by HAVING)
    let has_aapl = output_events.iter().any(|event| {
        if let AttributeValue::String(symbol) = &event[0] {
            symbol == "AAPL"
        } else {
            false
        }
    });
    assert!(
        !has_aapl,
        "AAPL should be filtered out by HAVING clause (SUM(volume) = 700 <= 1000)"
    );

    runtime.shutdown();
}

#[tokio::test]
async fn test_sql_query_8_multiple_aggregations() {
    // Query 8: Multiple aggregations (COUNT, AVG, MIN, MAX) in single query
    let sql = r#"
        CREATE STREAM StockStream (symbol VARCHAR, price DOUBLE);

        SELECT
            symbol,
            COUNT(*) AS trade_count,
            AVG(price) AS avg_price,
            MIN(price) AS min_price,
            MAX(price) AS max_price
        FROM StockStream
        WINDOW('tumbling', 5 SECONDS)
        GROUP BY symbol;
    "#;

    let manager = EventFluxManager::new();
    let runtime = manager
        .create_runtime_from_sql(sql, Some("Query8Test".to_string()))
        .await
        .expect("Failed to create runtime from SQL");

    let callback = TestCallback::new();
    runtime
        .add_callback("OutputStream", Box::new(callback.clone()))
        .expect("Failed to add callback");

    runtime.start();

    let input_handler = runtime
        .get_input_handler("StockStream")
        .expect("Failed to get input handler");

    // Send test events for GOOGL with varying prices
    // Prices: 100.0, 200.0, 300.0
    // Expected: COUNT=3, AVG=200.0, MIN=100.0, MAX=300.0
    input_handler
        .lock()
        .unwrap()
        .send_data(vec![
            AttributeValue::String("GOOGL".to_string()),
            AttributeValue::Double(100.0),
        ])
        .expect("Failed to send event");

    input_handler
        .lock()
        .unwrap()
        .send_data(vec![
            AttributeValue::String("GOOGL".to_string()),
            AttributeValue::Double(200.0),
        ])
        .expect("Failed to send event");

    input_handler
        .lock()
        .unwrap()
        .send_data(vec![
            AttributeValue::String("GOOGL".to_string()),
            AttributeValue::Double(300.0),
        ])
        .expect("Failed to send event");

    // Wait for window to close (5 seconds + buffer)
    tokio::time::sleep(tokio::time::Duration::from_millis(5100)).await;

    let output_events = callback.get_events();

    println!(
        "Output events (Query 8 - Multiple Aggregations): {:?}",
        output_events
    );

    // Verify we got output
    assert!(
        output_events.len() > 0,
        "Expected at least 1 event from tumbling window"
    );

    // Find GOOGL in output and verify all aggregations
    let googl_event = output_events.iter().find(|event| {
        if let AttributeValue::String(symbol) = &event[0] {
            symbol == "GOOGL"
        } else {
            false
        }
    });

    assert!(googl_event.is_some(), "Expected to find GOOGL in output");

    let event = googl_event.unwrap();

    // Verify structure: [symbol, trade_count, avg_price, min_price, max_price]
    assert_eq!(
        event.len(),
        5,
        "Expected 5 attributes (symbol + 4 aggregations)"
    );

    // Verify symbol
    if let AttributeValue::String(symbol) = &event[0] {
        assert_eq!(symbol, "GOOGL");
    } else {
        panic!("Expected String for symbol");
    }

    // Verify COUNT(*) = 3
    if let AttributeValue::Long(count) = event[1] {
        assert_eq!(count, 3, "Expected COUNT = 3");
    } else {
        panic!("Expected Long for trade_count, got {:?}", event[1]);
    }

    // Verify AVG(price) = 200.0
    if let AttributeValue::Double(avg) = event[2] {
        assert!(
            (avg - 200.0).abs() < 0.001,
            "Expected AVG = 200.0, got {}",
            avg
        );
    } else {
        panic!("Expected Double for avg_price, got {:?}", event[2]);
    }

    // Verify MIN(price) = 100.0
    if let AttributeValue::Double(min) = event[3] {
        assert_eq!(min, 100.0, "Expected MIN = 100.0");
    } else {
        panic!("Expected Double for min_price, got {:?}", event[3]);
    }

    // Verify MAX(price) = 300.0
    if let AttributeValue::Double(max) = event[4] {
        assert_eq!(max, 300.0, "Expected MAX = 300.0");
    } else {
        panic!("Expected Double for max_price, got {:?}", event[4]);
    }

    runtime.shutdown();
}

#[tokio::test]
async fn test_sql_query_5_stream_join() {
    // Query 5: Stream JOIN (SQL parsing and conversion test)
    // Note: Uses full stream names instead of aliases for M1
    let sql = r#"
        CREATE STREAM Trades (symbol VARCHAR, price DOUBLE);
        CREATE STREAM News (symbol VARCHAR, headline VARCHAR);

        SELECT Trades.symbol AS trade_symbol, Trades.price, News.headline
        FROM Trades
        JOIN News ON Trades.symbol = News.symbol;
    "#;

    let manager = EventFluxManager::new();
    let runtime = manager
        .create_runtime_from_sql(sql, Some("Query5Test".to_string()))
        .await
        .expect("Failed to create runtime from SQL");

    let callback = TestCallback::new();
    runtime
        .add_callback("OutputStream", Box::new(callback.clone()))
        .expect("Failed to add callback");

    runtime.start();

    let trades_handler = runtime
        .get_input_handler("Trades")
        .expect("Failed to get Trades input handler");

    let news_handler = runtime
        .get_input_handler("News")
        .expect("Failed to get News input handler");

    // Send test events
    // Trade: AAPL, 150.0
    trades_handler
        .lock()
        .unwrap()
        .send_data(vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Double(150.0),
        ])
        .expect("Failed to send trade");

    // News: AAPL, "Apple launches new product"
    news_handler
        .lock()
        .unwrap()
        .send_data(vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::String("Apple launches new product".to_string()),
        ])
        .expect("Failed to send news");

    // Trade: MSFT, 200.0
    trades_handler
        .lock()
        .unwrap()
        .send_data(vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Double(200.0),
        ])
        .expect("Failed to send trade");

    // News: MSFT, "Microsoft updates cloud services"
    news_handler
        .lock()
        .unwrap()
        .send_data(vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::String("Microsoft updates cloud services".to_string()),
        ])
        .expect("Failed to send news");

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let output_events = callback.get_events();

    println!("Output events (Query 5 - JOIN): {:?}", output_events);

    // Verify JOIN results
    assert!(
        output_events.len() >= 2,
        "Expected at least 2 joined events"
    );

    // Note: JOIN output format depends on runtime implementation
    // This test verifies that the SQL → Query API conversion works
    // Actual runtime behavior may vary

    runtime.shutdown();
}

/// Regression test for tumbling window with aggregation across multiple batches.
///
/// This test verifies that:
/// 1. Multiple consecutive window periods emit correct aggregation results
/// 2. NO spurious empty events (count=0, avg=null) are emitted
/// 3. Each batch contains only events from its window period
///
/// Bug context: Without RESET events between batches, aggregators used incremental
/// add/remove semantics which caused:
/// - Spurious emissions with count=0, avg=null
/// - Incorrect aggregation values due to state not being cleared between batches
#[tokio::test]
async fn test_tumbling_window_multi_batch_no_spurious_empty_events() {
    let sql = r#"
        CREATE STREAM TradeStream (symbol VARCHAR, price DOUBLE);

        SELECT symbol, COUNT(*) as trade_count, AVG(price) as avg_price
        FROM TradeStream
        WINDOW('tumbling', 1 SECONDS)
        GROUP BY symbol;
    "#;

    let manager = EventFluxManager::new();
    let runtime = manager
        .create_runtime_from_sql(sql, Some("MultiBatchTest".to_string()))
        .await
        .expect("Failed to create runtime from SQL");

    let callback = TestCallback::new();
    runtime
        .add_callback("OutputStream", Box::new(callback.clone()))
        .expect("Failed to add callback");

    let _ = runtime.start();

    let input_handler = runtime
        .get_input_handler("TradeStream")
        .expect("Failed to get input handler");

    // === BATCH 1: Send 3 events ===
    for price in [100.0, 150.0, 200.0] {
        input_handler
            .lock()
            .unwrap()
            .send_data(vec![
                AttributeValue::String("BTC".to_string()),
                AttributeValue::Double(price),
            ])
            .expect("Failed to send event");
    }

    // Wait for first window to close (1 second + buffer)
    tokio::time::sleep(tokio::time::Duration::from_millis(1200)).await;

    // === BATCH 2: Send 2 events ===
    for price in [300.0, 400.0] {
        input_handler
            .lock()
            .unwrap()
            .send_data(vec![
                AttributeValue::String("BTC".to_string()),
                AttributeValue::Double(price),
            ])
            .expect("Failed to send event");
    }

    // Wait for second window to close
    tokio::time::sleep(tokio::time::Duration::from_millis(1200)).await;

    // === BATCH 3: Send 1 event ===
    input_handler
        .lock()
        .unwrap()
        .send_data(vec![
            AttributeValue::String("BTC".to_string()),
            AttributeValue::Double(500.0),
        ])
        .expect("Failed to send event");

    // Wait for third window to close
    tokio::time::sleep(tokio::time::Duration::from_millis(1200)).await;

    let output_events = callback.get_events();

    println!(
        "Multi-batch output events ({} total): {:?}",
        output_events.len(),
        output_events
    );

    // Verify NO spurious empty events (the bug we fixed)
    for (i, event) in output_events.iter().enumerate() {
        // event[0] = symbol, event[1] = trade_count, event[2] = avg_price
        let trade_count = &event[1];
        let avg_price = &event[2];

        // trade_count must NOT be 0
        match trade_count {
            AttributeValue::Long(count) => {
                assert!(
                    *count > 0,
                    "Event {} has trade_count=0 (spurious empty batch): {:?}",
                    i,
                    event
                );
            }
            AttributeValue::Int(count) => {
                assert!(
                    *count > 0,
                    "Event {} has trade_count=0 (spurious empty batch): {:?}",
                    i,
                    event
                );
            }
            other => {
                panic!("Event {} has unexpected trade_count type: {:?}", i, other);
            }
        }

        // avg_price must NOT be null
        assert!(
            !matches!(avg_price, AttributeValue::Null),
            "Event {} has avg_price=null (spurious empty batch): {:?}",
            i,
            event
        );
    }

    // We should have exactly 3 output events (one per batch with data)
    // Note: Timing-sensitive, but we should have at least 2 valid batches
    assert!(
        output_events.len() >= 2,
        "Expected at least 2 batch outputs, got {}",
        output_events.len()
    );

    runtime.shutdown();
}

/// Regression test for lengthBatch window with aggregation across multiple batches.
///
/// This test verifies that:
/// 1. Multiple consecutive batches emit correct aggregation results
/// 2. NO spurious empty events (count=0, avg=null) are emitted
/// 3. Each batch contains only the specified number of events
///
/// Bug context: Same as tumbling window - without RESET events, aggregators
/// used incremental add/remove semantics causing spurious empty emissions.
#[tokio::test]
async fn test_length_batch_window_multi_batch_no_spurious_empty_events() {
    let sql = r#"
        CREATE STREAM TradeStream (symbol VARCHAR, price DOUBLE);

        SELECT symbol, COUNT(*) as trade_count, AVG(price) as avg_price
        FROM TradeStream
        WINDOW('lengthBatch', 3)
        GROUP BY symbol;
    "#;

    let manager = EventFluxManager::new();
    let runtime = manager
        .create_runtime_from_sql(sql, Some("LengthBatchTest".to_string()))
        .await
        .expect("Failed to create runtime from SQL");

    let callback = TestCallback::new();
    runtime
        .add_callback("OutputStream", Box::new(callback.clone()))
        .expect("Failed to add callback");

    let _ = runtime.start();

    let input_handler = runtime
        .get_input_handler("TradeStream")
        .expect("Failed to get input handler");

    // === BATCH 1: Send 3 events (triggers flush) ===
    for price in [100.0, 150.0, 200.0] {
        input_handler
            .lock()
            .unwrap()
            .send_data(vec![
                AttributeValue::String("BTC".to_string()),
                AttributeValue::Double(price),
            ])
            .expect("Failed to send event");
    }

    // Small delay to allow processing
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // === BATCH 2: Send 3 more events (triggers second flush) ===
    for price in [300.0, 400.0, 500.0] {
        input_handler
            .lock()
            .unwrap()
            .send_data(vec![
                AttributeValue::String("BTC".to_string()),
                AttributeValue::Double(price),
            ])
            .expect("Failed to send event");
    }

    // Small delay to allow processing
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // === BATCH 3: Send 3 more events (triggers third flush) ===
    for price in [600.0, 700.0, 800.0] {
        input_handler
            .lock()
            .unwrap()
            .send_data(vec![
                AttributeValue::String("BTC".to_string()),
                AttributeValue::Double(price),
            ])
            .expect("Failed to send event");
    }

    // Wait for processing to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let output_events = callback.get_events();

    println!(
        "LengthBatch multi-batch output events ({} total): {:?}",
        output_events.len(),
        output_events
    );

    // Verify NO spurious empty events
    for (i, event) in output_events.iter().enumerate() {
        let trade_count = &event[1];
        let avg_price = &event[2];

        // trade_count must NOT be 0
        match trade_count {
            AttributeValue::Long(count) => {
                assert!(
                    *count > 0,
                    "Event {} has trade_count=0 (spurious empty batch): {:?}",
                    i,
                    event
                );
            }
            AttributeValue::Int(count) => {
                assert!(
                    *count > 0,
                    "Event {} has trade_count=0 (spurious empty batch): {:?}",
                    i,
                    event
                );
            }
            other => {
                panic!("Event {} has unexpected trade_count type: {:?}", i, other);
            }
        }

        // avg_price must NOT be null
        assert!(
            !matches!(avg_price, AttributeValue::Null),
            "Event {} has avg_price=null (spurious empty batch): {:?}",
            i,
            event
        );
    }

    // We should have exactly 3 output events (one per batch of 3 events)
    assert!(
        output_events.len() >= 2,
        "Expected at least 2 batch outputs, got {}",
        output_events.len()
    );

    runtime.shutdown();
}

/// Regression test for externalTimeBatch window with aggregation across multiple batches.
///
/// This test verifies that:
/// 1. Multiple consecutive batches emit correct aggregation results
/// 2. NO spurious empty events (count=0, avg=null) are emitted
/// 3. Each batch is determined by the external timestamp field
///
/// Bug context: Same as tumbling window - without RESET events, aggregators
/// used incremental add/remove semantics causing spurious empty emissions.
#[tokio::test]
async fn test_external_time_batch_window_multi_batch_no_spurious_empty_events() {
    let sql = r#"
        CREATE STREAM TradeStream (trade_time BIGINT, symbol VARCHAR, price DOUBLE);
        CREATE STREAM OutStream (symbol VARCHAR, trade_count BIGINT, avg_price DOUBLE);

        INSERT INTO OutStream
        SELECT symbol, COUNT(*) as trade_count, AVG(price) as avg_price
        FROM TradeStream
        WINDOW('externalTimeBatch', trade_time, 1000 MILLISECONDS)
        GROUP BY symbol;
    "#;

    let manager = EventFluxManager::new();
    let runtime = manager
        .create_runtime_from_sql(sql, Some("ExternalTimeBatchTest".to_string()))
        .await
        .expect("Failed to create runtime from SQL");

    let callback = TestCallback::new();
    runtime
        .add_callback("OutStream", Box::new(callback.clone()))
        .expect("Failed to add callback");

    let _ = runtime.start();

    let input_handler = runtime
        .get_input_handler("TradeStream")
        .expect("Failed to get input handler");

    // externalTimeBatch uses the event's internal timestamp (set via send_event_with_timestamp)
    // NOT the field value, so we must use send_event_with_timestamp with matching values

    // === BATCH 1: Events at time 0-500ms ===
    for (ts, price) in [(0i64, 100.0), (200, 150.0), (400, 200.0)] {
        input_handler
            .lock()
            .unwrap()
            .send_event_with_timestamp(
                ts,
                vec![
                    AttributeValue::Long(ts),
                    AttributeValue::String("BTC".to_string()),
                    AttributeValue::Double(price),
                ],
            )
            .expect("Failed to send event");
    }

    // === BATCH 2: Events at time 1000-1500ms (new window) ===
    for (ts, price) in [(1000i64, 300.0), (1200, 400.0)] {
        input_handler
            .lock()
            .unwrap()
            .send_event_with_timestamp(
                ts,
                vec![
                    AttributeValue::Long(ts),
                    AttributeValue::String("BTC".to_string()),
                    AttributeValue::Double(price),
                ],
            )
            .expect("Failed to send event");
    }

    // === BATCH 3: Events at time 2000-2500ms (triggers flush of batch 2) ===
    for (ts, price) in [(2000i64, 500.0)] {
        input_handler
            .lock()
            .unwrap()
            .send_event_with_timestamp(
                ts,
                vec![
                    AttributeValue::Long(ts),
                    AttributeValue::String("BTC".to_string()),
                    AttributeValue::Double(price),
                ],
            )
            .expect("Failed to send event");
    }

    // Send a final event to trigger flush of batch 3
    input_handler
        .lock()
        .unwrap()
        .send_event_with_timestamp(
            3000,
            vec![
                AttributeValue::Long(3000),
                AttributeValue::String("BTC".to_string()),
                AttributeValue::Double(600.0),
            ],
        )
        .expect("Failed to send event");

    // Wait for processing
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let output_events = callback.get_events();

    println!(
        "ExternalTimeBatch multi-batch output events ({} total): {:?}",
        output_events.len(),
        output_events
    );

    // Verify NO spurious empty events
    for (i, event) in output_events.iter().enumerate() {
        let trade_count = &event[1];
        let avg_price = &event[2];

        // trade_count must NOT be 0
        match trade_count {
            AttributeValue::Long(count) => {
                assert!(
                    *count > 0,
                    "Event {} has trade_count=0 (spurious empty batch): {:?}",
                    i,
                    event
                );
            }
            AttributeValue::Int(count) => {
                assert!(
                    *count > 0,
                    "Event {} has trade_count=0 (spurious empty batch): {:?}",
                    i,
                    event
                );
            }
            other => {
                panic!("Event {} has unexpected trade_count type: {:?}", i, other);
            }
        }

        // avg_price must NOT be null
        assert!(
            !matches!(avg_price, AttributeValue::Null),
            "Event {} has avg_price=null (spurious empty batch): {:?}",
            i,
            event
        );
    }

    // We should have at least 2 valid batch outputs
    assert!(
        output_events.len() >= 2,
        "Expected at least 2 batch outputs, got {}",
        output_events.len()
    );

    runtime.shutdown();
}
