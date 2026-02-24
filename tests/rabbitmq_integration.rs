// SPDX-License-Identifier: MIT OR Apache-2.0

//! # RabbitMQ Integration Tests
//!
//! These tests require a running RabbitMQ broker and are marked with `#[ignore]`.
//! Run with: `cargo test --test rabbitmq_integration -- --ignored`
//!
//! ## Setup
//!
//! Start RabbitMQ with Docker:
//! ```bash
//! docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
//! ```

#[path = "common/mod.rs"]
mod common;

use eventflux::core::exception::EventFluxError;
use eventflux::core::extension::{SinkFactory, SourceFactory};
use eventflux::core::stream::input::source::rabbitmq_source::RabbitMQSourceFactory;
use eventflux::core::stream::input::source::{Source, SourceCallback};
use eventflux::core::stream::output::sink::rabbitmq_sink::RabbitMQSinkFactory;
use eventflux::core::stream::output::sink::Sink;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// ============================================================================
// Configuration Tests (No broker required)
// ============================================================================

#[test]
fn test_rabbitmq_source_factory_interface() {
    let factory = RabbitMQSourceFactory;
    assert_eq!(factory.name(), "rabbitmq");
    assert!(factory.supported_formats().contains(&"json"));
    assert!(factory.supported_formats().contains(&"csv"));
    assert!(factory.required_parameters().contains(&"rabbitmq.host"));
    assert!(factory.required_parameters().contains(&"rabbitmq.queue"));
}

#[test]
fn test_rabbitmq_sink_factory_interface() {
    let factory = RabbitMQSinkFactory;
    assert_eq!(factory.name(), "rabbitmq");
    assert!(factory.supported_formats().contains(&"json"));
    assert!(factory.supported_formats().contains(&"csv"));
    assert!(factory.required_parameters().contains(&"rabbitmq.host"));
    assert!(factory.required_parameters().contains(&"rabbitmq.exchange"));
}

#[test]
fn test_source_config_validation() {
    let factory = RabbitMQSourceFactory;

    // Missing host should fail
    let mut config = HashMap::new();
    config.insert("rabbitmq.queue".to_string(), "test-queue".to_string());
    assert!(factory.create_initialized(&config).is_err());

    // Missing queue should fail
    let mut config = HashMap::new();
    config.insert("rabbitmq.host".to_string(), "localhost".to_string());
    assert!(factory.create_initialized(&config).is_err());

    // Valid config should succeed
    let mut config = HashMap::new();
    config.insert("rabbitmq.host".to_string(), "localhost".to_string());
    config.insert("rabbitmq.queue".to_string(), "test-queue".to_string());
    assert!(factory.create_initialized(&config).is_ok());
}

#[test]
fn test_sink_config_validation() {
    let factory = RabbitMQSinkFactory;

    // Missing host should fail
    let mut config = HashMap::new();
    config.insert("rabbitmq.exchange".to_string(), "test-exchange".to_string());
    assert!(factory.create_initialized(&config).is_err());

    // Missing exchange should fail
    let mut config = HashMap::new();
    config.insert("rabbitmq.host".to_string(), "localhost".to_string());
    assert!(factory.create_initialized(&config).is_err());

    // Valid config should succeed
    let mut config = HashMap::new();
    config.insert("rabbitmq.host".to_string(), "localhost".to_string());
    config.insert("rabbitmq.exchange".to_string(), "test-exchange".to_string());
    assert!(factory.create_initialized(&config).is_ok());
}

// ============================================================================
// Helper: Test callback that collects received data
// ============================================================================

#[derive(Debug)]
struct TestSourceCallback {
    received_data: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl TestSourceCallback {
    fn new() -> Self {
        Self {
            received_data: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn get_received(&self) -> Vec<Vec<u8>> {
        self.received_data.lock().unwrap().clone()
    }
}

impl SourceCallback for TestSourceCallback {
    fn on_data(&self, data: &[u8]) -> Result<(), EventFluxError> {
        self.received_data.lock().unwrap().push(data.to_vec());
        Ok(())
    }
}

// ============================================================================
// Helper: Setup test queue and exchange
// ============================================================================

async fn setup_test_infrastructure(
    channel: &lapin::Channel,
    queue_name: &str,
    exchange_name: &str,
    routing_key: &str,
) -> Result<(), lapin::Error> {
    // Declare exchange
    // Note: Using auto_delete: false to match the source's queue declaration behavior
    channel
        .exchange_declare(
            exchange_name,
            lapin::ExchangeKind::Direct,
            lapin::options::ExchangeDeclareOptions {
                durable: false,
                auto_delete: false,
                ..Default::default()
            },
            lapin::types::FieldTable::default(),
        )
        .await?;

    // Declare queue
    // Note: Using auto_delete: false to match the source's queue declaration behavior
    channel
        .queue_declare(
            queue_name,
            lapin::options::QueueDeclareOptions {
                durable: false,
                auto_delete: false,
                ..Default::default()
            },
            lapin::types::FieldTable::default(),
        )
        .await?;

    // Bind queue to exchange
    channel
        .queue_bind(
            queue_name,
            exchange_name,
            routing_key,
            lapin::options::QueueBindOptions::default(),
            lapin::types::FieldTable::default(),
        )
        .await?;

    Ok(())
}

async fn publish_test_messages(
    channel: &lapin::Channel,
    exchange: &str,
    routing_key: &str,
    messages: &[&[u8]],
) -> Result<(), lapin::Error> {
    for msg in messages {
        channel
            .basic_publish(
                exchange,
                routing_key,
                lapin::options::BasicPublishOptions::default(),
                msg,
                lapin::BasicProperties::default(),
            )
            .await?
            .await?;
    }
    Ok(())
}

async fn cleanup_test_infrastructure(
    channel: &lapin::Channel,
    queue_name: &str,
    exchange_name: &str,
) {
    let _ = channel
        .queue_delete(queue_name, lapin::options::QueueDeleteOptions::default())
        .await;
    let _ = channel
        .exchange_delete(
            exchange_name,
            lapin::options::ExchangeDeleteOptions::default(),
        )
        .await;
}

// ============================================================================
// Integration Tests (Require RabbitMQ broker)
// ============================================================================

/// Test RabbitMQ source connectivity validation
#[test]
#[ignore = "Requires RabbitMQ broker - run with --ignored"]
fn test_rabbitmq_source_validate_connectivity() {
    use eventflux::core::stream::input::source::rabbitmq_source::RabbitMQSource;

    // First, create the queue using lapin directly
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup: create test queue
    rt.block_on(async {
        let conn = lapin::Connection::connect(
            "amqp://guest:guest@localhost:5672/%2f",
            lapin::ConnectionProperties::default(),
        )
        .await
        .expect("Failed to connect to RabbitMQ");

        let channel = conn.create_channel().await.unwrap();

        // Create test queue
        // Note: Using auto_delete: false to match the source's queue declaration behavior
        channel
            .queue_declare(
                "test-connectivity-queue",
                lapin::options::QueueDeclareOptions {
                    durable: false,
                    auto_delete: false,
                    ..Default::default()
                },
                lapin::types::FieldTable::default(),
            )
            .await
            .expect("Failed to declare queue");

        let _ = conn.close(200, "Setup complete").await;
    });

    // Test our source (validate_connectivity creates its own runtime)
    let mut config = HashMap::new();
    config.insert("rabbitmq.host".to_string(), "localhost".to_string());
    config.insert(
        "rabbitmq.queue".to_string(),
        "test-connectivity-queue".to_string(),
    );

    let source = RabbitMQSource::from_properties(&config, None, "TestStream").unwrap();
    let result = source.validate_connectivity();

    assert!(
        result.is_ok(),
        "validate_connectivity should succeed when queue exists: {:?}",
        result.err()
    );

    // Test with non-existent queue (with declare_queue=false, queue must already exist)
    let mut bad_config = HashMap::new();
    bad_config.insert("rabbitmq.host".to_string(), "localhost".to_string());
    bad_config.insert(
        "rabbitmq.queue".to_string(),
        "non-existent-queue-12345".to_string(),
    );
    // Set declare_queue=false to require the queue to already exist
    bad_config.insert("rabbitmq.declare.queue".to_string(), "false".to_string());

    let bad_source = RabbitMQSource::from_properties(&bad_config, None, "TestStream").unwrap();
    let bad_result = bad_source.validate_connectivity();

    assert!(
        bad_result.is_err(),
        "validate_connectivity should fail when queue doesn't exist and declare_queue=false"
    );

    // Cleanup
    rt.block_on(async {
        let conn = lapin::Connection::connect(
            "amqp://guest:guest@localhost:5672/%2f",
            lapin::ConnectionProperties::default(),
        )
        .await
        .expect("Failed to connect");
        let channel = conn.create_channel().await.unwrap();
        let _ = channel
            .queue_delete(
                "test-connectivity-queue",
                lapin::options::QueueDeleteOptions::default(),
            )
            .await;
        let _ = conn.close(200, "Test complete").await;
    });
}

/// Test RabbitMQ sink connectivity validation
#[test]
#[ignore = "Requires RabbitMQ broker - run with --ignored"]
fn test_rabbitmq_sink_validate_connectivity() {
    use eventflux::core::stream::output::sink::rabbitmq_sink::RabbitMQSink;

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup: create test exchange
    rt.block_on(async {
        let conn = lapin::Connection::connect(
            "amqp://guest:guest@localhost:5672/%2f",
            lapin::ConnectionProperties::default(),
        )
        .await
        .expect("Failed to connect to RabbitMQ");

        let channel = conn.create_channel().await.unwrap();

        // Create test exchange
        // Note: Using auto_delete: false to match the sink's exchange declaration behavior
        channel
            .exchange_declare(
                "test-connectivity-exchange",
                lapin::ExchangeKind::Direct,
                lapin::options::ExchangeDeclareOptions {
                    durable: false,
                    auto_delete: false,
                    ..Default::default()
                },
                lapin::types::FieldTable::default(),
            )
            .await
            .expect("Failed to declare exchange");

        let _ = conn.close(200, "Setup complete").await;
    });

    // Test with existing exchange (validate_connectivity creates its own runtime)
    let mut config = HashMap::new();
    config.insert("rabbitmq.host".to_string(), "localhost".to_string());
    config.insert(
        "rabbitmq.exchange".to_string(),
        "test-connectivity-exchange".to_string(),
    );

    let sink = RabbitMQSink::from_properties(&config).unwrap();
    let result = sink.validate_connectivity();

    assert!(
        result.is_ok(),
        "validate_connectivity should succeed when exchange exists: {:?}",
        result.err()
    );

    // Test with non-existent exchange
    let mut bad_config = HashMap::new();
    bad_config.insert("rabbitmq.host".to_string(), "localhost".to_string());
    bad_config.insert(
        "rabbitmq.exchange".to_string(),
        "non-existent-exchange-12345".to_string(),
    );

    let bad_sink = RabbitMQSink::from_properties(&bad_config).unwrap();
    let bad_result = bad_sink.validate_connectivity();

    assert!(
        bad_result.is_err(),
        "validate_connectivity should fail when exchange doesn't exist"
    );

    // Cleanup
    rt.block_on(async {
        let conn = lapin::Connection::connect(
            "amqp://guest:guest@localhost:5672/%2f",
            lapin::ConnectionProperties::default(),
        )
        .await
        .expect("Failed to connect");
        let channel = conn.create_channel().await.unwrap();
        let _ = channel
            .exchange_delete(
                "test-connectivity-exchange",
                lapin::options::ExchangeDeleteOptions::default(),
            )
            .await;
        let _ = conn.close(200, "Test complete").await;
    });
}

/// Test RabbitMQ sink publish and verify message arrives
#[test]
#[ignore = "Requires RabbitMQ broker - run with --ignored"]
fn test_rabbitmq_sink_publish_and_verify() {
    use eventflux::core::stream::output::sink::rabbitmq_sink::RabbitMQSink;

    let rt = tokio::runtime::Runtime::new().unwrap();

    let queue_name = "test-sink-verify-queue";
    let exchange_name = "test-sink-verify-exchange";
    let routing_key = "test-key";

    // Setup test infrastructure
    rt.block_on(async {
        let conn = lapin::Connection::connect(
            "amqp://guest:guest@localhost:5672/%2f",
            lapin::ConnectionProperties::default(),
        )
        .await
        .expect("Failed to connect to RabbitMQ");

        let channel = conn.create_channel().await.unwrap();

        setup_test_infrastructure(&channel, queue_name, exchange_name, routing_key)
            .await
            .expect("Failed to setup test infrastructure");

        let _ = conn.close(200, "Setup complete").await;
    });

    // Create and start the sink (uses its own runtime internally)
    let mut config = HashMap::new();
    config.insert("rabbitmq.host".to_string(), "localhost".to_string());
    config.insert("rabbitmq.exchange".to_string(), exchange_name.to_string());
    config.insert("rabbitmq.routing.key".to_string(), routing_key.to_string());
    config.insert(
        "rabbitmq.content.type".to_string(),
        "application/json".to_string(),
    );

    let sink = RabbitMQSink::from_properties(&config).unwrap();
    sink.start();

    // Test messages to publish
    let test_messages = vec![
        br#"{"event":"test1","value":100}"#.to_vec(),
        br#"{"event":"test2","value":200}"#.to_vec(),
        br#"{"event":"test3","value":300}"#.to_vec(),
    ];

    // Publish messages through our sink
    for msg in &test_messages {
        let result = sink.publish(msg);
        assert!(result.is_ok(), "Publish should succeed: {:?}", result.err());
    }

    // Stop the sink
    sink.stop();

    // Give messages time to be delivered
    std::thread::sleep(Duration::from_millis(200));

    // Verify messages were received
    let received_messages = rt.block_on(async {
        let conn = lapin::Connection::connect(
            "amqp://guest:guest@localhost:5672/%2f",
            lapin::ConnectionProperties::default(),
        )
        .await
        .expect("Failed to connect");

        let channel = conn.create_channel().await.unwrap();

        // Consume and verify messages
        let mut consumer = channel
            .basic_consume(
                queue_name,
                "test-consumer",
                lapin::options::BasicConsumeOptions {
                    no_ack: true,
                    ..Default::default()
                },
                lapin::types::FieldTable::default(),
            )
            .await
            .expect("Failed to create consumer");

        use futures::StreamExt;
        let mut received = Vec::new();

        // Collect messages with timeout
        for _ in 0..3 {
            match tokio::time::timeout(Duration::from_secs(2), consumer.next()).await {
                Ok(Some(Ok(delivery))) => {
                    received.push(delivery.data.clone());
                }
                _ => break,
            }
        }

        // Cleanup
        cleanup_test_infrastructure(&channel, queue_name, exchange_name).await;
        let _ = conn.close(200, "Test complete").await;

        received
    });

    // Assert we received all messages
    assert_eq!(
        received_messages.len(),
        test_messages.len(),
        "Should receive {} messages, got {}",
        test_messages.len(),
        received_messages.len()
    );

    // Assert message content matches
    for (i, (sent, received)) in test_messages
        .iter()
        .zip(received_messages.iter())
        .enumerate()
    {
        assert_eq!(
            sent,
            received,
            "Message {} content mismatch.\nSent: {:?}\nReceived: {:?}",
            i,
            String::from_utf8_lossy(sent),
            String::from_utf8_lossy(received)
        );
    }
}

/// Test RabbitMQ source consumes messages correctly
#[test]
#[ignore = "Requires RabbitMQ broker - run with --ignored"]
fn test_rabbitmq_source_consume_and_verify() {
    use eventflux::core::stream::input::source::rabbitmq_source::RabbitMQSource;

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let conn = lapin::Connection::connect(
            "amqp://guest:guest@localhost:5672/%2f",
            lapin::ConnectionProperties::default(),
        )
        .await
        .expect("Failed to connect to RabbitMQ");

        let channel = conn.create_channel().await.unwrap();

        // Setup test infrastructure
        let queue_name = "test-source-verify-queue";
        let exchange_name = "test-source-verify-exchange";
        let routing_key = "test-key";

        setup_test_infrastructure(&channel, queue_name, exchange_name, routing_key)
            .await
            .expect("Failed to setup test infrastructure");

        // Test messages to publish
        let test_messages: Vec<&[u8]> = vec![
            br#"{"event":"source-test1","value":111}"#,
            br#"{"event":"source-test2","value":222}"#,
            br#"{"event":"source-test3","value":333}"#,
        ];

        // Publish test messages directly to queue
        publish_test_messages(&channel, exchange_name, routing_key, &test_messages)
            .await
            .expect("Failed to publish test messages");

        // Give messages time to be queued
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create our source
        let mut config = HashMap::new();
        config.insert("rabbitmq.host".to_string(), "localhost".to_string());
        config.insert("rabbitmq.queue".to_string(), queue_name.to_string());
        config.insert("rabbitmq.auto.ack".to_string(), "true".to_string());

        let mut source = RabbitMQSource::from_properties(&config, None, "TestStream").unwrap();

        // Create callback to collect received data
        let callback = Arc::new(TestSourceCallback::new());
        let callback_clone = callback.clone();

        // Start the source
        source.start(callback_clone);

        // Wait for messages to be consumed
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Stop the source
        source.stop();

        // Give time for graceful shutdown
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Get received messages
        let received = callback.get_received();

        // Assert we received all messages
        assert_eq!(
            received.len(),
            test_messages.len(),
            "Should receive {} messages, got {}",
            test_messages.len(),
            received.len()
        );

        // Assert message content matches
        for (i, (sent, received)) in test_messages.iter().zip(received.iter()).enumerate() {
            assert_eq!(
                *sent,
                received.as_slice(),
                "Message {} content mismatch.\nSent: {:?}\nReceived: {:?}",
                i,
                String::from_utf8_lossy(sent),
                String::from_utf8_lossy(received)
            );
        }

        // Cleanup
        cleanup_test_infrastructure(&channel, queue_name, exchange_name).await;
        let _ = conn.close(200, "Test complete").await;
    });
}

/// Test full round-trip: Source -> EventFlux Events -> Sink
#[test]
#[ignore = "Requires RabbitMQ broker - run with --ignored"]
fn test_rabbitmq_roundtrip_source_to_sink() {
    use eventflux::core::stream::input::source::rabbitmq_source::RabbitMQSource;
    use eventflux::core::stream::output::sink::rabbitmq_sink::RabbitMQSink;

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup input queue
    let input_queue = "test-roundtrip-input";
    let input_exchange = "test-roundtrip-input-exchange";
    let input_routing_key = "input-key";

    // Setup output queue
    let output_queue = "test-roundtrip-output";
    let output_exchange = "test-roundtrip-output-exchange";
    let output_routing_key = "output-key";

    // Test messages
    let test_messages: Vec<Vec<u8>> = vec![
        br#"{"id":1,"name":"Alice","score":95}"#.to_vec(),
        br#"{"id":2,"name":"Bob","score":87}"#.to_vec(),
        br#"{"id":3,"name":"Charlie","score":92}"#.to_vec(),
    ];
    let test_messages_refs: Vec<&[u8]> = test_messages.iter().map(|v| v.as_slice()).collect();

    // Setup infrastructure and publish input messages
    rt.block_on(async {
        let conn = lapin::Connection::connect(
            "amqp://guest:guest@localhost:5672/%2f",
            lapin::ConnectionProperties::default(),
        )
        .await
        .expect("Failed to connect to RabbitMQ");

        let channel = conn.create_channel().await.unwrap();

        setup_test_infrastructure(&channel, input_queue, input_exchange, input_routing_key)
            .await
            .expect("Failed to setup input infrastructure");

        setup_test_infrastructure(&channel, output_queue, output_exchange, output_routing_key)
            .await
            .expect("Failed to setup output infrastructure");

        // Publish to input queue
        publish_test_messages(
            &channel,
            input_exchange,
            input_routing_key,
            &test_messages_refs,
        )
        .await
        .expect("Failed to publish input messages");

        let _ = conn.close(200, "Setup complete").await;
    });

    // Wait for messages to be queued
    std::thread::sleep(Duration::from_millis(100));

    // Create sink (uses its own runtime)
    let mut sink_config = HashMap::new();
    sink_config.insert("rabbitmq.host".to_string(), "localhost".to_string());
    sink_config.insert("rabbitmq.exchange".to_string(), output_exchange.to_string());
    sink_config.insert(
        "rabbitmq.routing.key".to_string(),
        output_routing_key.to_string(),
    );

    let sink = Arc::new(RabbitMQSink::from_properties(&sink_config).unwrap());
    sink.start();

    // Create a callback that forwards to sink (simulating EventFlux pipeline)
    #[derive(Debug)]
    struct ForwardToSinkCallback {
        sink: Arc<eventflux::core::stream::output::sink::rabbitmq_sink::RabbitMQSink>,
        forwarded_count: Arc<Mutex<usize>>,
    }

    impl SourceCallback for ForwardToSinkCallback {
        fn on_data(&self, data: &[u8]) -> Result<(), EventFluxError> {
            // Forward data to sink (simulating passthrough pipeline)
            self.sink.publish(data)?;
            *self.forwarded_count.lock().unwrap() += 1;
            Ok(())
        }
    }

    let forwarded_count = Arc::new(Mutex::new(0));
    let callback = Arc::new(ForwardToSinkCallback {
        sink: sink.clone(),
        forwarded_count: forwarded_count.clone(),
    });

    // Create and start source (uses its own runtime via thread::spawn)
    let mut source_config = HashMap::new();
    source_config.insert("rabbitmq.host".to_string(), "localhost".to_string());
    source_config.insert("rabbitmq.queue".to_string(), input_queue.to_string());

    let mut source = RabbitMQSource::from_properties(&source_config, None, "TestStream").unwrap();
    source.start(callback);

    // Wait for processing
    std::thread::sleep(Duration::from_millis(500));

    // Stop source and sink
    source.stop();
    std::thread::sleep(Duration::from_millis(100));
    sink.stop();

    // Verify forwarded count
    let count = *forwarded_count.lock().unwrap();
    assert_eq!(
        count,
        test_messages.len(),
        "Should forward {} messages, forwarded {}",
        test_messages.len(),
        count
    );

    // Consume from output queue and verify
    let output_messages = rt.block_on(async {
        let conn = lapin::Connection::connect(
            "amqp://guest:guest@localhost:5672/%2f",
            lapin::ConnectionProperties::default(),
        )
        .await
        .expect("Failed to connect");

        let channel = conn.create_channel().await.unwrap();

        let mut consumer = channel
            .basic_consume(
                output_queue,
                "verify-consumer",
                lapin::options::BasicConsumeOptions {
                    no_ack: true,
                    ..Default::default()
                },
                lapin::types::FieldTable::default(),
            )
            .await
            .expect("Failed to create consumer");

        use futures::StreamExt;
        let mut output = Vec::new();

        for _ in 0..test_messages.len() {
            match tokio::time::timeout(Duration::from_secs(2), consumer.next()).await {
                Ok(Some(Ok(delivery))) => {
                    output.push(delivery.data.clone());
                }
                _ => break,
            }
        }

        // Cleanup
        cleanup_test_infrastructure(&channel, input_queue, input_exchange).await;
        cleanup_test_infrastructure(&channel, output_queue, output_exchange).await;
        let _ = conn.close(200, "Test complete").await;

        output
    });

    // Assert output matches input
    assert_eq!(
        output_messages.len(),
        test_messages.len(),
        "Output queue should have {} messages, got {}",
        test_messages.len(),
        output_messages.len()
    );

    for (i, (input, output)) in test_messages.iter().zip(output_messages.iter()).enumerate() {
        assert_eq!(
            input.as_slice(),
            output.as_slice(),
            "Round-trip message {} mismatch.\nInput: {:?}\nOutput: {:?}",
            i,
            String::from_utf8_lossy(input),
            String::from_utf8_lossy(output)
        );
    }

    println!(
        "Round-trip test passed: {} messages sent through pipeline",
        test_messages.len()
    );
}

// ============================================================================
// End-to-End Test: RabbitMQ → EventFlux Processing → RabbitMQ
// ============================================================================

/// End-to-end test demonstrating a complete EventFlux pipeline:
/// 1. External client publishes stock trade events to INPUT queue
/// 2. RabbitMQ Source consumes events
/// 3. EventFlux processes: filters by volume > 1000, adds price_category via CASE
/// 4. RabbitMQ Sink publishes processed events to OUTPUT exchange
/// 5. External client verifies results from OUTPUT queue
///
/// ## RabbitMQ Admin UI Testing Instructions
///
/// 1. Start RabbitMQ with management UI:
///    ```bash
///    docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
///    ```
///
/// 2. Open http://localhost:15672 (guest/guest)
///
/// 3. Create queues manually for testing:
///    - Go to "Queues" tab → "Add a new queue"
///    - Create: `e2e-input-queue` (durable: false)
///    - Create: `e2e-output-queue` (durable: false)
///
/// 4. Create exchanges:
///    - Go to "Exchanges" tab → "Add a new exchange"
///    - Create: `e2e-input-exchange` (type: direct)
///    - Create: `e2e-output-exchange` (type: direct)
///
/// 5. Bind queues to exchanges:
///    - Click on `e2e-input-exchange` → "Bindings"
///    - Bind `e2e-input-queue` with routing key `trades`
///    - Click on `e2e-output-exchange` → "Bindings"
///    - Bind `e2e-output-queue` with routing key `processed`
///
/// 6. Publish test messages via Admin UI:
///    - Go to `e2e-input-exchange` → "Publish message"
///    - Routing key: `trades`
///    - Payload (test events that PASS filter - volume > 1000):
///      ```json
///      {"symbol":"AAPL","price":185.50,"volume":5000,"timestamp":1702500000}
///      {"symbol":"GOOGL","price":142.30,"volume":2500,"timestamp":1702500001}
///      {"symbol":"MSFT","price":378.25,"volume":1500,"timestamp":1702500002}
///      ```
///    - Payload (test events that FAIL filter - volume <= 1000):
///      ```json
///      {"symbol":"META","price":325.00,"volume":800,"timestamp":1702500003}
///      {"symbol":"NVDA","price":495.50,"volume":500,"timestamp":1702500004}
///      ```
///
/// 7. Run this test and check `e2e-output-queue` for processed events:
///    - Only 3 events should appear (filtered by volume > 1000)
///    - Each should have `price_category` field added via CASE expression:
///      - price < 150: "low"
///      - price < 300: "medium"
///      - else: "high"
#[test]
#[ignore = "Requires RabbitMQ broker - run with --ignored"]
fn test_e2e_rabbitmq_filter_and_case_processing() {
    use eventflux::core::stream::input::source::rabbitmq_source::RabbitMQSource;
    use eventflux::core::stream::output::sink::rabbitmq_sink::RabbitMQSink;
    use serde_json::{json, Value};

    let rt = tokio::runtime::Runtime::new().unwrap();

    // ========================================================================
    // Configuration
    // ========================================================================
    let input_queue = "e2e-test-input-queue";
    let input_exchange = "e2e-test-input-exchange";
    let input_routing_key = "trades";

    let output_queue = "e2e-test-output-queue";
    let output_exchange = "e2e-test-output-exchange";
    let output_routing_key = "processed";

    // ========================================================================
    // Test Data: Stock trade events
    // ========================================================================
    // Events that PASS the filter (volume > 1000)
    let pass_events = vec![
        json!({"symbol": "AAPL", "price": 185.50, "volume": 5000, "timestamp": 1702500000}),
        json!({"symbol": "GOOGL", "price": 142.30, "volume": 2500, "timestamp": 1702500001}),
        json!({"symbol": "MSFT", "price": 378.25, "volume": 1500, "timestamp": 1702500002}),
    ];

    // Events that FAIL the filter (volume <= 1000) - should NOT appear in output
    let fail_events = vec![
        json!({"symbol": "META", "price": 325.00, "volume": 800, "timestamp": 1702500003}),
        json!({"symbol": "NVDA", "price": 495.50, "volume": 500, "timestamp": 1702500004}),
    ];

    // Expected output after processing (PASS events with price_category added)
    let expected_outputs = vec![
        json!({"symbol": "AAPL", "price": 185.50, "volume": 5000, "timestamp": 1702500000, "price_category": "medium"}),
        json!({"symbol": "GOOGL", "price": 142.30, "volume": 2500, "timestamp": 1702500001, "price_category": "low"}),
        json!({"symbol": "MSFT", "price": 378.25, "volume": 1500, "timestamp": 1702500002, "price_category": "high"}),
    ];

    // ========================================================================
    // Step 1: Setup RabbitMQ infrastructure (separate client)
    // ========================================================================
    println!("\n[E2E Test] Setting up RabbitMQ infrastructure...");

    rt.block_on(async {
        let conn = lapin::Connection::connect(
            "amqp://guest:guest@localhost:5672/%2f",
            lapin::ConnectionProperties::default(),
        )
        .await
        .expect("Failed to connect to RabbitMQ for setup");

        let channel = conn.create_channel().await.unwrap();

        // Setup input infrastructure
        setup_test_infrastructure(&channel, input_queue, input_exchange, input_routing_key)
            .await
            .expect("Failed to setup input infrastructure");

        // Setup output infrastructure
        setup_test_infrastructure(&channel, output_queue, output_exchange, output_routing_key)
            .await
            .expect("Failed to setup output infrastructure");

        let _ = conn.close(200, "Setup complete").await;
    });

    println!("[E2E Test] Infrastructure ready.");
    println!(
        "  Input:  {} → {} (routing: {})",
        input_exchange, input_queue, input_routing_key
    );
    println!(
        "  Output: {} → {} (routing: {})",
        output_exchange, output_queue, output_routing_key
    );

    // ========================================================================
    // Step 2: Publish input events via SEPARATE client (simulating external producer)
    // ========================================================================
    println!(
        "\n[E2E Test] Publishing {} input events via external client...",
        pass_events.len() + fail_events.len()
    );

    rt.block_on(async {
        let conn = lapin::Connection::connect(
            "amqp://guest:guest@localhost:5672/%2f",
            lapin::ConnectionProperties::default(),
        )
        .await
        .expect("Failed to connect to RabbitMQ for publishing");

        let channel = conn.create_channel().await.unwrap();

        // Publish all events (both pass and fail)
        let all_events: Vec<&Value> = pass_events.iter().chain(fail_events.iter()).collect();
        for event in &all_events {
            let payload = serde_json::to_vec(event).unwrap();
            channel
                .basic_publish(
                    input_exchange,
                    input_routing_key,
                    lapin::options::BasicPublishOptions::default(),
                    &payload,
                    lapin::BasicProperties::default().with_content_type("application/json".into()),
                )
                .await
                .expect("Failed to publish")
                .await
                .expect("Publish confirmation failed");
            println!("  Published: {}", event);
        }

        let _ = conn.close(200, "Publishing complete").await;
    });

    // Wait for messages to be queued
    std::thread::sleep(Duration::from_millis(100));

    // ========================================================================
    // Step 3: Create EventFlux Sink (output)
    // ========================================================================
    println!("\n[E2E Test] Starting EventFlux Sink...");

    let mut sink_config = HashMap::new();
    sink_config.insert("rabbitmq.host".to_string(), "localhost".to_string());
    sink_config.insert("rabbitmq.exchange".to_string(), output_exchange.to_string());
    sink_config.insert(
        "rabbitmq.routing.key".to_string(),
        output_routing_key.to_string(),
    );
    sink_config.insert(
        "rabbitmq.content.type".to_string(),
        "application/json".to_string(),
    );

    let sink = Arc::new(RabbitMQSink::from_properties(&sink_config).unwrap());
    sink.start();

    // ========================================================================
    // Step 4: Create processing callback (Filter + CASE transformation)
    // ========================================================================
    println!("[E2E Test] Starting EventFlux Source with processing pipeline...");

    #[derive(Debug)]
    struct ProcessingCallback {
        sink: Arc<RabbitMQSink>,
        processed_count: Arc<Mutex<usize>>,
        filtered_count: Arc<Mutex<usize>>,
    }

    impl SourceCallback for ProcessingCallback {
        fn on_data(&self, data: &[u8]) -> Result<(), EventFluxError> {
            // Parse incoming JSON event
            let mut event: Value = serde_json::from_slice(data)
                .map_err(|e| EventFluxError::app_runtime(format!("Failed to parse JSON: {}", e)))?;

            // ================================================================
            // FILTER: Only process events with volume > 1000
            // ================================================================
            let volume = event.get("volume").and_then(|v| v.as_i64()).unwrap_or(0);

            if volume <= 1000 {
                *self.filtered_count.lock().unwrap() += 1;
                println!("  [FILTERED] volume={} <= 1000: {}", volume, event);
                return Ok(()); // Skip this event
            }

            // ================================================================
            // CASE: Add price_category based on price ranges
            // CASE
            //   WHEN price < 150 THEN 'low'
            //   WHEN price < 300 THEN 'medium'
            //   ELSE 'high'
            // END as price_category
            // ================================================================
            let price = event.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);

            let price_category = if price < 150.0 {
                "low"
            } else if price < 300.0 {
                "medium"
            } else {
                "high"
            };

            event
                .as_object_mut()
                .unwrap()
                .insert("price_category".to_string(), json!(price_category));

            // ================================================================
            // Publish processed event to sink
            // ================================================================
            let output = serde_json::to_vec(&event).unwrap();
            self.sink.publish(&output)?;

            *self.processed_count.lock().unwrap() += 1;
            println!("  [PROCESSED] price_category={}: {}", price_category, event);

            Ok(())
        }
    }

    let processed_count = Arc::new(Mutex::new(0));
    let filtered_count = Arc::new(Mutex::new(0));
    let callback = Arc::new(ProcessingCallback {
        sink: sink.clone(),
        processed_count: processed_count.clone(),
        filtered_count: filtered_count.clone(),
    });

    // ========================================================================
    // Step 5: Create and start EventFlux Source
    // ========================================================================
    let mut source_config = HashMap::new();
    source_config.insert("rabbitmq.host".to_string(), "localhost".to_string());
    source_config.insert("rabbitmq.queue".to_string(), input_queue.to_string());
    source_config.insert("rabbitmq.auto.ack".to_string(), "true".to_string());

    let mut source = RabbitMQSource::from_properties(&source_config, None, "TradeStream").unwrap();
    source.start(callback);

    // Wait for processing
    std::thread::sleep(Duration::from_millis(800));

    // Stop source and sink
    source.stop();
    std::thread::sleep(Duration::from_millis(100));
    sink.stop();

    // ========================================================================
    // Step 6: Verify processing counts
    // ========================================================================
    let final_processed = *processed_count.lock().unwrap();
    let final_filtered = *filtered_count.lock().unwrap();

    println!("\n[E2E Test] Processing Summary:");
    println!(
        "  Total input events: {}",
        pass_events.len() + fail_events.len()
    );
    println!("  Processed (passed filter): {}", final_processed);
    println!("  Filtered out (volume <= 1000): {}", final_filtered);

    assert_eq!(
        final_processed,
        pass_events.len(),
        "Should process {} events (volume > 1000), processed {}",
        pass_events.len(),
        final_processed
    );

    assert_eq!(
        final_filtered,
        fail_events.len(),
        "Should filter {} events (volume <= 1000), filtered {}",
        fail_events.len(),
        final_filtered
    );

    // ========================================================================
    // Step 7: Consume and verify output via SEPARATE client
    // ========================================================================
    println!("\n[E2E Test] Consuming output events via external client...");

    let output_events = rt.block_on(async {
        let conn = lapin::Connection::connect(
            "amqp://guest:guest@localhost:5672/%2f",
            lapin::ConnectionProperties::default(),
        )
        .await
        .expect("Failed to connect to RabbitMQ for verification");

        let channel = conn.create_channel().await.unwrap();

        let mut consumer = channel
            .basic_consume(
                output_queue,
                "e2e-verifier",
                lapin::options::BasicConsumeOptions {
                    no_ack: true,
                    ..Default::default()
                },
                lapin::types::FieldTable::default(),
            )
            .await
            .expect("Failed to create consumer");

        use futures::StreamExt;
        let mut received: Vec<Value> = Vec::new();

        for _ in 0..expected_outputs.len() {
            match tokio::time::timeout(Duration::from_secs(3), consumer.next()).await {
                Ok(Some(Ok(delivery))) => {
                    let event: Value = serde_json::from_slice(&delivery.data)
                        .expect("Failed to parse output JSON");
                    println!("  Received: {}", event);
                    received.push(event);
                }
                Ok(Some(Err(e))) => {
                    panic!("Consumer error: {:?}", e);
                }
                Ok(None) => {
                    println!("  Consumer stream ended");
                    break;
                }
                Err(_) => {
                    println!("  Timeout waiting for message");
                    break;
                }
            }
        }

        // Cleanup
        cleanup_test_infrastructure(&channel, input_queue, input_exchange).await;
        cleanup_test_infrastructure(&channel, output_queue, output_exchange).await;
        let _ = conn.close(200, "Verification complete").await;

        received
    });

    // ========================================================================
    // Step 8: Assert output matches expected
    // ========================================================================
    println!("\n[E2E Test] Verifying output events...");

    assert_eq!(
        output_events.len(),
        expected_outputs.len(),
        "Should receive {} processed events, got {}",
        expected_outputs.len(),
        output_events.len()
    );

    for (i, (expected, actual)) in expected_outputs
        .iter()
        .zip(output_events.iter())
        .enumerate()
    {
        // Verify each field
        assert_eq!(
            expected.get("symbol"),
            actual.get("symbol"),
            "Event {} symbol mismatch",
            i
        );
        assert_eq!(
            expected.get("price"),
            actual.get("price"),
            "Event {} price mismatch",
            i
        );
        assert_eq!(
            expected.get("volume"),
            actual.get("volume"),
            "Event {} volume mismatch",
            i
        );
        assert_eq!(
            expected.get("timestamp"),
            actual.get("timestamp"),
            "Event {} timestamp mismatch",
            i
        );
        assert_eq!(
            expected.get("price_category"),
            actual.get("price_category"),
            "Event {} price_category mismatch.\nExpected: {:?}\nActual: {:?}",
            i,
            expected.get("price_category"),
            actual.get("price_category")
        );

        println!(
            "  [OK] Event {}: {} → category '{}'",
            i,
            actual.get("symbol").unwrap(),
            actual.get("price_category").unwrap()
        );
    }

    println!(
        "\n[E2E Test] SUCCESS! All {} events processed correctly.",
        expected_outputs.len()
    );
    println!("============================================================");
    println!("Test demonstrated:");
    println!("  - RabbitMQ Source consuming from queue");
    println!(
        "  - Filter: volume > 1000 (filtered {} events)",
        fail_events.len()
    );
    println!("  - CASE expression: price_category (low/medium/high)");
    println!("  - RabbitMQ Sink publishing to exchange");
    println!("  - Separate clients for input/output verification");
    println!("============================================================\n");
}

// ============================================================================
// Query-Based End-to-End Test: RabbitMQ → EventFluxQL → RabbitMQ
// ============================================================================

/// Query-based E2E test using EventFluxQL for all processing logic.
///
/// This test demonstrates the COMPLETE EventFlux architecture:
/// 1. RabbitMQ Source configured via SQL WITH clause (consumes JSON)
/// 2. EventFluxQL query: SELECT with filter and CASE expression
/// 3. RabbitMQ Sink configured via SQL WITH clause (publishes JSON)
/// 4. Separate RabbitMQ clients for input/output verification
///
/// ## The Query Logic
///
/// ```sql
/// CREATE STREAM InputTrades (symbol STRING, price DOUBLE, volume INT) WITH (
///     type = 'source',
///     extension = 'rabbitmq',
///     format = 'json',
///     "rabbitmq.host" = 'localhost',
///     "rabbitmq.queue" = 'query-input-queue'
/// );
///
/// CREATE STREAM OutputTrades (symbol STRING, price DOUBLE, volume INT, price_category STRING) WITH (
///     type = 'sink',
///     extension = 'rabbitmq',
///     format = 'json',
///     "rabbitmq.host" = 'localhost',
///     "rabbitmq.exchange" = 'query-output-exchange',
///     "rabbitmq.routing.key" = 'processed'
/// );
///
/// INSERT INTO OutputTrades
/// SELECT
///     symbol,
///     price,
///     volume,
///     CASE
///         WHEN price < 150 THEN 'low'
///         WHEN price < 300 THEN 'medium'
///         ELSE 'high'
///     END as price_category
/// FROM InputTrades
/// WHERE volume > 1000;
/// ```
///
/// ## RabbitMQ Admin UI Testing
///
/// 1. Open http://localhost:15672 (guest/guest)
/// 2. Create queue: `query-input-queue`
/// 3. Create exchange: `query-output-exchange` (direct)
/// 4. Bind `query-output-queue` to `query-output-exchange` with routing key `processed`
/// 5. Publish test messages to `query-input-queue`
/// 6. Check `query-output-queue` for processed results
#[test]
#[ignore = "Requires RabbitMQ broker - run with --ignored"]
fn test_e2e_query_based_rabbitmq_processing() {
    use crate::common::AppRunner;
    use eventflux::core::event::value::AttributeValue;
    use eventflux::core::eventflux_manager::EventFluxManager;
    use serde_json::{json, Value};

    // Create a separate runtime for setup/verification (NOT for EventFlux)
    let setup_rt = tokio::runtime::Runtime::new().unwrap();

    // ========================================================================
    // Configuration
    // ========================================================================
    let input_queue = "query-test-input-queue";
    let input_exchange = "query-test-input-exchange";
    let input_routing_key = "input";

    let output_queue = "query-test-output-queue";
    let output_exchange = "query-test-output-exchange";
    let output_routing_key = "processed";

    // ========================================================================
    // Test Data
    // ========================================================================
    // Events that PASS the filter (volume > 1000)
    let pass_events = vec![
        json!({"symbol": "AAPL", "price": 185.50, "volume": 5000}),
        json!({"symbol": "GOOGL", "price": 142.30, "volume": 2500}),
        json!({"symbol": "MSFT", "price": 378.25, "volume": 1500}),
    ];

    // Events that FAIL the filter (volume <= 1000)
    let fail_events = vec![
        json!({"symbol": "META", "price": 325.00, "volume": 800}),
        json!({"symbol": "NVDA", "price": 495.50, "volume": 500}),
    ];

    // ========================================================================
    // Step 1: Setup RabbitMQ infrastructure (using setup runtime)
    // ========================================================================
    println!("\n[Query E2E Test] Setting up RabbitMQ infrastructure...");

    let channel = setup_rt.block_on(async {
        let conn = lapin::Connection::connect(
            "amqp://guest:guest@localhost:5672/%2f",
            lapin::ConnectionProperties::default(),
        )
        .await
        .expect("Failed to connect to RabbitMQ");

        let channel = conn.create_channel().await.unwrap();

        // Setup input infrastructure
        setup_test_infrastructure(&channel, input_queue, input_exchange, input_routing_key)
            .await
            .expect("Failed to setup input infrastructure");

        // Setup output infrastructure
        setup_test_infrastructure(&channel, output_queue, output_exchange, output_routing_key)
            .await
            .expect("Failed to setup output infrastructure");

        channel
    });

    println!("[Query E2E Test] Infrastructure ready.");

    // ========================================================================
    // Step 2: Publish input events via external client
    // ========================================================================
    println!(
        "[Query E2E Test] Publishing {} input events...",
        pass_events.len() + fail_events.len()
    );

    setup_rt.block_on(async {
        let all_events: Vec<&Value> = pass_events.iter().chain(fail_events.iter()).collect();
        for event in &all_events {
            let payload = serde_json::to_vec(event).unwrap();
            channel
                .basic_publish(
                    input_exchange,
                    input_routing_key,
                    lapin::options::BasicPublishOptions::default(),
                    &payload,
                    lapin::BasicProperties::default().with_content_type("application/json".into()),
                )
                .await
                .expect("Failed to publish")
                .await
                .expect("Publish confirmation failed");
            println!("  Published: {}", event);
        }
    });

    // ========================================================================
    // Step 3: Create EventFlux application with SQL query (needs its own runtime)
    // ========================================================================
    println!("\n[Query E2E Test] Starting EventFlux with SQL query...");

    // All factories (rabbitmq, json, csv) are registered by default in EventFluxContext
    let manager = EventFluxManager::new();

    // EventFluxQL query with RabbitMQ source/sink and CASE expression
    let sql = format!(
        r#"
        -- Input stream from RabbitMQ
        CREATE STREAM InputTrades (symbol STRING, price DOUBLE, volume INT) WITH (
            type = 'source',
            extension = 'rabbitmq',
            format = 'json',
            "rabbitmq.host" = 'localhost',
            "rabbitmq.queue" = '{input_queue}'
        );

        -- Output stream to RabbitMQ
        CREATE STREAM OutputTrades (symbol STRING, price DOUBLE, volume INT, price_category STRING) WITH (
            type = 'sink',
            extension = 'rabbitmq',
            format = 'json',
            "rabbitmq.host" = 'localhost',
            "rabbitmq.exchange" = '{output_exchange}',
            "rabbitmq.routing.key" = '{output_routing_key}'
        );

        -- Intermediate stream for collecting results
        CREATE STREAM ProcessedTrades (symbol STRING, price DOUBLE, volume INT, price_category STRING);

        -- Query with filter and CASE expression
        INSERT INTO ProcessedTrades
        SELECT
            symbol,
            price,
            volume,
            CASE
                WHEN price < 150.0 THEN 'low'
                WHEN price < 300.0 THEN 'medium'
                ELSE 'high'
            END as price_category
        FROM InputTrades
        WHERE volume > 1000;

        -- Forward to RabbitMQ sink
        INSERT INTO OutputTrades
        SELECT symbol, price, volume, price_category
        FROM ProcessedTrades;
    "#,
        input_queue = input_queue,
        output_exchange = output_exchange,
        output_routing_key = output_routing_key
    );

    // Run EventFlux in a completely separate thread to avoid runtime conflicts
    let (tx, rx) = std::sync::mpsc::channel();
    let sql_clone = sql.clone();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let runner = rt.block_on(async {
            AppRunner::new_with_manager(manager, &sql_clone, "ProcessedTrades").await
        });

        // Wait for events to be processed - give more time for consumption
        std::thread::sleep(Duration::from_millis(5000));

        // Print debug info
        let collected = runner.collected.lock().unwrap().len();
        println!(
            "[EventFlux Thread] Collected {} events before shutdown",
            collected
        );

        // Shutdown and get collected events
        let out = runner.shutdown();
        println!(
            "[EventFlux Thread] Shutdown complete, returning {} events",
            out.len()
        );
        tx.send(out).unwrap();
    });

    // Wait for results from the EventFlux thread
    let out = rx
        .recv_timeout(Duration::from_secs(30))
        .expect("Timeout waiting for EventFlux processing");

    // ========================================================================
    // Step 4: Verify processing via AppRunner collected events
    // ========================================================================
    println!("\n[Query E2E Test] Verifying processed events...");
    println!(
        "  Collected {} events from ProcessedTrades stream",
        out.len()
    );

    assert_eq!(
        out.len(),
        pass_events.len(),
        "Expected {} processed events (volume > 1000), got {}",
        pass_events.len(),
        out.len()
    );

    // Verify each processed event
    for (i, event_data) in out.iter().enumerate() {
        println!("  Event {}: {:?}", i, event_data);

        // Each event should have 4 fields: symbol, price, volume, price_category
        assert_eq!(event_data.len(), 4, "Event should have 4 fields");

        // Verify price_category was added via CASE expression
        match &event_data[3] {
            AttributeValue::String(category) => {
                assert!(
                    category == "low" || category == "medium" || category == "high",
                    "Invalid price_category: {}",
                    category
                );
                println!("    price_category = {}", category);
            }
            _ => panic!("price_category should be a String"),
        }
    }

    // ========================================================================
    // Step 5: Verify output queue via external RabbitMQ client
    // ========================================================================
    println!("\n[Query E2E Test] Consuming from output queue...");

    let output_events = setup_rt.block_on(async {
        let mut consumer = channel
            .basic_consume(
                output_queue,
                "query-test-verifier",
                lapin::options::BasicConsumeOptions {
                    no_ack: true,
                    ..Default::default()
                },
                lapin::types::FieldTable::default(),
            )
            .await
            .expect("Failed to create consumer");

        use futures::StreamExt;
        let mut output_events: Vec<Value> = Vec::new();

        for _ in 0..pass_events.len() {
            match tokio::time::timeout(Duration::from_secs(3), consumer.next()).await {
                Ok(Some(Ok(delivery))) => {
                    let event: Value = serde_json::from_slice(&delivery.data)
                        .expect("Failed to parse output JSON");
                    println!("  Received from queue: {}", event);
                    output_events.push(event);
                }
                _ => {
                    println!("  Timeout or error waiting for message");
                    break;
                }
            }
        }

        // Cleanup
        cleanup_test_infrastructure(&channel, input_queue, input_exchange).await;
        cleanup_test_infrastructure(&channel, output_queue, output_exchange).await;

        output_events
    });

    // ========================================================================
    // Step 6: Final assertions
    // ========================================================================
    println!("\n[Query E2E Test] Final verification...");

    assert_eq!(
        output_events.len(),
        pass_events.len(),
        "Expected {} events in output queue, got {}",
        pass_events.len(),
        output_events.len()
    );

    // Verify each output event has price_category
    // Note: The JSON sink mapper uses generic field names (field_0, field_1, etc.)
    // field_3 = price_category (4th field in the schema)
    for (i, event) in output_events.iter().enumerate() {
        let category = event
            .get("field_3")
            .expect("Output event should have field_3 (price_category)");
        assert!(
            category == "low" || category == "medium" || category == "high",
            "Event {} has invalid price_category: {:?}",
            i,
            category
        );
        let symbol = event.get("field_0").unwrap();
        println!("  [OK] Event {}: {} → {}", i, symbol, category);
    }

    println!("\n[Query E2E Test] SUCCESS!");
    println!("============================================================");
    println!("Query-based test demonstrated:");
    println!("  - RabbitMQ Source via SQL WITH (format = 'json')");
    println!("  - EventFluxQL: SELECT with WHERE filter (volume > 1000)");
    println!("  - EventFluxQL: CASE expression for price_category");
    println!("  - RabbitMQ Sink via SQL WITH (format = 'json')");
    println!(
        "  - {} events processed, {} filtered out",
        pass_events.len(),
        fail_events.len()
    );
    println!("============================================================\n");
}

/// Test error handling when broker is unreachable
#[test]
fn test_rabbitmq_unreachable_broker() {
    use eventflux::core::stream::input::source::rabbitmq_source::RabbitMQSource;
    use eventflux::core::stream::output::sink::rabbitmq_sink::RabbitMQSink;

    // Test source with unreachable broker
    let mut source_config = HashMap::new();
    source_config.insert("rabbitmq.host".to_string(), "192.0.2.1".to_string()); // TEST-NET, should be unreachable
    source_config.insert("rabbitmq.port".to_string(), "59999".to_string());
    source_config.insert("rabbitmq.queue".to_string(), "test".to_string());

    let source = RabbitMQSource::from_properties(&source_config, None, "TestStream").unwrap();
    let result = source.validate_connectivity();

    assert!(
        result.is_err(),
        "validate_connectivity should fail for unreachable broker"
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("Failed to connect")
            || err.to_string().contains("Connection")
            || err.to_string().contains("timeout"),
        "Error should indicate connection failure: {}",
        err
    );

    // Test sink with unreachable broker
    let mut sink_config = HashMap::new();
    sink_config.insert("rabbitmq.host".to_string(), "192.0.2.1".to_string());
    sink_config.insert("rabbitmq.port".to_string(), "59999".to_string());
    sink_config.insert("rabbitmq.exchange".to_string(), "test".to_string());

    let sink = RabbitMQSink::from_properties(&sink_config).unwrap();
    let result = sink.validate_connectivity();

    assert!(
        result.is_err(),
        "validate_connectivity should fail for unreachable broker"
    );
}
