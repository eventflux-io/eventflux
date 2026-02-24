// SPDX-License-Identifier: MIT OR Apache-2.0

// Integration test for TCP transport in distributed mode

use eventflux::core::distributed::transport::{
    Message, MessageType, TcpTransport, Transport, TransportFactory,
};
use eventflux::core::distributed::DistributedError;
use std::sync::Arc;
use tokio;

fn is_socket_permission_denied(err: &DistributedError) -> bool {
    match err {
        DistributedError::TransportError { message } => {
            let message = message.to_ascii_lowercase();
            message.contains("operation not permitted")
                || message.contains("permission denied")
                || message.contains("eacces")
                || message.contains("eperm")
                || message.contains("os error 1")
                || message.contains("os error 13")
        }
        _ => false,
    }
}

fn should_panic_on_socket_skip() -> bool {
    std::env::var("CI").is_ok() || std::env::var("GITHUB_ACTIONS").is_ok()
}

fn skip_or_panic_socket_denied(test_name: &str, err: &DistributedError) {
    if should_panic_on_socket_skip() {
        panic!("CI environment cannot bind sockets; failing {test_name} ({err:?})");
    } else {
        eprintln!("skipping {test_name}: cannot bind sockets ({err:?})");
    }
}

#[tokio::test]
async fn test_tcp_transport_multi_node_communication() {
    // Create transport layer
    let transport = Arc::new(TcpTransport::new());

    // Node 1: Start listener
    let node1_listener = match transport.listen("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(err) if is_socket_permission_denied(&err) => {
            skip_or_panic_socket_denied("test_tcp_transport_multi_node_communication", &err);
            return;
        }
        Err(err) => panic!("Failed to bind node1 listener: {err:?}"),
    };
    let node1_addr = node1_listener.local_addr().await.unwrap();

    // Node 2: Start listener
    let node2_listener = match transport.listen("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(err) if is_socket_permission_denied(&err) => {
            skip_or_panic_socket_denied("test_tcp_transport_multi_node_communication", &err);
            return;
        }
        Err(err) => panic!("Failed to bind node2 listener: {err:?}"),
    };
    let node2_addr = node2_listener.local_addr().await.unwrap();

    println!("Node 1 listening on: {}", node1_addr);
    println!("Node 2 listening on: {}", node2_addr);

    // Spawn Node 1 acceptor
    let transport_node1 = transport.clone();
    let node1_task = tokio::spawn(async move {
        let connection = transport_node1.accept(&node1_listener).await.unwrap();
        println!("Node 1: Accepted connection from {}", connection.endpoint);

        // Receive message
        let message = transport_node1.receive(&connection).await.unwrap();
        assert_eq!(message.message_type, MessageType::Event);
        assert_eq!(message.payload, b"Hello from Node 2");
        println!(
            "Node 1: Received message - {:?}",
            std::str::from_utf8(&message.payload)
        );

        // Send response
        let response = Message::event(b"Hello from Node 1".to_vec())
            .with_header("node_id".to_string(), "node1".to_string());
        transport_node1.send(&connection, response).await.unwrap();
        println!("Node 1: Sent response");
    });

    // Give acceptor time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Node 2: Connect to Node 1
    let connection = transport.connect(&node1_addr).await.unwrap();
    println!("Node 2: Connected to Node 1");

    // Send message
    let message = Message::event(b"Hello from Node 2".to_vec())
        .with_header("node_id".to_string(), "node2".to_string());
    transport.send(&connection, message).await.unwrap();
    println!("Node 2: Sent message");

    // Receive response
    let response = transport.receive(&connection).await.unwrap();
    assert_eq!(response.message_type, MessageType::Event);
    assert_eq!(response.payload, b"Hello from Node 1");
    println!(
        "Node 2: Received response - {:?}",
        std::str::from_utf8(&response.payload)
    );

    // Clean up
    transport.close(connection).await.unwrap();
    node1_task.await.unwrap();

    println!("Test completed successfully!");
}

#[tokio::test]
async fn test_tcp_transport_broadcast_pattern() {
    // Create transport layer
    let transport = Arc::new(TcpTransport::new());

    // Start 3 nodes
    let mut node_addrs = Vec::new();
    let mut listeners = Vec::new();
    for i in 0..3 {
        let listener = match transport.listen("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if is_socket_permission_denied(&err) => {
                skip_or_panic_socket_denied("test_tcp_transport_broadcast_pattern", &err);
                return;
            }
            Err(err) => panic!("Failed to bind node listener: {err:?}"),
        };
        let addr = listener.local_addr().await.unwrap();
        println!("Node {} listening on: {}", i, addr);
        node_addrs.push(addr);
        listeners.push(Arc::new(listener));
    }

    // Each node accepts connections
    let mut acceptor_tasks = Vec::new();

    for (i, listener) in listeners.into_iter().enumerate() {
        let transport_clone = transport.clone();
        let task = tokio::spawn(async move {
            let connection = transport_clone.accept(&listener).await.unwrap();
            println!("Node {}: Accepted connection", i);

            // Receive broadcast message
            let message = transport_clone.receive(&connection).await.unwrap();
            assert_eq!(message.message_type, MessageType::Control);
            println!(
                "Node {}: Received broadcast - {:?}",
                i,
                std::str::from_utf8(&message.payload)
            );

            // Send acknowledgment
            let ack = Message {
                id: uuid::Uuid::new_v4().to_string(),
                payload: format!("ACK from Node {}", i).into_bytes(),
                headers: std::collections::HashMap::new(),
                message_type: MessageType::Control,
            };
            transport_clone.send(&connection, ack).await.unwrap();
        });
        acceptor_tasks.push(task);
    }

    // Give acceptors time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Broadcaster connects to all nodes and sends message
    let mut connections = Vec::new();
    for addr in &node_addrs {
        let conn = transport.connect(addr).await.unwrap();
        connections.push(conn);
    }

    // Broadcast message to all nodes
    let broadcast_msg = Message {
        id: uuid::Uuid::new_v4().to_string(),
        payload: b"BROADCAST: System update".to_vec(),
        headers: std::collections::HashMap::new(),
        message_type: MessageType::Control,
    };

    for conn in &connections {
        transport.send(conn, broadcast_msg.clone()).await.unwrap();
    }
    println!("Broadcaster: Sent broadcast to all nodes");

    // Collect acknowledgments
    for (i, conn) in connections.iter().enumerate() {
        let ack = transport.receive(conn).await.unwrap();
        assert_eq!(ack.message_type, MessageType::Control);
        println!(
            "Broadcaster: Received ACK from Node {} - {:?}",
            i,
            std::str::from_utf8(&ack.payload)
        );
    }

    // Clean up
    for conn in connections {
        transport.close(conn).await.unwrap();
    }

    for task in acceptor_tasks {
        task.await.unwrap();
    }

    println!("Broadcast test completed successfully!");
}

#[tokio::test]
async fn test_tcp_transport_with_heartbeat() {
    // Create transport layer
    let transport = Arc::new(TcpTransport::new());

    // Start server
    let listener = match transport.listen("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(err) if is_socket_permission_denied(&err) => {
            skip_or_panic_socket_denied("test_tcp_transport_with_heartbeat", &err);
            return;
        }
        Err(err) => panic!("Failed to bind heartbeat listener: {err:?}"),
    };
    let addr = listener.local_addr().await.unwrap();

    // Server task - accepts connection and responds to heartbeats
    let transport_server = transport.clone();
    let server_task = tokio::spawn(async move {
        let connection = transport_server.accept(&listener).await.unwrap();
        println!("Server: Accepted connection");

        // Handle 3 heartbeats
        for i in 0..3 {
            let message = transport_server.receive(&connection).await.unwrap();
            assert_eq!(message.message_type, MessageType::Heartbeat);
            println!("Server: Received heartbeat {}", i + 1);

            // Send heartbeat response
            let response = Message::heartbeat()
                .with_header("server_time".to_string(), chrono::Utc::now().to_rfc3339());
            transport_server.send(&connection, response).await.unwrap();
            println!("Server: Sent heartbeat response {}", i + 1);
        }
    });

    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Client connects and sends heartbeats
    let connection = transport.connect(&addr).await.unwrap();
    println!("Client: Connected to server");

    for i in 0..3 {
        // Send heartbeat
        let heartbeat =
            Message::heartbeat().with_header("sequence".to_string(), (i + 1).to_string());
        transport.send(&connection, heartbeat).await.unwrap();
        println!("Client: Sent heartbeat {}", i + 1);

        // Receive response
        let response = transport.receive(&connection).await.unwrap();
        assert_eq!(response.message_type, MessageType::Heartbeat);
        assert!(response.headers.contains_key("server_time"));
        println!(
            "Client: Received heartbeat response {} with server time: {}",
            i + 1,
            response.headers.get("server_time").unwrap()
        );

        // Small delay between heartbeats
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    // Clean up
    transport.close(connection).await.unwrap();
    server_task.await.unwrap();

    println!("Heartbeat test completed successfully!");
}

#[test]
fn test_transport_factory_creation() {
    // Test creating TCP transport via factory
    let transport = TransportFactory::create("tcp");
    assert!(transport.is_ok());

    // Test creating with custom config
    let config = eventflux::core::distributed::transport::TcpTransportConfig {
        connection_timeout_ms: 10000,
        read_timeout_ms: 60000,
        write_timeout_ms: 60000,
        keepalive_enabled: true,
        keepalive_interval_secs: 60,
        nodelay: false,
        send_buffer_size: Some(131072),
        recv_buffer_size: Some(131072),
        max_message_size: 50 * 1024 * 1024, // 50MB
    };
    let _transport = TransportFactory::create_tcp_with_config(config);
    // Transport created successfully
}
