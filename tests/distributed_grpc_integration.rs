// SPDX-License-Identifier: MIT OR Apache-2.0

// Integration test for gRPC transport in distributed mode

use eventflux::core::distributed::grpc::simple_transport::{
    SimpleGrpcConfig, SimpleGrpcTransport,
};
use eventflux::core::distributed::transport::{Message, MessageType};

#[tokio::test]
async fn test_simple_grpc_transport_creation() {
    let transport = SimpleGrpcTransport::new();
    // Transport created successfully

    let custom_config = SimpleGrpcConfig {
        connection_timeout_ms: 5000,
        enable_compression: false,
        server_address: "127.0.0.1:50052".to_string(),
    };
    let _transport_custom = SimpleGrpcTransport::with_config(custom_config);
    // Custom transport created successfully
}

#[test]
fn test_grpc_message_conversion() {
    let transport = SimpleGrpcTransport::new();

    let original_message = Message::event(b"test data".to_vec())
        .with_header("source_node".to_string(), "node1".to_string())
        .with_header("target_node".to_string(), "node2".to_string());

    let proto_message = transport.to_proto_message(&original_message);
    let converted_message = transport.from_proto_message(&proto_message);

    assert_eq!(original_message.id, converted_message.id);
    assert_eq!(original_message.payload, converted_message.payload);
    assert_eq!(
        original_message.message_type,
        converted_message.message_type
    );
    assert_eq!(original_message.headers, converted_message.headers);
}

#[test]
fn test_grpc_message_types() {
    let transport = SimpleGrpcTransport::new();

    let test_cases = vec![
        (MessageType::Event, "Event message"),
        (MessageType::Query, "Query message"),
        (MessageType::State, "State message"),
        (MessageType::Control, "Control message"),
        (MessageType::Heartbeat, "Heartbeat message"),
        (MessageType::Checkpoint, "Checkpoint message"),
    ];

    for (msg_type, payload) in test_cases {
        let original_message = Message {
            id: uuid::Uuid::new_v4().to_string(),
            payload: payload.as_bytes().to_vec(),
            headers: std::collections::HashMap::new(),
            message_type: msg_type.clone(),
        };

        let proto_message = transport.to_proto_message(&original_message);
        let converted_message = transport.from_proto_message(&proto_message);

        assert_eq!(
            original_message.message_type,
            converted_message.message_type
        );
        assert_eq!(original_message.payload, converted_message.payload);
    }
}

#[test]
fn test_grpc_config_validation() {
    let config = SimpleGrpcConfig::default();
    assert_eq!(config.connection_timeout_ms, 10000);
    assert_eq!(config.enable_compression, true);
    assert_eq!(config.server_address, "127.0.0.1:50051");

    let custom_config = SimpleGrpcConfig {
        connection_timeout_ms: 15000,
        enable_compression: false,
        server_address: "0.0.0.0:9090".to_string(),
    };
    assert_eq!(custom_config.connection_timeout_ms, 15000);
    assert_eq!(custom_config.enable_compression, false);
    assert_eq!(custom_config.server_address, "0.0.0.0:9090");
}

#[tokio::test]
async fn test_grpc_connection_attempt() {
    let transport = SimpleGrpcTransport::new();

    // Test connection to a non-existent endpoint (should fail gracefully)
    let result = transport.connect("127.0.0.1:99999").await;
    assert!(result.is_err());

    // Verify error message is meaningful
    if let Err(e) = result {
        let error_msg = format!("{}", e);
        assert!(error_msg.contains("Connection failed"));
    }
}

#[tokio::test]
async fn test_grpc_message_serialization_with_headers() {
    let transport = SimpleGrpcTransport::new();

    let mut headers = std::collections::HashMap::new();
    headers.insert("priority".to_string(), "high".to_string());
    headers.insert("source_node".to_string(), "node-A".to_string());
    headers.insert("target_node".to_string(), "node-B".to_string());
    headers.insert("timestamp".to_string(), "1672531200000".to_string());

    let original_message = Message {
        id: "test-message-123".to_string(),
        payload: b"Complex message with headers".to_vec(),
        headers,
        message_type: MessageType::Event,
    };

    let proto_message = transport.to_proto_message(&original_message);

    // Verify protobuf fields are set correctly
    assert_eq!(proto_message.id, "test-message-123");
    assert_eq!(proto_message.payload, b"Complex message with headers");
    assert_eq!(
        proto_message.headers.get("priority"),
        Some(&"high".to_string())
    );
    assert_eq!(
        proto_message.headers.get("source_node"),
        Some(&"node-A".to_string())
    );
    assert_eq!(
        proto_message.headers.get("target_node"),
        Some(&"node-B".to_string())
    );
    assert!(proto_message.timestamp > 0);
    assert_eq!(proto_message.priority, 5);

    // Test round-trip conversion
    let converted_message = transport.from_proto_message(&proto_message);
    assert_eq!(original_message.id, converted_message.id);
    assert_eq!(original_message.payload, converted_message.payload);
    assert_eq!(
        original_message.message_type,
        converted_message.message_type
    );
    assert_eq!(original_message.headers, converted_message.headers);
}

#[test]
fn test_grpc_compression_settings() {
    // Test with compression enabled
    let config_compressed = SimpleGrpcConfig {
        enable_compression: true,
        ..Default::default()
    };
    let transport_compressed = SimpleGrpcTransport::with_config(config_compressed);

    let message = Message::event(b"test".to_vec());
    let proto_msg_compressed = transport_compressed.to_proto_message(&message);

    // Should use Zstd compression when enabled
    assert_ne!(
        proto_msg_compressed.compression,
        eventflux::core::distributed::grpc::CompressionType::None as i32
    );

    // Test with compression disabled
    let config_uncompressed = SimpleGrpcConfig {
        enable_compression: false,
        ..Default::default()
    };
    let transport_uncompressed = SimpleGrpcTransport::with_config(config_uncompressed);

    let proto_msg_uncompressed = transport_uncompressed.to_proto_message(&message);
    assert_eq!(
        proto_msg_uncompressed.compression,
        eventflux::core::distributed::grpc::CompressionType::None as i32
    );
}
