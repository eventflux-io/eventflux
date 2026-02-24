// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/distributed/grpc/simple_transport.rs

//! Simplified gRPC Transport Implementation
//!
//! This is a working, simplified gRPC transport implementation that demonstrates
//! the core concepts without the full complexity of the unified transport interface.

use super::{CompressionType, HeartbeatRequest, HeartbeatResponse, MessageType, TransportMessage};
use crate::core::distributed::transport::Message;
use crate::core::distributed::{DistributedError, DistributedResult};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tonic::Request;

/// Simplified gRPC transport configuration
#[derive(Debug, Clone)]
pub struct SimpleGrpcConfig {
    /// Connection timeout in milliseconds
    pub connection_timeout_ms: u64,
    /// Enable compression
    pub enable_compression: bool,
    /// Server address
    pub server_address: String,
}

impl Default for SimpleGrpcConfig {
    fn default() -> Self {
        SimpleGrpcConfig {
            connection_timeout_ms: 10000,
            enable_compression: true,
            server_address: "127.0.0.1:50051".to_string(),
        }
    }
}

/// Simplified gRPC transport
pub struct SimpleGrpcTransport {
    /// Configuration
    config: SimpleGrpcConfig,
    /// Client connections
    clients: Arc<RwLock<HashMap<String, super::transport_client::TransportClient<Channel>>>>,
}

impl SimpleGrpcTransport {
    /// Create a new simple gRPC transport
    pub fn new() -> Self {
        Self {
            config: SimpleGrpcConfig::default(),
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create with custom configuration
    pub fn with_config(config: SimpleGrpcConfig) -> Self {
        Self {
            config,
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Connect to a gRPC server
    pub async fn connect(&self, endpoint: &str) -> DistributedResult<()> {
        let channel = Channel::from_shared(format!("http://{}", endpoint))
            .map_err(|e| DistributedError::TransportError {
                message: format!("Invalid endpoint: {}", e),
            })?
            .timeout(Duration::from_millis(self.config.connection_timeout_ms))
            .connect()
            .await
            .map_err(|e| DistributedError::TransportError {
                message: format!("Connection failed: {}", e),
            })?;

        let client = super::transport_client::TransportClient::new(channel);

        let mut clients = self.clients.write().await;
        clients.insert(endpoint.to_string(), client);

        Ok(())
    }

    /// Send a message via gRPC
    pub async fn send_message(
        &self,
        endpoint: &str,
        message: Message,
    ) -> DistributedResult<Message> {
        let clients = self.clients.read().await;
        let client = clients
            .get(endpoint)
            .ok_or_else(|| DistributedError::TransportError {
                message: "Not connected to endpoint".to_string(),
            })?;

        let proto_message = self.to_proto_message(&message);
        let request = Request::new(proto_message);

        let response = client.clone().send_message(request).await.map_err(|e| {
            DistributedError::TransportError {
                message: format!("gRPC call failed: {}", e),
            }
        })?;

        let response_message = self.from_proto_message(&response.into_inner());
        Ok(response_message)
    }

    /// Send heartbeat
    pub async fn heartbeat(
        &self,
        endpoint: &str,
        node_id: String,
    ) -> DistributedResult<HeartbeatResponse> {
        let clients = self.clients.read().await;
        let client = clients
            .get(endpoint)
            .ok_or_else(|| DistributedError::TransportError {
                message: "Not connected to endpoint".to_string(),
            })?;

        let request = Request::new(HeartbeatRequest {
            node_id,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            status: None, // Simplified for now
        });

        let response = client.clone().heartbeat(request).await.map_err(|e| {
            DistributedError::TransportError {
                message: format!("Heartbeat failed: {}", e),
            }
        })?;

        Ok(response.into_inner())
    }

    /// Convert local message to protobuf message
    pub fn to_proto_message(&self, message: &Message) -> TransportMessage {
        TransportMessage {
            id: message.id.clone(),
            message_type: self.to_proto_message_type(&message.message_type) as i32,
            payload: message.payload.clone(),
            headers: message.headers.clone(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            source_node: message
                .headers
                .get("source_node")
                .cloned()
                .unwrap_or_default(),
            target_node: message
                .headers
                .get("target_node")
                .cloned()
                .unwrap_or_default(),
            priority: 5,
            compression: if self.config.enable_compression {
                CompressionType::Zstd as i32
            } else {
                CompressionType::None as i32
            },
        }
    }

    /// Convert protobuf message to local message
    pub fn from_proto_message(&self, proto_msg: &TransportMessage) -> Message {
        Message {
            id: proto_msg.id.clone(),
            payload: proto_msg.payload.clone(),
            headers: proto_msg.headers.clone(),
            message_type: self.from_proto_message_type(proto_msg.message_type),
        }
    }

    /// Convert message type
    fn to_proto_message_type(
        &self,
        msg_type: &crate::core::distributed::transport::MessageType,
    ) -> MessageType {
        match msg_type {
            crate::core::distributed::transport::MessageType::Event => MessageType::Event,
            crate::core::distributed::transport::MessageType::Query => MessageType::Query,
            crate::core::distributed::transport::MessageType::State => MessageType::State,
            crate::core::distributed::transport::MessageType::Control => MessageType::Control,
            crate::core::distributed::transport::MessageType::Heartbeat => MessageType::Heartbeat,
            crate::core::distributed::transport::MessageType::Checkpoint => MessageType::Checkpoint,
        }
    }

    /// Convert protobuf message type to local message type
    fn from_proto_message_type(
        &self,
        proto_type: i32,
    ) -> crate::core::distributed::transport::MessageType {
        match MessageType::try_from(proto_type).unwrap_or(MessageType::Unspecified) {
            MessageType::Event => crate::core::distributed::transport::MessageType::Event,
            MessageType::Query => crate::core::distributed::transport::MessageType::Query,
            MessageType::State => crate::core::distributed::transport::MessageType::State,
            MessageType::Control => crate::core::distributed::transport::MessageType::Control,
            MessageType::Heartbeat => crate::core::distributed::transport::MessageType::Heartbeat,
            MessageType::Checkpoint => crate::core::distributed::transport::MessageType::Checkpoint,
            _ => crate::core::distributed::transport::MessageType::Event,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::distributed::transport::MessageType;

    #[test]
    fn test_simple_grpc_transport_creation() {
        let transport = SimpleGrpcTransport::new();
        assert_eq!(transport.config.connection_timeout_ms, 10000);
        assert_eq!(transport.config.enable_compression, true);
    }

    #[test]
    fn test_message_conversion() {
        let transport = SimpleGrpcTransport::new();

        let original_message = Message::event(b"test data".to_vec())
            .with_header("source_node".to_string(), "node1".to_string());

        let proto_message = transport.to_proto_message(&original_message);
        let converted_message = transport.from_proto_message(&proto_message);

        assert_eq!(original_message.id, converted_message.id);
        assert_eq!(original_message.payload, converted_message.payload);
        assert_eq!(
            original_message.message_type,
            converted_message.message_type
        );
    }

    #[test]
    fn test_config_default() {
        let config = SimpleGrpcConfig::default();
        assert_eq!(config.connection_timeout_ms, 10000);
        assert_eq!(config.enable_compression, true);
        assert_eq!(config.server_address, "127.0.0.1:50051");
    }
}
