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

// src/core/distributed/grpc/simple_transport.rs

//! Simple gRPC transport.
//!
//! A small gRPC client that calls the transport service directly. It does not
//! implement the shared `Transport` trait.

use super::{CompressionType, HeartbeatRequest, HeartbeatResponse, MessageType, TransportMessage};
use crate::core::distributed::transport::Message;
use crate::core::distributed::{DistributedError, DistributedResult};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tonic::Request;

/// Settings for [`SimpleGrpcTransport`].
#[derive(Debug, Clone)]
pub struct SimpleGrpcConfig {
    /// Request timeout in milliseconds, set on the channel. Default 10000.
    pub connection_timeout_ms: u64,
    /// When true, outgoing messages are tagged `Zstd`. The payload is not
    /// compressed here. Default true.
    pub enable_compression: bool,
    /// Server address. Default `127.0.0.1:50051`. Not read by the client, which
    /// takes the endpoint on each call.
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

/// Simple gRPC client transport.
pub struct SimpleGrpcTransport {
    config: SimpleGrpcConfig,
    /// Connected clients, keyed by endpoint.
    clients: Arc<RwLock<HashMap<String, super::transport_client::TransportClient<Channel>>>>,
}

impl Default for SimpleGrpcTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl SimpleGrpcTransport {
    /// Create a transport with the default settings.
    pub fn new() -> Self {
        Self {
            config: SimpleGrpcConfig::default(),
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a transport with the given settings.
    pub fn with_config(config: SimpleGrpcConfig) -> Self {
        Self {
            config,
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Connect to a gRPC server over `http://` and keep the client for later calls.
    ///
    /// Replaces any client already stored for the endpoint.
    ///
    /// # Errors
    ///
    /// Returns `TransportError` if the endpoint is not a valid URI or the connection
    /// fails.
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

    /// Send a message and return the server reply.
    ///
    /// # Errors
    ///
    /// Returns `TransportError` if [`connect`](Self::connect) was not called for the
    /// endpoint, or the gRPC call fails.
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

        let response_message = self.decode_proto_message(&response.into_inner());
        Ok(response_message)
    }

    /// Send a heartbeat for `node_id` and return the server reply.
    ///
    /// # Errors
    ///
    /// Returns `TransportError` if [`connect`](Self::connect) was not called for the
    /// endpoint, or the gRPC call fails.
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
            status: None, // node status is not reported yet
        });

        let response = client.clone().heartbeat(request).await.map_err(|e| {
            DistributedError::TransportError {
                message: format!("Heartbeat failed: {}", e),
            }
        })?;

        Ok(response.into_inner())
    }

    /// Convert a local message to its protobuf form.
    ///
    /// Stamps the current time in milliseconds and reads `source_node` and
    /// `target_node` from the headers. Priority is always 5.
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

    /// Convert a protobuf message back to a local message.
    pub fn decode_proto_message(&self, proto_msg: &TransportMessage) -> Message {
        Message {
            id: proto_msg.id.clone(),
            payload: proto_msg.payload.clone(),
            headers: proto_msg.headers.clone(),
            message_type: self.decode_proto_message_type(proto_msg.message_type),
        }
    }

    /// Map a local message type to the protobuf type.
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

    /// Map a protobuf message type to the local type. Types with no local match
    /// become `Event`.
    fn decode_proto_message_type(
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

    #[test]
    fn test_simple_grpc_transport_creation() {
        let transport = SimpleGrpcTransport::new();
        assert_eq!(transport.config.connection_timeout_ms, 10000);
        assert!(transport.config.enable_compression);
    }

    #[test]
    fn test_message_conversion() {
        let transport = SimpleGrpcTransport::new();

        let original_message = Message::event(b"test data".to_vec())
            .with_header("source_node".to_string(), "node1".to_string());

        let proto_message = transport.to_proto_message(&original_message);
        let converted_message = transport.decode_proto_message(&proto_message);

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
        assert!(config.enable_compression);
        assert_eq!(config.server_address, "127.0.0.1:50051");
    }
}
