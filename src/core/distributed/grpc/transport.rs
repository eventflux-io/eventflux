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

// src/core/distributed/grpc/transport.rs

//! gRPC transport.
//!
//! Adapts the Tonic gRPC client to the shared [`Transport`] trait. Sending works
//! through unary `send_message` calls with one cached client per endpoint. Listening,
//! receiving and accepting are not wired to a real gRPC server yet. `listen` returns a
//! handle only, and `receive` and `accept` return errors.

use super::transport_client::TransportClient;
use super::{
    ClusterInfo, CompressionType, HeartbeatRequest, HeartbeatResponse, MessageType, NodeHealth,
    NodeStatus, TransportMessage,
};

use crate::core::distributed::transport::{
    Connection, ConnectionState, Listener, Message, MessageType as LocalMessageType, Transport,
    TransportFactory,
};
use crate::core::distributed::{DistributedError, DistributedResult};

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::time::Duration;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::{Request, Response, Status};

/// gRPC transport built on Tonic.
#[allow(dead_code)]
pub struct GrpcTransport {
    /// Cached clients, keyed by endpoint.
    clients: Arc<RwLock<HashMap<String, TransportClient<Channel>>>>,
    config: GrpcTransportConfig,
    /// Server handle when running as a server. Never set yet.
    server_handle: Arc<RwLock<Option<GrpcServerHandle>>>,
}

/// gRPC transport settings.
#[derive(Debug, Clone)]
pub struct GrpcTransportConfig {
    /// Connect timeout in milliseconds. Also used as the per request timeout on the
    /// channel. Default 10000.
    pub connection_timeout_ms: u64,
    /// Request timeout in milliseconds. Default 30000. Not read by the channel setup.
    pub request_timeout_ms: u64,
    /// Keep alive interval in seconds. Default 30. Not read by the channel setup.
    pub keep_alive_interval_secs: u64,
    /// Largest message size in bytes. Default 50 MB. Not enforced yet.
    pub max_message_size: usize,
    /// When true, outgoing messages are tagged with `preferred_compression`. The
    /// gRPC channel itself does not compress. Default true.
    pub enable_compression: bool,
    /// Compression type written into outgoing messages. Default `Zstd`.
    pub preferred_compression: CompressionType,
    /// Send HTTP/2 keep alive pings while the connection is idle. Default true.
    pub http2_keep_alive_enabled: bool,
    /// HTTP/2 keep alive ping interval. Default 30 seconds.
    pub http2_keep_alive_interval: Duration,
    /// How long to wait for a keep alive reply. Default 5 seconds.
    pub http2_keep_alive_timeout: Duration,
    /// TLS settings. `None` means plain text.
    pub tls_config: Option<GrpcTlsConfig>,
    /// Concurrent requests allowed per connection. Default 1000. Not enforced yet.
    pub max_concurrent_requests: usize,
}

/// TLS settings for gRPC.
///
/// The client reads only `enabled`, `ca_file` and `server_name`.
#[derive(Debug, Clone)]
pub struct GrpcTlsConfig {
    pub enabled: bool,
    /// Certificate file path.
    pub cert_file: Option<String>,
    /// Private key file path.
    pub key_file: Option<String>,
    /// CA certificate file path, in PEM format.
    pub ca_file: Option<String>,
    /// Server name to check the certificate against.
    pub server_name: Option<String>,
    /// Skip certificate checks. For tests only.
    pub insecure_skip_verify: bool,
}

impl Default for GrpcTransportConfig {
    fn default() -> Self {
        GrpcTransportConfig {
            connection_timeout_ms: 10000,
            request_timeout_ms: 30000,
            keep_alive_interval_secs: 30,
            max_message_size: 50 * 1024 * 1024, // 50MB
            enable_compression: true,
            preferred_compression: CompressionType::Zstd,
            http2_keep_alive_enabled: true,
            http2_keep_alive_interval: Duration::from_secs(30),
            http2_keep_alive_timeout: Duration::from_secs(5),
            tls_config: None,
            max_concurrent_requests: 1000,
        }
    }
}

/// Handle to a running gRPC server.
#[allow(dead_code)]
struct GrpcServerHandle {
    /// Send on this to stop the server.
    shutdown_tx: oneshot::Sender<()>,
    join_handle: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

/// One gRPC connection to a remote node.
#[allow(dead_code)]
pub struct GrpcConnection {
    pub id: String,
    /// Remote endpoint.
    pub endpoint: String,
    pub state: ConnectionState,
    client: TransportClient<Channel>,
    /// Outgoing side of a streaming call.
    message_tx: Option<mpsc::UnboundedSender<TransportMessage>>,
    /// Incoming side of a streaming call.
    response_rx: Option<mpsc::UnboundedReceiver<TransportMessage>>,
}

/// A gRPC server listening on an endpoint.
#[allow(dead_code)]
pub struct GrpcListener {
    pub endpoint: String,
    server_handle: Option<GrpcServerHandle>,
    /// Receives connections accepted by the server.
    connection_rx: Arc<RwLock<Option<mpsc::UnboundedReceiver<GrpcConnection>>>>,
}

impl Default for GrpcTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl GrpcTransport {
    /// Create a transport with the default settings.
    pub fn new() -> Self {
        GrpcTransport {
            clients: Arc::new(RwLock::new(HashMap::new())),
            config: GrpcTransportConfig::default(),
            server_handle: Arc::new(RwLock::new(None)),
        }
    }

    /// Create a transport with the given settings.
    pub fn with_config(config: GrpcTransportConfig) -> Self {
        GrpcTransport {
            clients: Arc::new(RwLock::new(HashMap::new())),
            config,
            server_handle: Arc::new(RwLock::new(None)),
        }
    }

    /// Convert a local message to its protobuf form.
    ///
    /// Stamps the current time in milliseconds. Reads `source_node`, `target_node`
    /// and `priority` from the headers. Priority defaults to 5 when missing or not a
    /// number.
    fn to_proto_message(&self, message: &Message) -> TransportMessage {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        TransportMessage {
            id: message.id.clone(),
            message_type: self.to_proto_message_type(&message.message_type) as i32,
            payload: message.payload.clone(),
            headers: message.headers.clone(),
            timestamp,
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
            priority: message
                .headers
                .get("priority")
                .and_then(|p| p.parse().ok())
                .unwrap_or(5),
            compression: if self.config.enable_compression {
                self.config.preferred_compression as i32
            } else {
                CompressionType::None as i32
            },
        }
    }

    /// Convert a protobuf message back to a local message.
    #[allow(dead_code)]
    fn decode_proto_message(&self, proto_msg: &TransportMessage) -> Message {
        Message {
            id: proto_msg.id.clone(),
            payload: proto_msg.payload.clone(),
            headers: proto_msg.headers.clone(),
            message_type: self.decode_proto_message_type(proto_msg.message_type),
        }
    }

    /// Map a local message type to the protobuf type.
    fn to_proto_message_type(&self, msg_type: &LocalMessageType) -> MessageType {
        match msg_type {
            LocalMessageType::Event => MessageType::Event,
            LocalMessageType::Query => MessageType::Query,
            LocalMessageType::State => MessageType::State,
            LocalMessageType::Control => MessageType::Control,
            LocalMessageType::Heartbeat => MessageType::Heartbeat,
            LocalMessageType::Checkpoint => MessageType::Checkpoint,
        }
    }

    /// Map a protobuf message type to the local type.
    ///
    /// Unknown, unspecified and types with no local match (such as `Acknowledgment`)
    /// become `Event`.
    #[allow(dead_code)]
    fn decode_proto_message_type(&self, proto_type: i32) -> LocalMessageType {
        match MessageType::try_from(proto_type).unwrap_or(MessageType::Unspecified) {
            MessageType::Event => LocalMessageType::Event,
            MessageType::Query => LocalMessageType::Query,
            MessageType::State => LocalMessageType::State,
            MessageType::Control => LocalMessageType::Control,
            MessageType::Heartbeat => LocalMessageType::Heartbeat,
            MessageType::Checkpoint => LocalMessageType::Checkpoint,
            _ => LocalMessageType::Event,
        }
    }

    /// Return the cached client for an endpoint, or connect and cache a new one.
    ///
    /// Connects over `http://` unless TLS is enabled in the settings.
    ///
    /// # Errors
    ///
    /// Returns `TransportError` if the endpoint is not a valid URI, the CA file
    /// cannot be read, TLS setup fails, or the connection fails.
    async fn get_client(&self, endpoint: &str) -> DistributedResult<TransportClient<Channel>> {
        {
            let clients = self.clients.read().await;
            if let Some(client) = clients.get(endpoint) {
                return Ok(client.clone());
            }
        }

        let mut channel_builder =
            Channel::from_shared(format!("http://{}", endpoint)).map_err(|e| {
                DistributedError::TransportError {
                    message: format!("Invalid endpoint {}: {}", endpoint, e),
                }
            })?;

        channel_builder = channel_builder
            .timeout(Duration::from_millis(self.config.connection_timeout_ms))
            .connect_timeout(Duration::from_millis(self.config.connection_timeout_ms))
            .http2_keep_alive_interval(self.config.http2_keep_alive_interval)
            .keep_alive_timeout(self.config.http2_keep_alive_timeout)
            .keep_alive_while_idle(self.config.http2_keep_alive_enabled);

        if let Some(tls_config) = &self.config.tls_config {
            if tls_config.enabled {
                let mut client_tls = ClientTlsConfig::new();

                if let Some(ca_file) = &tls_config.ca_file {
                    let ca_cert =
                        std::fs::read(ca_file).map_err(|e| DistributedError::TransportError {
                            message: format!("Failed to read CA certificate: {}", e),
                        })?;
                    client_tls = client_tls.ca_certificate(Certificate::from_pem(ca_cert));
                }

                if let Some(server_name) = &tls_config.server_name {
                    client_tls = client_tls.domain_name(server_name);
                }

                channel_builder = channel_builder.tls_config(client_tls).map_err(|e| {
                    DistributedError::TransportError {
                        message: format!("Failed to configure TLS: {}", e),
                    }
                })?;
            }
        }

        let channel =
            channel_builder
                .connect()
                .await
                .map_err(|e| DistributedError::TransportError {
                    message: format!("Failed to connect to {}: {}", endpoint, e),
                })?;

        let client = TransportClient::new(channel);

        // The channel does not compress. Outgoing messages only carry a compression
        // tag, and payloads are expected to be compressed by the caller. Tonic
        // compression needs specific feature flags, so the code below stays off.
        // if self.config.enable_compression {
        //     client = client
        //         .send_compressed(tonic::codec::CompressionEncoding::Gzip)
        //         .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
        // }

        {
            let mut clients = self.clients.write().await;
            clients.insert(endpoint.to_string(), client.clone());
        }

        Ok(client)
    }
}

#[async_trait]
impl Transport for GrpcTransport {
    async fn connect(&self, endpoint: &str) -> DistributedResult<Connection> {
        let _client = self.get_client(endpoint).await?;

        // Connecting only warms the client cache. The returned connection holds the
        // endpoint, and `send` looks the client up again by that endpoint.
        Ok(Connection {
            id: uuid::Uuid::new_v4().to_string(),
            endpoint: endpoint.to_string(),
            state: ConnectionState::Connected,
            handle: Some(Arc::new(RwLock::new(
                crate::core::distributed::transport::ConnectionHandle::Grpc(endpoint.to_string()),
            ))),
        })
    }

    async fn listen(&self, endpoint: &str) -> DistributedResult<Listener> {
        // No gRPC server is started. This only returns a handle naming the endpoint.
        Ok(Listener {
            endpoint: endpoint.to_string(),
            handle: Some(Arc::new(RwLock::new(
                crate::core::distributed::transport::ListenerHandle::Grpc(endpoint.to_string()),
            ))),
        })
    }

    async fn send(&self, connection: &Connection, message: Message) -> DistributedResult<()> {
        let client = self.get_client(&connection.endpoint).await?;
        let proto_message = self.to_proto_message(&message);

        let request = Request::new(proto_message);
        let response = client.clone().send_message(request).await.map_err(|e| {
            DistributedError::TransportError {
                message: format!("Failed to send message: {}", e),
            }
        })?;

        // The reply is an acknowledgment and is not used.
        let _response_message = response.into_inner();

        Ok(())
    }

    async fn receive(&self, _connection: &Connection) -> DistributedResult<Message> {
        // gRPC delivers incoming messages through streaming calls, which are not
        // wired up here. Direct receive always fails.
        Err(DistributedError::TransportError {
            message: "Direct receive not supported for gRPC. Use streaming instead.".to_string(),
        })
    }

    async fn close(&self, connection: Connection) -> DistributedResult<()> {
        // Drops the cached client, so every connection to this endpoint is affected.
        let mut clients = self.clients.write().await;
        clients.remove(&connection.endpoint);

        Ok(())
    }

    async fn accept(&self, _listener: &Listener) -> DistributedResult<Connection> {
        // The gRPC server framework accepts connections itself. No server is wired
        // up here, so this always fails.
        Err(DistributedError::TransportError {
            message: "Accept not implemented for gRPC. Use service handlers instead.".to_string(),
        })
    }
}

/// Server side request handlers for the gRPC transport service.
#[allow(dead_code)]
pub struct GrpcTransportService {
    /// Announces new connections. Not used by the handlers yet.
    connection_tx: mpsc::UnboundedSender<GrpcConnection>,
}

impl GrpcTransportService {
    pub fn new(connection_tx: mpsc::UnboundedSender<GrpcConnection>) -> Self {
        Self { connection_tx }
    }
}

/// Stub handlers. They do not implement the generated service trait, which needs
/// its associated stream types defined first.
impl GrpcTransportService {
    /// Handle a send message request.
    ///
    /// Does not process the message. Always replies with an `Acknowledgment` whose
    /// payload is `ACK`, addressed back to the sender.
    pub async fn handle_send_message(
        &self,
        request: Request<TransportMessage>,
    ) -> Result<Response<TransportMessage>, Status> {
        let message = request.into_inner();

        let response_message = TransportMessage {
            id: uuid::Uuid::new_v4().to_string(),
            message_type: MessageType::Acknowledgment as i32,
            payload: b"ACK".to_vec(),
            headers: HashMap::new(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            source_node: "server".to_string(),
            target_node: message.source_node.clone(),
            priority: 0,
            compression: CompressionType::None as i32,
        };

        Ok(Response::new(response_message))
    }

    /// Handle a heartbeat request.
    ///
    /// Replies with the current server time and fixed placeholder node and cluster
    /// status. The numbers are not measured.
    pub async fn handle_heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let _heartbeat_req = request.into_inner();

        let response = HeartbeatResponse {
            server_timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            server_node_id: "grpc-server".to_string(),
            server_status: Some(NodeStatus {
                health: NodeHealth::Healthy as i32,
                cpu_usage: 25.0,
                memory_usage: 40.0,
                active_connections: 5,
                events_per_second: 1000,
                version: "1.0.0".to_string(),
                metadata: HashMap::new(),
            }),
            cluster_info: Some(ClusterInfo {
                cluster_id: "eventflux-cluster".to_string(),
                total_nodes: 3,
                healthy_nodes: 3,
                leader_node: "node-1".to_string(),
                cluster_version: "1.0.0".to_string(),
            }),
        };

        Ok(Response::new(response))
    }
}

/// gRPC constructors for the transport factory.
impl TransportFactory {
    /// Create a gRPC transport with the default settings.
    pub fn create_grpc() -> Arc<dyn Transport> {
        Arc::new(GrpcTransport::new())
    }

    /// Create a gRPC transport with the given settings.
    pub fn create_grpc_with_config(config: GrpcTransportConfig) -> Arc<dyn Transport> {
        Arc::new(GrpcTransport::with_config(config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_grpc_transport_creation() {
        let transport = GrpcTransport::new();
        assert_eq!(transport.config.connection_timeout_ms, 10000);
        assert!(transport.config.enable_compression);
    }

    #[tokio::test]
    async fn test_message_conversion() {
        let transport = GrpcTransport::new();

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
    fn test_grpc_config_default() {
        let config = GrpcTransportConfig::default();
        assert_eq!(config.connection_timeout_ms, 10000);
        assert!(config.enable_compression);
        assert_eq!(config.preferred_compression, CompressionType::Zstd);
        assert_eq!(config.max_concurrent_requests, 1000);
    }

    #[test]
    fn test_grpc_factory() {
        let _transport = TransportFactory::create_grpc();

        let custom_config = GrpcTransportConfig {
            connection_timeout_ms: 5000,
            enable_compression: false,
            ..Default::default()
        };
        let _transport_custom = TransportFactory::create_grpc_with_config(custom_config);
    }
}
