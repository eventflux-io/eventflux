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

// src/core/distributed/transport.rs

//! Transport layer.
//!
//! Defines one trait for sending messages between nodes, with a TCP transport. The
//! gRPC transport lives in the `grpc` module.

use super::{DistributedError, DistributedResult};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

/// Sends and receives messages between nodes.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Connect to a remote node.
    async fn connect(&self, endpoint: &str) -> DistributedResult<Connection>;

    /// Start listening for incoming connections on `endpoint`.
    async fn listen(&self, endpoint: &str) -> DistributedResult<Listener>;

    async fn send(&self, connection: &Connection, message: Message) -> DistributedResult<()>;

    /// Wait for the next message on a connection.
    async fn receive(&self, connection: &Connection) -> DistributedResult<Message>;

    async fn close(&self, connection: Connection) -> DistributedResult<()>;

    /// Wait for the next incoming connection on a listener.
    async fn accept(&self, listener: &Listener) -> DistributedResult<Connection>;
}

/// Connection to a remote node.
#[derive(Debug)]
pub struct Connection {
    pub id: String,
    pub endpoint: String,
    pub state: ConnectionState,
    /// The underlying socket or client. Shared with the transport pool.
    pub(crate) handle: Option<Arc<RwLock<ConnectionHandle>>>,
}

/// The underlying connection for each transport.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum ConnectionHandle {
    Tcp(TcpStream),
    // Holds only the endpoint. The gRPC client lives in the grpc module.
    Grpc(String),
    // Other transports such as RDMA may be added later.
}

#[derive(Debug, Clone)]
pub enum ConnectionState {
    Connected,
    Disconnected,
    Error(String),
}

/// Listens for incoming connections.
pub struct Listener {
    pub endpoint: String,
    /// The underlying listener.
    pub(crate) handle: Option<Arc<RwLock<ListenerHandle>>>,
}

impl Listener {
    /// Returns the address the listener is bound to.
    ///
    /// Useful after binding to port 0 to learn the port the OS picked. For gRPC
    /// this is the endpoint string as given. Returns `None` if there is no handle
    /// or the TCP address cannot be read.
    pub async fn local_addr(&self) -> Option<String> {
        if let Some(handle) = &self.handle {
            let handle_guard = handle.read().await;
            match &*handle_guard {
                ListenerHandle::Tcp(tcp_listener) => {
                    tcp_listener.local_addr().ok().map(|addr| addr.to_string())
                }
                ListenerHandle::Grpc(addr) => Some(addr.clone()),
            }
        } else {
            None
        }
    }
}

/// The underlying listener for each transport.
pub(crate) enum ListenerHandle {
    Tcp(TcpListener),
    // Holds only the endpoint. No gRPC server is started.
    Grpc(String),
    // Other transports such as RDMA may be added later.
}

/// A message sent between nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub payload: Vec<u8>,
    pub headers: HashMap<String, String>,
    pub message_type: MessageType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MessageType {
    Event,
    Query,
    State,
    Control,
    Heartbeat,
    Checkpoint,
}

impl Message {
    /// Create an event message with a new random id and no headers.
    pub fn event(payload: Vec<u8>) -> Self {
        Message {
            id: uuid::Uuid::new_v4().to_string(),
            payload,
            headers: HashMap::new(),
            message_type: MessageType::Event,
        }
    }

    /// Create a heartbeat message with a new random id and an empty payload.
    pub fn heartbeat() -> Self {
        Message {
            id: uuid::Uuid::new_v4().to_string(),
            payload: vec![],
            headers: HashMap::new(),
            message_type: MessageType::Heartbeat,
        }
    }

    /// Add a header, replacing any value already set for `key`.
    pub fn with_header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }
}

/// TCP transport.
///
/// Each message is sent as a 4 byte big endian length followed by the serialized
/// message.
pub struct TcpTransport {
    /// Open connections, keyed by endpoint, so `connect` can reuse them.
    connection_pool: Arc<RwLock<HashMap<String, Arc<RwLock<ConnectionHandle>>>>>,
    config: TcpTransportConfig,
}

/// TCP transport settings.
#[derive(Debug, Clone)]
pub struct TcpTransportConfig {
    /// Connect timeout in milliseconds. Default 5000.
    pub connection_timeout_ms: u64,
    /// Timeout for reading one message, in milliseconds. Default 30000.
    pub read_timeout_ms: u64,
    /// Timeout for writing one message, in milliseconds. Default 30000.
    pub write_timeout_ms: u64,
    /// Default true. Not applied to sockets yet.
    pub keepalive_enabled: bool,
    /// Keepalive interval in seconds. Default 30. Not applied to sockets yet.
    pub keepalive_interval_secs: u64,
    /// Send small packets right away instead of batching them (turns off Nagle's
    /// algorithm). Default true.
    pub nodelay: bool,
    /// Socket send buffer size in bytes. Default 65536. Not applied yet.
    pub send_buffer_size: Option<usize>,
    /// Socket receive buffer size in bytes. Default 65536. Not applied yet.
    pub recv_buffer_size: Option<usize>,
    /// Largest serialized message in bytes, checked on send and receive. Default
    /// 10 MB.
    pub max_message_size: usize,
}

impl Default for TcpTransportConfig {
    fn default() -> Self {
        TcpTransportConfig {
            connection_timeout_ms: 5000,
            read_timeout_ms: 30000,
            write_timeout_ms: 30000,
            keepalive_enabled: true,
            keepalive_interval_secs: 30,
            nodelay: true,
            send_buffer_size: Some(65536),
            recv_buffer_size: Some(65536),
            max_message_size: 10 * 1024 * 1024, // 10MB
        }
    }
}

impl Default for TcpTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl TcpTransport {
    /// Create a transport with the default settings.
    pub fn new() -> Self {
        TcpTransport {
            connection_pool: Arc::new(RwLock::new(HashMap::new())),
            config: TcpTransportConfig::default(),
        }
    }

    /// Create a transport with the given settings.
    pub fn with_config(config: TcpTransportConfig) -> Self {
        TcpTransport {
            connection_pool: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Apply socket settings to a stream. Only `nodelay` is applied.
    async fn configure_stream(&self, stream: &TcpStream) -> DistributedResult<()> {
        if self.config.nodelay {
            stream
                .set_nodelay(true)
                .map_err(|e| DistributedError::TransportError {
                    message: format!("Failed to set nodelay: {}", e),
                })?;
        }

        // Keepalive and buffer sizes are not applied. They need the platform
        // specific socket2 crate.

        Ok(())
    }

    /// Write one length prefixed message and flush.
    ///
    /// # Errors
    ///
    /// Returns `TransportError` if serializing fails, the message is larger than
    /// `max_message_size`, or the write fails.
    async fn write_message(
        &self,
        stream: &mut TcpStream,
        message: &Message,
    ) -> DistributedResult<()> {
        let serialized = crate::core::util::serialization::to_bytes(message).map_err(|e| {
            DistributedError::TransportError {
                message: format!("Failed to serialize message: {}", e),
            }
        })?;

        if serialized.len() > self.config.max_message_size {
            return Err(DistributedError::TransportError {
                message: format!(
                    "Message size {} exceeds maximum {}",
                    serialized.len(),
                    self.config.max_message_size
                ),
            });
        }

        // 4 byte big endian length prefix.
        let len = serialized.len() as u32;
        stream.write_all(&len.to_be_bytes()).await.map_err(|e| {
            DistributedError::TransportError {
                message: format!("Failed to write message length: {}", e),
            }
        })?;

        stream
            .write_all(&serialized)
            .await
            .map_err(|e| DistributedError::TransportError {
                message: format!("Failed to write message data: {}", e),
            })?;

        stream
            .flush()
            .await
            .map_err(|e| DistributedError::TransportError {
                message: format!("Failed to flush stream: {}", e),
            })?;

        Ok(())
    }

    /// Read one length prefixed message.
    ///
    /// # Errors
    ///
    /// Returns `TransportError` if the read fails, the length is larger than
    /// `max_message_size`, or the bytes do not deserialize.
    async fn read_message(&self, stream: &mut TcpStream) -> DistributedResult<Message> {
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .map_err(|e| DistributedError::TransportError {
                message: format!("Failed to read message length: {}", e),
            })?;

        let len = u32::from_be_bytes(len_buf) as usize;

        // Check before allocating so a bad length cannot force a huge buffer.
        if len > self.config.max_message_size {
            return Err(DistributedError::TransportError {
                message: format!(
                    "Message size {} exceeds maximum {}",
                    len, self.config.max_message_size
                ),
            });
        }

        let mut buf = vec![0u8; len];
        stream
            .read_exact(&mut buf)
            .await
            .map_err(|e| DistributedError::TransportError {
                message: format!("Failed to read message data: {}", e),
            })?;

        let message = crate::core::util::serialization::from_bytes(&buf).map_err(|e| {
            DistributedError::TransportError {
                message: format!("Failed to deserialize message: {}", e),
            }
        })?;

        Ok(message)
    }
}

#[async_trait]
impl Transport for TcpTransport {
    /// Reuse the pooled stream for `endpoint`, or open a new one and pool it.
    ///
    /// A reused stream is shared with every other connection to the same endpoint.
    ///
    /// # Errors
    ///
    /// Returns `TransportError` if connecting times out or fails.
    async fn connect(&self, endpoint: &str) -> DistributedResult<Connection> {
        {
            let pool = self.connection_pool.read().await;
            if pool.contains_key(endpoint) {
                return Ok(Connection {
                    id: uuid::Uuid::new_v4().to_string(),
                    endpoint: endpoint.to_string(),
                    state: ConnectionState::Connected,
                    handle: Some(pool.get(endpoint).unwrap().clone()),
                });
            }
        }

        let timeout = tokio::time::Duration::from_millis(self.config.connection_timeout_ms);
        let stream = tokio::time::timeout(timeout, TcpStream::connect(endpoint))
            .await
            .map_err(|_| DistributedError::TransportError {
                message: format!("Connection timeout to {}", endpoint),
            })?
            .map_err(|e| DistributedError::TransportError {
                message: format!("Failed to connect to {}: {}", endpoint, e),
            })?;

        self.configure_stream(&stream).await?;

        let handle = Arc::new(RwLock::new(ConnectionHandle::Tcp(stream)));

        {
            let mut pool = self.connection_pool.write().await;
            pool.insert(endpoint.to_string(), handle.clone());
        }

        Ok(Connection {
            id: uuid::Uuid::new_v4().to_string(),
            endpoint: endpoint.to_string(),
            state: ConnectionState::Connected,
            handle: Some(handle),
        })
    }

    async fn listen(&self, endpoint: &str) -> DistributedResult<Listener> {
        let listener =
            TcpListener::bind(endpoint)
                .await
                .map_err(|e| DistributedError::TransportError {
                    message: format!("Failed to bind to {}: {}", endpoint, e),
                })?;

        Ok(Listener {
            endpoint: endpoint.to_string(),
            handle: Some(Arc::new(RwLock::new(ListenerHandle::Tcp(listener)))),
        })
    }

    async fn send(&self, connection: &Connection, message: Message) -> DistributedResult<()> {
        let handle =
            connection
                .handle
                .as_ref()
                .ok_or_else(|| DistributedError::TransportError {
                    message: "Connection handle not found".to_string(),
                })?;

        let mut handle_guard = handle.write().await;

        match &mut *handle_guard {
            ConnectionHandle::Tcp(stream) => {
                let timeout = tokio::time::Duration::from_millis(self.config.write_timeout_ms);
                tokio::time::timeout(timeout, self.write_message(stream, &message))
                    .await
                    .map_err(|_| DistributedError::TransportError {
                        message: "Write timeout".to_string(),
                    })?
            }
            ConnectionHandle::Grpc(_) => {
                return Err(DistributedError::TransportError {
                    message: "gRPC connection send not implemented in unified interface"
                        .to_string(),
                });
            }
        }
    }

    async fn receive(&self, connection: &Connection) -> DistributedResult<Message> {
        let handle =
            connection
                .handle
                .as_ref()
                .ok_or_else(|| DistributedError::TransportError {
                    message: "Connection handle not found".to_string(),
                })?;

        let mut handle_guard = handle.write().await;

        match &mut *handle_guard {
            ConnectionHandle::Tcp(stream) => {
                let timeout = tokio::time::Duration::from_millis(self.config.read_timeout_ms);
                tokio::time::timeout(timeout, self.read_message(stream))
                    .await
                    .map_err(|_| DistributedError::TransportError {
                        message: "Read timeout".to_string(),
                    })?
            }
            ConnectionHandle::Grpc(_) => {
                return Err(DistributedError::TransportError {
                    message: "gRPC connection receive not implemented in unified interface"
                        .to_string(),
                });
            }
        }
    }

    async fn close(&self, connection: Connection) -> DistributedResult<()> {
        let mut pool = self.connection_pool.write().await;
        pool.remove(&connection.endpoint);

        // The socket closes once the last handle to it is dropped. Other
        // connections sharing it keep it open.
        Ok(())
    }

    async fn accept(&self, listener: &Listener) -> DistributedResult<Connection> {
        let handle = listener
            .handle
            .as_ref()
            .ok_or_else(|| DistributedError::TransportError {
                message: "Listener handle not found".to_string(),
            })?;

        let mut handle_guard = handle.write().await;

        match &mut *handle_guard {
            ListenerHandle::Tcp(tcp_listener) => {
                let (stream, addr) =
                    tcp_listener
                        .accept()
                        .await
                        .map_err(|e| DistributedError::TransportError {
                            message: format!("Failed to accept connection: {}", e),
                        })?;

                self.configure_stream(&stream).await?;

                let connection_handle = Arc::new(RwLock::new(ConnectionHandle::Tcp(stream)));
                let endpoint = addr.to_string();

                // Pooled under the peer address.
                {
                    let mut pool = self.connection_pool.write().await;
                    pool.insert(endpoint.clone(), connection_handle.clone());
                }

                Ok(Connection {
                    id: uuid::Uuid::new_v4().to_string(),
                    endpoint,
                    state: ConnectionState::Connected,
                    handle: Some(connection_handle),
                })
            }
            ListenerHandle::Grpc(_) => Err(DistributedError::TransportError {
                message: "gRPC listener accept not implemented in unified interface".to_string(),
            }),
        }
    }
}

/// Creates transports.
pub struct TransportFactory;

impl TransportFactory {
    /// Create a transport by name, ignoring case.
    ///
    /// # Errors
    ///
    /// Returns `TransportError` for `grpc`, which is not available here, and for
    /// any unknown name. Only `tcp` works.
    pub fn create(transport_type: &str) -> DistributedResult<Arc<dyn Transport>> {
        match transport_type.to_lowercase().as_str() {
            "tcp" => Ok(Arc::new(TcpTransport::new())),
            "grpc" => {
                // Use TransportFactory::create_grpc or GrpcTransport from the grpc
                // module instead.
                Err(DistributedError::TransportError {
                    message: "gRPC transport integration with unified interface is pending. Use grpc::transport::GrpcTransport directly.".to_string()
                })
            }
            // Not built yet: "rdma" => Ok(Arc::new(RdmaTransport::new())),
            _ => Err(DistributedError::TransportError {
                message: format!("Unknown transport type: {}", transport_type),
            }),
        }
    }

    /// Create a TCP transport with the given settings.
    pub fn create_tcp_with_config(config: TcpTransportConfig) -> Arc<dyn Transport> {
        Arc::new(TcpTransport::with_config(config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn should_panic_on_socket_skip() -> bool {
        // CI should always be able to bind loopback sockets. If it cannot, fail
        // instead of skipping so a broken setup or regression is not hidden.
        std::env::var("CI").is_ok() || std::env::var("GITHUB_ACTIONS").is_ok()
    }

    fn is_socket_permission_denied_message(message: &str) -> bool {
        let message = message.to_ascii_lowercase();
        message.contains("operation not permitted")
            || message.contains("permission denied")
            || message.contains("eacces")
            || message.contains("eperm")
            || message.contains("os error 1")
            || message.contains("os error 13")
    }

    #[tokio::test]
    async fn test_tcp_transport_connect_and_send() {
        let transport = TcpTransport::new();

        // Port 0 lets the OS pick a free port.
        let listener = match transport.listen("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(DistributedError::TransportError { message })
                if is_socket_permission_denied_message(&message) =>
            {
                // Some sandboxes do not allow binding sockets at all. Skip the
                // test there.
                if should_panic_on_socket_skip() {
                    panic!(
                        "CI environment cannot bind sockets; failing test_tcp_transport_connect_and_send ({message})"
                    );
                } else {
                    eprintln!(
                        "skipping test_tcp_transport_connect_and_send: cannot bind sockets ({message})"
                    );
                    return;
                }
            }
            Err(err) => panic!("Failed to start listener: {err:?}"),
        };

        let actual_addr = {
            let handle = listener.handle.as_ref().unwrap();
            let handle_guard = handle.read().await;
            match &*handle_guard {
                ListenerHandle::Tcp(tcp_listener) => tcp_listener.local_addr().unwrap().to_string(),
                ListenerHandle::Grpc(addr) => addr.clone(),
            }
        };

        let transport_clone = Arc::new(transport);
        let transport_acceptor = transport_clone.clone();
        let listener = Arc::new(listener);
        let listener_clone = listener.clone();

        let acceptor_task = tokio::spawn(async move {
            let connection = transport_acceptor.accept(&listener_clone).await.unwrap();
            let message = transport_acceptor.receive(&connection).await.unwrap();
            assert_eq!(message.message_type, MessageType::Event);
            assert_eq!(message.payload, b"test data");

            let response = Message::event(b"response".to_vec());
            transport_acceptor
                .send(&connection, response)
                .await
                .unwrap();
        });

        // Give the acceptor time to start.
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let connection = transport_clone.connect(&actual_addr).await.unwrap();
        let message = Message::event(b"test data".to_vec());
        transport_clone.send(&connection, message).await.unwrap();

        let response = transport_clone.receive(&connection).await.unwrap();
        assert_eq!(response.payload, b"response");

        transport_clone.close(connection).await.unwrap();
        acceptor_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_message_creation() {
        let message = Message::event(b"test".to_vec())
            .with_header("node_id".to_string(), "node1".to_string())
            .with_header("timestamp".to_string(), "123456".to_string());

        assert_eq!(message.message_type, MessageType::Event);
        assert_eq!(message.payload, b"test");
        assert_eq!(message.headers.get("node_id"), Some(&"node1".to_string()));
        assert_eq!(
            message.headers.get("timestamp"),
            Some(&"123456".to_string())
        );
    }

    #[tokio::test]
    async fn test_heartbeat_message() {
        let message = Message::heartbeat();
        assert_eq!(message.message_type, MessageType::Heartbeat);
        assert!(message.payload.is_empty());
    }

    #[test]
    fn test_tcp_config_default() {
        let config = TcpTransportConfig::default();
        assert_eq!(config.connection_timeout_ms, 5000);
        assert!(config.keepalive_enabled);
        assert!(config.nodelay);
        assert_eq!(config.max_message_size, 10 * 1024 * 1024);
    }

    #[test]
    fn test_transport_factory() {
        let _transport = TransportFactory::create("tcp").unwrap();

        let result = TransportFactory::create("unknown");
        assert!(result.is_err());
    }
}
