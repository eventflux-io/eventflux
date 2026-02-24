// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/distributed/transport.rs

//! Transport Layer Abstraction
//!
//! This module provides the transport layer abstraction for distributed communication.
//! It supports multiple transport implementations (TCP, gRPC, RDMA, etc.) while
//! maintaining a consistent interface.

use super::{DistributedError, DistributedResult};
use async_trait::async_trait;
use bincode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

/// Transport layer trait for distributed communication
#[async_trait]
pub trait Transport: Send + Sync {
    /// Connect to a remote node
    async fn connect(&self, endpoint: &str) -> DistributedResult<Connection>;

    /// Listen for incoming connections
    async fn listen(&self, endpoint: &str) -> DistributedResult<Listener>;

    /// Send a message to a remote node
    async fn send(&self, connection: &Connection, message: Message) -> DistributedResult<()>;

    /// Receive a message from a remote node
    async fn receive(&self, connection: &Connection) -> DistributedResult<Message>;

    /// Close a connection
    async fn close(&self, connection: Connection) -> DistributedResult<()>;

    /// Accept incoming connection from listener
    async fn accept(&self, listener: &Listener) -> DistributedResult<Connection>;
}

/// Connection to a remote node
#[derive(Debug)]
pub struct Connection {
    pub id: String,
    pub endpoint: String,
    pub state: ConnectionState,
    /// Internal handle for the actual connection
    pub(crate) handle: Option<Arc<RwLock<ConnectionHandle>>>,
}

/// Internal connection handle
#[derive(Debug)]
pub(crate) enum ConnectionHandle {
    Tcp(TcpStream),
    // Placeholder for gRPC - actual implementation in grpc module
    Grpc(String),
    // Future: RDMA, etc.
}

/// Connection state
#[derive(Debug, Clone)]
pub enum ConnectionState {
    Connected,
    Disconnected,
    Error(String),
}

/// Listener for incoming connections
pub struct Listener {
    pub endpoint: String,
    /// Internal handle for the actual listener
    pub(crate) handle: Option<Arc<RwLock<ListenerHandle>>>,
}

impl Listener {
    /// Get the actual local address the listener bound to (useful for port 0 bindings)
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

/// Internal listener handle
pub(crate) enum ListenerHandle {
    Tcp(TcpListener),
    // Placeholder for gRPC - actual implementation in grpc module
    Grpc(String),
    // Future: RDMA, etc.
}

/// Message for transport
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub payload: Vec<u8>,
    pub headers: HashMap<String, String>,
    pub message_type: MessageType,
}

/// Type of message being sent
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
    /// Create a new message with event payload
    pub fn event(payload: Vec<u8>) -> Self {
        Message {
            id: uuid::Uuid::new_v4().to_string(),
            payload,
            headers: HashMap::new(),
            message_type: MessageType::Event,
        }
    }

    /// Create a heartbeat message
    pub fn heartbeat() -> Self {
        Message {
            id: uuid::Uuid::new_v4().to_string(),
            payload: vec![],
            headers: HashMap::new(),
            message_type: MessageType::Heartbeat,
        }
    }

    /// Add a header to the message
    pub fn with_header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }
}

/// TCP transport implementation
pub struct TcpTransport {
    /// Connection pool for reusing connections
    connection_pool: Arc<RwLock<HashMap<String, Arc<RwLock<ConnectionHandle>>>>>,
    /// Configuration for TCP transport
    config: TcpTransportConfig,
}

/// Configuration for TCP transport
#[derive(Debug, Clone)]
pub struct TcpTransportConfig {
    /// Connection timeout in milliseconds
    pub connection_timeout_ms: u64,
    /// Read timeout in milliseconds
    pub read_timeout_ms: u64,
    /// Write timeout in milliseconds
    pub write_timeout_ms: u64,
    /// Enable TCP keepalive
    pub keepalive_enabled: bool,
    /// Keepalive interval in seconds
    pub keepalive_interval_secs: u64,
    /// Enable TCP nodelay (disable Nagle's algorithm)
    pub nodelay: bool,
    /// Send buffer size
    pub send_buffer_size: Option<usize>,
    /// Receive buffer size
    pub recv_buffer_size: Option<usize>,
    /// Maximum message size in bytes
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

impl TcpTransport {
    /// Create a new TCP transport with default configuration
    pub fn new() -> Self {
        TcpTransport {
            connection_pool: Arc::new(RwLock::new(HashMap::new())),
            config: TcpTransportConfig::default(),
        }
    }

    /// Create a new TCP transport with custom configuration
    pub fn with_config(config: TcpTransportConfig) -> Self {
        TcpTransport {
            connection_pool: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Configure TCP stream with transport settings
    async fn configure_stream(&self, stream: &TcpStream) -> DistributedResult<()> {
        // Set TCP nodelay if configured
        if self.config.nodelay {
            stream
                .set_nodelay(true)
                .map_err(|e| DistributedError::TransportError {
                    message: format!("Failed to set nodelay: {}", e),
                })?;
        }

        // Set socket options if available
        // Note: Some options require platform-specific socket2 crate for full control

        Ok(())
    }

    /// Write a message to a TCP stream
    async fn write_message(
        &self,
        stream: &mut TcpStream,
        message: &Message,
    ) -> DistributedResult<()> {
        // Serialize the message
        let serialized =
            bincode::serialize(message).map_err(|e| DistributedError::TransportError {
                message: format!("Failed to serialize message: {}", e),
            })?;

        // Check message size
        if serialized.len() > self.config.max_message_size {
            return Err(DistributedError::TransportError {
                message: format!(
                    "Message size {} exceeds maximum {}",
                    serialized.len(),
                    self.config.max_message_size
                ),
            });
        }

        // Write message length (4 bytes)
        let len = serialized.len() as u32;
        stream.write_all(&len.to_be_bytes()).await.map_err(|e| {
            DistributedError::TransportError {
                message: format!("Failed to write message length: {}", e),
            }
        })?;

        // Write message data
        stream
            .write_all(&serialized)
            .await
            .map_err(|e| DistributedError::TransportError {
                message: format!("Failed to write message data: {}", e),
            })?;

        // Flush the stream
        stream
            .flush()
            .await
            .map_err(|e| DistributedError::TransportError {
                message: format!("Failed to flush stream: {}", e),
            })?;

        Ok(())
    }

    /// Read a message from a TCP stream
    async fn read_message(&self, stream: &mut TcpStream) -> DistributedResult<Message> {
        // Read message length (4 bytes)
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .map_err(|e| DistributedError::TransportError {
                message: format!("Failed to read message length: {}", e),
            })?;

        let len = u32::from_be_bytes(len_buf) as usize;

        // Check message size
        if len > self.config.max_message_size {
            return Err(DistributedError::TransportError {
                message: format!(
                    "Message size {} exceeds maximum {}",
                    len, self.config.max_message_size
                ),
            });
        }

        // Read message data
        let mut buf = vec![0u8; len];
        stream
            .read_exact(&mut buf)
            .await
            .map_err(|e| DistributedError::TransportError {
                message: format!("Failed to read message data: {}", e),
            })?;

        // Deserialize the message
        let message = bincode::deserialize(&buf).map_err(|e| DistributedError::TransportError {
            message: format!("Failed to deserialize message: {}", e),
        })?;

        Ok(message)
    }
}

#[async_trait]
impl Transport for TcpTransport {
    async fn connect(&self, endpoint: &str) -> DistributedResult<Connection> {
        // Check if we have a cached connection
        {
            let pool = self.connection_pool.read().await;
            if pool.contains_key(endpoint) {
                // Reuse existing connection
                return Ok(Connection {
                    id: uuid::Uuid::new_v4().to_string(),
                    endpoint: endpoint.to_string(),
                    state: ConnectionState::Connected,
                    handle: Some(pool.get(endpoint).unwrap().clone()),
                });
            }
        }

        // Create new connection
        let timeout = tokio::time::Duration::from_millis(self.config.connection_timeout_ms);
        let stream = tokio::time::timeout(timeout, TcpStream::connect(endpoint))
            .await
            .map_err(|_| DistributedError::TransportError {
                message: format!("Connection timeout to {}", endpoint),
            })?
            .map_err(|e| DistributedError::TransportError {
                message: format!("Failed to connect to {}: {}", endpoint, e),
            })?;

        // Configure the stream
        self.configure_stream(&stream).await?;

        // Create connection handle
        let handle = Arc::new(RwLock::new(ConnectionHandle::Tcp(stream)));

        // Add to connection pool
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
        // Remove from connection pool
        let mut pool = self.connection_pool.write().await;
        pool.remove(&connection.endpoint);

        // Connection will be closed when handle is dropped
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

                // Configure the stream
                self.configure_stream(&stream).await?;

                // Create connection handle
                let connection_handle = Arc::new(RwLock::new(ConnectionHandle::Tcp(stream)));
                let endpoint = addr.to_string();

                // Add to connection pool
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

/// Transport factory for creating transport instances
pub struct TransportFactory;

impl TransportFactory {
    /// Create a transport instance based on the transport type
    pub fn create(transport_type: &str) -> DistributedResult<Arc<dyn Transport>> {
        match transport_type.to_lowercase().as_str() {
            "tcp" => Ok(Arc::new(TcpTransport::new())),
            "grpc" => {
                // Note: gRPC transport is implemented in the grpc module
                // For now, return TCP as fallback - full gRPC integration requires more work
                Err(DistributedError::TransportError {
                    message: "gRPC transport integration with unified interface is pending. Use grpc::transport::GrpcTransport directly.".to_string()
                })
            }
            // Future: "rdma" => Ok(Arc::new(RdmaTransport::new())),
            _ => Err(DistributedError::TransportError {
                message: format!("Unknown transport type: {}", transport_type),
            }),
        }
    }

    /// Create a TCP transport with custom configuration
    pub fn create_tcp_with_config(config: TcpTransportConfig) -> Arc<dyn Transport> {
        Arc::new(TcpTransport::with_config(config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn should_panic_on_socket_skip() -> bool {
        // In CI we generally expect loopback socket binds to work. If they don't, treat it as a
        // real failure to avoid silently masking regressions or misconfiguration.
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

        // Start a listener on a random port
        let listener = match transport.listen("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(DistributedError::TransportError { message })
                if is_socket_permission_denied_message(&message) =>
            {
                // Some sandboxed environments disallow binding sockets entirely.
                // Skip this integration-style test in that case.
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

        // Get the actual address the listener bound to
        let actual_addr = {
            let handle = listener.handle.as_ref().unwrap();
            let handle_guard = handle.read().await;
            match &*handle_guard {
                ListenerHandle::Tcp(tcp_listener) => tcp_listener.local_addr().unwrap().to_string(),
                ListenerHandle::Grpc(addr) => addr.clone(),
            }
        };

        // Spawn acceptor task
        let transport_clone = Arc::new(transport);
        let transport_acceptor = transport_clone.clone();
        let listener = Arc::new(listener);
        let listener_clone = listener.clone();

        let acceptor_task = tokio::spawn(async move {
            let connection = transport_acceptor.accept(&listener_clone).await.unwrap();
            let message = transport_acceptor.receive(&connection).await.unwrap();
            assert_eq!(message.message_type, MessageType::Event);
            assert_eq!(message.payload, b"test data");

            // Send response
            let response = Message::event(b"response".to_vec());
            transport_acceptor
                .send(&connection, response)
                .await
                .unwrap();
        });

        // Give acceptor time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Connect and send message
        let connection = transport_clone.connect(&actual_addr).await.unwrap();
        let message = Message::event(b"test data".to_vec());
        transport_clone.send(&connection, message).await.unwrap();

        // Receive response
        let response = transport_clone.receive(&connection).await.unwrap();
        assert_eq!(response.payload, b"response");

        // Clean up
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
        assert_eq!(config.keepalive_enabled, true);
        assert_eq!(config.nodelay, true);
        assert_eq!(config.max_message_size, 10 * 1024 * 1024);
    }

    #[test]
    fn test_transport_factory() {
        let _transport = TransportFactory::create("tcp").unwrap();
        // Transport created successfully

        let result = TransportFactory::create("unknown");
        assert!(result.is_err());
    }
}
