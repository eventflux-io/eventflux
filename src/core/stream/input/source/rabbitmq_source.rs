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

//! # RabbitMQ Source
//!
//! Consumes messages from RabbitMQ queues and delivers them to the EventFlux pipeline.
//!
//! ## Architecture
//!
//! Uses synchronous Source trait with internal tokio runtime for lapin async operations:
//! ```text
//! RabbitMQ Queue → lapin consumer → bytes → SourceCallback → SourceMapper → Events
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use std::collections::HashMap;
//!
//! let mut properties = HashMap::new();
//! properties.insert("rabbitmq.host".to_string(), "localhost".to_string());
//! properties.insert("rabbitmq.queue".to_string(), "events".to_string());
//!
//! let source = RabbitMQSource::from_properties(&properties, None, "RabbitMQStream")?;
//! ```

use super::{Source, SourceCallback};
use crate::core::error::handler::ErrorAction;
use crate::core::error::source_support::{ErrorConfigBuilder, SourceErrorContext};
use crate::core::event::value::AttributeValue;
use crate::core::event::Event;
use crate::core::exception::EventFluxError;
use crate::core::extension::SourceFactory;
use crate::core::stream::input::input_handler::InputHandler;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::Duration;

/// Configuration for RabbitMQ source
#[derive(Debug, Clone)]
pub struct RabbitMQSourceConfig {
    /// RabbitMQ broker hostname
    pub host: String,
    /// RabbitMQ broker port (default: 5672)
    pub port: u16,
    /// Virtual host (default: "/")
    pub vhost: String,
    /// Queue name to consume from
    pub queue: String,
    /// Username for authentication (default: "guest")
    pub username: String,
    /// Password for authentication (default: "guest")
    pub password: String,
    /// Prefetch count for consumer (default: 10)
    pub prefetch_count: u16,
    /// Consumer tag (auto-generated if not specified)
    pub consumer_tag: Option<String>,
    /// Whether to auto-acknowledge messages (default: true)
    pub auto_ack: bool,
    /// Whether to declare the queue if it doesn't exist (default: true)
    /// When true, the source will create the queue during start() if missing.
    /// When false, the queue must already exist or an error occurs.
    pub declare_queue: bool,
}

impl Default for RabbitMQSourceConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5672,
            vhost: "/".to_string(),
            queue: String::new(),
            username: "guest".to_string(),
            password: "guest".to_string(),
            prefetch_count: 10,
            consumer_tag: None,
            auto_ack: true,
            declare_queue: true, // Default: auto-create queue if missing
        }
    }
}

impl RabbitMQSourceConfig {
    /// Build AMQP URI from configuration
    pub fn amqp_uri(&self) -> String {
        // The default vhost "/" must be URL-encoded as "%2f"
        let encoded_vhost = urlencoding::encode(&self.vhost);
        format!(
            "amqp://{}:{}@{}:{}/{}",
            self.username, self.password, self.host, self.port, encoded_vhost
        )
    }

    /// Parse configuration from properties HashMap
    #[allow(clippy::field_reassign_with_default)]
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        let mut config = Self::default();

        // Required: host
        config.host = properties
            .get("rabbitmq.host")
            .ok_or("Missing required property: rabbitmq.host")?
            .clone();

        // Required: queue
        config.queue = properties
            .get("rabbitmq.queue")
            .ok_or("Missing required property: rabbitmq.queue")?
            .clone();

        // Optional properties
        if let Some(port) = properties.get("rabbitmq.port") {
            config.port = port
                .parse()
                .map_err(|e| format!("Invalid rabbitmq.port: {}", e))?;
        }

        if let Some(vhost) = properties.get("rabbitmq.vhost") {
            config.vhost = vhost.clone();
        }

        if let Some(username) = properties.get("rabbitmq.username") {
            config.username = username.clone();
        }

        if let Some(password) = properties.get("rabbitmq.password") {
            config.password = password.clone();
        }

        if let Some(prefetch) = properties.get("rabbitmq.prefetch") {
            config.prefetch_count = prefetch
                .parse()
                .map_err(|e| format!("Invalid rabbitmq.prefetch: {}", e))?;
        }

        if let Some(tag) = properties.get("rabbitmq.consumer.tag") {
            config.consumer_tag = Some(tag.clone());
        }

        if let Some(auto_ack) = properties.get("rabbitmq.auto.ack") {
            config.auto_ack = auto_ack
                .parse()
                .map_err(|e| format!("Invalid rabbitmq.auto.ack: {}", e))?;
        }

        if let Some(declare) = properties.get("rabbitmq.declare.queue") {
            config.declare_queue = declare
                .parse()
                .map_err(|e| format!("Invalid rabbitmq.declare.queue: {}", e))?;
        }

        Ok(config)
    }
}

/// RabbitMQ source that consumes messages from a queue
///
/// This implementation:
/// - Uses sync Source trait with internal tokio runtime for lapin async operations
/// - Supports graceful shutdown via AtomicBool flag
/// - Integrates with M5 error handling context
#[derive(Debug)]
pub struct RabbitMQSource {
    /// Configuration
    config: RabbitMQSourceConfig,
    /// Running flag for graceful shutdown
    running: Arc<AtomicBool>,
    /// Optional error handling context (M5 integration)
    error_ctx: Option<SourceErrorContext>,
}

impl RabbitMQSource {
    /// Create a new RabbitMQ source without error handling
    pub fn new(config: RabbitMQSourceConfig) -> Self {
        Self {
            config,
            running: Arc::new(AtomicBool::new(false)),
            error_ctx: None,
        }
    }

    /// Create a RabbitMQ source from properties
    ///
    /// # Required Properties
    /// - `rabbitmq.host`: Broker hostname
    /// - `rabbitmq.queue`: Queue name to consume from
    ///
    /// # Optional Properties
    /// - `rabbitmq.port`: Broker port (default: 5672)
    /// - `rabbitmq.vhost`: Virtual host (default: "/")
    /// - `rabbitmq.username`: Username (default: "guest")
    /// - `rabbitmq.password`: Password (default: "guest")
    /// - `rabbitmq.prefetch`: Prefetch count (default: 10)
    /// - `rabbitmq.consumer.tag`: Consumer tag (auto-generated if not specified)
    /// - `rabbitmq.auto.ack`: Auto-acknowledge messages (default: true)
    /// - `error.*`: Error handling properties (see SourceErrorContext)
    pub fn from_properties(
        properties: &HashMap<String, String>,
        dlq_junction: Option<Arc<Mutex<InputHandler>>>,
        stream_name: &str,
    ) -> Result<Self, String> {
        let config = RabbitMQSourceConfig::from_properties(properties)?;

        // Parse error handling configuration (optional)
        let error_config_builder = ErrorConfigBuilder::from_properties(properties);
        let error_ctx = if error_config_builder.is_configured() {
            use crate::core::config::{FlatConfig, PropertySource};
            let mut flat_config = FlatConfig::new();
            for (key, value) in properties {
                if key.starts_with("error.") {
                    flat_config.set(key.clone(), value.clone(), PropertySource::SqlWith);
                }
            }

            Some(SourceErrorContext::from_config(
                &flat_config,
                dlq_junction,
                stream_name.to_string(),
            )?)
        } else {
            None
        };

        Ok(Self {
            config,
            running: Arc::new(AtomicBool::new(false)),
            error_ctx,
        })
    }
}

impl Clone for RabbitMQSource {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            running: Arc::new(AtomicBool::new(false)),
            error_ctx: None, // Error context contains runtime state, not cloneable
        }
    }
}

impl Source for RabbitMQSource {
    fn start(&mut self, callback: Arc<dyn SourceCallback>) {
        let running = self.running.clone();
        running.store(true, Ordering::SeqCst);
        let config = self.config.clone();

        // Move error_ctx into the thread
        let mut error_ctx = self.error_ctx.take();

        thread::spawn(move || {
            // Create a tokio runtime for lapin async operations
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    log::error!("[RabbitMQSource] Failed to create tokio runtime: {}", e);
                    return;
                }
            };

            rt.block_on(async move {
                // Connect to RabbitMQ
                let uri = config.amqp_uri();
                log::info!("[RabbitMQSource] Connecting to {}", config.host);

                let connection = match lapin::Connection::connect(
                    &uri,
                    lapin::ConnectionProperties::default(),
                )
                .await
                {
                    Ok(conn) => conn,
                    Err(e) => {
                        let err = EventFluxError::ConnectionUnavailable {
                            message: format!("Failed to connect to RabbitMQ: {}", e),
                            source: Some(Box::new(e)),
                        };
                        if let Some(ctx) = &mut error_ctx {
                            ctx.handle_error(None, &err);
                        } else {
                            log::error!("[RabbitMQSource] {}", err);
                        }
                        return;
                    }
                };

                // Create channel
                let channel = match connection.create_channel().await {
                    Ok(ch) => ch,
                    Err(e) => {
                        let err = EventFluxError::ConnectionUnavailable {
                            message: format!("Failed to create RabbitMQ channel: {}", e),
                            source: Some(Box::new(e)),
                        };
                        if let Some(ctx) = &mut error_ctx {
                            ctx.handle_error(None, &err);
                        } else {
                            log::error!("[RabbitMQSource] {}", err);
                        }
                        return;
                    }
                };

                // Set QoS (prefetch count)
                if let Err(e) = channel
                    .basic_qos(config.prefetch_count, lapin::options::BasicQosOptions::default())
                    .await
                {
                    log::warn!("[RabbitMQSource] Failed to set QoS: {}", e);
                }

                // Handle queue declaration based on declare_queue setting
                let channel = if config.declare_queue {
                    // Check if queue exists using passive declare
                    // Note: Passive declare may fail with PRECONDITION_FAILED if queue exists
                    // with different properties (e.g., durable=true vs our durable=false default).
                    // This is still a success case - the queue exists and we can consume from it.
                    match channel
                        .queue_declare(
                            &config.queue,
                            lapin::options::QueueDeclareOptions {
                                passive: true, // First check if exists
                                ..Default::default()
                            },
                            lapin::types::FieldTable::default(),
                        )
                        .await
                    {
                        Ok(_) => channel, // Queue exists with matching properties, reuse channel
                        Err(e) => {
                            // Channel is closed after failed passive declare - need new channel
                            let new_channel = match connection.create_channel().await {
                                Ok(ch) => ch,
                                Err(e) => {
                                    let err = EventFluxError::ConnectionUnavailable {
                                        message: format!("Failed to create new channel: {}", e),
                                        source: Some(Box::new(e)),
                                    };
                                    if let Some(ctx) = &mut error_ctx {
                                        ctx.handle_error(None, &err);
                                    } else {
                                        log::error!("[RabbitMQSource] {}", err);
                                    }
                                    return;
                                }
                            };

                            // Check error type to determine if queue exists with different properties
                            // AMQP reply code 406 = PRECONDITION_FAILED (queue exists, properties differ)
                            // AMQP reply code 404 = NOT_FOUND (queue doesn't exist)
                            let is_precondition_failed = e.to_string().contains("PRECONDITION_FAILED")
                                || e.to_string().contains("406");

                            if is_precondition_failed {
                                // Queue exists but with different properties (e.g., durable)
                                // This is fine - we can still consume from it
                                log::debug!(
                                    "[RabbitMQSource] Queue '{}' exists with different properties, using as-is",
                                    config.queue
                                );
                                // Set QoS on new channel
                                if let Err(e) = new_channel
                                    .basic_qos(
                                        config.prefetch_count,
                                        lapin::options::BasicQosOptions::default(),
                                    )
                                    .await
                                {
                                    log::warn!(
                                        "[RabbitMQSource] Failed to set QoS on new channel: {}",
                                        e
                                    );
                                }
                                new_channel
                            } else {
                                // Queue doesn't exist - create it
                                log::info!(
                                    "[RabbitMQSource] Queue '{}' does not exist, creating it: {}",
                                    config.queue,
                                    e
                                );

                                // Set QoS on new channel
                                if let Err(e) = new_channel
                                    .basic_qos(
                                        config.prefetch_count,
                                        lapin::options::BasicQosOptions::default(),
                                    )
                                    .await
                                {
                                    log::warn!(
                                        "[RabbitMQSource] Failed to set QoS on new channel: {}",
                                        e
                                    );
                                }

                                // Declare the queue on the new channel
                                if let Err(e) = new_channel
                                    .queue_declare(
                                        &config.queue,
                                        lapin::options::QueueDeclareOptions::default(),
                                        lapin::types::FieldTable::default(),
                                    )
                                    .await
                                {
                                    let err = EventFluxError::configuration(format!(
                                        "Failed to declare queue '{}': {}",
                                        config.queue, e
                                    ));
                                    if let Some(ctx) = &mut error_ctx {
                                        ctx.handle_error(None, &err);
                                    } else {
                                        log::error!("[RabbitMQSource] {}", err);
                                    }
                                    return;
                                }
                                new_channel
                            }
                        }
                    }
                } else {
                    // declare_queue=false: Queue must exist, don't create
                    // Note: Passive declare may fail with PRECONDITION_FAILED if queue exists
                    // with different properties - this is still a success case.
                    match channel
                        .queue_declare(
                            &config.queue,
                            lapin::options::QueueDeclareOptions {
                                passive: true, // Just check existence
                                ..Default::default()
                            },
                            lapin::types::FieldTable::default(),
                        )
                        .await
                    {
                        Ok(_) => channel, // Queue exists with matching properties
                        Err(e) => {
                            // Check if it's PRECONDITION_FAILED (queue exists, properties differ)
                            let is_precondition_failed = e.to_string().contains("PRECONDITION_FAILED")
                                || e.to_string().contains("406");

                            if is_precondition_failed {
                                // Queue exists but with different properties - this is fine
                                log::debug!(
                                    "[RabbitMQSource] Queue '{}' exists with different properties, using as-is",
                                    config.queue
                                );
                                // Channel is closed after failed passive declare - need new channel
                                match connection.create_channel().await {
                                    Ok(ch) => {
                                        // Set QoS on new channel
                                        if let Err(e) = ch
                                            .basic_qos(
                                                config.prefetch_count,
                                                lapin::options::BasicQosOptions::default(),
                                            )
                                            .await
                                        {
                                            log::warn!(
                                                "[RabbitMQSource] Failed to set QoS on new channel: {}",
                                                e
                                            );
                                        }
                                        ch
                                    }
                                    Err(e) => {
                                        let err = EventFluxError::ConnectionUnavailable {
                                            message: format!("Failed to create new channel: {}", e),
                                            source: Some(Box::new(e)),
                                        };
                                        if let Some(ctx) = &mut error_ctx {
                                            ctx.handle_error(None, &err);
                                        } else {
                                            log::error!("[RabbitMQSource] {}", err);
                                        }
                                        return;
                                    }
                                }
                            } else {
                                // Queue truly doesn't exist
                                let err = EventFluxError::configuration(format!(
                                    "Queue '{}' does not exist and declare_queue=false: {}",
                                    config.queue, e
                                ));
                                if let Some(ctx) = &mut error_ctx {
                                    ctx.handle_error(None, &err);
                                } else {
                                    log::error!("[RabbitMQSource] {}", err);
                                }
                                return;
                            }
                        }
                    }
                };

                // Start consumer
                let consumer_tag = config
                    .consumer_tag
                    .clone()
                    .unwrap_or_else(|| format!("eventflux-{}", uuid::Uuid::new_v4()));

                let mut consumer = match channel
                    .basic_consume(
                        &config.queue,
                        &consumer_tag,
                        lapin::options::BasicConsumeOptions {
                            no_ack: config.auto_ack,
                            ..Default::default()
                        },
                        lapin::types::FieldTable::default(),
                    )
                    .await
                {
                    Ok(consumer) => consumer,
                    Err(e) => {
                        let err = EventFluxError::ConnectionUnavailable {
                            message: format!("Failed to start consumer: {}", e),
                            source: Some(Box::new(e)),
                        };
                        if let Some(ctx) = &mut error_ctx {
                            ctx.handle_error(None, &err);
                        } else {
                            log::error!("[RabbitMQSource] {}", err);
                        }
                        return;
                    }
                };

                log::info!(
                    "[RabbitMQSource] Started consuming from queue '{}'",
                    config.queue
                );

                // Consume messages
                use futures::StreamExt;
                while running.load(Ordering::SeqCst) {
                    // Use timeout to periodically check running flag
                    let delivery = tokio::time::timeout(
                        Duration::from_millis(100),
                        consumer.next(),
                    )
                    .await;

                    match delivery {
                        Ok(Some(Ok(delivery))) => {
                            // Got a message - deliver to callback with retry loop
                            let data = delivery.data.as_slice();
                            let mut should_stop = false;
                            // None = success, Some(action) = error occurred
                            let mut final_action: Option<ErrorAction> = None;

                            // Retry loop - keep trying until success or non-retry action
                            loop {
                                match callback.on_data(data) {
                                    Ok(()) => {
                                        // Success - acknowledge if not auto-ack
                                        if !config.auto_ack {
                                            if let Err(e) = delivery
                                                .ack(lapin::options::BasicAckOptions::default())
                                                .await
                                            {
                                                log::warn!(
                                                    "[RabbitMQSource] Failed to ack message: {}",
                                                    e
                                                );
                                            }
                                        }

                                        // Reset error counter on success
                                        if let Some(ctx) = &mut error_ctx {
                                            ctx.reset_errors();
                                        }
                                        // final_action stays None (success)
                                        break;
                                    }
                                    Err(e) => {
                                        // Callback failed - handle error and get action
                                        // Create fallback event from raw bytes for DLQ support
                                        let fallback_event = Event::new_with_data(
                                            std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .map(|d| d.as_millis() as i64)
                                                .unwrap_or(0),
                                            vec![AttributeValue::Bytes(data.to_vec())],
                                        );

                                        let action = if let Some(ctx) = &mut error_ctx {
                                            ctx.handle_error_with_action(Some(&fallback_event), &e)
                                        } else {
                                            log::error!(
                                                "[RabbitMQSource] Callback error: {}",
                                                e
                                            );
                                            // No error context - default to drop behavior
                                            ErrorAction::Drop
                                        };

                                        match action {
                                            ErrorAction::Retry { .. } => {
                                                // Retry action - delay already applied by handle_error_with_action
                                                // Loop back to retry the callback
                                                continue;
                                            }
                                            _ => {
                                                // Non-retry action (Drop, DLQ, Fail) - exit loop
                                                final_action = Some(action);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }

                            // Handle final action after retry loop exits (only for errors)
                            if let Some(action) = final_action {
                                match action {
                                    ErrorAction::Retry { .. } => {
                                        // Should not reach here - Retry loops back
                                    }
                                    ErrorAction::Fail => {
                                        // Reject message permanently - don't requeue
                                        if !config.auto_ack {
                                            if let Err(e) = delivery
                                                .nack(lapin::options::BasicNackOptions {
                                                    requeue: false,
                                                    ..Default::default()
                                                })
                                                .await
                                            {
                                                log::warn!(
                                                    "[RabbitMQSource] Failed to nack message: {}",
                                                    e
                                                );
                                            }
                                        }
                                        log::error!(
                                            "[RabbitMQSource] Unrecoverable error, stopping"
                                        );
                                        should_stop = true;
                                    }
                                    ErrorAction::Drop | ErrorAction::SendToDlq => {
                                        // Error was handled (dropped or sent to DLQ)
                                        // ACK the message to remove it from the queue
                                        if !config.auto_ack {
                                            if let Err(e) = delivery
                                                .ack(lapin::options::BasicAckOptions::default())
                                                .await
                                            {
                                                log::warn!(
                                                    "[RabbitMQSource] Failed to ack message after error handling: {}",
                                                    e
                                                );
                                            }
                                        }
                                    }
                                }
                            }

                            if should_stop {
                                break;
                            }
                        }
                        Ok(Some(Err(e))) => {
                            // Consumer error
                            let err = EventFluxError::ConnectionUnavailable {
                                message: format!("RabbitMQ consumer error: {}", e),
                                source: Some(Box::new(e)),
                            };
                            if let Some(ctx) = &mut error_ctx {
                                if !ctx.handle_error(None, &err) {
                                    break;
                                }
                            } else {
                                log::error!("[RabbitMQSource] {}", err);
                            }
                        }
                        Ok(None) => {
                            // Consumer stream ended
                            log::info!("[RabbitMQSource] Consumer stream ended");
                            break;
                        }
                        Err(_) => {
                            // Timeout - continue loop to check running flag
                            continue;
                        }
                    }
                }

                log::info!("[RabbitMQSource] Stopped consuming");

                // Clean up
                if let Err(e) = channel.close(200, "Normal shutdown").await {
                    log::warn!("[RabbitMQSource] Error closing channel: {}", e);
                }
                if let Err(e) = connection.close(200, "Normal shutdown").await {
                    log::warn!("[RabbitMQSource] Error closing connection: {}", e);
                }
            });
        });
    }

    fn stop(&mut self) {
        log::info!("[RabbitMQSource] Stopping...");
        self.running.store(false, Ordering::SeqCst);
    }

    fn clone_box(&self) -> Box<dyn Source> {
        Box::new(self.clone())
    }

    fn set_error_dlq_junction(&mut self, junction: Arc<Mutex<InputHandler>>) {
        if let Some(ref mut ctx) = self.error_ctx {
            ctx.set_dlq_junction(junction);
        }
    }

    fn validate_connectivity(&self) -> Result<(), EventFluxError> {
        let config = self.config.clone();

        // Create the async validation logic
        let validate_future = async move {
            let uri = config.amqp_uri();

            // Try to connect
            let connection =
                lapin::Connection::connect(&uri, lapin::ConnectionProperties::default())
                    .await
                    .map_err(|e| EventFluxError::ConnectionUnavailable {
                        message: format!(
                            "Failed to connect to RabbitMQ at {}:{}: {}",
                            config.host, config.port, e
                        ),
                        source: Some(Box::new(e)),
                    })?;

            // Create channel and check queue exists
            let channel = connection.create_channel().await.map_err(|e| {
                EventFluxError::ConnectionUnavailable {
                    message: format!("Failed to create channel: {}", e),
                    source: Some(Box::new(e)),
                }
            })?;

            // Validate queue access using passive-then-create logic (mirrors start())
            // This avoids PRECONDITION_FAILED errors when queue exists with different flags.
            //
            // 1. First try passive declare to check if queue exists
            // 2. If exists: validation passes (queue is accessible)
            // 3. If not exists and declare_queue=true: try to create it
            // 4. If not exists and declare_queue=false: fail with clear error
            let passive_result = channel
                .queue_declare(
                    &config.queue,
                    lapin::options::QueueDeclareOptions {
                        passive: true, // Just check existence, don't create
                        ..Default::default()
                    },
                    lapin::types::FieldTable::default(),
                )
                .await;

            match passive_result {
                Ok(_) => {
                    // Queue exists and is accessible - validation passes
                }
                Err(e) => {
                    // Check if PRECONDITION_FAILED (queue exists with different properties)
                    let is_precondition_failed = e.to_string().contains("PRECONDITION_FAILED")
                        || e.to_string().contains("406");

                    if is_precondition_failed {
                        // Queue exists but with different properties (e.g., durable)
                        // This is fine - validation passes
                        log::debug!(
                            "[RabbitMQSource] Queue '{}' exists with different properties, validation passes",
                            config.queue
                        );
                    } else if config.declare_queue {
                        // Queue doesn't exist - try to create it
                        // Channel is closed after failed passive declare, need new channel
                        let new_channel = connection.create_channel().await.map_err(|e| {
                            EventFluxError::ConnectionUnavailable {
                                message: format!(
                                    "Failed to create channel for queue declaration: {}",
                                    e
                                ),
                                source: Some(Box::new(e)),
                            }
                        })?;

                        new_channel
                            .queue_declare(
                                &config.queue,
                                lapin::options::QueueDeclareOptions::default(),
                                lapin::types::FieldTable::default(),
                            )
                            .await
                            .map_err(|e| {
                                EventFluxError::configuration(format!(
                                    "Failed to declare queue '{}': {}. \
                                    Check permissions.",
                                    config.queue, e
                                ))
                            })?;

                        // Clean up new channel
                        let _ = new_channel.close(200, "Validation complete").await;
                    } else {
                        // declare_queue=false and queue doesn't exist
                        return Err(EventFluxError::configuration(format!(
                            "Queue '{}' does not exist and declare_queue=false: {}",
                            config.queue, e
                        )));
                    }
                }
            }

            // Clean up
            let _ = channel.close(200, "Validation complete").await;
            let _ = connection.close(200, "Validation complete").await;

            Ok::<(), EventFluxError>(())
        };

        // Check if we're already inside a tokio runtime
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // Use block_in_place to avoid nested runtime panic
            tokio::task::block_in_place(|| handle.block_on(validate_future))
        } else {
            // Not inside a runtime - create a temporary one
            let rt = tokio::runtime::Runtime::new().map_err(|e| {
                EventFluxError::configuration(format!("Failed to create tokio runtime: {}", e))
            })?;
            rt.block_on(validate_future)
        }
    }
}

// ============================================================================
// RabbitMQ Source Factory
// ============================================================================

/// Factory for creating RabbitMQ source instances.
///
/// This factory is registered with EventFluxContext and used to create
/// RabbitMQ sources from SQL WITH clause configuration.
#[derive(Debug, Clone)]
pub struct RabbitMQSourceFactory;

impl SourceFactory for RabbitMQSourceFactory {
    fn name(&self) -> &'static str {
        "rabbitmq"
    }

    fn supported_formats(&self) -> &[&str] {
        &["json", "csv", "bytes"]
    }

    fn required_parameters(&self) -> &[&str] {
        &["rabbitmq.host", "rabbitmq.queue"]
    }

    fn optional_parameters(&self) -> &[&str] {
        &[
            "rabbitmq.port",
            "rabbitmq.vhost",
            "rabbitmq.username",
            "rabbitmq.password",
            "rabbitmq.prefetch",
            "rabbitmq.consumer.tag",
            "rabbitmq.auto.ack",
            "rabbitmq.declare.queue",
            // Error handling options
            "error.strategy",
            "error.retry.max-attempts",
            "error.retry.initial-delay-ms",
            "error.retry.max-delay-ms",
            "error.retry.backoff-multiplier",
            "error.dlq.stream",
        ]
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn Source>, EventFluxError> {
        // Use from_properties to parse both RabbitMQ config and error handling config
        // DLQ junction will be None initially - stream_initializer calls
        // set_error_dlq_junction() after creation to wire the DLQ stream.
        //
        // Use the queue name as the stream identifier for error context.
        // This uniquely identifies each RabbitMQ source stream and is more useful
        // for debugging than a generic placeholder.
        let queue_name = config
            .get("rabbitmq.queue")
            .cloned()
            .unwrap_or_else(|| "rabbitmq-source".to_string());

        let source = RabbitMQSource::from_properties(config, None, &queue_name)
            .map_err(EventFluxError::configuration)?;

        Ok(Box::new(source))
    }

    fn clone_box(&self) -> Box<dyn SourceFactory> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_from_properties_required_only() {
        let mut props = HashMap::new();
        props.insert("rabbitmq.host".to_string(), "myhost".to_string());
        props.insert("rabbitmq.queue".to_string(), "myqueue".to_string());

        let config = RabbitMQSourceConfig::from_properties(&props).unwrap();
        assert_eq!(config.host, "myhost");
        assert_eq!(config.queue, "myqueue");
        assert_eq!(config.port, 5672);
        assert_eq!(config.vhost, "/");
        assert_eq!(config.username, "guest");
        assert_eq!(config.password, "guest");
        assert_eq!(config.prefetch_count, 10);
    }

    #[test]
    fn test_config_from_properties_all_options() {
        let mut props = HashMap::new();
        props.insert(
            "rabbitmq.host".to_string(),
            "broker.example.com".to_string(),
        );
        props.insert("rabbitmq.queue".to_string(), "events".to_string());
        props.insert("rabbitmq.port".to_string(), "5673".to_string());
        props.insert("rabbitmq.vhost".to_string(), "production".to_string());
        props.insert("rabbitmq.username".to_string(), "admin".to_string());
        props.insert("rabbitmq.password".to_string(), "secret".to_string());
        props.insert("rabbitmq.prefetch".to_string(), "50".to_string());
        props.insert(
            "rabbitmq.consumer.tag".to_string(),
            "my-consumer".to_string(),
        );
        props.insert("rabbitmq.auto.ack".to_string(), "false".to_string());

        let config = RabbitMQSourceConfig::from_properties(&props).unwrap();
        assert_eq!(config.host, "broker.example.com");
        assert_eq!(config.queue, "events");
        assert_eq!(config.port, 5673);
        assert_eq!(config.vhost, "production");
        assert_eq!(config.username, "admin");
        assert_eq!(config.password, "secret");
        assert_eq!(config.prefetch_count, 50);
        assert_eq!(config.consumer_tag, Some("my-consumer".to_string()));
        assert!(!config.auto_ack);
    }

    #[test]
    fn test_config_missing_host() {
        let mut props = HashMap::new();
        props.insert("rabbitmq.queue".to_string(), "myqueue".to_string());

        let result = RabbitMQSourceConfig::from_properties(&props);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("rabbitmq.host"));
    }

    #[test]
    fn test_config_missing_queue() {
        let mut props = HashMap::new();
        props.insert("rabbitmq.host".to_string(), "localhost".to_string());

        let result = RabbitMQSourceConfig::from_properties(&props);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("rabbitmq.queue"));
    }

    #[test]
    fn test_amqp_uri_default_vhost() {
        let config = RabbitMQSourceConfig {
            host: "localhost".to_string(),
            port: 5672,
            vhost: "/".to_string(),
            queue: "test".to_string(),
            username: "guest".to_string(),
            password: "guest".to_string(),
            ..Default::default()
        };

        // Default vhost "/" is URL-encoded as "%2f"
        assert_eq!(config.amqp_uri(), "amqp://guest:guest@localhost:5672/%2F");
    }

    #[test]
    fn test_amqp_uri_custom_vhost() {
        let config = RabbitMQSourceConfig {
            host: "broker.example.com".to_string(),
            port: 5673,
            vhost: "my-vhost".to_string(),
            queue: "test".to_string(),
            username: "admin".to_string(),
            password: "secret".to_string(),
            ..Default::default()
        };

        assert_eq!(
            config.amqp_uri(),
            "amqp://admin:secret@broker.example.com:5673/my-vhost"
        );
    }

    #[test]
    fn test_source_from_properties() {
        let mut props = HashMap::new();
        props.insert("rabbitmq.host".to_string(), "localhost".to_string());
        props.insert("rabbitmq.queue".to_string(), "test-queue".to_string());

        let source = RabbitMQSource::from_properties(&props, None, "TestStream");
        assert!(source.is_ok());

        let source = source.unwrap();
        assert_eq!(source.config.host, "localhost");
        assert_eq!(source.config.queue, "test-queue");
        assert!(source.error_ctx.is_none());
    }

    #[test]
    fn test_source_from_properties_with_error_handling() {
        let mut props = HashMap::new();
        props.insert("rabbitmq.host".to_string(), "localhost".to_string());
        props.insert("rabbitmq.queue".to_string(), "test-queue".to_string());
        props.insert("error.strategy".to_string(), "drop".to_string());

        let source = RabbitMQSource::from_properties(&props, None, "TestStream");
        assert!(source.is_ok());

        let source = source.unwrap();
        assert!(source.error_ctx.is_some());
    }

    #[test]
    fn test_source_clone() {
        let config = RabbitMQSourceConfig {
            host: "localhost".to_string(),
            queue: "test".to_string(),
            ..Default::default()
        };
        let source = RabbitMQSource::new(config);
        let cloned = source.clone();

        assert_eq!(cloned.config.host, "localhost");
        assert_eq!(cloned.config.queue, "test");
        assert!(cloned.error_ctx.is_none()); // Error ctx not cloned
    }

    // =========================================================================
    // Factory Tests
    // =========================================================================

    #[test]
    fn test_factory_metadata() {
        let factory = RabbitMQSourceFactory;
        assert_eq!(factory.name(), "rabbitmq");
        assert!(factory.supported_formats().contains(&"json"));
        assert!(factory.supported_formats().contains(&"csv"));
        assert!(factory.supported_formats().contains(&"bytes"));
        assert!(factory.required_parameters().contains(&"rabbitmq.host"));
        assert!(factory.required_parameters().contains(&"rabbitmq.queue"));
    }

    #[test]
    fn test_factory_create() {
        let factory = RabbitMQSourceFactory;
        let mut config = HashMap::new();
        config.insert("rabbitmq.host".to_string(), "localhost".to_string());
        config.insert("rabbitmq.queue".to_string(), "test-queue".to_string());

        let result = factory.create_initialized(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_factory_missing_host() {
        let factory = RabbitMQSourceFactory;
        let mut config = HashMap::new();
        config.insert("rabbitmq.queue".to_string(), "test-queue".to_string());

        let result = factory.create_initialized(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_factory_missing_queue() {
        let factory = RabbitMQSourceFactory;
        let mut config = HashMap::new();
        config.insert("rabbitmq.host".to_string(), "localhost".to_string());

        let result = factory.create_initialized(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_factory_clone() {
        let factory = RabbitMQSourceFactory;
        let cloned = factory.clone_box();
        assert_eq!(cloned.name(), "rabbitmq");
    }

    #[test]
    fn test_factory_create_with_error_handling_config() {
        // Verify that factory properly wires error handling config
        let factory = RabbitMQSourceFactory;
        let mut config = HashMap::new();
        config.insert("rabbitmq.host".to_string(), "localhost".to_string());
        config.insert("rabbitmq.queue".to_string(), "test-queue".to_string());
        // Add error handling config
        config.insert("error.strategy".to_string(), "retry".to_string());
        config.insert("error.retry.max-attempts".to_string(), "3".to_string());
        config.insert(
            "error.retry.initial-delay-ms".to_string(),
            "100".to_string(),
        );

        let result = factory.create_initialized(&config);
        assert!(result.is_ok(), "Factory should accept error.* config");
    }

    #[test]
    fn test_factory_accepts_dlq_strategy() {
        // DLQ strategy is accepted by factory - stream_initializer will wire the junction later
        let factory = RabbitMQSourceFactory;
        let mut config = HashMap::new();
        config.insert("rabbitmq.host".to_string(), "localhost".to_string());
        config.insert("rabbitmq.queue".to_string(), "test-queue".to_string());
        config.insert("error.strategy".to_string(), "dlq".to_string());
        config.insert("error.dlq.stream".to_string(), "ErrorStream".to_string());

        let result = factory.create_initialized(&config);
        assert!(
            result.is_ok(),
            "Factory should accept DLQ strategy (junction wired later by stream_initializer)"
        );
    }
}
