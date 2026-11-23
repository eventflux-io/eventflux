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
//! Reads messages from a RabbitMQ queue and delivers them to the EventFlux pipeline.
//!
//! ## Architecture
//!
//! The Source trait is blocking, so the worker thread runs its own tokio
//! runtime for the async lapin client.
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

use super::{Source, SourceCallback, SourceWorker};
use crate::core::error::handler::ErrorAction;
use crate::core::error::source_support::{ErrorConfigBuilder, SourceErrorContext};
use crate::core::event::value::AttributeValue;
use crate::core::event::Event;
use crate::core::exception::EventFluxError;
use crate::core::extension::SourceFactory;
use crate::core::stream::input::input_handler::InputHandler;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{atomic::Ordering, Arc, Mutex};
use std::time::Duration;

/// Configuration for RabbitMQ source
#[derive(Debug, Clone)]
pub struct RabbitMQSourceConfig {
    /// RabbitMQ broker host name
    pub host: String,
    /// RabbitMQ broker port. Default 5672.
    pub port: u16,
    /// Virtual host. Default "/".
    pub vhost: String,
    /// Queue to read from
    pub queue: String,
    /// User name for login. Default "guest".
    pub username: String,
    /// Password for login. Default "guest". Hidden in Debug output. Call
    /// `.expose()` only where it is used.
    pub password: crate::core::stream::connector_util::Redacted,
    /// Most unacked messages the broker sends at once. Default 10.
    pub prefetch_count: u16,
    /// Consumer tag. When not set, `eventflux-<uuid>` is used.
    pub consumer_tag: Option<String>,
    /// Let the broker treat messages as acked on delivery. Default true.
    pub auto_ack: bool,
    /// Create the queue in start() if it is missing. Default true.
    /// When false, the queue must already exist or start() reports an error.
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
            password: "guest".into(),
            prefetch_count: 10,
            consumer_tag: None,
            auto_ack: true,
            declare_queue: true, // create the queue if it is missing
        }
    }
}

impl RabbitMQSourceConfig {
    /// Builds the AMQP URI. The result holds the raw password, so never log it.
    pub fn amqp_uri(&self) -> String {
        // The default vhost "/" must be URL encoded, which gives "%2F"
        let encoded_vhost = urlencoding::encode(&self.vhost);
        format!(
            "amqp://{}:{}@{}:{}/{}",
            self.username,
            self.password.expose(),
            self.host,
            self.port,
            encoded_vhost
        )
    }

    /// Reads the config from properties.
    ///
    /// # Errors
    ///
    /// Returns an error when `rabbitmq.host` or `rabbitmq.queue` is missing,
    /// or a port, prefetch, or bool value does not parse.
    #[allow(clippy::field_reassign_with_default)]
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        let mut config = Self::default();

        config.host = properties
            .get("rabbitmq.host")
            .ok_or("Missing required property: rabbitmq.host")?
            .clone();

        config.queue = properties
            .get("rabbitmq.queue")
            .ok_or("Missing required property: rabbitmq.queue")?
            .clone();

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
            config.password = password.clone().into();
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

/// RabbitMQ source that reads messages from one queue.
///
/// - Runs its own tokio runtime on the worker thread for the async lapin client
/// - Shuts down cleanly when the worker's running flag is cleared
/// - Uses the `error.*` handling strategies
#[derive(Debug)]
pub struct RabbitMQSource {
    config: RabbitMQSourceConfig,
    /// Runs the worker thread. Starts it, signals it, and joins it on stop.
    worker: SourceWorker,
    /// Optional error handling context
    error_ctx: Option<SourceErrorContext>,
}

impl RabbitMQSource {
    /// Creates a RabbitMQ source with no error handling
    pub fn new(config: RabbitMQSourceConfig) -> Self {
        Self {
            config,
            worker: SourceWorker::new("RabbitMQSource"),
            error_ctx: None,
        }
    }

    /// Creates a RabbitMQ source from properties.
    ///
    /// # Required Properties
    /// - `rabbitmq.host`: broker host name
    /// - `rabbitmq.queue`: queue to read from
    ///
    /// # Optional Properties
    /// - `rabbitmq.port`: broker port (default 5672)
    /// - `rabbitmq.vhost`: virtual host (default "/")
    /// - `rabbitmq.username`: user name (default "guest")
    /// - `rabbitmq.password`: password (default "guest")
    /// - `rabbitmq.prefetch`: prefetch count (default 10)
    /// - `rabbitmq.consumer.tag`: consumer tag (default `eventflux-<uuid>`)
    /// - `rabbitmq.auto.ack`: let the broker ack on delivery (default true)
    /// - `rabbitmq.declare.queue`: create the queue if missing (default true)
    /// - `error.*`: error handling properties (see SourceErrorContext)
    ///
    /// # Errors
    ///
    /// Returns an error when the RabbitMQ config or the error config is invalid.
    pub fn from_properties(
        properties: &HashMap<String, String>,
        dlq_junction: Option<Arc<Mutex<InputHandler>>>,
        stream_name: &str,
    ) -> Result<Self, String> {
        let config = RabbitMQSourceConfig::from_properties(properties)?;

        // Error handling is optional. Only `error.*` keys are passed on.
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
            worker: SourceWorker::new("RabbitMQSource"),
            error_ctx,
        })
    }
}

impl Clone for RabbitMQSource {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            worker: self.worker.clone(), // the clone is a new worker that has not started
            // New context with the same strategy and DLQ, and runtime counters reset
            error_ctx: self.error_ctx.as_ref().map(SourceErrorContext::fresh),
        }
    }
}

impl Source for RabbitMQSource {
    fn start(&mut self, callback: Arc<dyn SourceCallback>) {
        let config = self.config.clone();

        // The thread gets a new copy, so self keeps its config for a later restart
        let mut error_ctx = self.error_ctx.as_ref().map(SourceErrorContext::fresh);

        self.worker.start(move |running| {
            if let Some(ctx) = &mut error_ctx {
                ctx.bind_cancellation(Arc::clone(&running));
            }
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    log::error!("[RabbitMQSource] Failed to create tokio runtime: {}", e);
                    return;
                }
            };

            rt.block_on(async move {
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

                // QoS sets the prefetch count. A failure is only a warning.
                if let Err(e) = channel
                    .basic_qos(config.prefetch_count, lapin::options::BasicQosOptions::default())
                    .await
                {
                    log::warn!("[RabbitMQSource] Failed to set QoS: {}", e);
                }

                // Check the queue, and create it when declare_queue allows
                let channel = if config.declare_queue {
                    // A passive declare only checks that the queue exists. It
                    // is treated as possibly failing with PRECONDITION_FAILED
                    // when the queue exists with other properties, such as
                    // durable=true against our durable=false default. That
                    // still counts as success, since the queue exists and can
                    // be read.
                    match channel
                        .queue_declare(
                            &config.queue,
                            lapin::options::QueueDeclareOptions {
                                passive: true, // only check that it exists
                                ..Default::default()
                            },
                            lapin::types::FieldTable::default(),
                        )
                        .await
                    {
                        Ok(_) => channel, // the queue exists, so keep using this channel
                        Err(e) => {
                            // The broker closes the channel after a failed
                            // passive declare, so open a new one
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

                            // Tell the error kinds apart by the error text.
                            // AMQP 406 is PRECONDITION_FAILED (queue exists,
                            // properties differ). AMQP 404 is NOT_FOUND (no
                            // such queue). Anything that is not 406 is taken
                            // to mean the queue is missing.
                            let is_precondition_failed = e.to_string().contains("PRECONDITION_FAILED")
                                || e.to_string().contains("406");

                            if is_precondition_failed {
                                // The queue exists with other properties, such
                                // as durable. It can still be read.
                                log::debug!(
                                    "[RabbitMQSource] Queue '{}' exists with different properties, using as-is",
                                    config.queue
                                );
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
                                // The queue does not exist, so create it
                                log::info!(
                                    "[RabbitMQSource] Queue '{}' does not exist, creating it: {}",
                                    config.queue,
                                    e
                                );

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
                    // declare_queue=false, so the queue must exist and is never
                    // created. PRECONDITION_FAILED still counts as success, as
                    // in the branch above.
                    match channel
                        .queue_declare(
                            &config.queue,
                            lapin::options::QueueDeclareOptions {
                                passive: true, // only check that it exists
                                ..Default::default()
                            },
                            lapin::types::FieldTable::default(),
                        )
                        .await
                    {
                        Ok(_) => channel, // the queue exists
                        Err(e) => {
                            // PRECONDITION_FAILED means it exists with other properties
                            let is_precondition_failed = e.to_string().contains("PRECONDITION_FAILED")
                                || e.to_string().contains("406");

                            if is_precondition_failed {
                                // The queue exists with other properties, which is fine
                                log::debug!(
                                    "[RabbitMQSource] Queue '{}' exists with different properties, using as-is",
                                    config.queue
                                );
                                // The broker closes the channel after a failed
                                // passive declare, so open a new one
                                match connection.create_channel().await {
                                    Ok(ch) => {
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
                                // The queue does not exist
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

                use futures::StreamExt;
                while running.load(Ordering::SeqCst) {
                    // Wait at most 100 ms so the running flag is checked often
                    let delivery = tokio::time::timeout(
                        Duration::from_millis(100),
                        consumer.next(),
                    )
                    .await;

                    match delivery {
                        Ok(Some(Ok(delivery))) => {
                            let data = delivery.data.as_slice();
                            let mut should_stop = false;
                            // None means success. Some(action) means an error
                            // and what to do about it.
                            let mut final_action: Option<ErrorAction> = None;

                            // Keep calling the callback until it succeeds or the
                            // error strategy picks something other than Retry
                            loop {
                                match callback.on_data(data) {
                                    Ok(()) => {
                                        // Ack by hand unless the broker auto acks
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

                                        if let Some(ctx) = &mut error_ctx {
                                            ctx.reset_errors();
                                        }
                                        break;
                                    }
                                    Err(e) => {
                                        // Wrap the raw bytes in an event so the
                                        // DLQ has something to carry
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
                                            // With no error context, drop the message
                                            ErrorAction::Drop
                                        };

                                        match action {
                                            ErrorAction::Retry { .. } => {
                                                // handle_error_with_action has
                                                // already waited, so try again
                                                continue;
                                            }
                                            _ => {
                                                // Drop, DLQ, Fail, or Cancelled ends the loop
                                                final_action = Some(action);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }

                            // Act on the error outcome once the retry loop ends
                            if let Some(action) = final_action {
                                match action {
                                    ErrorAction::Retry { .. } => {
                                        // Not reached, since Retry loops back above
                                    }
                                    ErrorAction::Fail => {
                                        // Reject the message for good, without requeue
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
                                    ErrorAction::Cancelled => {
                                        // Normal shutdown during a retry. The
                                        // message did nothing wrong, so requeue
                                        // it to be delivered again after restart
                                        if !config.auto_ack {
                                            if let Err(e) = delivery
                                                .nack(lapin::options::BasicNackOptions {
                                                    requeue: true,
                                                    ..Default::default()
                                                })
                                                .await
                                            {
                                                log::warn!(
                                                    "[RabbitMQSource] Failed to requeue message: {}",
                                                    e
                                                );
                                            }
                                        }
                                        should_stop = true;
                                    }
                                    ErrorAction::Drop | ErrorAction::SendToDlq => {
                                        // The error was handled by dropping it or
                                        // sending it to the DLQ. ACK the message
                                        // to remove it from the queue.
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
                            log::info!("[RabbitMQSource] Consumer stream ended");
                            break;
                        }
                        Err(_) => {
                            // Timed out, so loop to check the running flag
                            continue;
                        }
                    }
                }

                log::info!("[RabbitMQSource] Stopped consuming");

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
        self.worker.stop();
    }

    fn clone_box(&self) -> Box<dyn Source> {
        Box::new(self.clone())
    }

    fn set_error_dlq_junction(
        &mut self,
        stream_name: &str,
        junction: Arc<Mutex<InputHandler>>,
    ) -> Result<(), EventFluxError> {
        // With no error.strategy the source drops failures, so there is no DLQ
        // to connect
        if let Some(ref mut ctx) = self.error_ctx {
            ctx.set_dlq_junction(stream_name, junction);
        }
        Ok(())
    }

    fn validate_connectivity(&self) -> Result<(), EventFluxError> {
        let config = self.config.clone();

        let validate_future = async move {
            let uri = config.amqp_uri();

            // Give up after 10 s so a broker that cannot be reached does not hang startup
            let connection = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                lapin::Connection::connect(&uri, lapin::ConnectionProperties::default()),
            )
            .await
            .map_err(|_| EventFluxError::ConnectionUnavailable {
                message: format!(
                    "Failed to connect to RabbitMQ at {}:{}: Connection timeout after 10s",
                    config.host, config.port
                ),
                source: None,
            })?
            .map_err(|e| EventFluxError::ConnectionUnavailable {
                message: format!(
                    "Failed to connect to RabbitMQ at {}:{}: {}",
                    config.host, config.port, e
                ),
                source: Some(Box::new(e)),
            })?;

            let channel = connection.create_channel().await.map_err(|e| {
                EventFluxError::ConnectionUnavailable {
                    message: format!("Failed to create channel: {}", e),
                    source: Some(Box::new(e)),
                }
            })?;

            // Check the queue the same way start() does. Try a passive declare
            // first, so a queue that exists with other flags does not fail with
            // PRECONDITION_FAILED.
            //
            // 1. A passive declare checks whether the queue exists
            // 2. If it exists, the check passes
            // 3. If it does not exist and declare_queue=true, try to create it
            // 4. If it does not exist and declare_queue=false, fail with a clear error
            let passive_result = channel
                .queue_declare(
                    &config.queue,
                    lapin::options::QueueDeclareOptions {
                        passive: true, // only check that it exists, do not create
                        ..Default::default()
                    },
                    lapin::types::FieldTable::default(),
                )
                .await;

            match passive_result {
                Ok(_) => {
                    // The queue exists and can be used, so the check passes
                }
                Err(e) => {
                    // PRECONDITION_FAILED means it exists with other properties
                    let is_precondition_failed = e.to_string().contains("PRECONDITION_FAILED")
                        || e.to_string().contains("406");

                    if is_precondition_failed {
                        // The queue exists with other properties, such as
                        // durable. The check still passes.
                        log::debug!(
                            "[RabbitMQSource] Queue '{}' exists with different properties, validation passes",
                            config.queue
                        );
                    } else if config.declare_queue {
                        // The queue does not exist, so try to create it. The
                        // broker closed the channel after the failed passive
                        // declare, so open a new one.
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

                        let _ = new_channel.close(200, "Validation complete").await;
                    } else {
                        // declare_queue=false and the queue does not exist
                        return Err(EventFluxError::configuration(format!(
                            "Queue '{}' does not exist and declare_queue=false: {}",
                            config.queue, e
                        )));
                    }
                }
            }

            let _ = channel.close(200, "Validation complete").await;
            let _ = connection.close(200, "Validation complete").await;

            Ok::<(), EventFluxError>(())
        };

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // Already inside a tokio runtime. block_in_place avoids the panic
            // from starting a runtime inside a runtime. It needs the multi
            // thread runtime.
            tokio::task::block_in_place(|| handle.block_on(validate_future))
        } else {
            // Not inside a runtime, so make a short lived one
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

/// Factory for RabbitMQ sources.
///
/// Registered with EventFluxContext and used to build RabbitMQ sources from
/// the SQL WITH clause config.
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
        // RabbitMQ keys plus the shared error handling keys owned by
        // SourceErrorContext. Joined once, on first use.
        static PARAMS: std::sync::LazyLock<Vec<&'static str>> = std::sync::LazyLock::new(|| {
            [
                &[
                    "rabbitmq.port",
                    "rabbitmq.vhost",
                    "rabbitmq.username",
                    "rabbitmq.password",
                    "rabbitmq.prefetch",
                    "rabbitmq.consumer.tag",
                    "rabbitmq.auto.ack",
                    "rabbitmq.declare.queue",
                ][..],
                crate::core::error::source_support::SOURCE_ERROR_PARAMETERS,
            ]
            .concat()
        });
        &PARAMS
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn Source>, EventFluxError> {
        // from_properties reads both the RabbitMQ config and the error config.
        // The DLQ junction starts as None. The runtime calls
        // set_error_dlq_junction() when it attaches the source, which connects
        // the DLQ stream and sets the stream name.
        //
        // Until then the queue name names the stream in error messages. It
        // tells sources apart better than a fixed placeholder.
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
        assert_eq!(config.password.expose(), "guest");
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
        assert_eq!(config.password.expose(), "secret");
        let debug = format!("{config:?}");
        assert!(!debug.contains("secret"), "leaked: {debug}");
        assert!(debug.contains("***"));
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
            password: "guest".into(),
            ..Default::default()
        };

        // The default vhost "/" is URL encoded as "%2F"
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
            password: "secret".into(),
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
        assert!(cloned.error_ctx.is_none()); // the source had none to clone
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
        // The factory must accept error handling config
        let factory = RabbitMQSourceFactory;
        let mut config = HashMap::new();
        config.insert("rabbitmq.host".to_string(), "localhost".to_string());
        config.insert("rabbitmq.queue".to_string(), "test-queue".to_string());
        config.insert("error.strategy".to_string(), "retry".to_string());
        config.insert("error.retry.max-attempts".to_string(), "3".to_string());
        config.insert("error.retry.initial-delay".to_string(), "100ms".to_string());

        let result = factory.create_initialized(&config);
        assert!(result.is_ok(), "Factory should accept error.* config");
    }

    #[test]
    fn test_factory_accepts_dlq_strategy() {
        // The factory accepts the DLQ strategy. The runtime connects the junction
        // when it attaches the source.
        let factory = RabbitMQSourceFactory;
        let mut config = HashMap::new();
        config.insert("rabbitmq.host".to_string(), "localhost".to_string());
        config.insert("rabbitmq.queue".to_string(), "test-queue".to_string());
        config.insert("error.strategy".to_string(), "dlq".to_string());
        config.insert("error.dlq.stream".to_string(), "ErrorStream".to_string());

        let result = factory.create_initialized(&config);
        assert!(
            result.is_ok(),
            "Factory should accept DLQ strategy (junction wired later by the runtime)"
        );
    }

    // =========================================================================
    // Shutdown Lifecycle Tests (no broker required)
    // =========================================================================
    // Repeat stops and stop before start are covered by the SourceWorker tests
    // in source/mod.rs. This test adds the join after a failed connect.

    #[test]
    fn test_start_stop_joins_worker() {
        #[derive(Debug)]
        struct NoopCallback;
        impl SourceCallback for NoopCallback {
            fn on_data(&self, _data: &[u8]) -> Result<(), EventFluxError> {
                Ok(())
            }
        }

        // Port 1 refuses connections at once. The worker fails to connect and
        // exits, and stop() must join it without hanging.
        let config = RabbitMQSourceConfig {
            host: "127.0.0.1".to_string(),
            port: 1,
            queue: "test".to_string(),
            ..Default::default()
        };
        let mut source = RabbitMQSource::new(config);
        source.start(Arc::new(NoopCallback));
        source.stop();
        source.stop(); // handle already taken, so this must do nothing
    }
}
