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

//! Application Configuration Types
//!
//! Defines per-application configuration structures with definition-level granularity.

use super::{duration_serde, optional_duration_serde};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Application-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ApplicationConfig {
    /// Per-stream configuration (input/output streams with sources/sinks)
    #[serde(default)]
    pub streams: HashMap<String, StreamConfig>,

    /// Per-definition configuration (tables, windows, etc.)
    #[serde(default)]
    pub definitions: HashMap<String, DefinitionConfig>,

    /// Per-query configuration
    #[serde(default)]
    pub queries: HashMap<String, QueryConfig>,

    /// Application-level persistence configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistence: Option<PersistenceConfig>,

    /// Application-level monitoring configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub monitoring: Option<MonitoringConfig>,

    /// Application-level error handling configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_handling: Option<ErrorHandlingConfig>,
}

/// Configuration for individual definitions (streams, tables, windows, etc.)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
#[allow(clippy::large_enum_variant)]
pub enum DefinitionConfig {
    /// Stream definition configuration
    Stream(StreamConfig),

    /// Table definition configuration
    Table(TableConfig),

    /// Window definition configuration
    Window(WindowConfig),

    /// Aggregation definition configuration
    Aggregation(AggregationConfig),

    /// Function definition configuration
    Function(FunctionConfig),

    /// Trigger definition configuration
    Trigger(TriggerConfig),
}

/// Stream configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamConfig {
    /// Source configuration (for input streams)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceConfig>,

    /// Sink configuration (for output streams)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink: Option<SinkConfig>,

    /// Schema validation configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaConfig>,

    /// Stream-specific processing configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processing: Option<StreamProcessingConfig>,
}

/// Source configuration for input streams
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceConfig {
    /// Source type (kafka, http, tcp, file, etc.)
    #[serde(rename = "type")]
    pub source_type: String,

    /// Data format (json, avro, protobuf, csv, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,

    /// Connection configuration
    #[serde(flatten)]
    pub connection: serde_yaml::Value,

    /// Security configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<SourceSecurityConfig>,

    /// Error handling for source
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_handling: Option<SourceErrorHandling>,

    /// Rate limiting
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<RateLimitConfig>,
}

/// Sink configuration for output streams
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SinkConfig {
    /// Sink type (kafka, http, tcp, file, etc.)
    #[serde(rename = "type")]
    pub sink_type: String,

    /// Data format (json, avro, protobuf, csv, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,

    /// Connection configuration
    #[serde(flatten)]
    pub connection: serde_yaml::Value,

    /// Security configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<SinkSecurityConfig>,

    /// Delivery guarantees
    #[serde(default)]
    pub delivery_guarantee: DeliveryGuarantee,

    /// Retry configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryConfig>,

    /// Batching configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batching: Option<BatchingConfig>,
}

/// Source security configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceSecurityConfig {
    /// Authentication configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authentication: Option<serde_yaml::Value>,

    /// TLS configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfig>,

    /// Authorization configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorization: Option<serde_yaml::Value>,
}

/// Sink security configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SinkSecurityConfig {
    /// Authentication configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authentication: Option<serde_yaml::Value>,

    /// TLS configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfig>,

    /// Authorization configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorization: Option<serde_yaml::Value>,
}

/// TLS configuration for sources and sinks
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TlsConfig {
    /// Enable TLS
    #[serde(default)]
    pub enabled: bool,

    /// Certificate file path or content
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert: Option<String>,

    /// Private key file path or content
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,

    /// CA certificate file path or content
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca: Option<String>,

    /// Verify peer certificates
    #[serde(default = "default_true")]
    pub verify_peer: bool,

    /// Server name for SNI
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_name: Option<String>,
}

/// Delivery guarantee options
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeliveryGuarantee {
    /// At most once delivery
    AtMostOnce,

    /// At least once delivery
    AtLeastOnce,

    /// Exactly once delivery
    ExactlyOnce,
}

impl Default for DeliveryGuarantee {
    fn default() -> Self {
        Self::AtLeastOnce
    }
}

/// Source error handling configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceErrorHandling {
    /// Strategy for handling source errors
    #[serde(default)]
    pub strategy: ErrorStrategy,

    /// Dead letter queue configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dead_letter_queue: Option<String>,

    /// Maximum retry attempts
    #[serde(default = "default_max_retries")]
    pub max_retry_attempts: usize,

    /// Retry backoff configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backoff: Option<BackoffConfig>,
}

/// Error handling strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ErrorStrategy {
    /// Log error and continue processing
    LogAndContinue,

    /// Log error and stop processing
    LogAndStop,

    /// Ignore errors silently
    Ignore,

    /// Send to dead letter queue
    DeadLetterQueue,

    /// Retry with backoff
    RetryWithBackoff,

    /// Fail fast
    FailFast,
}

impl Default for ErrorStrategy {
    fn default() -> Self {
        Self::LogAndContinue
    }
}

/// Retry configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetryConfig {
    /// Maximum retry attempts
    #[serde(default = "default_max_retries")]
    pub max_attempts: usize,

    /// Initial retry delay
    #[serde(with = "duration_serde", default = "default_initial_delay")]
    pub initial_delay: Duration,

    /// Maximum retry delay
    #[serde(with = "duration_serde", default = "default_max_delay")]
    pub max_delay: Duration,

    /// Backoff multiplier
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,

    /// Jitter configuration
    #[serde(default)]
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_max_retries(),
            initial_delay: default_initial_delay(),
            max_delay: default_max_delay(),
            backoff_multiplier: default_backoff_multiplier(),
            jitter: true,
        }
    }
}

/// Backoff configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BackoffConfig {
    /// Backoff type
    #[serde(default)]
    pub backoff_type: BackoffType,

    /// Base delay
    #[serde(with = "duration_serde", default = "default_base_delay")]
    pub base_delay: Duration,

    /// Maximum delay
    #[serde(with = "duration_serde", default = "default_max_delay")]
    pub max_delay: Duration,

    /// Multiplier for exponential backoff
    #[serde(default = "default_backoff_multiplier")]
    pub multiplier: f64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            backoff_type: BackoffType::Exponential,
            base_delay: default_base_delay(),
            max_delay: default_max_delay(),
            multiplier: default_backoff_multiplier(),
        }
    }
}

/// Backoff strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackoffType {
    /// Fixed delay between retries
    Fixed,

    /// Linear increase in delay
    Linear,

    /// Exponential increase in delay
    Exponential,
}

impl Default for BackoffType {
    fn default() -> Self {
        Self::Exponential
    }
}

/// Rate limiting configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RateLimitConfig {
    /// Maximum requests per second
    pub max_rate: f64,

    /// Burst capacity
    #[serde(default = "default_burst_capacity")]
    pub burst_capacity: usize,

    /// Rate limiting strategy
    #[serde(default)]
    pub strategy: RateLimitStrategy,
}

/// Rate limiting strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RateLimitStrategy {
    /// Token bucket algorithm
    TokenBucket,

    /// Leaky bucket algorithm
    LeakyBucket,

    /// Fixed window algorithm
    FixedWindow,

    /// Sliding window algorithm
    SlidingWindow,
}

impl Default for RateLimitStrategy {
    fn default() -> Self {
        Self::TokenBucket
    }
}

/// Batching configuration for sinks
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BatchingConfig {
    /// Maximum batch size
    #[serde(default = "default_batch_size")]
    pub max_batch_size: usize,

    /// Maximum batch timeout
    #[serde(with = "duration_serde", default = "default_batch_timeout")]
    pub max_batch_timeout: Duration,

    /// Memory limit for batching
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_batch_memory: Option<usize>,
}

impl Default for BatchingConfig {
    fn default() -> Self {
        Self {
            max_batch_size: default_batch_size(),
            max_batch_timeout: default_batch_timeout(),
            max_batch_memory: None,
        }
    }
}

/// Schema validation configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaConfig {
    /// Schema definition
    pub schema: serde_yaml::Value,

    /// Validation mode
    #[serde(default)]
    pub validation_mode: SchemaValidationMode,

    /// Schema evolution configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evolution: Option<SchemaEvolutionConfig>,
}

/// Schema validation modes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaValidationMode {
    /// Strict validation (reject invalid data)
    Strict,

    /// Lenient validation (log and continue)
    Lenient,

    /// No validation
    None,
}

impl Default for SchemaValidationMode {
    fn default() -> Self {
        Self::Strict
    }
}

/// Schema evolution configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaEvolutionConfig {
    /// Allow schema evolution
    #[serde(default)]
    pub enabled: bool,

    /// Auto-migrate data
    #[serde(default)]
    pub auto_migrate: bool,
}

/// Stream processing configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamProcessingConfig {
    /// Enable async processing
    #[serde(default)]
    pub async_mode: bool,

    /// Buffer size for stream
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buffer_size: Option<usize>,

    /// Parallelism level
    #[serde(default = "default_parallelism")]
    pub parallelism: usize,

    /// Ordering guarantees
    #[serde(default)]
    pub ordering: OrderingGuarantee,
}

/// Ordering guarantee options
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OrderingGuarantee {
    /// No ordering guarantees
    None,

    /// Per-key ordering
    PerKey,

    /// Global ordering
    Global,
}

impl Default for OrderingGuarantee {
    fn default() -> Self {
        Self::None
    }
}

/// Table configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableConfig {
    /// Store configuration
    pub store: StoreConfig,

    /// Schema configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaConfig>,

    /// Caching configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caching: Option<CachingConfig>,

    /// Indexing configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexing: Option<IndexingConfig>,
}

/// Store configuration for tables
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoreConfig {
    /// Store type (rdbms, mongodb, redis, etc.)
    #[serde(rename = "type")]
    pub store_type: String,

    /// Connection configuration
    #[serde(flatten)]
    pub connection: serde_yaml::Value,

    /// Connection pooling
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool: Option<ConnectionPoolConfig>,

    /// Store-specific security configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<serde_yaml::Value>,
}

/// Connection pool configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectionPoolConfig {
    /// Maximum pool size
    #[serde(default = "default_max_pool_size")]
    pub max_size: usize,

    /// Minimum pool size
    #[serde(default = "default_min_pool_size")]
    pub min_size: usize,

    /// Connection timeout
    #[serde(with = "duration_serde", default = "default_connection_timeout")]
    pub connection_timeout: Duration,

    /// Idle timeout
    #[serde(
        with = "optional_duration_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub idle_timeout: Option<Duration>,

    /// Maximum connection lifetime
    #[serde(
        with = "optional_duration_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_lifetime: Option<Duration>,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_size: default_max_pool_size(),
            min_size: default_min_pool_size(),
            connection_timeout: default_connection_timeout(),
            idle_timeout: None,
            max_lifetime: None,
        }
    }
}

/// Caching configuration for tables
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CachingConfig {
    /// Enable caching
    #[serde(default)]
    pub enabled: bool,

    /// Cache type
    #[serde(default)]
    pub cache_type: CacheType,

    /// Maximum cache size
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_size: Option<usize>,

    /// Cache TTL
    #[serde(
        with = "optional_duration_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub ttl: Option<Duration>,

    /// Cache eviction policy
    #[serde(default)]
    pub eviction_policy: CacheEvictionPolicy,
}

/// Cache types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheType {
    /// In-memory cache
    Memory,

    /// Disk-based cache
    Disk,

    /// Distributed cache
    Distributed,
}

impl Default for CacheType {
    fn default() -> Self {
        Self::Memory
    }
}

/// Cache eviction policies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CacheEvictionPolicy {
    /// Least Recently Used
    Lru,

    /// Least Frequently Used
    Lfu,

    /// First In, First Out
    Fifo,

    /// Time-based expiration
    TimeToLive,
}

impl Default for CacheEvictionPolicy {
    fn default() -> Self {
        Self::Lru
    }
}

/// Indexing configuration for tables
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexingConfig {
    /// Primary key configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<Vec<String>>,

    /// Secondary indexes
    #[serde(default)]
    pub indexes: Vec<IndexConfig>,

    /// Auto-indexing configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_indexing: Option<AutoIndexingConfig>,
}

/// Individual index configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexConfig {
    /// Index name
    pub name: String,

    /// Indexed columns
    pub columns: Vec<String>,

    /// Index type
    #[serde(default)]
    pub index_type: IndexType,

    /// Unique constraint
    #[serde(default)]
    pub unique: bool,
}

/// Index types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IndexType {
    /// B-tree index
    BTree,

    /// Hash index
    Hash,

    /// Full-text index
    FullText,

    /// Spatial index
    Spatial,
}

impl Default for IndexType {
    fn default() -> Self {
        Self::BTree
    }
}

/// Auto-indexing configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AutoIndexingConfig {
    /// Enable auto-indexing
    #[serde(default)]
    pub enabled: bool,

    /// Threshold for creating indexes
    #[serde(default = "default_auto_index_threshold")]
    pub threshold: usize,

    /// Maximum number of auto-created indexes
    #[serde(default = "default_max_auto_indexes")]
    pub max_indexes: usize,
}

/// Window configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WindowConfig {
    /// Window type (time, length, session, etc.)
    pub window_type: String,

    /// Window parameters
    #[serde(default)]
    pub parameters: serde_yaml::Value,

    /// Persistence configuration for window
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistence: Option<WindowPersistenceConfig>,

    /// Memory management for window
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<WindowMemoryConfig>,
}

/// Window persistence configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WindowPersistenceConfig {
    /// Enable persistence for this window
    #[serde(default)]
    pub enabled: bool,

    /// Checkpoint interval for window state
    #[serde(
        with = "duration_serde",
        default = "default_window_checkpoint_interval"
    )]
    pub checkpoint_interval: Duration,

    /// Compression for window state
    #[serde(default)]
    pub compression: CompressionType,

    /// Persistence backend override
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend: Option<String>,
}

/// Compression types for window persistence
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionType {
    None,
    Lz4,
    Snappy,
    Zstd,
}

impl Default for CompressionType {
    fn default() -> Self {
        Self::Lz4
    }
}

/// Window memory management configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WindowMemoryConfig {
    /// Maximum memory usage for window
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_memory: Option<String>,

    /// Memory threshold for spilling to disk
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spill_threshold: Option<f32>,

    /// Enable memory monitoring
    #[serde(default = "default_true")]
    pub monitoring_enabled: bool,
}

/// Aggregation configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AggregationConfig {
    /// Store configuration for aggregation
    pub store: StoreConfig,

    /// Retention policy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention: Option<RetentionConfig>,

    /// Partitioning strategy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitioning: Option<PartitioningConfig>,
}

/// Retention configuration for aggregations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetentionConfig {
    /// Time-based retention
    #[serde(
        with = "optional_duration_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub by_time: Option<Duration>,

    /// Memory-based retention
    #[serde(skip_serializing_if = "Option::is_none")]
    pub by_memory: Option<String>,

    /// Count-based retention
    #[serde(skip_serializing_if = "Option::is_none")]
    pub by_count: Option<usize>,
}

/// Partitioning configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartitioningConfig {
    /// Partitioning strategy
    pub strategy: PartitioningStrategy,

    /// Number of partitions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_count: Option<usize>,

    /// Partition key fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_keys: Option<Vec<String>>,
}

/// Partitioning strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PartitioningStrategy {
    /// Hash-based partitioning
    Hash,

    /// Range-based partitioning
    Range,

    /// Time-based partitioning
    Time,

    /// Custom partitioning
    Custom,
}

/// Function configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FunctionConfig {
    /// Function implementation details
    #[serde(flatten)]
    pub implementation: serde_yaml::Value,

    /// Security configuration for function
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<FunctionSecurityConfig>,

    /// Resource limits for function
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<FunctionResourceConfig>,
}

/// Function security configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FunctionSecurityConfig {
    /// Sandboxing configuration
    #[serde(default = "default_true")]
    pub sandboxing_enabled: bool,

    /// Allowed operations
    #[serde(default)]
    pub allowed_operations: Vec<String>,

    /// Denied operations
    #[serde(default)]
    pub denied_operations: Vec<String>,
}

/// Function resource configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FunctionResourceConfig {
    /// Maximum memory usage
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_memory: Option<String>,

    /// Maximum execution time
    #[serde(
        with = "optional_duration_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_execution_time: Option<Duration>,

    /// CPU limits
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_cpu: Option<String>,
}

/// Trigger configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TriggerConfig {
    /// Trigger-specific configuration
    #[serde(flatten)]
    pub config: serde_yaml::Value,

    /// Error handling for trigger
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_handling: Option<TriggerErrorHandling>,
}

/// Trigger error handling
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TriggerErrorHandling {
    /// Error strategy
    #[serde(default)]
    pub strategy: ErrorStrategy,

    /// Maximum failures before stopping
    #[serde(default = "default_max_failures")]
    pub max_failures: usize,

    /// Recovery strategy
    #[serde(default)]
    pub recovery: TriggerRecovery,
}

/// Trigger recovery strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TriggerRecovery {
    /// Restart trigger
    Restart,

    /// Skip failed execution
    Skip,

    /// Manual intervention required
    Manual,
}

impl Default for TriggerRecovery {
    fn default() -> Self {
        Self::Restart
    }
}

/// Query-level configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryConfig {
    /// Parallelism level for query
    #[serde(default = "default_parallelism")]
    pub parallelism: usize,

    /// Enable async mode for query
    #[serde(default)]
    pub async_mode: bool,

    /// Error handling for query
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_handling: Option<QueryErrorHandling>,

    /// Checkpointing configuration for query
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpointing: Option<QueryCheckpointingConfig>,

    /// Resource limits for query
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<QueryResourceConfig>,
}

/// Query error handling configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryErrorHandling {
    /// Error strategy
    #[serde(default)]
    pub strategy: ErrorStrategy,

    /// Dead letter queue for failed events
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dead_letter_queue: Option<String>,

    /// Maximum retry attempts
    #[serde(default = "default_max_retries")]
    pub max_retry_attempts: usize,
}

/// Query checkpointing configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryCheckpointingConfig {
    /// Checkpoint interval
    #[serde(with = "duration_serde", default = "default_checkpoint_interval")]
    pub interval: Duration,

    /// Checkpointing strategy
    #[serde(default)]
    pub strategy: CheckpointingStrategy,

    /// Enable incremental checkpointing
    #[serde(default = "default_true")]
    pub incremental: bool,
}

/// Checkpointing strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckpointingStrategy {
    /// Synchronous checkpointing
    Sync,

    /// Asynchronous checkpointing
    Async,

    /// Adaptive checkpointing
    Adaptive,
}

impl Default for CheckpointingStrategy {
    fn default() -> Self {
        Self::Async
    }
}

/// Query resource configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryResourceConfig {
    /// Maximum memory usage
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_memory: Option<String>,

    /// CPU allocation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,

    /// Maximum execution time per event
    #[serde(
        with = "optional_duration_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_execution_time: Option<Duration>,
}

/// Application-level persistence configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistenceConfig {
    /// Enable persistence
    #[serde(default)]
    pub enabled: bool,

    /// Store type override
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store_type: Option<String>,

    /// Checkpoint interval
    #[serde(with = "duration_serde", default = "default_checkpoint_interval")]
    pub checkpoint_interval: Duration,

    /// Enable incremental checkpointing
    #[serde(default = "default_true")]
    pub incremental: bool,

    /// Persistence-specific configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<serde_yaml::Value>,
}

/// Application-level monitoring configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MonitoringConfig {
    /// Enable metrics collection
    #[serde(default = "default_true")]
    pub metrics_enabled: bool,

    /// Prometheus metrics port
    #[serde(default = "default_prometheus_port")]
    pub prometheus_port: u16,

    /// Health check port
    #[serde(default = "default_health_port")]
    pub health_check_port: u16,

    /// Log level for application
    #[serde(default)]
    pub log_level: LogLevel,

    /// Custom monitoring configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom: Option<serde_yaml::Value>,
}

/// Log levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl Default for LogLevel {
    fn default() -> Self {
        Self::Info
    }
}

/// Application-level error handling configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ErrorHandlingConfig {
    /// Default error strategy
    #[serde(default)]
    pub default_strategy: ErrorStrategy,

    /// Maximum retry attempts
    #[serde(default = "default_max_retries")]
    pub max_retry_attempts: usize,

    /// Default dead letter queue
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dead_letter_queue: Option<String>,

    /// Global error handlers
    #[serde(default)]
    pub handlers: Vec<ErrorHandlerConfig>,
}

/// Error handler configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ErrorHandlerConfig {
    /// Handler name
    pub name: String,

    /// Error types to handle
    pub error_types: Vec<String>,

    /// Handler configuration
    #[serde(flatten)]
    pub config: serde_yaml::Value,
}

// Helper functions for defaults
fn default_true() -> bool {
    true
}

fn default_max_retries() -> usize {
    3
}

fn default_initial_delay() -> Duration {
    Duration::from_millis(100)
}

fn default_max_delay() -> Duration {
    Duration::from_secs(30)
}

fn default_base_delay() -> Duration {
    Duration::from_millis(100)
}

fn default_backoff_multiplier() -> f64 {
    2.0
}

fn default_burst_capacity() -> usize {
    100
}

fn default_batch_size() -> usize {
    100
}

fn default_batch_timeout() -> Duration {
    Duration::from_millis(100)
}

fn default_parallelism() -> usize {
    1
}

fn default_max_pool_size() -> usize {
    10
}

fn default_min_pool_size() -> usize {
    1
}

fn default_connection_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_auto_index_threshold() -> usize {
    1000
}

fn default_max_auto_indexes() -> usize {
    10
}

fn default_window_checkpoint_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_checkpoint_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_max_failures() -> usize {
    5
}

fn default_prometheus_port() -> u16 {
    9090
}

fn default_health_port() -> u16 {
    8080
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_application_config_default() {
        let config = ApplicationConfig::default();
        assert!(config.definitions.is_empty());
        assert!(config.queries.is_empty());
        assert!(config.persistence.is_none());
    }

    #[test]
    fn test_stream_config_serialization() {
        let stream_config = StreamConfig {
            source: Some(SourceConfig {
                source_type: "kafka".to_string(),
                format: Some("json".to_string()),
                connection: serde_yaml::Value::Mapping(serde_yaml::Mapping::new()),
                security: None,
                error_handling: None,
                rate_limit: None,
            }),
            sink: None,
            schema: None,
            processing: None,
        };

        let definition = DefinitionConfig::Stream(stream_config);
        let yaml = serde_yaml::to_string(&definition).unwrap();
        let deserialized: DefinitionConfig = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(definition, deserialized);
    }

    #[test]
    fn test_error_strategy_serde() {
        let strategy = ErrorStrategy::RetryWithBackoff;
        let yaml = serde_yaml::to_string(&strategy).unwrap();
        assert_eq!(yaml.trim(), "retry-with-backoff");

        let deserialized: ErrorStrategy = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(deserialized, strategy);
    }

    #[test]
    fn test_delivery_guarantee_serde() {
        let guarantee = DeliveryGuarantee::ExactlyOnce;
        let yaml = serde_yaml::to_string(&guarantee).unwrap();
        assert_eq!(yaml.trim(), "exactly-once");
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.backoff_multiplier, 2.0);
        assert!(config.jitter);
    }

    #[test]
    fn test_window_config() {
        let window_config = WindowConfig {
            window_type: "time".to_string(),
            parameters: serde_yaml::Value::String("5min".to_string()),
            persistence: Some(WindowPersistenceConfig {
                enabled: true,
                checkpoint_interval: Duration::from_secs(30),
                compression: CompressionType::Zstd,
                backend: None,
            }),
            memory: None,
        };

        let yaml = serde_yaml::to_string(&window_config).unwrap();
        let deserialized: WindowConfig = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(window_config, deserialized);
    }

    #[test]
    fn test_query_config_defaults() {
        let config = QueryConfig {
            parallelism: default_parallelism(),
            async_mode: false,
            error_handling: None,
            checkpointing: None,
            resources: None,
        };

        assert_eq!(config.parallelism, 1);
        assert!(!config.async_mode);
    }
}
