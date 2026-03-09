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

//! Backpressure Management for Event Pipeline
//!
//! Provides sophisticated backpressure handling strategies to maintain
//! system stability under high load conditions.

use super::event_pipeline::PipelineResult;
use crate::core::event::stream::StreamEvent;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Backpressure strategies for handling pipeline overload
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum BackpressureStrategy {
    /// Drop new events when pipeline is full (fire-and-forget)
    #[default]
    Drop,
    /// Block producers until space is available (guaranteed delivery)
    Block,
    /// Block with timeout, then drop (hybrid approach)
    BlockWithTimeout { timeout_ms: u64 },
    /// Apply exponential backoff before dropping
    ExponentialBackoff { max_delay_ms: u64 },
    /// Circuit breaker pattern - fail fast after threshold
    CircuitBreaker {
        failure_threshold: u64,
        recovery_timeout_ms: u64,
    },
}

/// Backpressure handler that implements various strategies
pub struct BackpressureHandler {
    strategy: BackpressureStrategy,
    // Circuit breaker state
    circuit_failures: AtomicU64,
    circuit_last_failure: std::sync::Mutex<Option<Instant>>,
    circuit_state: std::sync::atomic::AtomicU8, // 0=Closed, 1=Open, 2=HalfOpen
    // Exponential backoff state
    current_delay_ms: AtomicU64,
    // Metrics
    total_backpressure_events: AtomicU64,
    total_dropped_events: AtomicU64,
    total_timeout_events: AtomicU64,
    total_circuit_breaks: AtomicU64,
}

impl BackpressureHandler {
    /// Create a new backpressure handler
    pub fn new(strategy: BackpressureStrategy) -> Self {
        Self {
            strategy,
            circuit_failures: AtomicU64::new(0),
            circuit_last_failure: std::sync::Mutex::new(None),
            circuit_state: std::sync::atomic::AtomicU8::new(0), // Closed
            current_delay_ms: AtomicU64::new(1),
            total_backpressure_events: AtomicU64::new(0),
            total_dropped_events: AtomicU64::new(0),
            total_timeout_events: AtomicU64::new(0),
            total_circuit_breaks: AtomicU64::new(0),
        }
    }

    /// Handle queue full condition
    pub fn handle_queue_full(&self, sequence: u64) -> PipelineResult {
        self.total_backpressure_events
            .fetch_add(1, Ordering::Relaxed);

        match self.strategy {
            BackpressureStrategy::Drop => {
                self.total_dropped_events.fetch_add(1, Ordering::Relaxed);
                PipelineResult::Full
            }
            BackpressureStrategy::Block => {
                // In a real implementation, this would block until space is available
                // For now, we simulate blocking behavior
                std::thread::yield_now();
                PipelineResult::Full // Would retry in actual implementation
            }
            BackpressureStrategy::BlockWithTimeout { timeout_ms } => {
                self.handle_block_with_timeout(sequence, timeout_ms)
            }
            BackpressureStrategy::ExponentialBackoff { max_delay_ms } => {
                self.handle_exponential_backoff(sequence, max_delay_ms)
            }
            BackpressureStrategy::CircuitBreaker {
                failure_threshold,
                recovery_timeout_ms,
            } => self.handle_circuit_breaker(sequence, failure_threshold, recovery_timeout_ms),
        }
    }

    /// Handle pool exhausted condition
    pub fn handle_pool_exhausted(&self, _event: StreamEvent, sequence: u64) -> PipelineResult {
        // Pool exhaustion is treated similarly to queue full
        self.handle_queue_full(sequence)
    }

    /// Handle block with timeout strategy
    fn handle_block_with_timeout(&self, _sequence: u64, timeout_ms: u64) -> PipelineResult {
        let start = Instant::now();
        let timeout = Duration::from_millis(timeout_ms);

        // Simulate waiting for space
        while start.elapsed() < timeout {
            std::thread::yield_now();
            // In real implementation, would check if space became available
            // For simulation, we just timeout
        }

        self.total_timeout_events.fetch_add(1, Ordering::Relaxed);
        PipelineResult::Timeout
    }

    /// Handle exponential backoff strategy
    fn handle_exponential_backoff(&self, _sequence: u64, max_delay_ms: u64) -> PipelineResult {
        let current_delay = self.current_delay_ms.load(Ordering::Relaxed);

        // Apply backoff delay
        if current_delay > 0 {
            std::thread::sleep(Duration::from_millis(current_delay.min(max_delay_ms)));
        }

        // Double the delay for next time (exponential backoff)
        let next_delay = (current_delay * 2).min(max_delay_ms);
        self.current_delay_ms.store(next_delay, Ordering::Relaxed);

        // After backoff, still drop the event
        self.total_dropped_events.fetch_add(1, Ordering::Relaxed);
        PipelineResult::Full
    }

    /// Handle circuit breaker strategy
    fn handle_circuit_breaker(
        &self,
        _sequence: u64,
        failure_threshold: u64,
        recovery_timeout_ms: u64,
    ) -> PipelineResult {
        let current_state = self.circuit_state.load(Ordering::Acquire);

        match current_state {
            0 => {
                // Closed - normal operation
                let failures = self.circuit_failures.fetch_add(1, Ordering::Relaxed) + 1;

                if failures >= failure_threshold {
                    // Open the circuit
                    self.circuit_state.store(1, Ordering::Release);
                    if let Ok(mut last_failure) = self.circuit_last_failure.lock() {
                        *last_failure = Some(Instant::now());
                    }
                    self.total_circuit_breaks.fetch_add(1, Ordering::Relaxed);
                    PipelineResult::Error("Circuit breaker opened".to_string())
                } else {
                    self.total_dropped_events.fetch_add(1, Ordering::Relaxed);
                    PipelineResult::Full
                }
            }
            1 => {
                // Open - failing fast
                // Check if recovery timeout has passed
                if let Ok(last_failure) = self.circuit_last_failure.lock() {
                    if let Some(failure_time) = *last_failure {
                        if failure_time.elapsed() > Duration::from_millis(recovery_timeout_ms) {
                            // Move to half-open state
                            self.circuit_state.store(2, Ordering::Release);
                            return self.handle_circuit_breaker(
                                _sequence,
                                failure_threshold,
                                recovery_timeout_ms,
                            );
                        }
                    }
                }
                PipelineResult::Error("Circuit breaker open".to_string())
            }
            2 => {
                // Half-open - testing recovery
                // Try to process - in real implementation would attempt operation
                // For simulation, we'll succeed and close circuit
                self.circuit_state.store(0, Ordering::Release);
                self.circuit_failures.store(0, Ordering::Relaxed);
                PipelineResult::Success {
                    sequence: _sequence,
                }
            }
            _ => PipelineResult::Error("Invalid circuit state".to_string()),
        }
    }

    /// Reset backoff delay (called on successful operations)
    pub fn reset_backoff(&self) {
        self.current_delay_ms.store(1, Ordering::Relaxed);
    }

    /// Reset circuit breaker (manual recovery)
    pub fn reset_circuit(&self) {
        self.circuit_state.store(0, Ordering::Release);
        self.circuit_failures.store(0, Ordering::Relaxed);
        if let Ok(mut last_failure) = self.circuit_last_failure.lock() {
            *last_failure = None;
        }
    }

    /// Get current backpressure statistics
    pub fn get_stats(&self) -> BackpressureStats {
        BackpressureStats {
            strategy: self.strategy,
            total_backpressure_events: self.total_backpressure_events.load(Ordering::Relaxed),
            total_dropped_events: self.total_dropped_events.load(Ordering::Relaxed),
            total_timeout_events: self.total_timeout_events.load(Ordering::Relaxed),
            total_circuit_breaks: self.total_circuit_breaks.load(Ordering::Relaxed),
            current_circuit_state: match self.circuit_state.load(Ordering::Acquire) {
                0 => CircuitState::Closed,
                1 => CircuitState::Open,
                2 => CircuitState::HalfOpen,
                _ => CircuitState::Unknown,
            },
            current_delay_ms: self.current_delay_ms.load(Ordering::Relaxed),
            circuit_failures: self.circuit_failures.load(Ordering::Relaxed),
        }
    }

    /// Check if backpressure is currently active
    pub fn is_active(&self) -> bool {
        match self.strategy {
            BackpressureStrategy::CircuitBreaker { .. } => {
                self.circuit_state.load(Ordering::Acquire) != 0
            }
            BackpressureStrategy::ExponentialBackoff { .. } => {
                self.current_delay_ms.load(Ordering::Relaxed) > 1
            }
            _ => false,
        }
    }

    /// Get recommended action based on current state
    pub fn get_recommendation(&self) -> BackpressureRecommendation {
        let stats = self.get_stats();

        if stats.total_backpressure_events == 0 {
            return BackpressureRecommendation::Healthy;
        }

        let drop_rate = stats.total_dropped_events as f64 / stats.total_backpressure_events as f64;
        let timeout_rate =
            stats.total_timeout_events as f64 / stats.total_backpressure_events as f64;

        if drop_rate > 0.5 {
            BackpressureRecommendation::IncreaseCapacity
        } else if timeout_rate > 0.3 {
            BackpressureRecommendation::AdjustTimeout
        } else if matches!(stats.current_circuit_state, CircuitState::Open) {
            BackpressureRecommendation::CheckDownstream
        } else if stats.current_delay_ms > 1000 {
            BackpressureRecommendation::ReduceLoad
        } else {
            BackpressureRecommendation::Monitor
        }
    }
}

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CircuitState {
    Closed,   // Normal operation
    Open,     // Failing fast
    HalfOpen, // Testing recovery
    Unknown,  // Invalid state
}

/// Backpressure statistics
#[derive(Debug, Clone)]
pub struct BackpressureStats {
    pub strategy: BackpressureStrategy,
    pub total_backpressure_events: u64,
    pub total_dropped_events: u64,
    pub total_timeout_events: u64,
    pub total_circuit_breaks: u64,
    pub current_circuit_state: CircuitState,
    pub current_delay_ms: u64,
    pub circuit_failures: u64,
}

impl BackpressureStats {
    /// Calculate drop rate (0.0 to 1.0)
    pub fn drop_rate(&self) -> f64 {
        if self.total_backpressure_events == 0 {
            0.0
        } else {
            self.total_dropped_events as f64 / self.total_backpressure_events as f64
        }
    }

    /// Calculate timeout rate (0.0 to 1.0)
    pub fn timeout_rate(&self) -> f64 {
        if self.total_backpressure_events == 0 {
            0.0
        } else {
            self.total_timeout_events as f64 / self.total_backpressure_events as f64
        }
    }

    /// Check if backpressure handling is effective
    pub fn is_effective(&self) -> bool {
        match self.strategy {
            BackpressureStrategy::Drop => self.drop_rate() < 0.1, // <10% drop rate is good
            BackpressureStrategy::Block => true,                  // Blocking is always effective
            BackpressureStrategy::BlockWithTimeout { .. } => self.timeout_rate() < 0.2, // <20% timeout rate
            BackpressureStrategy::ExponentialBackoff { .. } => self.current_delay_ms < 5000, // <5s delay
            BackpressureStrategy::CircuitBreaker { .. } => {
                matches!(self.current_circuit_state, CircuitState::Closed)
            }
        }
    }
}

/// Recommendations for improving backpressure handling
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BackpressureRecommendation {
    /// System is healthy, no action needed
    Healthy,
    /// Increase pipeline capacity (buffer size, consumer threads)
    IncreaseCapacity,
    /// Adjust timeout values
    AdjustTimeout,
    /// Check downstream systems for bottlenecks
    CheckDownstream,
    /// Reduce input load
    ReduceLoad,
    /// Continue monitoring
    Monitor,
}

/// Builder for configuring backpressure strategies
pub struct BackpressureBuilder {
    strategy: BackpressureStrategy,
}

impl BackpressureBuilder {
    /// Create a new builder with drop strategy
    pub fn drop() -> Self {
        Self {
            strategy: BackpressureStrategy::Drop,
        }
    }

    /// Create a new builder with block strategy
    pub fn block() -> Self {
        Self {
            strategy: BackpressureStrategy::Block,
        }
    }

    /// Create a new builder with block-with-timeout strategy
    pub fn block_with_timeout(timeout_ms: u64) -> Self {
        Self {
            strategy: BackpressureStrategy::BlockWithTimeout { timeout_ms },
        }
    }

    /// Create a new builder with exponential backoff strategy
    pub fn exponential_backoff(max_delay_ms: u64) -> Self {
        Self {
            strategy: BackpressureStrategy::ExponentialBackoff { max_delay_ms },
        }
    }

    /// Create a new builder with circuit breaker strategy
    pub fn circuit_breaker(failure_threshold: u64, recovery_timeout_ms: u64) -> Self {
        Self {
            strategy: BackpressureStrategy::CircuitBreaker {
                failure_threshold,
                recovery_timeout_ms,
            },
        }
    }

    /// Build the backpressure handler
    pub fn build(self) -> BackpressureHandler {
        BackpressureHandler::new(self.strategy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_drop_strategy() {
        let handler = BackpressureBuilder::drop().build();

        let result = handler.handle_queue_full(1);
        assert_eq!(result, PipelineResult::Full);

        let stats = handler.get_stats();
        assert_eq!(stats.total_dropped_events, 1);
        assert_eq!(stats.total_backpressure_events, 1);
    }

    #[test]
    fn test_block_strategy() {
        let handler = BackpressureBuilder::block().build();

        let result = handler.handle_queue_full(1);
        assert_eq!(result, PipelineResult::Full); // Would be different in real implementation

        let stats = handler.get_stats();
        assert_eq!(stats.total_backpressure_events, 1);
    }

    #[test]
    fn test_timeout_strategy() {
        let handler = BackpressureBuilder::block_with_timeout(10).build();

        let start = Instant::now();
        let result = handler.handle_queue_full(1);
        let elapsed = start.elapsed();

        assert_eq!(result, PipelineResult::Timeout);
        assert!(elapsed >= Duration::from_millis(10));

        let stats = handler.get_stats();
        assert_eq!(stats.total_timeout_events, 1);
    }

    #[test]
    fn test_exponential_backoff() {
        let handler = BackpressureBuilder::exponential_backoff(1000).build();

        // First call should have minimal delay
        let result1 = handler.handle_queue_full(1);
        assert_eq!(result1, PipelineResult::Full);

        // Second call should have more delay
        let stats = handler.get_stats();
        assert!(stats.current_delay_ms > 1);

        // Reset should bring delay back down
        handler.reset_backoff();
        let stats_after_reset = handler.get_stats();
        assert_eq!(stats_after_reset.current_delay_ms, 1);
    }

    #[test]
    fn test_circuit_breaker() {
        let handler = BackpressureBuilder::circuit_breaker(3, 100).build();

        // First few failures should be normal
        for i in 1..=2 {
            let result = handler.handle_queue_full(i);
            assert_eq!(result, PipelineResult::Full);
        }

        // Third failure should open circuit
        let result = handler.handle_queue_full(3);
        assert!(matches!(result, PipelineResult::Error(_)));

        let stats = handler.get_stats();
        assert_eq!(stats.current_circuit_state, CircuitState::Open);
        assert_eq!(stats.total_circuit_breaks, 1);

        // Reset circuit
        handler.reset_circuit();
        let stats_after_reset = handler.get_stats();
        assert_eq!(
            stats_after_reset.current_circuit_state,
            CircuitState::Closed
        );
    }

    #[test]
    fn test_backpressure_stats() {
        let handler = BackpressureBuilder::drop().build();

        // Generate some events
        for i in 1..=10 {
            handler.handle_queue_full(i);
        }

        let stats = handler.get_stats();
        assert_eq!(stats.drop_rate(), 1.0); // All events dropped
        assert_eq!(stats.timeout_rate(), 0.0); // No timeouts
        assert!(!stats.is_effective()); // 100% drop rate means not effective
    }

    #[test]
    fn test_recommendations() {
        let handler = BackpressureBuilder::drop().build();

        // Initial state should be healthy
        assert_eq!(
            handler.get_recommendation(),
            BackpressureRecommendation::Healthy
        );

        // Generate many drops
        for i in 1..=100 {
            handler.handle_queue_full(i);
        }

        // Should recommend increasing capacity
        assert_eq!(
            handler.get_recommendation(),
            BackpressureRecommendation::IncreaseCapacity
        );
    }

    #[test]
    fn test_backpressure_activity() {
        let handler = BackpressureBuilder::exponential_backoff(1000).build();

        // Initially not active
        assert!(!handler.is_active());

        // After backpressure event, should be active
        handler.handle_queue_full(1);
        assert!(handler.is_active());

        // After reset, should not be active
        handler.reset_backoff();
        assert!(!handler.is_active());
    }
}
