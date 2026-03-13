// SPDX-License-Identifier: MIT OR Apache-2.0

//! Health check endpoints for production monitoring (M6 milestone)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

/// Health check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheck {
    pub component: String,
    pub status: HealthStatus,
    pub message: String,
    pub timestamp: u64,
}

impl HealthCheck {
    pub fn new(component: &str, status: HealthStatus, message: &str) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        Self {
            component: component.to_string(),
            status,
            message: message.to_string(),
            timestamp,
        }
    }
}

/// Health checker trait
pub trait HealthChecker: Send + Sync {
    fn check(&self) -> HealthCheck;
}

/// Composite health checker
pub struct HealthAggregator {
    checkers: Vec<Box<dyn HealthChecker>>,
}

impl HealthAggregator {
    pub fn new() -> Self {
        Self { checkers: Vec::new() }
    }

    pub fn add_checker(&mut self, checker: Box<dyn HealthChecker>) {
        self.checkers.push(checker);
    }

    pub fn overall_health(&self) -> HealthStatus {
        let mut has_degraded = false;
        for checker in &self.checkers {
            match checker.check().status {
                HealthStatus::Unhealthy => return HealthStatus::Unhealthy,
                HealthStatus::Degraded => has_degraded = true,
                _ => {}
            }
        }
        if has_degraded { HealthStatus::Degraded } else { HealthStatus::Healthy }
    }

    pub fn all_checks(&self) -> Vec<HealthCheck> {
        self.checkers.iter().map(|c| c.check()).collect()
    }
}

/// Example persistence health checker
pub struct PersistenceHealthChecker {
    // Assume some way to check
}

impl PersistenceHealthChecker {
    pub fn new() -> Self {
        Self {}
    }
}

impl HealthChecker for PersistenceHealthChecker {
    fn check(&self) -> HealthCheck {
        // Simulate check
        HealthCheck::new("persistence", HealthStatus::Healthy, "All good")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_aggregator() {
        let mut agg = HealthAggregator::new();
        agg.add_checker(Box::new(PersistenceHealthChecker::new()));
        let status = agg.overall_health();
        assert!(matches!(status, HealthStatus::Healthy));
    }
}