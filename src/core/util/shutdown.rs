// SPDX-License-Identifier: MIT OR Apache-2.0

//! Graceful shutdown handling (M6 milestone)

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::signal;

/// Shutdown coordinator
pub struct ShutdownCoordinator {
    shutdown_flag: Arc<AtomicBool>,
}

impl ShutdownCoordinator {
    pub fn new() -> Self {
        Self {
            shutdown_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown_flag.load(Ordering::Relaxed)
    }

    pub fn shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::Relaxed);
    }

    pub async fn wait_for_shutdown_signal(&self) {
        signal::ctrl_c().await.expect("Failed to listen for shutdown signal");
        self.shutdown();
    }

    pub fn handle(&self) -> ShutdownHandle {
        ShutdownHandle {
            flag: Arc::clone(&self.shutdown_flag),
        }
    }
}

/// Handle for checking shutdown
pub struct ShutdownHandle {
    flag: Arc<AtomicBool>,
}

impl ShutdownHandle {
    pub fn is_shutdown(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shutdown_coordinator() {
        let coord = ShutdownCoordinator::new();
        assert!(!coord.is_shutdown());
        coord.shutdown();
        assert!(coord.is_shutdown());
    }
}