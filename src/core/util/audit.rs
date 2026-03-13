// SPDX-License-Identifier: MIT OR Apache-2.0

//! Audit logging for compliance (GDPR, banking regulations)
//! Provides immutable, tamper-evident logs for data operations.

use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Audit log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    pub timestamp: u64,
    pub operation: String,  // e.g., "save", "load", "delete"
    pub user: Option<String>,  // If available
    pub resource: String,  // e.g., "app1/rev1"
    pub details: HashMap<String, String>,
    pub hash: String,  // For immutability (chain previous hash)
}

impl AuditEntry {
    pub fn new(operation: &str, resource: &str) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        Self {
            timestamp,
            operation: operation.to_string(),
            user: None,
            resource: resource.to_string(),
            details: HashMap::new(),
            hash: "".to_string(),  // To be set
        }
    }

    pub fn with_user(mut self, user: &str) -> Self {
        self.user = Some(user.to_string());
        self
    }

    pub fn with_detail(mut self, key: &str, value: &str) -> Self {
        self.details.insert(key.to_string(), value.to_string());
        self
    }

    pub fn compute_hash(&mut self, prev_hash: &str) {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(format!("{:?}{}{}{}{}", self.timestamp, self.operation, self.resource, self.details, prev_hash));
        self.hash = format!("{:x}", hasher.finalize());
    }
}

/// Simple in-memory audit logger (for production, use persistent store)
pub struct AuditLogger {
    entries: Mutex<Vec<AuditEntry>>,
    last_hash: Mutex<String>,
}

impl AuditLogger {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
            last_hash: Mutex::new("genesis".to_string()),
        }
    }

    pub fn log(&self, mut entry: AuditEntry) {
        let mut last_hash = self.last_hash.lock().unwrap();
        entry.compute_hash(&last_hash);
        *last_hash = entry.hash.clone();
        drop(last_hash);

        let mut entries = self.entries.lock().unwrap();
        entries.push(entry.clone());

        info!("Audit: {} on {} at {}", entry.operation, entry.resource, entry.timestamp);
    }

    pub fn get_entries(&self) -> Vec<AuditEntry> {
        self.entries.lock().unwrap().clone()
    }

    /// Export for data portability
    pub fn export_json(&self) -> String {
        serde_json::to_string(&self.get_entries()).unwrap_or_else(|_| "[]".to_string())
    }
}

impl Default for AuditLogger {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_entry() {
        let entry = AuditEntry::new("save", "app1/rev1").with_detail("size", "100");
        assert_eq!(entry.operation, "save");
        assert_eq!(entry.resource, "app1/rev1");
    }

    #[test]
    fn test_audit_logger() {
        let logger = AuditLogger::new();
        let entry = AuditEntry::new("delete", "app1/rev1");
        logger.log(entry);
        let entries = logger.get_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].operation, "delete");
    }
}