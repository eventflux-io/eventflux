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

//! Window Type Constants
//!
//! Defines type-safe constants for all supported window types in EventFlux.

/// Length-based window: Keeps last N events
pub const WINDOW_TYPE_LENGTH: &str = "length";

/// Time-based window: Keeps events for duration D
pub const WINDOW_TYPE_TIME: &str = "time";

/// Length batch window: Collects N events, then emits batch
pub const WINDOW_TYPE_LENGTH_BATCH: &str = "lengthBatch";

/// Time batch window: Collects events for duration D, then emits batch (tumbling)
pub const WINDOW_TYPE_TIME_BATCH: &str = "timeBatch";

/// External time window: Uses event attribute as timestamp
pub const WINDOW_TYPE_EXTERNAL_TIME: &str = "externalTime";

/// External time batch window: Batch processing with event-provided timestamp
pub const WINDOW_TYPE_EXTERNAL_TIME_BATCH: &str = "externalTimeBatch";

/// Session window: Groups events separated by gaps
pub const WINDOW_TYPE_SESSION: &str = "session";

/// Sort window: Maintains sorted order of events
pub const WINDOW_TYPE_SORT: &str = "sort";

/// Cron window: Periodic window based on cron expression
pub const WINDOW_TYPE_CRON: &str = "cron";

/// Lossy counting window: Approximate frequency counting
pub const WINDOW_TYPE_LOSSY_COUNTING: &str = "lossyCounting";

/// Check if a window type is supported
pub fn is_supported_window_type(window_type: &str) -> bool {
    matches!(
        window_type,
        WINDOW_TYPE_LENGTH
            | WINDOW_TYPE_TIME
            | WINDOW_TYPE_LENGTH_BATCH
            | WINDOW_TYPE_TIME_BATCH
            | WINDOW_TYPE_EXTERNAL_TIME
            | WINDOW_TYPE_EXTERNAL_TIME_BATCH
            | WINDOW_TYPE_SESSION
            | WINDOW_TYPE_SORT
            | WINDOW_TYPE_CRON
            | WINDOW_TYPE_LOSSY_COUNTING
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_supported_window_types() {
        assert!(is_supported_window_type(WINDOW_TYPE_LENGTH));
        assert!(is_supported_window_type(WINDOW_TYPE_TIME));
        assert!(is_supported_window_type(WINDOW_TYPE_TIME_BATCH));
        assert!(!is_supported_window_type("unknown"));
        assert!(!is_supported_window_type("tumbling")); // Alias, not a real type
        assert!(!is_supported_window_type("sliding")); // Not implemented yet
    }
}
