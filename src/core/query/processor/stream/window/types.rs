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

//! Window type name constants
//!
//! Names for the core window types, so code does not repeat the strings. Some
//! registered windows, such as unique, firstUnique, delay, frequent, lossyFrequent and
//! expression, have no constant here.

/// Length window: keeps the last N events
pub const WINDOW_TYPE_LENGTH: &str = "length";

/// Time window: keeps events for a duration D
pub const WINDOW_TYPE_TIME: &str = "time";

/// Length batch window: collects N events, then emits them as a batch
pub const WINDOW_TYPE_LENGTH_BATCH: &str = "lengthBatch";

/// Time batch window: collects events for a duration D, then emits them as a batch
/// (tumbling)
pub const WINDOW_TYPE_TIME_BATCH: &str = "timeBatch";

/// External time window: driven by event timestamps instead of the clock
pub const WINDOW_TYPE_EXTERNAL_TIME: &str = "externalTime";

/// External time batch window: batches driven by event timestamps
pub const WINDOW_TYPE_EXTERNAL_TIME_BATCH: &str = "externalTimeBatch";

/// Session window: groups events into sessions split by gaps
pub const WINDOW_TYPE_SESSION: &str = "session";

/// Sort window: keeps a fixed number of events, expiring by sort order
pub const WINDOW_TYPE_SORT: &str = "sort";

/// Cron window: emits batches on a cron schedule
pub const WINDOW_TYPE_CRON: &str = "cron";

/// Lossy counting window: meant for approximate frequency counting. The current
/// processor passes events through unchanged.
pub const WINDOW_TYPE_LOSSY_COUNTING: &str = "lossyCounting";

/// True when `window_type` is one of the constants above
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
        assert!(!is_supported_window_type("tumbling")); // an alias, not a real type
        assert!(!is_supported_window_type("sliding")); // not implemented
    }
}
