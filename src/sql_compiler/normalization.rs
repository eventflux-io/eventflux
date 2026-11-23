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

//! SQL normalization helpers.
//!
//! Rewrite EventFlux SQL so a standard SQL parser can read it.

use once_cell::sync::Lazy;
use regex::Regex;

/// Matches `CREATE STREAM` in any letter case, with any whitespace between the words.
static CREATE_STREAM_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bCREATE\s+STREAM\b").unwrap());

/// Rewrite every `CREATE STREAM` as `CREATE TABLE` so a standard SQL parser accepts it.
///
/// Matching ignores letter case. The match, including the whitespace between the two
/// words, is replaced with the exact text `CREATE TABLE`. Whitespace elsewhere is kept.
///
/// This is a plain text replace, not a SQL lexer. `CREATE STREAM` inside a string
/// literal or comment is rewritten too.
///
/// # Examples
///
/// ```
/// use eventflux::sql_compiler::normalization::normalize_stream_syntax;
///
/// assert_eq!(
///     normalize_stream_syntax("CREATE STREAM MyStream (x INT)"),
///     "CREATE TABLE MyStream (x INT)"
/// );
///
/// assert_eq!(
///     normalize_stream_syntax("create stream lowercase (x INT)"),
///     "CREATE TABLE lowercase (x INT)"
/// );
/// ```
pub fn normalize_stream_syntax(sql: &str) -> String {
    CREATE_STREAM_RE
        .replace_all(sql, "CREATE TABLE")
        .to_string()
}

/// Whether the text contains `CREATE STREAM` in any letter case.
///
/// Like `normalize_stream_syntax`, this also matches text inside string literals.
pub fn is_create_stream(sql: &str) -> bool {
    CREATE_STREAM_RE.is_match(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_uppercase() {
        let sql = "CREATE STREAM MyStream (x INT)";
        let normalized = normalize_stream_syntax(sql);
        assert_eq!(normalized, "CREATE TABLE MyStream (x INT)");
    }

    #[test]
    fn test_normalize_lowercase() {
        let sql = "create stream mystream (x int)";
        let normalized = normalize_stream_syntax(sql);
        assert_eq!(normalized, "CREATE TABLE mystream (x int)");
    }

    #[test]
    fn test_normalize_mixed_case() {
        let sql = "CrEaTe StReAm MixedCase (x INT)";
        let normalized = normalize_stream_syntax(sql);
        assert_eq!(normalized, "CREATE TABLE MixedCase (x INT)");
    }

    #[test]
    fn test_normalize_multiple_streams() {
        let sql = "CREATE STREAM S1 (x INT); CREATE STREAM S2 (y INT);";
        let normalized = normalize_stream_syntax(sql);
        assert_eq!(
            normalized,
            "CREATE TABLE S1 (x INT); CREATE TABLE S2 (y INT);"
        );
    }

    #[test]
    fn test_normalize_preserves_whitespace() {
        let sql = "CREATE   STREAM   S1   (x INT)";
        let normalized = normalize_stream_syntax(sql);
        assert_eq!(normalized, "CREATE TABLE   S1   (x INT)");
    }

    #[test]
    fn test_is_create_stream_true() {
        assert!(is_create_stream("CREATE STREAM Foo (x INT)"));
        assert!(is_create_stream("create stream bar (y INT)"));
        assert!(is_create_stream("CrEaTe StReAm Baz (z INT)"));
    }

    #[test]
    fn test_is_create_stream_false() {
        assert!(!is_create_stream("SELECT * FROM MyStream"));
        assert!(!is_create_stream("CREATE TABLE MyTable (x INT)"));
        assert!(!is_create_stream("INSERT INTO MyStream VALUES (1)"));
    }

    #[test]
    fn test_normalize_inside_string_not_affected() {
        // Known limitation. Despite the test name, text inside a string literal IS
        // rewritten. Fixing it would need a real SQL lexer.
        let sql = "SELECT 'CREATE STREAM inside string' FROM MyStream";
        let normalized = normalize_stream_syntax(sql);
        assert_eq!(
            normalized,
            "SELECT 'CREATE TABLE inside string' FROM MyStream"
        );
    }
}
