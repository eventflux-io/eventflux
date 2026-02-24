// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQL Normalization Utilities
//!
//! Provides utilities for normalizing EventFlux SQL syntax to be compatible
//! with standard SQL parsers.

use once_cell::sync::Lazy;
use regex::Regex;

/// Regex pattern for case-insensitive CREATE STREAM matching
static CREATE_STREAM_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bCREATE\s+STREAM\b").unwrap());

/// Normalize EventFlux-specific SQL syntax to standard SQL
///
/// This function converts EventFlux's `CREATE STREAM` syntax to `CREATE TABLE`
/// so it can be parsed by standard SQL parsers. The conversion is case-insensitive
/// and preserves whitespace patterns.
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

/// Check if SQL contains CREATE STREAM statement
///
/// This is a case-insensitive check for the presence of CREATE STREAM syntax.
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
        // Note: This is a limitation - we'd need a proper lexer to handle this correctly
        // For now, we document that string literals containing "CREATE STREAM" may be affected
        let sql = "SELECT 'CREATE STREAM inside string' FROM MyStream";
        let normalized = normalize_stream_syntax(sql);
        // This WILL replace inside the string (known limitation)
        assert_eq!(
            normalized,
            "SELECT 'CREATE TABLE inside string' FROM MyStream"
        );
    }
}
