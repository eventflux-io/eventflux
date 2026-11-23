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

use crate::query_api::eventflux_element::EventFluxElement;

// One Constant type covers every value kind, so there are no per type constant modules.
// pub mod bool_constant;
// pub mod double_constant;
// pub mod float_constant;
// pub mod int_constant;
// pub mod long_constant;
// pub mod string_constant;
// pub mod time_constant;

/// Older value enum without a Null variant. Nothing in the crate uses it. Constant holds a
/// ConstantValueWithFloat instead.
#[derive(Clone, Debug, PartialEq)]
pub enum ConstantValue {
    String(String),
    Int(i32),
    Long(i64),
    // The float variants are why this enum has no Eq or Hash. Keeping them out of hash
    // keys, or wrapping them in a type such as `ordered-float`, would allow both.
    Float(f32),
    Double(f64),
    Bool(bool),
    /// A duration in milliseconds.
    Time(i64),
}

/// The value of a constant. The name stresses that it holds float variants, so it has
/// PartialEq but no Eq or Hash. Do not use it as a hash key.
///
/// Defaults to an empty `String`.
#[derive(Clone, Debug, PartialEq)]
pub enum ConstantValueWithFloat {
    String(String),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bool(bool),
    /// A duration in milliseconds.
    Time(i64),
    /// SQL NULL, used for example when a CASE has no ELSE.
    Null,
}

impl Default for ConstantValueWithFloat {
    fn default() -> Self {
        ConstantValueWithFloat::String(String::new())
    }
}

/// A literal value in an expression. The default holds an empty string.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Constant {
    pub eventflux_element: EventFluxElement,
    pub value: ConstantValueWithFloat,
}

impl Constant {
    pub fn new(value: ConstantValueWithFloat) -> Self {
        Constant {
            eventflux_element: EventFluxElement::default(),
            value,
        }
    }

    pub fn get_value(&self) -> &ConstantValueWithFloat {
        &self.value
    }
}

// Shortcut constructors, one per value kind.
impl Constant {
    pub fn string(value: String) -> Self {
        Constant::new(ConstantValueWithFloat::String(value))
    }

    pub fn int(value: i32) -> Self {
        Constant::new(ConstantValueWithFloat::Int(value))
    }

    pub fn long(value: i64) -> Self {
        Constant::new(ConstantValueWithFloat::Long(value))
    }

    pub fn float(value: f32) -> Self {
        Constant::new(ConstantValueWithFloat::Float(value))
    }

    pub fn double(value: f64) -> Self {
        Constant::new(ConstantValueWithFloat::Double(value))
    }

    pub fn bool(value: bool) -> Self {
        Constant::new(ConstantValueWithFloat::Bool(value))
    }

    /// A duration constant. `value` is in milliseconds.
    pub fn time(value: i64) -> Self {
        Constant::new(ConstantValueWithFloat::Time(value))
    }

    /// A NULL constant. The SQL converter uses it as the CASE result when ELSE is missing.
    pub fn null() -> Self {
        Constant::new(ConstantValueWithFloat::Null)
    }
}

/// Converts time amounts to milliseconds.
///
/// The `*_val` functions return the plain number. The others wrap it in a Time constant.
/// None of them check for overflow.
pub struct TimeUtil;

impl TimeUtil {
    pub fn millisec_val(val: i64) -> i64 {
        val
    }
    pub fn sec_val(val: i64) -> i64 {
        val * 1000
    }
    pub fn minute_val(val: i64) -> i64 {
        val * 60 * 1000
    }
    pub fn hour_val(val: i64) -> i64 {
        val * 60 * 60 * 1000
    }
    pub fn day_val(val: i64) -> i64 {
        val * 24 * 60 * 60 * 1000
    }
    pub fn week_val(val: i64) -> i64 {
        val * 7 * 24 * 60 * 60 * 1000
    }
    // Month and year use average lengths, not 30 and 365 days. A month is 2_630_000_000 ms
    // (about 30.44 days) and a year is 31_556_900_000 ms (about 365.24 days). Note that
    // aggregation::Duration::to_millis uses 30 and 365 days instead.
    pub fn month_val(val: i64) -> i64 {
        val * 2_630_000_000
    }
    pub fn year_val(val: i64) -> i64 {
        val * 31_556_900_000
    }

    // These return a Time constant.
    pub fn millisec(val: i64) -> Constant {
        Constant::time(Self::millisec_val(val))
    }
    pub fn sec(val: i64) -> Constant {
        Constant::time(Self::sec_val(val))
    }
    pub fn minute(val: i64) -> Constant {
        Constant::time(Self::minute_val(val))
    }
    pub fn hour(val: i64) -> Constant {
        Constant::time(Self::hour_val(val))
    }
    pub fn day(val: i64) -> Constant {
        Constant::time(Self::day_val(val))
    }
    pub fn week(val: i64) -> Constant {
        Constant::time(Self::week_val(val))
    }
    pub fn month(val: i64) -> Constant {
        Constant::time(Self::month_val(val))
    }
    pub fn year(val: i64) -> Constant {
        Constant::time(Self::year_val(val))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant_string() {
        let c = Constant::string("hello".to_string());
        assert_eq!(
            c.get_value(),
            &ConstantValueWithFloat::String("hello".to_string())
        );
        assert_eq!(c.eventflux_element.query_context_start_index, None);
    }

    #[test]
    fn test_constant_int() {
        let c = Constant::int(123);
        assert_eq!(c.get_value(), &ConstantValueWithFloat::Int(123));
    }

    #[test]
    fn test_constant_long() {
        let c = Constant::long(1234567890);
        assert_eq!(c.get_value(), &ConstantValueWithFloat::Long(1234567890));
    }

    #[test]
    fn test_constant_float() {
        let c = Constant::float(12.34);
        // Exact float equality is fine here because the same literal is stored and read.
        assert_eq!(c.get_value(), &ConstantValueWithFloat::Float(12.34));
    }

    #[test]
    fn test_constant_double() {
        let c = Constant::double(123.456);
        assert_eq!(c.get_value(), &ConstantValueWithFloat::Double(123.456));
    }

    #[test]
    fn test_constant_bool() {
        let c = Constant::bool(true);
        assert_eq!(c.get_value(), &ConstantValueWithFloat::Bool(true));
    }

    #[test]
    fn test_constant_time() {
        let c = Constant::time(5000); // 5000 ms
        assert_eq!(c.get_value(), &ConstantValueWithFloat::Time(5000));
    }

    #[test]
    fn test_constant_default() {
        let c = Constant::default();
        assert_eq!(c.get_value(), &ConstantValueWithFloat::default()); // String("")
        assert_eq!(c.eventflux_element.query_context_start_index, None);
    }

    #[test]
    fn test_time_util_sec() {
        let c = TimeUtil::sec(5); // 5 seconds
        assert_eq!(c.get_value(), &ConstantValueWithFloat::Time(5000)); // 5 * 1000
    }

    #[test]
    fn test_time_util_minute() {
        let c = TimeUtil::minute(2); // 2 minutes
        assert_eq!(c.get_value(), &ConstantValueWithFloat::Time(2 * 60 * 1000));
    }

    #[test]
    fn test_time_util_hour() {
        let c = TimeUtil::hour(1); // 1 hour
        assert_eq!(c.get_value(), &ConstantValueWithFloat::Time(60 * 60 * 1000));
    }

    #[test]
    fn test_time_util_day() {
        let c = TimeUtil::day(1); // 1 day
        assert_eq!(
            c.get_value(),
            &ConstantValueWithFloat::Time(24 * 60 * 60 * 1000)
        );
    }

    #[test]
    fn test_time_util_week() {
        let c = TimeUtil::week(1); // 1 week
        assert_eq!(
            c.get_value(),
            &ConstantValueWithFloat::Time(7 * 24 * 60 * 60 * 1000)
        );
    }

    #[test]
    fn test_time_util_month() {
        let c = TimeUtil::month(1); // 1 month
        assert_eq!(c.get_value(), &ConstantValueWithFloat::Time(2_630_000_000));
    }

    #[test]
    fn test_time_util_year() {
        let c = TimeUtil::year(1); // 1 year
        assert_eq!(c.get_value(), &ConstantValueWithFloat::Time(31_556_900_000));
    }
}
