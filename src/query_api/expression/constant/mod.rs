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

// Individual constant files (bool_constant.rs, etc.) are not needed with this unified approach.
// Removing their module declarations.
// pub mod bool_constant;
// pub mod double_constant;
// pub mod float_constant;
// pub mod int_constant;
// pub mod long_constant;
// pub mod string_constant;
// pub mod time_constant;

#[derive(Clone, Debug, PartialEq)]
pub enum ConstantValue {
    String(String),
    Int(i32),
    Long(i64),
    Float(f32), // Note: f32/f64 do not derive Eq/Hash directly. Wrappers or custom impls needed if ConstantValue needs Eq/Hash and contains floats.
    // For now, removing Eq/Hash from ConstantValue if floats are to be kept directly.
    // Or, use a wrapper struct for f32/f64 that implements Eq/Hash (e.g. based on bit patterns).
    // Let's assume for now that direct comparison of f32/f64 is not needed for Hash/Eq at this level.
    // For PartialEq, f32/f64 is fine. If Constant itself needs to be in a HashMap key, this will be an issue.
    Double(f64),
    Bool(bool),
    Time(i64), // TimeConstant in Java is a subclass of LongConstant
}

// Make ConstantValue non-Eq and non-Hash for now due to floats.
// Or use a crate like `ordered-float` if these need to be hashable/sortable.
// For now, removing Eq, Hash from ConstantValue and Constant struct if it contains floats.
// Let's re-evaluate. The prompt asks for PartialEq. Default can be added.
// If Constant is used in HashMap keys later, this needs revisit.

// Re-adding Eq, Hash for ConstantValue assuming we handle floats appropriately when used in HashMaps (e.g. by not using float constants as keys).
// For derive purposes, PartialEq is fine. For Eq/Hash, direct f32/f64 is problematic.
// Let's remove Eq, Hash from ConstantValue and Constant for safety if they contain f32/f64.
// The prompt asks for PartialEq, so that's fine.

// Simpler approach: Keep PartialEq, add Default. Eq/Hash will be omitted for now for ConstantValue and Constant.
#[derive(Clone, Debug, PartialEq)]
pub enum ConstantValueWithFloat {
    // Renaming to clarify it has floats
    String(String),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bool(bool),
    Time(i64),
    Null, // NULL constant for CASE expressions
}

impl Default for ConstantValueWithFloat {
    fn default() -> Self {
        ConstantValueWithFloat::String(String::new())
    } // Default to empty string or Int(0)
}

#[derive(Clone, Debug, PartialEq, Default)] // Added Default
pub struct Constant {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement
    pub value: ConstantValueWithFloat,       // Using the renamed enum that includes floats
}

impl Constant {
    pub fn new(value: ConstantValueWithFloat) -> Self {
        Constant {
            eventflux_element: EventFluxElement::default(),
            value,
        }
    }

    // Getter method
    pub fn get_value(&self) -> &ConstantValueWithFloat {
        &self.value
    }
}

// Helper constructors similar to Expression.value() static methods in Java
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

    pub fn time(value: i64) -> Self {
        // Specifically for TimeConstant values which are long in Java
        Constant::new(ConstantValueWithFloat::Time(value))
    }

    pub fn null() -> Self {
        // NULL constant for CASE ELSE when missing in SQL
        Constant::new(ConstantValueWithFloat::Null)
    }
}

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
    // Using direct values from Java for month/year for precision if needed, or keep simplified.
    // Java uses 30 * 24 * 60 * 60 * 1000 for month, 365 * 24 * 60 * 60 * 1000 for year in Expression.Time
    // But TimeConstant itself uses 2630000000L for month and 31556900000L for year.
    // Let's use the more precise ones from TimeConstant.java if this TimeUtil is for constructing those.
    pub fn month_val(val: i64) -> i64 {
        val * 2_630_000_000
    }
    pub fn year_val(val: i64) -> i64 {
        val * 31_556_900_000
    }

    // Constructors for direct Time Constant creation
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
        // Direct comparison of floats can be tricky. PartialEq should handle it.
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
        let c = Constant::time(5000); // Represents a time duration in milliseconds
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
        assert_eq!(
            c.get_value(),
            &ConstantValueWithFloat::Time(1 * 60 * 60 * 1000)
        );
    }

    #[test]
    fn test_time_util_day() {
        let c = TimeUtil::day(1); // 1 day
        assert_eq!(
            c.get_value(),
            &ConstantValueWithFloat::Time(1 * 24 * 60 * 60 * 1000)
        );
    }

    #[test]
    fn test_time_util_week() {
        let c = TimeUtil::week(1); // 1 week
        assert_eq!(
            c.get_value(),
            &ConstantValueWithFloat::Time(1 * 7 * 24 * 60 * 60 * 1000)
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
