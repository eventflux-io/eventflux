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

//! Collection Aggregation Executors for Pattern Event Collections
//!
//! These executors compute aggregate values over the event collections that count
//! quantifiers build in pattern queries. Window aggregators update a running value as
//! events are added and removed. Collection aggregators instead compute the whole
//! result in one go over a complete, bounded event chain.
//!
//! # Key Differences from Window Aggregators
//!
//! | Aspect | Window Aggregators | Collection Aggregators |
//! |--------|-------------------|------------------------|
//! | Input | Streaming events | Complete event chain |
//! | State | Incremental (add/remove) | Stateless batch |
//! | Trait | `AttributeAggregatorExecutor` | `ExpressionExecutor` |
//! | Use case | Time/length windows | Pattern count quantifiers |
//!
//! # Example Usage
//!
//! ```sql
//! -- Pattern with count quantifier
//! FROM PATTERN (e1=FailedLogin{3,5} -> e2=AccountLocked)
//! SELECT count(e1) as attempts, avg(e1.responseTime) as avgTime;
//! ```
//!
//! # Architecture
//!
//! Collection aggregators need a `StateEvent` from pattern matching. Any other event,
//! or no event, gives `None`.
//! 1. Pattern queries build StateEvents that hold stream events at several positions
//! 2. Count quantifiers build a chain of events at each position (for example e1{3,5})
//! 3. Collection aggregators compute over these bounded, complete chains
//!
//! The executors read all events with `StateEvent::get_event_chain(position)` and
//! compute the aggregate from them. `count` uses `StateEvent::count_events_at` instead.

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::state::state_event::StateEvent;
use crate::core::event::stream::stream_event::StreamEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::core::util::eventflux_constants::{
    BEFORE_WINDOW_DATA_INDEX, ON_AFTER_WINDOW_DATA_INDEX, OUTPUT_DATA_INDEX,
    STATE_OUTPUT_DATA_INDEX,
};
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::Arc;

// ============================================================================
// Helper Functions
// ============================================================================

#[derive(Debug, Clone, Copy)]
enum NumericValue {
    Int(i128),
    Float(f64),
}

#[inline]
fn numeric_from_attribute(value: &AttributeValue) -> Option<NumericValue> {
    match value {
        AttributeValue::Int(i) => Some(NumericValue::Int(*i as i128)),
        AttributeValue::Long(l) => Some(NumericValue::Int(*l as i128)),
        AttributeValue::Float(f) => Some(NumericValue::Float(*f as f64)),
        AttributeValue::Double(d) => Some(NumericValue::Float(*d)),
        _ => None,
    }
}

/// Converts an INT, LONG, FLOAT or DOUBLE value to f64. Returns `None` for any other
/// type, NULL included.
#[inline]
fn value_as_f64(v: &AttributeValue) -> Option<f64> {
    match v {
        AttributeValue::Int(i) => Some(*i as f64),
        AttributeValue::Long(l) => Some(*l as f64),
        AttributeValue::Float(f) => Some(*f as f64),
        AttributeValue::Double(d) => Some(*d),
        _ => None,
    }
}

/// Reads one attribute value from a StreamEvent.
///
/// Position format is [data_type_index, attribute_index]
/// - data_type_index picks the data array. BEFORE_WINDOW_DATA_INDEX,
///   OUTPUT_DATA_INDEX, STATE_OUTPUT_DATA_INDEX or ON_AFTER_WINDOW_DATA_INDEX.
/// - attribute_index is the index within that array
///
/// If nothing is found there, falls back to the same index in `output_data`.
#[inline]
fn get_attribute_from_event(
    event: &StreamEvent,
    attribute_position: &[i32; 2],
) -> Option<AttributeValue> {
    let attr_idx = attribute_position[1] as usize;

    let result = match attribute_position[0] as usize {
        BEFORE_WINDOW_DATA_INDEX => event.before_window_data.get(attr_idx).cloned(),
        OUTPUT_DATA_INDEX | STATE_OUTPUT_DATA_INDEX => event
            .output_data
            .as_ref()
            .and_then(|v| v.get(attr_idx))
            .cloned(),
        ON_AFTER_WINDOW_DATA_INDEX => event.on_after_window_data.get(attr_idx).cloned(),
        _ => None,
    };

    // Not found in the given array, so try output_data
    if result.is_some() {
        result
    } else {
        event
            .output_data
            .as_ref()
            .and_then(|v| v.get(attr_idx))
            .cloned()
    }
}

// ============================================================================
// CollectionCountExecutor
// ============================================================================

/// Counts the events in a pattern collection, `count(e1)`.
///
/// Counts the events in the chain at one position. Used with count quantifiers like
/// `e1{3,5}` to get how many events actually matched. An empty or missing position
/// gives 0.
///
/// # Example
///
/// ```sql
/// FROM PATTERN (e1=FailedLogin{3,5} -> e2=AccountLocked)
/// SELECT count(e1) as failedAttempts;  -- Returns 3, 4, or 5
/// ```
///
/// # Return Type
///
/// Always returns `ApiAttributeType::LONG` (i64).
#[derive(Debug, Clone)]
pub struct CollectionCountExecutor {
    /// Position in StateEvent.stream_events[] (e1=0, e2=1, ...)
    pub chain_index: usize,
}

impl CollectionCountExecutor {
    /// Create a new CollectionCountExecutor.
    ///
    /// # Arguments
    /// * `chain_index` - Position in StateEvent (e1=0, e2=1, ...)
    pub fn new(chain_index: usize) -> Self {
        Self { chain_index }
    }

    /// Get the chain index.
    pub fn get_chain_index(&self) -> usize {
        self.chain_index
    }
}

impl ExpressionExecutor for CollectionCountExecutor {
    fn execute(&self, event_opt: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let complex_event = event_opt?;

        // Collection aggregations only work on a StateEvent from pattern matching
        let state_event = complex_event.as_any().downcast_ref::<StateEvent>()?;

        let count = state_event.count_events_at(self.chain_index);

        Some(AttributeValue::Long(count as i64))
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::LONG
    }

    fn clone_executor(
        &self,
        _eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(self.clone())
    }
}

// ============================================================================
// CollectionSumExecutor
// ============================================================================

/// Sums an attribute over a pattern collection, `sum(e1.price)`.
///
/// Sums one attribute across all events in the chain. NULL and other non numeric
/// values are skipped, as in SQL. Returns `None` when the chain is empty or has no
/// numeric value.
///
/// Integers are summed exactly in i128. Once a float value appears, or the sum
/// leaves the i64 range, the rest is summed as f64.
///
/// # Example
///
/// ```sql
/// FROM PATTERN (e1=Transaction{5} -> e2=Summary)
/// SELECT sum(e1.amount) as totalAmount;
/// ```
///
/// # Return Type
///
/// Follows `return_type`. INT and LONG give an Int or Long when the sum is an exact
/// integer in range, and a Double otherwise. FLOAT gives a Float. Anything else gives
/// a Double.
#[derive(Debug, Clone)]
pub struct CollectionSumExecutor {
    /// Position in StateEvent.stream_events[] (e1=0, e2=1, ...)
    pub chain_index: usize,

    /// Attribute position as [data_type_index, attribute_index]
    pub attribute_position: [i32; 2],

    /// Declared result type, set from the input attribute type
    pub return_type: ApiAttributeType,
}

impl CollectionSumExecutor {
    /// Create a new CollectionSumExecutor.
    ///
    /// # Arguments
    /// * `chain_index` - Position in StateEvent (e1=0, e2=1, ...)
    /// * `attribute_position` - [data_type_index, attribute_index]
    /// * `return_type` - Declared result type (see the struct docs)
    pub fn new(
        chain_index: usize,
        attribute_position: [i32; 2],
        return_type: ApiAttributeType,
    ) -> Self {
        Self {
            chain_index,
            attribute_position,
            return_type,
        }
    }

    /// Get the chain index.
    pub fn get_chain_index(&self) -> usize {
        self.chain_index
    }

    /// Get the attribute position.
    pub fn get_attribute_position(&self) -> [i32; 2] {
        self.attribute_position
    }
}

impl ExpressionExecutor for CollectionSumExecutor {
    fn execute(&self, event_opt: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let complex_event = event_opt?;
        let state_event = complex_event.as_any().downcast_ref::<StateEvent>()?;

        let chain = state_event.get_event_chain(self.chain_index);

        if chain.is_empty() {
            return None;
        }

        let mut int_sum: i128 = 0;
        let mut float_sum: f64 = 0.0;
        let mut use_float = false;
        let mut int_overflowed = false;
        let mut has_value = false;

        for stream_event in chain {
            if let Some(val) = get_attribute_from_event(stream_event, &self.attribute_position) {
                if let Some(num) = numeric_from_attribute(&val) {
                    has_value = true;
                    match num {
                        NumericValue::Int(v) if !use_float => {
                            int_sum = int_sum.saturating_add(v);
                            if int_sum > i64::MAX as i128 || int_sum < i64::MIN as i128 {
                                int_overflowed = true;
                                use_float = true;
                                float_sum = int_sum as f64;
                            }
                        }
                        NumericValue::Int(v) => {
                            float_sum += v as f64;
                        }
                        NumericValue::Float(f) => {
                            if !use_float {
                                float_sum = int_sum as f64;
                                use_float = true;
                            }
                            float_sum += f;
                        }
                    }
                }
            }
        }

        // No numeric value found, for example all NULL
        if !has_value {
            return None;
        }

        let final_double = if use_float { float_sum } else { int_sum as f64 };

        match self.return_type {
            ApiAttributeType::INT => {
                if !use_float
                    && !int_overflowed
                    && int_sum >= i32::MIN as i128
                    && int_sum <= i32::MAX as i128
                {
                    Some(AttributeValue::Int(int_sum as i32))
                } else {
                    Some(AttributeValue::Double(final_double))
                }
            }
            ApiAttributeType::LONG => {
                if !use_float
                    && !int_overflowed
                    && int_sum >= i64::MIN as i128
                    && int_sum <= i64::MAX as i128
                {
                    Some(AttributeValue::Long(int_sum as i64))
                } else {
                    Some(AttributeValue::Double(final_double))
                }
            }
            ApiAttributeType::FLOAT => {
                let value = final_double as f32;
                Some(AttributeValue::Float(value))
            }
            _ => Some(AttributeValue::Double(final_double)),
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        self.return_type
    }

    fn clone_executor(
        &self,
        _eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(self.clone())
    }
}

// ============================================================================
// CollectionAvgExecutor
// ============================================================================

/// Averages an attribute over a pattern collection, `avg(e1.price)`.
///
/// Averages one attribute across all events in the chain. NULL and other non numeric
/// values are left out of both the sum and the count, as in SQL. Returns `None` when
/// no numeric value is found.
///
/// # Example
///
/// ```sql
/// FROM PATTERN (e1=StockTrade{10} -> e2=Summary)
/// SELECT avg(e1.price) as avgPrice;
/// ```
///
/// # Return Type
///
/// Always returns `ApiAttributeType::DOUBLE`.
#[derive(Debug, Clone)]
pub struct CollectionAvgExecutor {
    /// Position in StateEvent.stream_events[] (e1=0, e2=1, ...)
    pub chain_index: usize,

    /// Attribute position as [data_type_index, attribute_index]
    pub attribute_position: [i32; 2],
}

impl CollectionAvgExecutor {
    /// Create a new CollectionAvgExecutor.
    ///
    /// # Arguments
    /// * `chain_index` - Position in StateEvent (e1=0, e2=1, ...)
    /// * `attribute_position` - [data_type_index, attribute_index]
    pub fn new(chain_index: usize, attribute_position: [i32; 2]) -> Self {
        Self {
            chain_index,
            attribute_position,
        }
    }

    /// Get the chain index.
    pub fn get_chain_index(&self) -> usize {
        self.chain_index
    }

    /// Get the attribute position.
    pub fn get_attribute_position(&self) -> [i32; 2] {
        self.attribute_position
    }
}

impl ExpressionExecutor for CollectionAvgExecutor {
    fn execute(&self, event_opt: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let complex_event = event_opt?;
        let state_event = complex_event.as_any().downcast_ref::<StateEvent>()?;

        let chain = state_event.get_event_chain(self.chain_index);

        if chain.is_empty() {
            return None;
        }

        let mut sum = 0.0f64;
        let mut count = 0usize;

        for stream_event in chain {
            if let Some(val) = get_attribute_from_event(stream_event, &self.attribute_position) {
                if let Some(num) = value_as_f64(&val) {
                    sum += num;
                    count += 1;
                }
            }
        }

        // No numeric value, so avoid dividing by zero
        if count == 0 {
            return None;
        }

        Some(AttributeValue::Double(sum / count as f64))
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(
        &self,
        _eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(self.clone())
    }
}

// ============================================================================
// CollectionMinMaxExecutor
// ============================================================================

/// Whether to find the minimum or the maximum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MinMaxType {
    Min,
    Max,
}

/// Finds the min or max of an attribute over a pattern collection, `min(e1.price)` or
/// `max(e1.price)`.
///
/// NULL and other non numeric values are ignored, as in SQL. Returns `None` when no
/// numeric value is found. Integers are compared exactly until a float value appears,
/// then all comparisons use f64.
///
/// # Example
///
/// ```sql
/// FROM PATTERN (e1=Temperature{10} -> e2=Alert)
/// SELECT min(e1.temp) as minTemp, max(e1.temp) as maxTemp;
/// ```
///
/// # Return Type
///
/// Follows `return_type` (INT, LONG, FLOAT, or DOUBLE). An integer result that does
/// not fit INT or LONG comes back as a Double. A float result is cast to the return
/// type, so it truncates for INT and LONG.
#[derive(Debug, Clone)]
pub struct CollectionMinMaxExecutor {
    /// Position in StateEvent.stream_events[] (e1=0, e2=1, ...)
    pub chain_index: usize,

    /// Attribute position as [data_type_index, attribute_index]
    pub attribute_position: [i32; 2],

    pub min_max_type: MinMaxType,

    /// Result type, taken from the input attribute type
    pub return_type: ApiAttributeType,
}

impl CollectionMinMaxExecutor {
    /// Create a new CollectionMinMaxExecutor for finding minimum.
    ///
    /// # Arguments
    /// * `chain_index` - Position in StateEvent (e1=0, e2=1, ...)
    /// * `attribute_position` - [data_type_index, attribute_index]
    /// * `return_type` - Type of the attribute
    pub fn new_min(
        chain_index: usize,
        attribute_position: [i32; 2],
        return_type: ApiAttributeType,
    ) -> Self {
        Self {
            chain_index,
            attribute_position,
            min_max_type: MinMaxType::Min,
            return_type,
        }
    }

    /// Create a new CollectionMinMaxExecutor for finding maximum.
    ///
    /// # Arguments
    /// * `chain_index` - Position in StateEvent (e1=0, e2=1, ...)
    /// * `attribute_position` - [data_type_index, attribute_index]
    /// * `return_type` - Type of the attribute
    pub fn new_max(
        chain_index: usize,
        attribute_position: [i32; 2],
        return_type: ApiAttributeType,
    ) -> Self {
        Self {
            chain_index,
            attribute_position,
            min_max_type: MinMaxType::Max,
            return_type,
        }
    }

    /// Create a new CollectionMinMaxExecutor with the given min or max type.
    ///
    /// # Arguments
    /// * `chain_index` - Position in StateEvent (e1=0, e2=1, ...)
    /// * `attribute_position` - [data_type_index, attribute_index]
    /// * `min_max_type` - Whether to find min or max
    /// * `return_type` - Type of the attribute
    pub fn new(
        chain_index: usize,
        attribute_position: [i32; 2],
        min_max_type: MinMaxType,
        return_type: ApiAttributeType,
    ) -> Self {
        Self {
            chain_index,
            attribute_position,
            min_max_type,
            return_type,
        }
    }

    /// Get the chain index.
    pub fn get_chain_index(&self) -> usize {
        self.chain_index
    }

    /// Get the attribute position.
    pub fn get_attribute_position(&self) -> [i32; 2] {
        self.attribute_position
    }

    /// Check if this is a min operation.
    pub fn is_min(&self) -> bool {
        self.min_max_type == MinMaxType::Min
    }

    /// Check if this is a max operation.
    pub fn is_max(&self) -> bool {
        self.min_max_type == MinMaxType::Max
    }

    /// Casts an f64 result to the return type. INT and LONG truncate.
    #[inline]
    fn to_result_value(&self, value: f64) -> AttributeValue {
        match self.return_type {
            ApiAttributeType::INT => AttributeValue::Int(value as i32),
            ApiAttributeType::LONG => AttributeValue::Long(value as i64),
            ApiAttributeType::FLOAT => AttributeValue::Float(value as f32),
            _ => AttributeValue::Double(value),
        }
    }
}

impl ExpressionExecutor for CollectionMinMaxExecutor {
    fn execute(&self, event_opt: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let complex_event = event_opt?;
        let state_event = complex_event.as_any().downcast_ref::<StateEvent>()?;

        let chain = state_event.get_event_chain(self.chain_index);

        if chain.is_empty() {
            return None;
        }

        let mut int_result: Option<i128> = None;
        let mut float_result: Option<f64> = None;
        let mut use_float = false;

        for stream_event in chain {
            if let Some(val) = get_attribute_from_event(stream_event, &self.attribute_position) {
                if let Some(num) = numeric_from_attribute(&val) {
                    match (num, use_float) {
                        (NumericValue::Int(v), false) => {
                            int_result = Some(match int_result {
                                None => v,
                                Some(current) => match self.min_max_type {
                                    MinMaxType::Min => current.min(v),
                                    MinMaxType::Max => current.max(v),
                                },
                            });
                        }
                        (NumericValue::Int(v), true) => {
                            float_result = Some(match float_result {
                                None => v as f64,
                                Some(current) => match self.min_max_type {
                                    MinMaxType::Min => current.min(v as f64),
                                    MinMaxType::Max => current.max(v as f64),
                                },
                            });
                        }
                        (NumericValue::Float(f), false) => {
                            // Switch to float comparisons and carry over any int result so far
                            let starting = int_result.map(|v| v as f64);
                            use_float = true;
                            float_result = Some(match starting {
                                None => f,
                                Some(current) => match self.min_max_type {
                                    MinMaxType::Min => current.min(f),
                                    MinMaxType::Max => current.max(f),
                                },
                            });
                            int_result = None;
                        }
                        (NumericValue::Float(f), true) => {
                            float_result = Some(match float_result {
                                None => f,
                                Some(current) => match self.min_max_type {
                                    MinMaxType::Min => current.min(f),
                                    MinMaxType::Max => current.max(f),
                                },
                            });
                        }
                    }
                }
            }
        }

        if use_float {
            float_result.map(|v| self.to_result_value(v))
        } else {
            int_result.map(|v| match self.return_type {
                ApiAttributeType::INT => {
                    if v >= i32::MIN as i128 && v <= i32::MAX as i128 {
                        AttributeValue::Int(v as i32)
                    } else {
                        AttributeValue::Double(v as f64)
                    }
                }
                ApiAttributeType::LONG => {
                    if v >= i64::MIN as i128 && v <= i64::MAX as i128 {
                        AttributeValue::Long(v as i64)
                    } else {
                        AttributeValue::Double(v as f64)
                    }
                }
                ApiAttributeType::FLOAT => AttributeValue::Float(v as f32),
                _ => AttributeValue::Double(v as f64),
            })
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        self.return_type
    }

    fn clone_executor(
        &self,
        _eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(self.clone())
    }
}

// ============================================================================
// CollectionStdDevExecutor (Standard Deviation)
// ============================================================================

/// Computes the standard deviation of an attribute over a pattern collection,
/// `stdDev(e1.price)`.
///
/// Uses the population standard deviation (divides by n, not n - 1). NULL and other
/// non numeric values are left out, as in SQL. Returns `None` when no numeric value
/// is found.
///
/// # Formula
///
/// stdDev = sqrt(sum((x - mean)^2) / n)
///
/// # Example
///
/// ```sql
/// FROM PATTERN (e1=Measurement{20} -> e2=Report)
/// SELECT stdDev(e1.value) as valueStdDev;
/// ```
///
/// # Return Type
///
/// Always returns `ApiAttributeType::DOUBLE`.
#[derive(Debug, Clone)]
pub struct CollectionStdDevExecutor {
    /// Position in StateEvent.stream_events[] (e1=0, e2=1, ...)
    pub chain_index: usize,

    /// Attribute position as [data_type_index, attribute_index]
    pub attribute_position: [i32; 2],
}

impl CollectionStdDevExecutor {
    /// Create a new CollectionStdDevExecutor.
    ///
    /// # Arguments
    /// * `chain_index` - Position in StateEvent (e1=0, e2=1, ...)
    /// * `attribute_position` - [data_type_index, attribute_index]
    pub fn new(chain_index: usize, attribute_position: [i32; 2]) -> Self {
        Self {
            chain_index,
            attribute_position,
        }
    }

    /// Get the chain index.
    pub fn get_chain_index(&self) -> usize {
        self.chain_index
    }

    /// Get the attribute position.
    pub fn get_attribute_position(&self) -> [i32; 2] {
        self.attribute_position
    }
}

impl ExpressionExecutor for CollectionStdDevExecutor {
    fn execute(&self, event_opt: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let complex_event = event_opt?;
        let state_event = complex_event.as_any().downcast_ref::<StateEvent>()?;

        let chain = state_event.get_event_chain(self.chain_index);

        if chain.is_empty() {
            return None;
        }

        // Collect the values first, since the mean is needed before the squared differences
        let values: Vec<f64> = chain
            .iter()
            .filter_map(|stream_event| {
                get_attribute_from_event(stream_event, &self.attribute_position)
                    .and_then(|val| value_as_f64(&val))
            })
            .collect();

        if values.is_empty() {
            return None;
        }

        let n = values.len() as f64;

        let mean: f64 = values.iter().sum::<f64>() / n;

        let sum_sq_diff: f64 = values.iter().map(|x| (x - mean).powi(2)).sum();

        let std_dev = (sum_sq_diff / n).sqrt();

        Some(AttributeValue::Double(std_dev))
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(
        &self,
        _eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(self.clone())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::state::state_event::StateEvent;
    use crate::core::event::stream::stream_event::StreamEvent;
    use crate::core::event::value::AttributeValue;

    // ========================================================================
    // Test Helpers
    // ========================================================================

    /// Create a StreamEvent with before_window_data.
    fn create_stream_event_with_data(data: Vec<AttributeValue>) -> StreamEvent {
        let mut event = StreamEvent::new(0, data.len(), 0, 0);
        event.before_window_data = data;
        event
    }

    /// Create a StateEvent with events at a specific position.
    fn create_state_event_with_events(position: usize, events: Vec<StreamEvent>) -> StateEvent {
        let mut state_event = StateEvent::new(position + 1, 0);
        for event in events {
            state_event.add_event(position, event);
        }
        state_event
    }

    /// Create events with numeric values for testing aggregations.
    fn create_numeric_events(values: &[f64]) -> Vec<StreamEvent> {
        values
            .iter()
            .map(|v| create_stream_event_with_data(vec![AttributeValue::Double(*v)]))
            .collect()
    }

    /// Create events with integer values for testing aggregations.
    fn create_int_events(values: &[i32]) -> Vec<StreamEvent> {
        values
            .iter()
            .map(|v| create_stream_event_with_data(vec![AttributeValue::Int(*v)]))
            .collect()
    }

    /// Create events with long values for testing aggregations.
    fn create_long_events(values: &[i64]) -> Vec<StreamEvent> {
        values
            .iter()
            .map(|v| create_stream_event_with_data(vec![AttributeValue::Long(*v)]))
            .collect()
    }

    // ========================================================================
    // CollectionCountExecutor Tests
    // ========================================================================

    #[test]
    fn test_collection_count_exact() {
        let executor = CollectionCountExecutor::new(0);

        let events = create_numeric_events(&[1.0, 2.0, 3.0]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Long(3)));
    }

    #[test]
    fn test_collection_count_single() {
        let executor = CollectionCountExecutor::new(0);

        let events = create_numeric_events(&[42.0]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Long(1)));
    }

    #[test]
    fn test_collection_count_empty() {
        let executor = CollectionCountExecutor::new(0);

        let state_event = StateEvent::new(1, 0);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Long(0)));
    }

    #[test]
    fn test_collection_count_many() {
        let executor = CollectionCountExecutor::new(0);

        let events = create_numeric_events(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Long(10)));
    }

    #[test]
    fn test_collection_count_multiple_positions() {
        let executor_e1 = CollectionCountExecutor::new(0);
        let executor_e2 = CollectionCountExecutor::new(1);

        let mut state_event = StateEvent::new(2, 0);

        // Add 3 events to position 0
        for v in &[1.0, 2.0, 3.0] {
            state_event.add_event(
                0,
                create_stream_event_with_data(vec![AttributeValue::Double(*v)]),
            );
        }

        // Add 5 events to position 1
        for v in &[10.0, 20.0, 30.0, 40.0, 50.0] {
            state_event.add_event(
                1,
                create_stream_event_with_data(vec![AttributeValue::Double(*v)]),
            );
        }

        let result_e1 = executor_e1.execute(Some(&state_event as &dyn ComplexEvent));
        let result_e2 = executor_e2.execute(Some(&state_event as &dyn ComplexEvent));

        assert_eq!(result_e1, Some(AttributeValue::Long(3)));
        assert_eq!(result_e2, Some(AttributeValue::Long(5)));
    }

    #[test]
    fn test_collection_count_no_event() {
        let executor = CollectionCountExecutor::new(0);

        let result = executor.execute(None);
        assert_eq!(result, None);
    }

    #[test]
    fn test_collection_count_non_state_event() {
        let executor = CollectionCountExecutor::new(0);

        // StreamEvent is not a StateEvent
        let stream_event = create_stream_event_with_data(vec![AttributeValue::Int(42)]);

        let result = executor.execute(Some(&stream_event as &dyn ComplexEvent));
        assert_eq!(result, None);
    }

    // ========================================================================
    // CollectionSumExecutor Tests
    // ========================================================================

    #[test]
    fn test_collection_sum_double() {
        let executor = CollectionSumExecutor::new(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        let events = create_numeric_events(&[10.5, 20.5, 30.0]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(61.0)));
    }

    #[test]
    fn test_collection_sum_int() {
        let executor = CollectionSumExecutor::new(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::LONG,
        );

        let events = create_int_events(&[10, 20, 30, 40]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Long(100)));
    }

    #[test]
    fn test_collection_sum_long() {
        let executor = CollectionSumExecutor::new(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::LONG,
        );

        let events = create_long_events(&[1000, 2000, 3000]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Long(6000)));
    }

    #[test]
    fn test_collection_sum_single_value() {
        let executor = CollectionSumExecutor::new(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        let events = create_numeric_events(&[42.5]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(42.5)));
    }

    #[test]
    fn test_collection_sum_empty() {
        let executor = CollectionSumExecutor::new(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        let state_event = StateEvent::new(1, 0);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, None);
    }

    #[test]
    fn test_collection_sum_with_nulls() {
        let executor = CollectionSumExecutor::new(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        let mut state_event = StateEvent::new(1, 0);
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Double(10.0)]),
        );
        state_event.add_event(0, create_stream_event_with_data(vec![AttributeValue::Null]));
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Double(30.0)]),
        );

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(40.0)));
    }

    #[test]
    fn test_collection_sum_all_nulls() {
        let executor = CollectionSumExecutor::new(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        let mut state_event = StateEvent::new(1, 0);
        state_event.add_event(0, create_stream_event_with_data(vec![AttributeValue::Null]));
        state_event.add_event(0, create_stream_event_with_data(vec![AttributeValue::Null]));

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, None);
    }

    #[test]
    fn test_collection_sum_large_long_precision() {
        let executor = CollectionSumExecutor::new(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::LONG,
        );

        let mut state_event = StateEvent::new(1, 0);
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Long(i64::MAX - 1)]),
        );
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Long(1)]),
        );

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Long(i64::MAX)));
    }

    #[test]
    fn test_collection_sum_overflow_returns_none() {
        let executor = CollectionSumExecutor::new(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::LONG,
        );

        let mut state_event = StateEvent::new(1, 0);
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Long(i64::MAX)]),
        );
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Long(1)]),
        );

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        match result {
            Some(AttributeValue::Double(v)) => {
                let expected = i64::MAX as f64 + 1.0;
                assert!((v - expected).abs() < 0.5);
            }
            other => panic!("expected Double overflow fallback, got {:?}", other),
        }
    }

    // ========================================================================
    // CollectionAvgExecutor Tests
    // ========================================================================

    #[test]
    fn test_collection_avg_basic() {
        let executor = CollectionAvgExecutor::new(0, [BEFORE_WINDOW_DATA_INDEX as i32, 0]);

        let events = create_numeric_events(&[10.0, 20.0, 30.0]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(20.0)));
    }

    #[test]
    fn test_collection_avg_int() {
        let executor = CollectionAvgExecutor::new(0, [BEFORE_WINDOW_DATA_INDEX as i32, 0]);

        let events = create_int_events(&[10, 20, 30, 40, 50]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(30.0)));
    }

    #[test]
    fn test_collection_avg_single() {
        let executor = CollectionAvgExecutor::new(0, [BEFORE_WINDOW_DATA_INDEX as i32, 0]);

        let events = create_numeric_events(&[42.5]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(42.5)));
    }

    #[test]
    fn test_collection_avg_empty() {
        let executor = CollectionAvgExecutor::new(0, [BEFORE_WINDOW_DATA_INDEX as i32, 0]);

        let state_event = StateEvent::new(1, 0);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, None);
    }

    #[test]
    fn test_collection_avg_with_nulls() {
        let executor = CollectionAvgExecutor::new(0, [BEFORE_WINDOW_DATA_INDEX as i32, 0]);

        let mut state_event = StateEvent::new(1, 0);
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Double(10.0)]),
        );
        state_event.add_event(0, create_stream_event_with_data(vec![AttributeValue::Null]));
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Double(20.0)]),
        );

        // Average of 10 and 20 = 15 (null excluded from both sum and count)
        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(15.0)));
    }

    #[test]
    fn test_collection_avg_all_nulls() {
        let executor = CollectionAvgExecutor::new(0, [BEFORE_WINDOW_DATA_INDEX as i32, 0]);

        let mut state_event = StateEvent::new(1, 0);
        state_event.add_event(0, create_stream_event_with_data(vec![AttributeValue::Null]));

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, None);
    }

    #[test]
    fn test_collection_avg_precision() {
        let executor = CollectionAvgExecutor::new(0, [BEFORE_WINDOW_DATA_INDEX as i32, 0]);

        // 1 + 2 = 3, 3 / 2 = 1.5
        let events = create_numeric_events(&[1.0, 2.0]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(1.5)));
    }

    // ========================================================================
    // CollectionMinMaxExecutor Tests
    // ========================================================================

    #[test]
    fn test_collection_min_basic() {
        let executor = CollectionMinMaxExecutor::new_min(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        let events = create_numeric_events(&[30.0, 10.0, 50.0, 20.0, 40.0]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(10.0)));
    }

    #[test]
    fn test_collection_max_basic() {
        let executor = CollectionMinMaxExecutor::new_max(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        let events = create_numeric_events(&[30.0, 10.0, 50.0, 20.0, 40.0]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(50.0)));
    }

    #[test]
    fn test_collection_min_int() {
        let executor = CollectionMinMaxExecutor::new_min(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::INT,
        );

        let events = create_int_events(&[30, 10, 50, 20, 40]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Int(10)));
    }

    #[test]
    fn test_collection_max_long() {
        let executor = CollectionMinMaxExecutor::new_max(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::LONG,
        );

        let events = create_long_events(&[3000, 1000, 5000, 2000, 4000]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Long(5000)));
    }

    #[test]
    fn test_collection_min_single() {
        let executor = CollectionMinMaxExecutor::new_min(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        let events = create_numeric_events(&[42.5]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(42.5)));
    }

    #[test]
    fn test_collection_max_single() {
        let executor = CollectionMinMaxExecutor::new_max(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        let events = create_numeric_events(&[42.5]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(42.5)));
    }

    #[test]
    fn test_collection_min_empty() {
        let executor = CollectionMinMaxExecutor::new_min(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        let state_event = StateEvent::new(1, 0);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, None);
    }

    #[test]
    fn test_collection_max_empty() {
        let executor = CollectionMinMaxExecutor::new_max(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        let state_event = StateEvent::new(1, 0);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, None);
    }

    #[test]
    fn test_collection_min_with_nulls() {
        let executor = CollectionMinMaxExecutor::new_min(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        let mut state_event = StateEvent::new(1, 0);
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Double(30.0)]),
        );
        state_event.add_event(0, create_stream_event_with_data(vec![AttributeValue::Null]));
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Double(10.0)]),
        );

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(10.0)));
    }

    #[test]
    fn test_collection_max_with_nulls() {
        let executor = CollectionMinMaxExecutor::new_max(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        let mut state_event = StateEvent::new(1, 0);
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Double(30.0)]),
        );
        state_event.add_event(0, create_stream_event_with_data(vec![AttributeValue::Null]));
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Double(10.0)]),
        );

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(30.0)));
    }

    #[test]
    fn test_collection_min_all_nulls() {
        let executor = CollectionMinMaxExecutor::new_min(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        let mut state_event = StateEvent::new(1, 0);
        state_event.add_event(0, create_stream_event_with_data(vec![AttributeValue::Null]));
        state_event.add_event(0, create_stream_event_with_data(vec![AttributeValue::Null]));

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, None);
    }

    #[test]
    fn test_collection_min_all_same() {
        let executor = CollectionMinMaxExecutor::new_min(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        let events = create_numeric_events(&[42.0, 42.0, 42.0]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(42.0)));
    }

    #[test]
    fn test_collection_max_all_same() {
        let executor = CollectionMinMaxExecutor::new_max(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        let events = create_numeric_events(&[42.0, 42.0, 42.0]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(42.0)));
    }

    #[test]
    fn test_collection_min_negative_values() {
        let executor = CollectionMinMaxExecutor::new_min(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        let events = create_numeric_events(&[-10.0, -50.0, -30.0]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(-50.0)));
    }

    #[test]
    fn test_collection_max_negative_values() {
        let executor = CollectionMinMaxExecutor::new_max(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        let events = create_numeric_events(&[-10.0, -50.0, -30.0]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(-10.0)));
    }

    #[test]
    fn test_collection_minmax_large_long_precision() {
        let min_exec = CollectionMinMaxExecutor::new_min(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::LONG,
        );
        let max_exec = CollectionMinMaxExecutor::new_max(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::LONG,
        );

        let mut state_event = StateEvent::new(1, 0);
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Long(i64::MAX - 1)]),
        );
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Long(i64::MIN + 2)]),
        );

        let min_result = min_exec.execute(Some(&state_event as &dyn ComplexEvent));
        let max_result = max_exec.execute(Some(&state_event as &dyn ComplexEvent));

        assert_eq!(min_result, Some(AttributeValue::Long(i64::MIN + 2)));
        assert_eq!(max_result, Some(AttributeValue::Long(i64::MAX - 1)));
    }

    #[test]
    fn test_collection_minmax_is_min_is_max() {
        let min_exec = CollectionMinMaxExecutor::new_min(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );
        let max_exec = CollectionMinMaxExecutor::new_max(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        assert!(min_exec.is_min());
        assert!(!min_exec.is_max());
        assert!(!max_exec.is_min());
        assert!(max_exec.is_max());
    }

    // ========================================================================
    // CollectionStdDevExecutor Tests
    // ========================================================================

    #[test]
    fn test_collection_stddev_basic() {
        let executor = CollectionStdDevExecutor::new(0, [BEFORE_WINDOW_DATA_INDEX as i32, 0]);

        // Values: 2, 4, 4, 4, 5, 5, 7, 9
        // Mean = 5, StdDev = 2.0
        let events = create_numeric_events(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        if let Some(AttributeValue::Double(std_dev)) = result {
            assert!((std_dev - 2.0).abs() < 0.0001);
        } else {
            panic!("Expected Double result");
        }
    }

    #[test]
    fn test_collection_stddev_zero() {
        let executor = CollectionStdDevExecutor::new(0, [BEFORE_WINDOW_DATA_INDEX as i32, 0]);

        // All values the same, so stddev is 0
        let events = create_numeric_events(&[5.0, 5.0, 5.0, 5.0]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(0.0)));
    }

    #[test]
    fn test_collection_stddev_single() {
        let executor = CollectionStdDevExecutor::new(0, [BEFORE_WINDOW_DATA_INDEX as i32, 0]);

        let events = create_numeric_events(&[42.0]);
        let state_event = create_state_event_with_events(0, events);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(0.0)));
    }

    #[test]
    fn test_collection_stddev_empty() {
        let executor = CollectionStdDevExecutor::new(0, [BEFORE_WINDOW_DATA_INDEX as i32, 0]);

        let state_event = StateEvent::new(1, 0);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, None);
    }

    #[test]
    fn test_collection_stddev_with_nulls() {
        let executor = CollectionStdDevExecutor::new(0, [BEFORE_WINDOW_DATA_INDEX as i32, 0]);

        let mut state_event = StateEvent::new(1, 0);
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Double(10.0)]),
        );
        state_event.add_event(0, create_stream_event_with_data(vec![AttributeValue::Null]));
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Double(20.0)]),
        );

        // StdDev of [10, 20]: mean = 15, sum_sq_diff = 50, stddev = sqrt(25) = 5
        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(5.0)));
    }

    // ========================================================================
    // Clone Executor Tests
    // ========================================================================

    #[test]
    fn test_clone_executors() {
        let app_ctx = Arc::new(EventFluxAppContext::default_for_testing());

        // Test CollectionCountExecutor clone
        let count_exec = CollectionCountExecutor::new(0);
        let cloned_count = count_exec.clone_executor(&app_ctx);
        assert_eq!(cloned_count.get_return_type(), ApiAttributeType::LONG);

        // Test CollectionSumExecutor clone
        let sum_exec = CollectionSumExecutor::new(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );
        let cloned_sum = sum_exec.clone_executor(&app_ctx);
        assert_eq!(cloned_sum.get_return_type(), ApiAttributeType::DOUBLE);

        // Test CollectionAvgExecutor clone
        let avg_exec = CollectionAvgExecutor::new(0, [BEFORE_WINDOW_DATA_INDEX as i32, 0]);
        let cloned_avg = avg_exec.clone_executor(&app_ctx);
        assert_eq!(cloned_avg.get_return_type(), ApiAttributeType::DOUBLE);

        // Test CollectionMinMaxExecutor clone
        let min_exec = CollectionMinMaxExecutor::new_min(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );
        let cloned_min = min_exec.clone_executor(&app_ctx);
        assert_eq!(cloned_min.get_return_type(), ApiAttributeType::DOUBLE);

        // Test CollectionStdDevExecutor clone
        let stddev_exec = CollectionStdDevExecutor::new(0, [BEFORE_WINDOW_DATA_INDEX as i32, 0]);
        let cloned_stddev = stddev_exec.clone_executor(&app_ctx);
        assert_eq!(cloned_stddev.get_return_type(), ApiAttributeType::DOUBLE);
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    #[test]
    fn test_collection_aggregation_second_attribute() {
        // Test accessing the second attribute (index 1)
        let executor = CollectionSumExecutor::new(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 1],
            ApiAttributeType::DOUBLE,
        );

        let mut state_event = StateEvent::new(1, 0);

        // Events with two attributes each
        let mut event1 = StreamEvent::new(0, 2, 0, 0);
        event1.before_window_data =
            vec![AttributeValue::Double(1.0), AttributeValue::Double(100.0)];

        let mut event2 = StreamEvent::new(0, 2, 0, 0);
        event2.before_window_data =
            vec![AttributeValue::Double(2.0), AttributeValue::Double(200.0)];

        state_event.add_event(0, event1);
        state_event.add_event(0, event2);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        // Sum of second attribute: 100 + 200 = 300
        assert_eq!(result, Some(AttributeValue::Double(300.0)));
    }

    #[test]
    fn test_collection_aggregation_out_of_bounds_position() {
        let executor = CollectionCountExecutor::new(5);

        let state_event = StateEvent::new(1, 0);

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        // Position 5 doesn't exist in a StateEvent with only 1 position
        assert_eq!(result, Some(AttributeValue::Long(0)));
    }

    #[test]
    fn test_collection_aggregation_mixed_types() {
        // Test with mixed numeric types (INT, LONG, DOUBLE)
        let executor = CollectionSumExecutor::new(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        let mut state_event = StateEvent::new(1, 0);
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Int(10)]),
        );
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Long(20)]),
        );
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Double(30.5)]),
        );

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        assert_eq!(result, Some(AttributeValue::Double(60.5)));
    }

    #[test]
    fn test_collection_aggregation_string_ignored() {
        // Non-numeric values should be ignored
        let executor = CollectionSumExecutor::new(
            0,
            [BEFORE_WINDOW_DATA_INDEX as i32, 0],
            ApiAttributeType::DOUBLE,
        );

        let mut state_event = StateEvent::new(1, 0);
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Double(10.0)]),
        );
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::String("not a number".to_string())]),
        );
        state_event.add_event(
            0,
            create_stream_event_with_data(vec![AttributeValue::Double(20.0)]),
        );

        let result = executor.execute(Some(&state_event as &dyn ComplexEvent));
        // String is ignored, sum of 10 + 20 = 30
        assert_eq!(result, Some(AttributeValue::Double(30.0)));
    }
}
