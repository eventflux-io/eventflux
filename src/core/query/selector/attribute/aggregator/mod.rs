// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use crate::core::event::complex_event::{ComplexEvent, ComplexEventType};
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::core::query::processor::ProcessingMode;
use crate::query_api::definition::attribute::Type as ApiAttributeType;

pub trait AttributeAggregatorExecutor: ExpressionExecutor {
    fn init(
        &mut self,
        executors: Vec<Box<dyn ExpressionExecutor>>,
        processing_mode: ProcessingMode,
        expired_output: bool,
        ctx: &EventFluxQueryContext,
    ) -> Result<(), String>;

    fn process_add(&self, data: Option<AttributeValue>) -> Option<AttributeValue>;
    fn process_remove(&self, data: Option<AttributeValue>) -> Option<AttributeValue>;
    fn reset(&self) -> Option<AttributeValue>;
    fn clone_box(&self) -> Box<dyn AttributeAggregatorExecutor>;
}

impl Clone for Box<dyn AttributeAggregatorExecutor> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Adapter that allows returning attribute aggregators as `Box<dyn ExpressionExecutor>`
/// on stable Rust, without relying on unstable trait upcasting coercions.
pub struct AttributeAggregatorExpressionExecutor {
    inner: Box<dyn AttributeAggregatorExecutor>,
}

impl std::fmt::Debug for AttributeAggregatorExpressionExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AttributeAggregatorExpressionExecutor")
            .finish_non_exhaustive()
    }
}

impl AttributeAggregatorExpressionExecutor {
    pub fn new(inner: Box<dyn AttributeAggregatorExecutor>) -> Self {
        Self { inner }
    }
}

impl ExpressionExecutor for AttributeAggregatorExpressionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        self.inner.execute(event)
    }

    fn get_return_type(&self) -> ApiAttributeType {
        self.inner.get_return_type()
    }

    fn clone_executor(
        &self,
        _eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(Self::new(self.inner.clone_box()))
    }

    fn is_attribute_aggregator(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod adapter_tests {
    use super::*;
    use crate::core::config::eventflux_context::EventFluxContext;
    use crate::query_api::EventFluxApp;

    #[derive(Debug, Clone)]
    struct TestAggregator;

    impl ExpressionExecutor for TestAggregator {
        fn execute(&self, _event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
            None
        }

        fn get_return_type(&self) -> ApiAttributeType {
            ApiAttributeType::INT
        }

        fn clone_executor(
            &self,
            _eventflux_app_context: &Arc<EventFluxAppContext>,
        ) -> Box<dyn ExpressionExecutor> {
            Box::new(self.clone())
        }

        fn is_attribute_aggregator(&self) -> bool {
            true
        }
    }

    impl AttributeAggregatorExecutor for TestAggregator {
        fn init(
            &mut self,
            _executors: Vec<Box<dyn ExpressionExecutor>>,
            _processing_mode: ProcessingMode,
            _expired_output: bool,
            _ctx: &EventFluxQueryContext,
        ) -> Result<(), String> {
            Ok(())
        }

        fn process_add(&self, _data: Option<AttributeValue>) -> Option<AttributeValue> {
            None
        }

        fn process_remove(&self, _data: Option<AttributeValue>) -> Option<AttributeValue> {
            None
        }

        fn reset(&self) -> Option<AttributeValue> {
            None
        }

        fn clone_box(&self) -> Box<dyn AttributeAggregatorExecutor> {
            Box::new(self.clone())
        }
    }

    #[test]
    fn adapter_clones_as_expression_executor_and_preserves_flag() {
        let ctx = Arc::new(EventFluxAppContext::new(
            Arc::new(EventFluxContext::default()),
            "test".to_string(),
            Arc::new(EventFluxApp::default()),
            "".to_string(),
        ));

        let adapter = AttributeAggregatorExpressionExecutor::new(Box::new(TestAggregator));
        assert!(adapter.is_attribute_aggregator());
        assert_eq!(adapter.get_return_type(), ApiAttributeType::INT);

        let cloned = adapter.clone_executor(&ctx);
        assert!(cloned.is_attribute_aggregator());
        assert_eq!(cloned.get_return_type(), ApiAttributeType::INT);

        let _ = format!("{cloned:?}");
    }
}

fn value_as_f64(v: &AttributeValue) -> Option<f64> {
    match v {
        AttributeValue::Int(i) => Some(*i as f64),
        AttributeValue::Long(l) => Some(*l as f64),
        AttributeValue::Float(f) => Some(*f as f64),
        AttributeValue::Double(d) => Some(*d),
        _ => None,
    }
}

#[derive(Debug, Clone, Default)]
struct SumState {
    sum: f64,
    count: u64,
}

#[derive(Debug)]
pub struct SumAttributeAggregatorExecutor {
    arg_exec: Option<Box<dyn ExpressionExecutor>>,
    return_type: ApiAttributeType,
    state: Mutex<SumState>,
    app_ctx: Option<Arc<EventFluxAppContext>>,
    state_holder: Option<SumAggregatorStateHolder>,
    // Shared state for persistence (same as used by state holder)
    shared_sum: Option<Arc<Mutex<f64>>>,
    shared_count: Option<Arc<Mutex<u64>>>,
}

impl Default for SumAttributeAggregatorExecutor {
    fn default() -> Self {
        Self {
            arg_exec: None,
            return_type: ApiAttributeType::DOUBLE,
            state: Mutex::new(SumState::default()),
            app_ctx: None,
            state_holder: None,
            shared_sum: None,
            shared_count: None,
        }
    }
}

impl AttributeAggregatorExecutor for SumAttributeAggregatorExecutor {
    fn init(
        &mut self,
        mut executors: Vec<Box<dyn ExpressionExecutor>>,
        _mode: ProcessingMode,
        _expired_output: bool,
        ctx: &EventFluxQueryContext,
    ) -> Result<(), String> {
        if executors.len() != 1 {
            return Err("sum() requires exactly one argument".to_string());
        }
        let exec = executors.remove(0);
        let rtype = match exec.get_return_type() {
            ApiAttributeType::INT | ApiAttributeType::LONG => ApiAttributeType::LONG,
            ApiAttributeType::FLOAT | ApiAttributeType::DOUBLE => ApiAttributeType::DOUBLE,
            t => return Err(format!("sum not supported for {t:?}")),
        };
        self.return_type = rtype;
        self.arg_exec = Some(exec);
        self.app_ctx = Some(Arc::clone(&ctx.eventflux_app_context));

        // Initialize state holder for persistence
        let sum_arc = Arc::new(Mutex::new(0.0f64));
        let count_arc = Arc::new(Mutex::new(0u64));
        // Use deterministic component ID with unique aggregator index for state recovery compatibility
        let aggregator_id = ctx.next_aggregator_id();
        let component_id = format!("sum_aggregator_{}", aggregator_id);

        let state_holder = SumAggregatorStateHolder::new(
            sum_arc.clone(),
            count_arc.clone(),
            component_id.clone(),
            rtype,
        );

        // Register state holder with SnapshotService for persistence
        let state_holder_arc: Arc<Mutex<dyn crate::core::persistence::StateHolder>> =
            Arc::new(Mutex::new(state_holder.clone()));
        ctx.register_state_holder(component_id, state_holder_arc);

        // Store shared state references for synchronized updates
        self.shared_sum = Some(sum_arc);
        self.shared_count = Some(count_arc);
        self.state_holder = Some(state_holder);

        Ok(())
    }

    fn process_add(&self, data: Option<AttributeValue>) -> Option<AttributeValue> {
        if let Some(v) = data.as_ref().and_then(value_as_f64) {
            let mut st = self.state.lock().unwrap();

            // Sync from shared state only after restoration (cheap atomic check)
            if let Some(ref holder) = self.state_holder {
                if holder
                    .base
                    .restored
                    .swap(false, std::sync::atomic::Ordering::AcqRel)
                {
                    if let (Some(ref shared_sum), Some(ref shared_count)) =
                        (&self.shared_sum, &self.shared_count)
                    {
                        st.sum = *shared_sum.lock().unwrap();
                        st.count = *shared_count.lock().unwrap();
                    }
                }
            }

            st.sum += v;
            st.count += 1;

            // Update shared state for persistence
            if let (Some(ref shared_sum), Some(ref shared_count)) =
                (&self.shared_sum, &self.shared_count)
            {
                *shared_sum.lock().unwrap() = st.sum;
                *shared_count.lock().unwrap() = st.count;
            }

            if let Some(ref state_holder) = self.state_holder {
                state_holder.record_value_added(v);
            }
        }
        let st = self.state.lock().unwrap();
        match self.return_type {
            ApiAttributeType::LONG => Some(AttributeValue::Long(st.sum as i64)),
            ApiAttributeType::DOUBLE => Some(AttributeValue::Double(st.sum)),
            _ => None,
        }
    }

    fn process_remove(&self, data: Option<AttributeValue>) -> Option<AttributeValue> {
        if let Some(v) = data.as_ref().and_then(value_as_f64) {
            let mut st = self.state.lock().unwrap();

            // Sync from shared state only after restoration (cheap atomic check)
            if let Some(ref holder) = self.state_holder {
                if holder
                    .base
                    .restored
                    .swap(false, std::sync::atomic::Ordering::AcqRel)
                {
                    if let (Some(ref shared_sum), Some(ref shared_count)) =
                        (&self.shared_sum, &self.shared_count)
                    {
                        st.sum = *shared_sum.lock().unwrap();
                        st.count = *shared_count.lock().unwrap();
                    }
                }
            }

            st.sum -= v;
            if st.count > 0 {
                st.count -= 1;
            }

            // Update shared state for persistence
            if let (Some(ref shared_sum), Some(ref shared_count)) =
                (&self.shared_sum, &self.shared_count)
            {
                *shared_sum.lock().unwrap() = st.sum;
                *shared_count.lock().unwrap() = st.count;
            }

            if let Some(ref state_holder) = self.state_holder {
                state_holder.record_value_removed(v);
            }
        }
        let st = self.state.lock().unwrap();
        match self.return_type {
            ApiAttributeType::LONG => Some(AttributeValue::Long(st.sum as i64)),
            ApiAttributeType::DOUBLE => Some(AttributeValue::Double(st.sum)),
            _ => None,
        }
    }

    fn reset(&self) -> Option<AttributeValue> {
        // Update internal state
        let mut st = self.state.lock().unwrap();
        let old_sum = st.sum;
        let old_count = st.count;

        st.sum = 0.0;
        st.count = 0;

        // Update shared state for persistence
        if let (Some(ref shared_sum), Some(ref shared_count)) =
            (&self.shared_sum, &self.shared_count)
        {
            *shared_sum.lock().unwrap() = 0.0;
            *shared_count.lock().unwrap() = 0;
        }

        // Record state reset for incremental checkpointing
        if let Some(ref state_holder) = self.state_holder {
            state_holder.record_reset(old_sum, old_count);
        }

        None
    }

    fn clone_box(&self) -> Box<dyn AttributeAggregatorExecutor> {
        let ctx = self.app_ctx.as_ref().unwrap();

        let synchronized_state = if let (Some(shared_sum), Some(shared_count)) =
            (&self.shared_sum, &self.shared_count)
        {
            SumState {
                sum: *shared_sum.lock().unwrap(),
                count: *shared_count.lock().unwrap(),
            }
        } else {
            self.state.lock().unwrap().clone()
        };

        Box::new(SumAttributeAggregatorExecutor {
            arg_exec: self.arg_exec.as_ref().map(|e| e.clone_executor(ctx)),
            return_type: self.return_type,
            state: Mutex::new(synchronized_state),
            app_ctx: Some(Arc::clone(ctx)),
            state_holder: None,
            shared_sum: None,
            shared_count: None,
        })
    }
}

impl ExpressionExecutor for SumAttributeAggregatorExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let event = event?;
        let data = self.arg_exec.as_ref().and_then(|e| e.execute(Some(event)));
        match event.get_event_type() {
            ComplexEventType::Current => self.process_add(data),
            ComplexEventType::Expired => self.process_remove(data),
            ComplexEventType::Reset => self.reset(),
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        self.return_type
    }

    fn clone_executor(&self, _ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(AttributeAggregatorExpressionExecutor::new(self.clone_box()))
    }

    fn is_attribute_aggregator(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Default)]
struct AvgState {
    sum: f64,
    count: u64,
}

#[derive(Debug)]
pub struct AvgAttributeAggregatorExecutor {
    arg_exec: Option<Box<dyn ExpressionExecutor>>,
    state: Mutex<AvgState>,
    app_ctx: Option<Arc<EventFluxAppContext>>,
    state_holder: Option<AvgAggregatorStateHolder>,
    // Shared state for persistence (same as used by state holder)
    shared_sum: Option<Arc<Mutex<f64>>>,
    shared_count: Option<Arc<Mutex<u64>>>,
}

impl Default for AvgAttributeAggregatorExecutor {
    fn default() -> Self {
        Self {
            arg_exec: None,
            state: Mutex::new(AvgState::default()),
            app_ctx: None,
            state_holder: None,
            shared_sum: None,
            shared_count: None,
        }
    }
}

impl AttributeAggregatorExecutor for AvgAttributeAggregatorExecutor {
    fn init(
        &mut self,
        mut execs: Vec<Box<dyn ExpressionExecutor>>,
        _m: ProcessingMode,
        _e: bool,
        ctx: &EventFluxQueryContext,
    ) -> Result<(), String> {
        if execs.len() != 1 {
            return Err("avg() requires one argument".to_string());
        }
        self.arg_exec = Some(execs.remove(0));
        self.app_ctx = Some(Arc::clone(&ctx.eventflux_app_context));

        // Initialize state holder for persistence
        let sum_arc = Arc::new(Mutex::new(0.0f64));
        let count_arc = Arc::new(Mutex::new(0u64));
        let aggregator_id = ctx.next_aggregator_id();
        let component_id = format!("avg_aggregator_{}", aggregator_id);

        let state_holder =
            AvgAggregatorStateHolder::new(sum_arc.clone(), count_arc.clone(), component_id.clone());

        // Register state holder with SnapshotService for persistence
        let state_holder_arc: Arc<Mutex<dyn crate::core::persistence::StateHolder>> =
            Arc::new(Mutex::new(state_holder.clone()));
        ctx.register_state_holder(component_id, state_holder_arc);

        // Store shared state references for synchronized updates
        self.shared_sum = Some(sum_arc);
        self.shared_count = Some(count_arc);
        self.state_holder = Some(state_holder);

        Ok(())
    }

    fn process_add(&self, data: Option<AttributeValue>) -> Option<AttributeValue> {
        if let Some(v) = data.as_ref().and_then(value_as_f64) {
            let mut st = self.state.lock().unwrap();

            if let Some(ref holder) = self.state_holder {
                if holder
                    .base
                    .restored
                    .swap(false, std::sync::atomic::Ordering::AcqRel)
                {
                    if let (Some(ref shared_sum), Some(ref shared_count)) =
                        (&self.shared_sum, &self.shared_count)
                    {
                        st.sum = *shared_sum.lock().unwrap();
                        st.count = *shared_count.lock().unwrap();
                    }
                }
            }

            st.sum += v;
            st.count += 1;

            if let (Some(ref shared_sum), Some(ref shared_count)) =
                (&self.shared_sum, &self.shared_count)
            {
                *shared_sum.lock().unwrap() = st.sum;
                *shared_count.lock().unwrap() = st.count;
            }

            if let Some(ref state_holder) = self.state_holder {
                state_holder.record_value_added(v);
            }
        }
        let st = self.state.lock().unwrap();
        if st.count == 0 {
            None
        } else {
            Some(AttributeValue::Double(st.sum / st.count as f64))
        }
    }

    fn process_remove(&self, data: Option<AttributeValue>) -> Option<AttributeValue> {
        if let Some(v) = data.as_ref().and_then(value_as_f64) {
            let mut st = self.state.lock().unwrap();

            if let Some(ref holder) = self.state_holder {
                if holder
                    .base
                    .restored
                    .swap(false, std::sync::atomic::Ordering::AcqRel)
                {
                    if let (Some(ref shared_sum), Some(ref shared_count)) =
                        (&self.shared_sum, &self.shared_count)
                    {
                        st.sum = *shared_sum.lock().unwrap();
                        st.count = *shared_count.lock().unwrap();
                    }
                }
            }

            st.sum -= v;
            if st.count > 0 {
                st.count -= 1;
            }

            if let (Some(ref shared_sum), Some(ref shared_count)) =
                (&self.shared_sum, &self.shared_count)
            {
                *shared_sum.lock().unwrap() = st.sum;
                *shared_count.lock().unwrap() = st.count;
            }

            if let Some(ref state_holder) = self.state_holder {
                state_holder.record_value_removed(v);
            }
        }
        let st = self.state.lock().unwrap();
        if st.count == 0 {
            None
        } else {
            Some(AttributeValue::Double(st.sum / st.count as f64))
        }
    }

    fn reset(&self) -> Option<AttributeValue> {
        let mut st = self.state.lock().unwrap();
        let old_sum = st.sum;
        let old_count = st.count;

        st.sum = 0.0;
        st.count = 0;

        // Update shared state for persistence
        if let (Some(ref shared_sum), Some(ref shared_count)) =
            (&self.shared_sum, &self.shared_count)
        {
            *shared_sum.lock().unwrap() = 0.0;
            *shared_count.lock().unwrap() = 0;
        }

        // Record state reset for incremental checkpointing
        if let Some(ref state_holder) = self.state_holder {
            state_holder.record_reset(old_sum, old_count);
        }

        None
    }

    fn clone_box(&self) -> Box<dyn AttributeAggregatorExecutor> {
        let ctx = self.app_ctx.as_ref().unwrap();

        let synchronized_state = if let (Some(shared_sum), Some(shared_count)) =
            (&self.shared_sum, &self.shared_count)
        {
            AvgState {
                sum: *shared_sum.lock().unwrap(),
                count: *shared_count.lock().unwrap(),
            }
        } else {
            self.state.lock().unwrap().clone()
        };

        Box::new(AvgAttributeAggregatorExecutor {
            arg_exec: self.arg_exec.as_ref().map(|e| e.clone_executor(ctx)),
            state: Mutex::new(synchronized_state),
            app_ctx: Some(Arc::clone(ctx)),
            state_holder: None,
            shared_sum: None,
            shared_count: None,
        })
    }
}

impl ExpressionExecutor for AvgAttributeAggregatorExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let event = event?;
        let data = self.arg_exec.as_ref().and_then(|e| e.execute(Some(event)));
        match event.get_event_type() {
            ComplexEventType::Current => self.process_add(data),
            ComplexEventType::Expired => self.process_remove(data),
            ComplexEventType::Reset => self.reset(),
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(&self, _ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(AttributeAggregatorExpressionExecutor::new(self.clone_box()))
    }

    fn is_attribute_aggregator(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Default)]
struct CountState {
    count: i64,
}

#[derive(Debug)]
pub struct CountAttributeAggregatorExecutor {
    state: Mutex<CountState>,
    app_ctx: Option<Arc<EventFluxAppContext>>,
    state_holder: Option<CountAggregatorStateHolder>,
    // Shared state for persistence (same as used by state holder)
    shared_count: Option<Arc<Mutex<i64>>>,
}

impl Default for CountAttributeAggregatorExecutor {
    fn default() -> Self {
        Self {
            state: Mutex::new(CountState::default()),
            app_ctx: None,
            state_holder: None,
            shared_count: None,
        }
    }
}

impl AttributeAggregatorExecutor for CountAttributeAggregatorExecutor {
    fn init(
        &mut self,
        _e: Vec<Box<dyn ExpressionExecutor>>,
        _m: ProcessingMode,
        _ex: bool,
        ctx: &EventFluxQueryContext,
    ) -> Result<(), String> {
        self.app_ctx = Some(Arc::clone(&ctx.eventflux_app_context));

        // Initialize state holder for persistence
        let count_arc = Arc::new(Mutex::new(0i64));
        let aggregator_id = ctx.next_aggregator_id();
        let component_id = format!("count_aggregator_{}", aggregator_id);

        let state_holder = CountAggregatorStateHolder::new(count_arc.clone(), component_id.clone());

        // Register state holder with SnapshotService for persistence
        let state_holder_arc: Arc<Mutex<dyn crate::core::persistence::StateHolder>> =
            Arc::new(Mutex::new(state_holder.clone()));
        ctx.register_state_holder(component_id, state_holder_arc);

        // Store shared state reference for synchronized updates
        self.shared_count = Some(count_arc);
        self.state_holder = Some(state_holder);

        Ok(())
    }

    fn process_add(&self, _d: Option<AttributeValue>) -> Option<AttributeValue> {
        let mut st = self.state.lock().unwrap();

        if let Some(ref holder) = self.state_holder {
            if holder
                .base
                .restored
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                if let Some(ref shared_count) = self.shared_count {
                    st.count = *shared_count.lock().unwrap();
                }
            }
        }

        st.count += 1;
        let new_count = st.count;

        if let Some(ref shared_count) = self.shared_count {
            *shared_count.lock().unwrap() = new_count;
        }

        if let Some(ref state_holder) = self.state_holder {
            state_holder.record_increment();
        }

        Some(AttributeValue::Long(new_count))
    }
    fn process_remove(&self, _d: Option<AttributeValue>) -> Option<AttributeValue> {
        let mut st = self.state.lock().unwrap();

        if let Some(ref holder) = self.state_holder {
            if holder
                .base
                .restored
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                if let Some(ref shared_count) = self.shared_count {
                    st.count = *shared_count.lock().unwrap();
                }
            }
        }

        st.count -= 1;
        let new_count = st.count;

        if let Some(ref shared_count) = self.shared_count {
            *shared_count.lock().unwrap() = new_count;
        }

        if let Some(ref state_holder) = self.state_holder {
            state_holder.record_decrement();
        }

        Some(AttributeValue::Long(new_count))
    }
    fn reset(&self) -> Option<AttributeValue> {
        let mut st = self.state.lock().unwrap();
        let old_count = st.count;
        st.count = 0;

        if let Some(ref shared_count) = self.shared_count {
            *shared_count.lock().unwrap() = 0;
        }

        // Record state reset for incremental checkpointing
        if let Some(ref state_holder) = self.state_holder {
            state_holder.record_reset(old_count);
        }

        Some(AttributeValue::Long(0))
    }
    fn clone_box(&self) -> Box<dyn AttributeAggregatorExecutor> {
        let ctx = self.app_ctx.as_ref().unwrap();

        let synchronized_state = if let Some(shared_count) = &self.shared_count {
            CountState {
                count: *shared_count.lock().unwrap(),
            }
        } else {
            self.state.lock().unwrap().clone()
        };

        Box::new(CountAttributeAggregatorExecutor {
            state: Mutex::new(synchronized_state),
            app_ctx: Some(Arc::clone(ctx)),
            state_holder: None,
            shared_count: None,
        })
    }
}

impl ExpressionExecutor for CountAttributeAggregatorExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let event = event?;
        match event.get_event_type() {
            ComplexEventType::Current => self.process_add(None),
            ComplexEventType::Expired => self.process_remove(None),
            ComplexEventType::Reset => self.reset(),
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::LONG
    }

    fn clone_executor(&self, _ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(AttributeAggregatorExpressionExecutor::new(self.clone_box()))
    }

    fn is_attribute_aggregator(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Default)]
struct DistinctCountState {
    map: HashMap<String, i64>,
}

#[derive(Debug)]
pub struct DistinctCountAttributeAggregatorExecutor {
    arg_exec: Option<Box<dyn ExpressionExecutor>>,
    state: Mutex<DistinctCountState>,
    app_ctx: Option<Arc<EventFluxAppContext>>,
    state_holder: Option<DistinctCountAggregatorStateHolder>,
    // Shared state for persistence (same as used by state holder)
    shared_map: Option<Arc<Mutex<HashMap<String, i64>>>>,
}

impl Default for DistinctCountAttributeAggregatorExecutor {
    fn default() -> Self {
        Self {
            arg_exec: None,
            state: Mutex::new(DistinctCountState::default()),
            app_ctx: None,
            state_holder: None,
            shared_map: None,
        }
    }
}

impl AttributeAggregatorExecutor for DistinctCountAttributeAggregatorExecutor {
    fn init(
        &mut self,
        mut e: Vec<Box<dyn ExpressionExecutor>>,
        _m: ProcessingMode,
        _ex: bool,
        ctx: &EventFluxQueryContext,
    ) -> Result<(), String> {
        if e.len() != 1 {
            return Err("distinctCount() requires one arg".to_string());
        }
        self.arg_exec = Some(e.remove(0));
        self.app_ctx = Some(Arc::clone(&ctx.eventflux_app_context));

        // Initialize state holder for persistence
        let map_arc = Arc::new(Mutex::new(HashMap::new()));
        let aggregator_id = ctx.next_aggregator_id();
        let component_id = format!("distinctcount_aggregator_{}", aggregator_id);

        let holder = DistinctCountAggregatorStateHolder::new(map_arc.clone(), component_id.clone());
        let state_holder_arc: Arc<Mutex<dyn crate::core::persistence::StateHolder>> =
            Arc::new(Mutex::new(holder.clone()));
        ctx.register_state_holder(component_id, state_holder_arc);

        // Store shared state reference for synchronized updates
        self.shared_map = Some(map_arc);
        self.state_holder = Some(holder);

        Ok(())
    }

    fn process_add(&self, data: Option<AttributeValue>) -> Option<AttributeValue> {
        if let Some(v) = data {
            let key = format!("{v:?}");
            let mut st = self.state.lock().unwrap();

            if let Some(ref holder) = self.state_holder {
                if holder
                    .base
                    .restored
                    .swap(false, std::sync::atomic::Ordering::AcqRel)
                {
                    if let Some(ref shared) = self.shared_map {
                        st.map = shared.lock().unwrap().clone();
                    }
                }
            }

            let old_count = st.map.get(&key).copied();
            let c = st.map.entry(key.clone()).or_insert(0);
            *c += 1;
            let new_count = *c;

            if let Some(ref shared) = self.shared_map {
                shared.lock().unwrap().insert(key.clone(), new_count);
            }

            if let Some(ref state_holder) = self.state_holder {
                state_holder.record_value_added(&key, old_count, new_count);
            }
        }
        Some(AttributeValue::Long(
            self.state.lock().unwrap().map.len() as i64
        ))
    }

    fn process_remove(&self, data: Option<AttributeValue>) -> Option<AttributeValue> {
        if let Some(v) = data {
            let key = format!("{v:?}");
            let mut st = self.state.lock().unwrap();

            if let Some(ref holder) = self.state_holder {
                if holder
                    .base
                    .restored
                    .swap(false, std::sync::atomic::Ordering::AcqRel)
                {
                    if let Some(ref shared) = self.shared_map {
                        st.map = shared.lock().unwrap().clone();
                    }
                }
            }

            if let Some(c) = st.map.get_mut(&key) {
                let old_count = *c;
                *c -= 1;
                let new_count = if *c <= 0 {
                    st.map.remove(&key);
                    None
                } else {
                    Some(*c)
                };

                // Update shared state for persistence
                if let Some(ref shared) = self.shared_map {
                    let mut shared_map = shared.lock().unwrap();
                    if let Some(nc) = new_count {
                        shared_map.insert(key.clone(), nc);
                    } else {
                        shared_map.remove(&key);
                    }
                }

                // Record state change
                if let Some(ref state_holder) = self.state_holder {
                    state_holder.record_value_removed(&key, old_count, new_count);
                }
            }
        }
        Some(AttributeValue::Long(
            self.state.lock().unwrap().map.len() as i64
        ))
    }

    fn reset(&self) -> Option<AttributeValue> {
        let old_map = {
            let mut st = self.state.lock().unwrap();
            let old_map = st.map.clone();
            st.map.clear();
            old_map
        };

        // Update shared state for persistence
        if let Some(ref shared) = self.shared_map {
            shared.lock().unwrap().clear();
        }

        // Record state change
        if let Some(ref state_holder) = self.state_holder {
            state_holder.record_reset(&old_map);
        }

        Some(AttributeValue::Long(0))
    }

    fn clone_box(&self) -> Box<dyn AttributeAggregatorExecutor> {
        let ctx = self.app_ctx.as_ref().unwrap();

        // Read from shared state for consistency
        let synchronized_state = if let Some(ref shared) = self.shared_map {
            DistinctCountState {
                map: shared.lock().unwrap().clone(),
            }
        } else {
            self.state.lock().unwrap().clone()
        };

        Box::new(DistinctCountAttributeAggregatorExecutor {
            arg_exec: self.arg_exec.as_ref().map(|e| e.clone_executor(ctx)),
            state: Mutex::new(synchronized_state),
            app_ctx: Some(Arc::clone(ctx)),
            state_holder: None,
            shared_map: None,
        })
    }
}

impl ExpressionExecutor for DistinctCountAttributeAggregatorExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let event = event?;
        let data = self.arg_exec.as_ref().and_then(|e| e.execute(Some(event)));
        match event.get_event_type() {
            ComplexEventType::Current => self.process_add(data),
            ComplexEventType::Expired => self.process_remove(data),
            ComplexEventType::Reset => self.reset(),
            _ => None,
        }
    }
    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::LONG
    }
    fn clone_executor(&self, _ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(AttributeAggregatorExpressionExecutor::new(self.clone_box()))
    }
    fn is_attribute_aggregator(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Default)]
struct MinMaxState {
    value: Option<f64>,
}

fn st_to_val(rt: ApiAttributeType, st: &MinMaxState) -> Option<AttributeValue> {
    st.value.map(|v| match rt {
        ApiAttributeType::INT => AttributeValue::Int(v as i32),
        ApiAttributeType::LONG => AttributeValue::Long(v as i64),
        ApiAttributeType::FLOAT => AttributeValue::Float(v as f32),
        _ => AttributeValue::Double(v),
    })
}

/// Comparison mode for min/max aggregators
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MinMaxMode {
    Min,
    Max,
}

impl MinMaxMode {
    /// Returns true if `v` should replace `current` based on the mode
    fn should_replace(&self, v: f64, current: f64) -> bool {
        match self {
            MinMaxMode::Min => v < current,
            MinMaxMode::Max => v > current,
        }
    }
}

/// Unified min/max aggregator executor that handles all 4 variants:
/// - min: tracks minimum, resets state on window reset
/// - max: tracks maximum, resets state on window reset
/// - minForever: tracks all-time minimum, preserves state on reset
/// - maxForever: tracks all-time maximum, preserves state on reset
#[derive(Debug)]
pub struct MinMaxAttributeAggregatorExecutor {
    arg_exec: Option<Box<dyn ExpressionExecutor>>,
    return_type: ApiAttributeType,
    state: Mutex<MinMaxState>,
    app_ctx: Option<Arc<EventFluxAppContext>>,
    mode: MinMaxMode,
    preserve_on_reset: bool, // true for "forever" variants
    min_state_holder: Option<MinAggregatorStateHolder>,
    max_state_holder: Option<MaxAggregatorStateHolder>,
    shared_value: Option<Arc<Mutex<Option<f64>>>>,
}

impl MinMaxAttributeAggregatorExecutor {
    pub fn new(mode: MinMaxMode, preserve_on_reset: bool) -> Self {
        Self {
            arg_exec: None,
            return_type: ApiAttributeType::DOUBLE,
            state: Mutex::new(MinMaxState::default()),
            app_ctx: None,
            mode,
            preserve_on_reset,
            min_state_holder: None,
            max_state_holder: None,
            shared_value: None,
        }
    }

    /// Check restoration flag from either min or max state holder.
    fn check_restored(&self) -> bool {
        if let Some(ref holder) = self.min_state_holder {
            if holder
                .base
                .restored
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                return true;
            }
        }
        if let Some(ref holder) = self.max_state_holder {
            if holder
                .base
                .restored
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                return true;
            }
        }
        false
    }

    /// Try to update the tracked value. If the value changes, sync shared state and record.
    fn try_update(&self, st: &mut MinMaxState, data: Option<AttributeValue>) {
        let old_value = st.value;
        if let Some(v) = data.and_then(|v| value_as_f64(&v)) {
            match st.value {
                Some(current) if !self.mode.should_replace(v, current) => {}
                _ => st.value = Some(v),
            }
        }
        if old_value != st.value {
            if let Some(ref shared) = self.shared_value {
                *shared.lock().unwrap() = st.value;
            }
            if let Some(ref holder) = self.min_state_holder {
                holder.record_value_updated(old_value, st.value);
            }
            if let Some(ref holder) = self.max_state_holder {
                holder.record_value_updated(old_value, st.value);
            }
        }
    }
}

impl AttributeAggregatorExecutor for MinMaxAttributeAggregatorExecutor {
    fn init(
        &mut self,
        mut e: Vec<Box<dyn ExpressionExecutor>>,
        _m: ProcessingMode,
        _ex: bool,
        ctx: &EventFluxQueryContext,
    ) -> Result<(), String> {
        if e.len() != 1 {
            return Err("aggregator requires one arg".into());
        }
        let exec = e.remove(0);
        self.return_type = exec.get_return_type();
        self.arg_exec = Some(exec);
        self.app_ctx = Some(Arc::clone(&ctx.eventflux_app_context));

        // Initialize state holder for persistence
        let value_arc = Arc::new(Mutex::new(None::<f64>));
        let aggregator_id = ctx.next_aggregator_id();
        let mode_name = match self.mode {
            MinMaxMode::Min => "min",
            MinMaxMode::Max => "max",
        };
        let forever_suffix = if self.preserve_on_reset {
            "_forever"
        } else {
            ""
        };
        let component_id = format!(
            "{}{}_aggregator_{}",
            mode_name, forever_suffix, aggregator_id
        );

        match self.mode {
            MinMaxMode::Min => {
                let holder = MinAggregatorStateHolder::new(
                    value_arc.clone(),
                    component_id.clone(),
                    self.return_type,
                );
                let state_holder_arc: Arc<Mutex<dyn crate::core::persistence::StateHolder>> =
                    Arc::new(Mutex::new(holder.clone()));
                ctx.register_state_holder(component_id, state_holder_arc);
                self.min_state_holder = Some(holder);
            }
            MinMaxMode::Max => {
                let holder = MaxAggregatorStateHolder::new(
                    value_arc.clone(),
                    component_id.clone(),
                    self.return_type,
                );
                let state_holder_arc: Arc<Mutex<dyn crate::core::persistence::StateHolder>> =
                    Arc::new(Mutex::new(holder.clone()));
                ctx.register_state_holder(component_id, state_holder_arc);
                self.max_state_holder = Some(holder);
            }
        }
        self.shared_value = Some(value_arc);

        Ok(())
    }

    fn process_add(&self, data: Option<AttributeValue>) -> Option<AttributeValue> {
        let mut st = self.state.lock().unwrap();

        if self.check_restored() {
            if let Some(ref shared) = self.shared_value {
                st.value = *shared.lock().unwrap();
            }
        }

        self.try_update(&mut st, data);
        st_to_val(self.return_type, &st)
    }

    fn process_remove(&self, _data: Option<AttributeValue>) -> Option<AttributeValue> {
        let mut st = self.state.lock().unwrap();

        if self.check_restored() {
            if let Some(ref shared) = self.shared_value {
                st.value = *shared.lock().unwrap();
            }
        }

        // For forever variants (preserve_on_reset), removal is a no-op
        // since we preserve the historical min/max.
        // For windowed variants, the window processor handles recomputation.
        st_to_val(self.return_type, &st)
    }

    fn reset(&self) -> Option<AttributeValue> {
        let mut st = self.state.lock().unwrap();
        if self.preserve_on_reset {
            st_to_val(self.return_type, &st)
        } else {
            let old_value = st.value;
            st.value = None;

            if let Some(ref shared) = self.shared_value {
                *shared.lock().unwrap() = None;
            }
            if let Some(ref holder) = self.min_state_holder {
                holder.record_reset(old_value);
            }
            if let Some(ref holder) = self.max_state_holder {
                holder.record_reset(old_value);
            }

            None
        }
    }

    fn clone_box(&self) -> Box<dyn AttributeAggregatorExecutor> {
        let ctx = self.app_ctx.as_ref().unwrap();

        // Read from shared state for consistency
        let synchronized_state = if let Some(ref shared) = self.shared_value {
            MinMaxState {
                value: *shared.lock().unwrap(),
            }
        } else {
            self.state.lock().unwrap().clone()
        };

        Box::new(MinMaxAttributeAggregatorExecutor {
            arg_exec: self.arg_exec.as_ref().map(|e| e.clone_executor(ctx)),
            return_type: self.return_type,
            state: Mutex::new(synchronized_state),
            app_ctx: Some(Arc::clone(ctx)),
            mode: self.mode,
            preserve_on_reset: self.preserve_on_reset,
            min_state_holder: None,
            max_state_holder: None,
            shared_value: None,
        })
    }
}

impl ExpressionExecutor for MinMaxAttributeAggregatorExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let event = event?;
        let data = self.arg_exec.as_ref().and_then(|e| e.execute(Some(event)));
        match event.get_event_type() {
            ComplexEventType::Current => self.process_add(data),
            ComplexEventType::Expired => self.process_remove(data),
            ComplexEventType::Reset => self.reset(),
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        self.return_type
    }

    fn clone_executor(&self, _ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(AttributeAggregatorExpressionExecutor::new(self.clone_box()))
    }

    fn is_attribute_aggregator(&self) -> bool {
        true
    }
}

// ============================================================================
// StdDev Aggregator - Standard Deviation using Welford's online algorithm
// ============================================================================

#[derive(Debug, Clone, Default)]
struct StdDevState {
    mean: f64,
    m2: f64, // Sum of squared deviations from mean (Welford's M2)
    sum: f64,
    count: u64,
}

impl StdDevState {
    /// Calculate current standard deviation from state
    fn current_stddev(&self) -> Option<AttributeValue> {
        match self.count {
            0 => None,
            1 => Some(AttributeValue::Double(0.0)),
            n => Some(AttributeValue::Double((self.m2 / n as f64).sqrt())),
        }
    }
}

#[derive(Debug)]
pub struct StdDevAttributeAggregatorExecutor {
    arg_exec: Option<Box<dyn ExpressionExecutor>>,
    state: Mutex<StdDevState>,
    app_ctx: Option<Arc<EventFluxAppContext>>,
    state_holder: Option<StdDevAggregatorStateHolder>,
    // Shared state for persistence (same as used by state holder)
    shared_mean: Option<Arc<Mutex<f64>>>,
    shared_m2: Option<Arc<Mutex<f64>>>,
    shared_sum: Option<Arc<Mutex<f64>>>,
    shared_count: Option<Arc<Mutex<u64>>>,
}

impl Default for StdDevAttributeAggregatorExecutor {
    fn default() -> Self {
        Self {
            arg_exec: None,
            state: Mutex::new(StdDevState::default()),
            app_ctx: None,
            state_holder: None,
            shared_mean: None,
            shared_m2: None,
            shared_sum: None,
            shared_count: None,
        }
    }
}

impl AttributeAggregatorExecutor for StdDevAttributeAggregatorExecutor {
    fn init(
        &mut self,
        mut e: Vec<Box<dyn ExpressionExecutor>>,
        _m: ProcessingMode,
        _ex: bool,
        ctx: &EventFluxQueryContext,
    ) -> Result<(), String> {
        if e.len() != 1 {
            return Err("stdDev aggregator requires exactly one argument".into());
        }
        self.arg_exec = Some(e.remove(0));
        self.app_ctx = Some(Arc::clone(&ctx.eventflux_app_context));

        // Initialize state holder for persistence
        let mean_arc = Arc::new(Mutex::new(0.0f64));
        let m2_arc = Arc::new(Mutex::new(0.0f64));
        let sum_arc = Arc::new(Mutex::new(0.0f64));
        let count_arc = Arc::new(Mutex::new(0u64));
        let aggregator_id = ctx.next_aggregator_id();
        let component_id = format!("stddev_aggregator_{}", aggregator_id);

        let holder = StdDevAggregatorStateHolder::new(
            mean_arc.clone(),
            m2_arc.clone(),
            sum_arc.clone(),
            count_arc.clone(),
            component_id.clone(),
        );

        let state_holder_arc: Arc<Mutex<dyn crate::core::persistence::StateHolder>> =
            Arc::new(Mutex::new(holder.clone()));
        ctx.register_state_holder(component_id, state_holder_arc);

        // Store shared state references for synchronized updates
        self.shared_mean = Some(mean_arc);
        self.shared_m2 = Some(m2_arc);
        self.shared_sum = Some(sum_arc);
        self.shared_count = Some(count_arc);
        self.state_holder = Some(holder);

        Ok(())
    }

    fn process_add(&self, data: Option<AttributeValue>) -> Option<AttributeValue> {
        let mut st = self.state.lock().unwrap();

        if let Some(ref holder) = self.state_holder {
            if holder
                .base
                .restored
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                if let (Some(ref s_mean), Some(ref s_m2), Some(ref s_sum), Some(ref s_count)) = (
                    &self.shared_mean,
                    &self.shared_m2,
                    &self.shared_sum,
                    &self.shared_count,
                ) {
                    st.mean = *s_mean.lock().unwrap();
                    st.m2 = *s_m2.lock().unwrap();
                    st.sum = *s_sum.lock().unwrap();
                    st.count = *s_count.lock().unwrap();
                }
            }
        }

        if let Some(v) = data.and_then(|v| value_as_f64(&v)) {
            st.count += 1;
            if st.count == 1 {
                st.sum = v;
                st.mean = v;
                st.m2 = 0.0;
            } else {
                let old_mean = st.mean;
                st.sum += v;
                st.mean = st.sum / st.count as f64;
                st.m2 += (v - old_mean) * (v - st.mean);
            }

            // Update shared state for persistence
            if let (Some(ref s_mean), Some(ref s_m2), Some(ref s_sum), Some(ref s_count)) = (
                &self.shared_mean,
                &self.shared_m2,
                &self.shared_sum,
                &self.shared_count,
            ) {
                *s_mean.lock().unwrap() = st.mean;
                *s_m2.lock().unwrap() = st.m2;
                *s_sum.lock().unwrap() = st.sum;
                *s_count.lock().unwrap() = st.count;
            }

            if let Some(ref holder) = self.state_holder {
                holder.record_value_added(v);
            }
        }
        st.current_stddev()
    }

    fn process_remove(&self, data: Option<AttributeValue>) -> Option<AttributeValue> {
        let mut st = self.state.lock().unwrap();

        if let Some(ref holder) = self.state_holder {
            if holder
                .base
                .restored
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                if let (Some(ref s_mean), Some(ref s_m2), Some(ref s_sum), Some(ref s_count)) = (
                    &self.shared_mean,
                    &self.shared_m2,
                    &self.shared_sum,
                    &self.shared_count,
                ) {
                    st.mean = *s_mean.lock().unwrap();
                    st.m2 = *s_m2.lock().unwrap();
                    st.sum = *s_sum.lock().unwrap();
                    st.count = *s_count.lock().unwrap();
                }
            }
        }

        if let Some(v) = data.and_then(|v| value_as_f64(&v)) {
            if st.count > 0 {
                st.count -= 1;
            }
            if st.count == 0 {
                st.sum = 0.0;
                st.mean = 0.0;
                st.m2 = 0.0;
            } else {
                let old_mean = st.mean;
                st.sum -= v;
                st.mean = st.sum / st.count as f64;
                st.m2 = (st.m2 - (v - old_mean) * (v - st.mean)).max(0.0);
            }

            if let (Some(ref s_mean), Some(ref s_m2), Some(ref s_sum), Some(ref s_count)) = (
                &self.shared_mean,
                &self.shared_m2,
                &self.shared_sum,
                &self.shared_count,
            ) {
                *s_mean.lock().unwrap() = st.mean;
                *s_m2.lock().unwrap() = st.m2;
                *s_sum.lock().unwrap() = st.sum;
                *s_count.lock().unwrap() = st.count;
            }

            if let Some(ref holder) = self.state_holder {
                holder.record_value_removed(v);
            }
        }
        st.current_stddev()
    }

    fn reset(&self) -> Option<AttributeValue> {
        let mut st = self.state.lock().unwrap();
        st.mean = 0.0;
        st.m2 = 0.0;
        st.sum = 0.0;
        st.count = 0;

        // Update shared state for persistence
        if let (Some(ref s_mean), Some(ref s_m2), Some(ref s_sum), Some(ref s_count)) = (
            &self.shared_mean,
            &self.shared_m2,
            &self.shared_sum,
            &self.shared_count,
        ) {
            *s_mean.lock().unwrap() = 0.0;
            *s_m2.lock().unwrap() = 0.0;
            *s_sum.lock().unwrap() = 0.0;
            *s_count.lock().unwrap() = 0;
        }

        if let Some(ref holder) = self.state_holder {
            holder.record_reset();
        }

        None
    }

    fn clone_box(&self) -> Box<dyn AttributeAggregatorExecutor> {
        let ctx = self.app_ctx.as_ref().unwrap();

        // Read from shared state for consistency
        let synchronized_state = if let (Some(s_mean), Some(s_m2), Some(s_sum), Some(s_count)) = (
            &self.shared_mean,
            &self.shared_m2,
            &self.shared_sum,
            &self.shared_count,
        ) {
            StdDevState {
                mean: *s_mean.lock().unwrap(),
                m2: *s_m2.lock().unwrap(),
                sum: *s_sum.lock().unwrap(),
                count: *s_count.lock().unwrap(),
            }
        } else {
            self.state.lock().unwrap().clone()
        };

        Box::new(StdDevAttributeAggregatorExecutor {
            arg_exec: self.arg_exec.as_ref().map(|e| e.clone_executor(ctx)),
            state: Mutex::new(synchronized_state),
            app_ctx: Some(Arc::clone(ctx)),
            state_holder: None,
            shared_mean: None,
            shared_m2: None,
            shared_sum: None,
            shared_count: None,
        })
    }
}

impl ExpressionExecutor for StdDevAttributeAggregatorExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let event = event?;
        let data = self.arg_exec.as_ref().and_then(|e| e.execute(Some(event)));
        match event.get_event_type() {
            ComplexEventType::Current => self.process_add(data),
            ComplexEventType::Expired => self.process_remove(data),
            ComplexEventType::Reset => self.reset(),
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(&self, _ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(AttributeAggregatorExpressionExecutor::new(self.clone_box()))
    }

    fn is_attribute_aggregator(&self) -> bool {
        true
    }
}

// ============================================================================
// First Aggregator - Returns first value in window
// ============================================================================

#[derive(Debug, Clone, Default)]
struct FirstState {
    values: VecDeque<AttributeValue>,
}

#[derive(Debug)]
pub struct FirstAttributeAggregatorExecutor {
    arg_exec: Option<Box<dyn ExpressionExecutor>>,
    return_type: ApiAttributeType,
    state: Mutex<FirstState>,
    app_ctx: Option<Arc<EventFluxAppContext>>,
    state_holder: Option<FirstAggregatorStateHolder>,
    // Shared state for persistence (same as used by state holder)
    shared_values: Option<Arc<Mutex<VecDeque<AttributeValue>>>>,
}

impl Default for FirstAttributeAggregatorExecutor {
    fn default() -> Self {
        Self {
            arg_exec: None,
            return_type: ApiAttributeType::STRING, // Will be set from input
            state: Mutex::new(FirstState::default()),
            app_ctx: None,
            state_holder: None,
            shared_values: None,
        }
    }
}

impl AttributeAggregatorExecutor for FirstAttributeAggregatorExecutor {
    fn init(
        &mut self,
        mut e: Vec<Box<dyn ExpressionExecutor>>,
        _m: ProcessingMode,
        _ex: bool,
        ctx: &EventFluxQueryContext,
    ) -> Result<(), String> {
        if e.len() != 1 {
            return Err("first aggregator requires exactly one argument".into());
        }
        let exec = e.remove(0);
        self.return_type = exec.get_return_type();
        self.arg_exec = Some(exec);
        self.app_ctx = Some(Arc::clone(&ctx.eventflux_app_context));

        // Initialize state holder for persistence
        let values_arc = Arc::new(Mutex::new(VecDeque::new()));
        let aggregator_id = ctx.next_aggregator_id();
        let component_id = format!("first_aggregator_{}", aggregator_id);

        let holder = FirstAggregatorStateHolder::new(values_arc.clone(), component_id.clone());
        let state_holder_arc: Arc<Mutex<dyn crate::core::persistence::StateHolder>> =
            Arc::new(Mutex::new(holder.clone()));
        ctx.register_state_holder(component_id, state_holder_arc);

        // Store shared state reference for synchronized updates
        self.shared_values = Some(values_arc);
        self.state_holder = Some(holder);

        Ok(())
    }

    fn process_add(&self, data: Option<AttributeValue>) -> Option<AttributeValue> {
        if let Some(v) = data {
            let mut st = self.state.lock().unwrap();

            if let Some(ref holder) = self.state_holder {
                if holder
                    .base
                    .restored
                    .swap(false, std::sync::atomic::Ordering::AcqRel)
                {
                    if let Some(ref shared) = self.shared_values {
                        st.values = shared.lock().unwrap().clone();
                    }
                }
            }

            st.values.push_back(v.clone());

            if let Some(ref shared) = self.shared_values {
                shared.lock().unwrap().push_back(v.clone());
            }

            if let Some(ref holder) = self.state_holder {
                holder.record_value_added(&v);
            }

            st.values.front().cloned()
        } else {
            let st = self.state.lock().unwrap();
            st.values.front().cloned()
        }
    }

    fn process_remove(&self, _data: Option<AttributeValue>) -> Option<AttributeValue> {
        let mut st = self.state.lock().unwrap();

        if let Some(ref holder) = self.state_holder {
            if holder
                .base
                .restored
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                if let Some(ref shared) = self.shared_values {
                    st.values = shared.lock().unwrap().clone();
                }
            }
        }

        st.values.pop_front();

        if let Some(ref shared) = self.shared_values {
            shared.lock().unwrap().pop_front();
        }

        if let Some(ref holder) = self.state_holder {
            holder.record_value_removed();
        }

        st.values.front().cloned()
    }

    fn reset(&self) -> Option<AttributeValue> {
        let mut st = self.state.lock().unwrap();
        st.values.clear();

        // Update shared state for persistence
        if let Some(ref shared) = self.shared_values {
            shared.lock().unwrap().clear();
        }

        if let Some(ref holder) = self.state_holder {
            holder.record_reset();
        }

        None
    }

    fn clone_box(&self) -> Box<dyn AttributeAggregatorExecutor> {
        let ctx = self.app_ctx.as_ref().unwrap();

        let synchronized_state = if let Some(ref shared) = self.shared_values {
            FirstState {
                values: shared.lock().unwrap().clone(),
            }
        } else {
            self.state.lock().unwrap().clone()
        };

        Box::new(FirstAttributeAggregatorExecutor {
            arg_exec: self.arg_exec.as_ref().map(|e| e.clone_executor(ctx)),
            return_type: self.return_type,
            state: Mutex::new(synchronized_state),
            app_ctx: Some(Arc::clone(ctx)),
            state_holder: None,
            shared_values: None,
        })
    }
}

impl ExpressionExecutor for FirstAttributeAggregatorExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let event = event?;
        let data = self.arg_exec.as_ref().and_then(|e| e.execute(Some(event)));
        match event.get_event_type() {
            ComplexEventType::Current => self.process_add(data),
            ComplexEventType::Expired => self.process_remove(data),
            ComplexEventType::Reset => self.reset(),
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        self.return_type
    }

    fn clone_executor(&self, _ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(AttributeAggregatorExpressionExecutor::new(self.clone_box()))
    }

    fn is_attribute_aggregator(&self) -> bool {
        true
    }
}

// ============================================================================
// Last Aggregator - Returns last/most recent value in window
// ============================================================================

#[derive(Debug, Clone, Default)]
struct LastState {
    last_value: Option<AttributeValue>,
}

#[derive(Debug)]
pub struct LastAttributeAggregatorExecutor {
    arg_exec: Option<Box<dyn ExpressionExecutor>>,
    return_type: ApiAttributeType,
    state: Mutex<LastState>,
    app_ctx: Option<Arc<EventFluxAppContext>>,
    state_holder: Option<LastAggregatorStateHolder>,
    // Shared state for persistence (same as used by state holder)
    shared_value: Option<Arc<Mutex<Option<AttributeValue>>>>,
}

impl Default for LastAttributeAggregatorExecutor {
    fn default() -> Self {
        Self {
            arg_exec: None,
            return_type: ApiAttributeType::STRING, // Will be set from input
            state: Mutex::new(LastState::default()),
            app_ctx: None,
            state_holder: None,
            shared_value: None,
        }
    }
}

impl AttributeAggregatorExecutor for LastAttributeAggregatorExecutor {
    fn init(
        &mut self,
        mut e: Vec<Box<dyn ExpressionExecutor>>,
        _m: ProcessingMode,
        _ex: bool,
        ctx: &EventFluxQueryContext,
    ) -> Result<(), String> {
        if e.len() != 1 {
            return Err("last aggregator requires exactly one argument".into());
        }
        let exec = e.remove(0);
        self.return_type = exec.get_return_type();
        self.arg_exec = Some(exec);
        self.app_ctx = Some(Arc::clone(&ctx.eventflux_app_context));

        // Initialize state holder for persistence
        let value_arc = Arc::new(Mutex::new(None::<AttributeValue>));
        let aggregator_id = ctx.next_aggregator_id();
        let component_id = format!("last_aggregator_{}", aggregator_id);

        let holder = LastAggregatorStateHolder::new(value_arc.clone(), component_id.clone());
        let state_holder_arc: Arc<Mutex<dyn crate::core::persistence::StateHolder>> =
            Arc::new(Mutex::new(holder.clone()));
        ctx.register_state_holder(component_id, state_holder_arc);

        // Store shared state reference for synchronized updates
        self.shared_value = Some(value_arc);
        self.state_holder = Some(holder);

        Ok(())
    }

    fn process_add(&self, data: Option<AttributeValue>) -> Option<AttributeValue> {
        if let Some(v) = data {
            let mut st = self.state.lock().unwrap();

            if let Some(ref holder) = self.state_holder {
                if holder
                    .base
                    .restored
                    .swap(false, std::sync::atomic::Ordering::AcqRel)
                {
                    if let Some(ref shared) = self.shared_value {
                        st.last_value = shared.lock().unwrap().clone();
                    }
                }
            }

            st.last_value = Some(v.clone());

            if let Some(ref shared) = self.shared_value {
                *shared.lock().unwrap() = Some(v.clone());
            }

            if let Some(ref holder) = self.state_holder {
                holder.record_value_updated(&v);
            }

            Some(v)
        } else {
            let st = self.state.lock().unwrap();
            st.last_value.clone()
        }
    }

    fn process_remove(&self, _data: Option<AttributeValue>) -> Option<AttributeValue> {
        let mut st = self.state.lock().unwrap();

        // Sync from shared state if restoration happened
        if let Some(ref holder) = self.state_holder {
            if holder
                .base
                .restored
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                if let Some(ref shared) = self.shared_value {
                    st.last_value = shared.lock().unwrap().clone();
                }
            }
        }

        st.last_value.clone()
    }

    fn reset(&self) -> Option<AttributeValue> {
        let mut st = self.state.lock().unwrap();
        st.last_value = None;

        // Update shared state for persistence
        if let Some(ref shared) = self.shared_value {
            *shared.lock().unwrap() = None;
        }

        if let Some(ref holder) = self.state_holder {
            holder.record_reset();
        }

        None
    }

    fn clone_box(&self) -> Box<dyn AttributeAggregatorExecutor> {
        let ctx = self.app_ctx.as_ref().unwrap();

        let synchronized_state = if let Some(ref shared) = self.shared_value {
            LastState {
                last_value: shared.lock().unwrap().clone(),
            }
        } else {
            self.state.lock().unwrap().clone()
        };

        Box::new(LastAttributeAggregatorExecutor {
            arg_exec: self.arg_exec.as_ref().map(|e| e.clone_executor(ctx)),
            return_type: self.return_type,
            state: Mutex::new(synchronized_state),
            app_ctx: Some(Arc::clone(ctx)),
            state_holder: None,
            shared_value: None,
        })
    }
}

impl ExpressionExecutor for LastAttributeAggregatorExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let event = event?;
        let data = self.arg_exec.as_ref().and_then(|e| e.execute(Some(event)));
        match event.get_event_type() {
            ComplexEventType::Current => self.process_add(data),
            ComplexEventType::Expired => self.process_remove(data),
            ComplexEventType::Reset => self.reset(),
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        self.return_type
    }

    fn clone_executor(&self, _ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(AttributeAggregatorExpressionExecutor::new(self.clone_box()))
    }

    fn is_attribute_aggregator(&self) -> bool {
        true
    }
}

use crate::core::extension::AttributeAggregatorFactory;

pub(crate) mod aggregator_state_holder_base;

pub mod sum_aggregator_state_holder;
pub use sum_aggregator_state_holder::SumAggregatorStateHolder;

pub mod count_aggregator_state_holder;
pub use count_aggregator_state_holder::CountAggregatorStateHolder;

pub mod avg_aggregator_state_holder;
pub use avg_aggregator_state_holder::AvgAggregatorStateHolder;

pub mod min_aggregator_state_holder;
pub use min_aggregator_state_holder::MinAggregatorStateHolder;

pub mod max_aggregator_state_holder;
pub use max_aggregator_state_holder::MaxAggregatorStateHolder;

pub mod distinctcount_aggregator_state_holder;
pub use distinctcount_aggregator_state_holder::DistinctCountAggregatorStateHolder;

pub mod stddev_aggregator_state_holder;
pub use stddev_aggregator_state_holder::StdDevAggregatorStateHolder;

pub mod first_aggregator_state_holder;
pub use first_aggregator_state_holder::FirstAggregatorStateHolder;

pub mod last_aggregator_state_holder;
pub use last_aggregator_state_holder::LastAggregatorStateHolder;

pub mod and_aggregator_state_holder;
pub use and_aggregator_state_holder::AndAggregatorStateHolder;

pub mod or_aggregator_state_holder;
pub use or_aggregator_state_holder::OrAggregatorStateHolder;

/// Shared helper for state holder changelog operation keys.
/// Generates a unique key by combining operation type, timestamp, and atomic counter.
pub(crate) fn generate_operation_key(operation_type: &str) -> Vec<u8> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let mut key = Vec::new();
    key.extend_from_slice(operation_type.as_bytes());
    key.push(b'_');
    let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    key.extend_from_slice(&timestamp.to_le_bytes());
    key.push(b'_');
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    key.extend_from_slice(&seq.to_le_bytes());
    key
}

#[derive(Debug, Clone)]
pub struct SumAttributeAggregatorFactory;

impl AttributeAggregatorFactory for SumAttributeAggregatorFactory {
    fn name(&self) -> &'static str {
        "sum"
    }
    fn create(&self) -> Box<dyn AttributeAggregatorExecutor> {
        Box::new(SumAttributeAggregatorExecutor::default())
    }
    fn clone_box(&self) -> Box<dyn AttributeAggregatorFactory> {
        Box::new(Self)
    }
}

#[derive(Debug, Clone)]
pub struct AvgAttributeAggregatorFactory;

impl AttributeAggregatorFactory for AvgAttributeAggregatorFactory {
    fn name(&self) -> &'static str {
        "avg"
    }
    fn create(&self) -> Box<dyn AttributeAggregatorExecutor> {
        Box::new(AvgAttributeAggregatorExecutor::default())
    }
    fn clone_box(&self) -> Box<dyn AttributeAggregatorFactory> {
        Box::new(Self)
    }
}

#[derive(Debug, Clone)]
pub struct CountAttributeAggregatorFactory;

impl AttributeAggregatorFactory for CountAttributeAggregatorFactory {
    fn name(&self) -> &'static str {
        "count"
    }
    fn create(&self) -> Box<dyn AttributeAggregatorExecutor> {
        Box::new(CountAttributeAggregatorExecutor::default())
    }
    fn clone_box(&self) -> Box<dyn AttributeAggregatorFactory> {
        Box::new(Self)
    }
}

#[derive(Debug, Clone)]
pub struct DistinctCountAttributeAggregatorFactory;

impl AttributeAggregatorFactory for DistinctCountAttributeAggregatorFactory {
    fn name(&self) -> &'static str {
        "distinctCount"
    }
    fn create(&self) -> Box<dyn AttributeAggregatorExecutor> {
        Box::new(DistinctCountAttributeAggregatorExecutor::default())
    }
    fn clone_box(&self) -> Box<dyn AttributeAggregatorFactory> {
        Box::new(Self)
    }
}

#[derive(Debug, Clone)]
pub struct MinAttributeAggregatorFactory;

impl AttributeAggregatorFactory for MinAttributeAggregatorFactory {
    fn name(&self) -> &'static str {
        "min"
    }
    fn create(&self) -> Box<dyn AttributeAggregatorExecutor> {
        Box::new(MinMaxAttributeAggregatorExecutor::new(
            MinMaxMode::Min,
            false,
        ))
    }
    fn clone_box(&self) -> Box<dyn AttributeAggregatorFactory> {
        Box::new(Self)
    }
}

#[derive(Debug, Clone)]
pub struct MaxAttributeAggregatorFactory;

impl AttributeAggregatorFactory for MaxAttributeAggregatorFactory {
    fn name(&self) -> &'static str {
        "max"
    }
    fn create(&self) -> Box<dyn AttributeAggregatorExecutor> {
        Box::new(MinMaxAttributeAggregatorExecutor::new(
            MinMaxMode::Max,
            false,
        ))
    }
    fn clone_box(&self) -> Box<dyn AttributeAggregatorFactory> {
        Box::new(Self)
    }
}

#[derive(Debug, Clone)]
pub struct MinForeverAttributeAggregatorFactory;

impl AttributeAggregatorFactory for MinForeverAttributeAggregatorFactory {
    fn name(&self) -> &'static str {
        "minForever"
    }
    fn create(&self) -> Box<dyn AttributeAggregatorExecutor> {
        Box::new(MinMaxAttributeAggregatorExecutor::new(
            MinMaxMode::Min,
            true,
        ))
    }
    fn clone_box(&self) -> Box<dyn AttributeAggregatorFactory> {
        Box::new(Self)
    }
}

#[derive(Debug, Clone)]
pub struct MaxForeverAttributeAggregatorFactory;

impl AttributeAggregatorFactory for MaxForeverAttributeAggregatorFactory {
    fn name(&self) -> &'static str {
        "maxForever"
    }
    fn create(&self) -> Box<dyn AttributeAggregatorExecutor> {
        Box::new(MinMaxAttributeAggregatorExecutor::new(
            MinMaxMode::Max,
            true,
        ))
    }
    fn clone_box(&self) -> Box<dyn AttributeAggregatorFactory> {
        Box::new(Self)
    }
}

#[derive(Debug, Clone)]
pub struct StdDevAttributeAggregatorFactory;

impl AttributeAggregatorFactory for StdDevAttributeAggregatorFactory {
    fn name(&self) -> &'static str {
        "stddev"
    }
    fn create(&self) -> Box<dyn AttributeAggregatorExecutor> {
        Box::new(StdDevAttributeAggregatorExecutor::default())
    }
    fn clone_box(&self) -> Box<dyn AttributeAggregatorFactory> {
        Box::new(Self)
    }
}

#[derive(Debug, Clone)]
pub struct FirstAttributeAggregatorFactory;

impl AttributeAggregatorFactory for FirstAttributeAggregatorFactory {
    fn name(&self) -> &'static str {
        "first"
    }
    fn create(&self) -> Box<dyn AttributeAggregatorExecutor> {
        Box::new(FirstAttributeAggregatorExecutor::default())
    }
    fn clone_box(&self) -> Box<dyn AttributeAggregatorFactory> {
        Box::new(Self)
    }
}

#[derive(Debug, Clone)]
pub struct LastAttributeAggregatorFactory;

impl AttributeAggregatorFactory for LastAttributeAggregatorFactory {
    fn name(&self) -> &'static str {
        "last"
    }
    fn create(&self) -> Box<dyn AttributeAggregatorExecutor> {
        Box::new(LastAttributeAggregatorExecutor::default())
    }
    fn clone_box(&self) -> Box<dyn AttributeAggregatorFactory> {
        Box::new(Self)
    }
}

// ============================================================================
// And Aggregator - Returns true only if ALL boolean values in window are true
// ============================================================================

#[derive(Debug, Clone, Default)]
struct AndState {
    true_count: i64,
    false_count: i64,
}

#[derive(Debug)]
pub struct AndAttributeAggregatorExecutor {
    arg_exec: Option<Box<dyn ExpressionExecutor>>,
    state: Mutex<AndState>,
    app_ctx: Option<Arc<EventFluxAppContext>>,
    state_holder: Option<AndAggregatorStateHolder>,
    // Shared state for persistence (same as used by state holder)
    shared_true_count: Option<Arc<Mutex<i64>>>,
    shared_false_count: Option<Arc<Mutex<i64>>>,
}

impl Default for AndAttributeAggregatorExecutor {
    fn default() -> Self {
        Self {
            arg_exec: None,
            state: Mutex::new(AndState::default()),
            app_ctx: None,
            state_holder: None,
            shared_true_count: None,
            shared_false_count: None,
        }
    }
}

impl AttributeAggregatorExecutor for AndAttributeAggregatorExecutor {
    fn init(
        &mut self,
        mut e: Vec<Box<dyn ExpressionExecutor>>,
        _m: ProcessingMode,
        _ex: bool,
        ctx: &EventFluxQueryContext,
    ) -> Result<(), String> {
        if e.len() != 1 {
            return Err("and() requires exactly one argument".into());
        }
        let exec = e.remove(0);
        if exec.get_return_type() != ApiAttributeType::BOOL {
            return Err("and() requires a boolean argument".into());
        }
        self.arg_exec = Some(exec);
        self.app_ctx = Some(Arc::clone(&ctx.eventflux_app_context));

        // Initialize state holder for persistence
        let true_count_arc = Arc::new(Mutex::new(0i64));
        let false_count_arc = Arc::new(Mutex::new(0i64));
        let aggregator_id = ctx.next_aggregator_id();
        let component_id = format!("and_aggregator_{}", aggregator_id);

        let holder = AndAggregatorStateHolder::new(
            true_count_arc.clone(),
            false_count_arc.clone(),
            component_id.clone(),
        );
        let state_holder_arc: Arc<Mutex<dyn crate::core::persistence::StateHolder>> =
            Arc::new(Mutex::new(holder.clone()));
        ctx.register_state_holder(component_id, state_holder_arc);

        // Store shared state references for synchronized updates
        self.shared_true_count = Some(true_count_arc);
        self.shared_false_count = Some(false_count_arc);
        self.state_holder = Some(holder);

        Ok(())
    }

    fn process_add(&self, data: Option<AttributeValue>) -> Option<AttributeValue> {
        let mut st = self.state.lock().unwrap();

        if let Some(ref holder) = self.state_holder {
            if holder
                .base
                .restored
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                if let (Some(ref shared_tc), Some(ref shared_fc)) =
                    (&self.shared_true_count, &self.shared_false_count)
                {
                    st.true_count = *shared_tc.lock().unwrap();
                    st.false_count = *shared_fc.lock().unwrap();
                }
            }
        }

        if let Some(b) = data.as_ref().and_then(|v| v.as_bool()) {
            if b {
                st.true_count += 1;
            } else {
                st.false_count += 1;
            }

            if let (Some(ref shared_tc), Some(ref shared_fc)) =
                (&self.shared_true_count, &self.shared_false_count)
            {
                *shared_tc.lock().unwrap() = st.true_count;
                *shared_fc.lock().unwrap() = st.false_count;
            }

            if let Some(ref holder) = self.state_holder {
                holder.record_value_added(b);
            }
        }
        Some(AttributeValue::Bool(
            st.true_count > 0 && st.false_count == 0,
        ))
    }

    fn process_remove(&self, data: Option<AttributeValue>) -> Option<AttributeValue> {
        let mut st = self.state.lock().unwrap();

        if let Some(ref holder) = self.state_holder {
            if holder
                .base
                .restored
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                if let (Some(ref shared_tc), Some(ref shared_fc)) =
                    (&self.shared_true_count, &self.shared_false_count)
                {
                    st.true_count = *shared_tc.lock().unwrap();
                    st.false_count = *shared_fc.lock().unwrap();
                }
            }
        }

        if let Some(b) = data.as_ref().and_then(|v| v.as_bool()) {
            if b {
                if st.true_count > 0 {
                    st.true_count -= 1;
                }
            } else if st.false_count > 0 {
                st.false_count -= 1;
            }

            // Update shared state for persistence
            if let (Some(ref shared_tc), Some(ref shared_fc)) =
                (&self.shared_true_count, &self.shared_false_count)
            {
                *shared_tc.lock().unwrap() = st.true_count;
                *shared_fc.lock().unwrap() = st.false_count;
            }

            if let Some(ref holder) = self.state_holder {
                holder.record_value_removed(b);
            }
        }
        Some(AttributeValue::Bool(
            st.true_count > 0 && st.false_count == 0,
        ))
    }

    fn reset(&self) -> Option<AttributeValue> {
        let mut st = self.state.lock().unwrap();
        let old_true = st.true_count;
        let old_false = st.false_count;
        st.true_count = 0;
        st.false_count = 0;

        // Update shared state for persistence
        if let (Some(ref shared_tc), Some(ref shared_fc)) =
            (&self.shared_true_count, &self.shared_false_count)
        {
            *shared_tc.lock().unwrap() = 0;
            *shared_fc.lock().unwrap() = 0;
        }

        if let Some(ref holder) = self.state_holder {
            holder.record_reset(old_true, old_false);
        }

        Some(AttributeValue::Bool(false))
    }

    fn clone_box(&self) -> Box<dyn AttributeAggregatorExecutor> {
        let ctx = self.app_ctx.as_ref().unwrap();

        // Read from shared state for consistency
        let synchronized_state = if let (Some(ref shared_tc), Some(ref shared_fc)) =
            (&self.shared_true_count, &self.shared_false_count)
        {
            AndState {
                true_count: *shared_tc.lock().unwrap(),
                false_count: *shared_fc.lock().unwrap(),
            }
        } else {
            self.state.lock().unwrap().clone()
        };

        Box::new(AndAttributeAggregatorExecutor {
            arg_exec: self.arg_exec.as_ref().map(|e| e.clone_executor(ctx)),
            state: Mutex::new(synchronized_state),
            app_ctx: Some(Arc::clone(ctx)),
            state_holder: None,
            shared_true_count: None,
            shared_false_count: None,
        })
    }
}

impl ExpressionExecutor for AndAttributeAggregatorExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let event = event?;
        let data = self.arg_exec.as_ref().and_then(|e| e.execute(Some(event)));
        match event.get_event_type() {
            ComplexEventType::Current => self.process_add(data),
            ComplexEventType::Expired => self.process_remove(data),
            ComplexEventType::Reset => self.reset(),
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::BOOL
    }

    fn clone_executor(&self, _ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(AttributeAggregatorExpressionExecutor::new(self.clone_box()))
    }

    fn is_attribute_aggregator(&self) -> bool {
        true
    }
}

// ============================================================================
// Or Aggregator - Returns true if ANY boolean value in window is true
// ============================================================================

#[derive(Debug, Clone, Default)]
struct OrState {
    true_count: i64,
}

#[derive(Debug)]
pub struct OrAttributeAggregatorExecutor {
    arg_exec: Option<Box<dyn ExpressionExecutor>>,
    state: Mutex<OrState>,
    app_ctx: Option<Arc<EventFluxAppContext>>,
    state_holder: Option<OrAggregatorStateHolder>,
    // Shared state for persistence (same as used by state holder)
    shared_true_count: Option<Arc<Mutex<i64>>>,
}

impl Default for OrAttributeAggregatorExecutor {
    fn default() -> Self {
        Self {
            arg_exec: None,
            state: Mutex::new(OrState::default()),
            app_ctx: None,
            state_holder: None,
            shared_true_count: None,
        }
    }
}

impl AttributeAggregatorExecutor for OrAttributeAggregatorExecutor {
    fn init(
        &mut self,
        mut e: Vec<Box<dyn ExpressionExecutor>>,
        _m: ProcessingMode,
        _ex: bool,
        ctx: &EventFluxQueryContext,
    ) -> Result<(), String> {
        if e.len() != 1 {
            return Err("or() requires exactly one argument".into());
        }
        let exec = e.remove(0);
        if exec.get_return_type() != ApiAttributeType::BOOL {
            return Err("or() requires a boolean argument".into());
        }
        self.arg_exec = Some(exec);
        self.app_ctx = Some(Arc::clone(&ctx.eventflux_app_context));

        // Initialize state holder for persistence
        let true_count_arc = Arc::new(Mutex::new(0i64));
        let aggregator_id = ctx.next_aggregator_id();
        let component_id = format!("or_aggregator_{}", aggregator_id);

        let holder = OrAggregatorStateHolder::new(true_count_arc.clone(), component_id.clone());
        let state_holder_arc: Arc<Mutex<dyn crate::core::persistence::StateHolder>> =
            Arc::new(Mutex::new(holder.clone()));
        ctx.register_state_holder(component_id, state_holder_arc);

        // Store shared state reference for synchronized updates
        self.shared_true_count = Some(true_count_arc);
        self.state_holder = Some(holder);

        Ok(())
    }

    fn process_add(&self, data: Option<AttributeValue>) -> Option<AttributeValue> {
        let mut st = self.state.lock().unwrap();

        if let Some(ref holder) = self.state_holder {
            if holder
                .base
                .restored
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                if let Some(ref shared_tc) = self.shared_true_count {
                    st.true_count = *shared_tc.lock().unwrap();
                }
            }
        }

        if let Some(true) = data.as_ref().and_then(|v| v.as_bool()) {
            st.true_count += 1;

            if let Some(ref shared_tc) = self.shared_true_count {
                *shared_tc.lock().unwrap() = st.true_count;
            }

            if let Some(ref holder) = self.state_holder {
                holder.record_increment();
            }
        }
        Some(AttributeValue::Bool(st.true_count > 0))
    }

    fn process_remove(&self, data: Option<AttributeValue>) -> Option<AttributeValue> {
        let mut st = self.state.lock().unwrap();

        if let Some(ref holder) = self.state_holder {
            if holder
                .base
                .restored
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                if let Some(ref shared_tc) = self.shared_true_count {
                    st.true_count = *shared_tc.lock().unwrap();
                }
            }
        }

        if let Some(true) = data.as_ref().and_then(|v| v.as_bool()) {
            if st.true_count > 0 {
                st.true_count -= 1;
            }

            // Update shared state for persistence
            if let Some(ref shared_tc) = self.shared_true_count {
                *shared_tc.lock().unwrap() = st.true_count;
            }

            if let Some(ref holder) = self.state_holder {
                holder.record_decrement();
            }
        }
        Some(AttributeValue::Bool(st.true_count > 0))
    }

    fn reset(&self) -> Option<AttributeValue> {
        let mut st = self.state.lock().unwrap();
        let old_count = st.true_count;
        st.true_count = 0;

        // Update shared state for persistence
        if let Some(ref shared_tc) = self.shared_true_count {
            *shared_tc.lock().unwrap() = 0;
        }

        if let Some(ref holder) = self.state_holder {
            holder.record_reset(old_count);
        }

        Some(AttributeValue::Bool(false))
    }

    fn clone_box(&self) -> Box<dyn AttributeAggregatorExecutor> {
        let ctx = self.app_ctx.as_ref().unwrap();

        let synchronized_state = if let Some(ref shared_tc) = self.shared_true_count {
            OrState {
                true_count: *shared_tc.lock().unwrap(),
            }
        } else {
            self.state.lock().unwrap().clone()
        };

        Box::new(OrAttributeAggregatorExecutor {
            arg_exec: self.arg_exec.as_ref().map(|e| e.clone_executor(ctx)),
            state: Mutex::new(synchronized_state),
            app_ctx: Some(Arc::clone(ctx)),
            state_holder: None,
            shared_true_count: None,
        })
    }
}

impl ExpressionExecutor for OrAttributeAggregatorExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let event = event?;
        let data = self.arg_exec.as_ref().and_then(|e| e.execute(Some(event)));
        match event.get_event_type() {
            ComplexEventType::Current => self.process_add(data),
            ComplexEventType::Expired => self.process_remove(data),
            ComplexEventType::Reset => self.reset(),
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::BOOL
    }

    fn clone_executor(&self, _ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(AttributeAggregatorExpressionExecutor::new(self.clone_box()))
    }

    fn is_attribute_aggregator(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
pub struct AndAttributeAggregatorFactory;

impl AttributeAggregatorFactory for AndAttributeAggregatorFactory {
    fn name(&self) -> &'static str {
        "and"
    }
    fn create(&self) -> Box<dyn AttributeAggregatorExecutor> {
        Box::new(AndAttributeAggregatorExecutor::default())
    }
    fn clone_box(&self) -> Box<dyn AttributeAggregatorFactory> {
        Box::new(Self)
    }
}

#[derive(Debug, Clone)]
pub struct OrAttributeAggregatorFactory;

impl AttributeAggregatorFactory for OrAttributeAggregatorFactory {
    fn name(&self) -> &'static str {
        "or"
    }
    fn create(&self) -> Box<dyn AttributeAggregatorExecutor> {
        Box::new(OrAttributeAggregatorExecutor::default())
    }
    fn clone_box(&self) -> Box<dyn AttributeAggregatorFactory> {
        Box::new(Self)
    }
}
