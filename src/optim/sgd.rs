use super::barrier::SynchronizationBarrier;
use super::{InnerOptimizer, Optimizer, SynchronizedOptimizer};
use std::ops::DerefMut;
use std::sync::Arc;
use {numerics, HogwildParameter, ParameterNode, Variable};

use ndarray::Axis;

/// Standard stochastic gradient descent optimizer with a fixed learning rate.
pub struct SGD {
    learning_rate: f32,
    clamp: Option<(f32, f32)>,
    pub(in optim) sync_barrier: SynchronizationBarrier,
}

impl SGD {
    /// Create a new optimizer instance with a given set of parameters.
    pub fn new() -> Self {
        SGD {
            learning_rate: 0.05,
            clamp: None,
            sync_barrier: SynchronizationBarrier::default(),
        }
    }

    /// Set the learning rate.
    pub fn learning_rate(mut self, learning_rate: f32) -> Self {
        self.learning_rate = learning_rate;
        self
    }

    /// Set the clamp bounds.
    pub fn clamp(mut self, min: f32, max: f32) -> Self {
        self.clamp = Some((min, max));
        self
    }

    /// Return a synchoronised wrapper for this optimizer.
    pub fn synchronized(&self) -> SynchronizedOptimizer<Self> {
        SynchronizedOptimizer::new(self, self.sync_barrier.register_thread())
    }
}

impl InnerOptimizer for SGD {
    fn inner_step<T: DerefMut<Target = ::nodes::GradientAccumulator>>(
        &self,
        param: &Arc<HogwildParameter>,
        mut sink: T,
    ) {
        let param_value = unsafe { param.value_mut() };
        let learning_rate = self.learning_rate;
        let sink = sink.deref_mut();

        if let Some((min, max)) = self.clamp {
            sink.clamp(min, max);
        }

        if sink.has_dense {
            param_value.scaled_add(-self.learning_rate, sink.dense_gradient());
        }

        for (row_idx, grad) in sink.sparse_gradient.iter() {
            let mut param_row = param_value.subview_mut(Axis(0), row_idx);

            numerics::map_add_assign_slice(param_row.into_slice().unwrap(), grad, |x| {
                -learning_rate * x
            });
        }
    }
}

impl Optimizer for SGD {
    fn step(&self, parameters: &[Variable<ParameterNode>]) {
        for parameter in parameters {
            self.inner_step(&parameter.node.value, parameter.node.gradient.borrow_mut())
        }
    }
}
