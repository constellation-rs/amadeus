use super::barrier::SynchronizationBarrier;
use super::{InnerOptimizer, Optimizer, SynchronizedOptimizer};
use numerics::{ArraySlice, ArraySliceMut};
use std::ops::DerefMut;
use std::sync::Arc;
use {numerics, HogwildParameter, ParameterNode, Variable};

use ndarray::Axis;

/// Adagrad optimizer, scaled the learning rate by the inverse of previously
/// accumulated gradients.
pub struct Adagrad {
    learning_rate: f32,
    l2: f32,
    clamp: Option<(f32, f32)>,
    eps: f32,
    sync_barrier: SynchronizationBarrier,
}

impl Adagrad {
    /// Create a new optimizer instance with a given set of parameters.
    pub fn new() -> Self {
        Adagrad {
            learning_rate: 0.05,
            l2: 0.0,
            clamp: None,
            eps: 1e-10,
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

    /// Set the L2 penalty.
    pub fn l2_penalty(mut self, l2_penalty: f32) -> Self {
        self.l2 = l2_penalty;
        self
    }

    /// Return a synchoronised wrapper for this optimizer.
    pub fn synchronized(&self) -> SynchronizedOptimizer<Self> {
        SynchronizedOptimizer::new(self, self.sync_barrier.register_thread())
    }
}

impl InnerOptimizer for Adagrad {
    fn inner_step<T: DerefMut<Target = ::nodes::GradientAccumulator>>(
        &self,
        param: &Arc<HogwildParameter>,
        mut sink: T,
    ) {
        let learning_rate = self.learning_rate;

        if let Some((min, max)) = self.clamp {
            sink.clamp(min, max);
        }

        let param_value = unsafe { param.value_mut() };
        let squared_gradient = unsafe { param.squared_gradient_mut() };

        if sink.has_dense {
            for (value, &gradient, squared_gradient) in izip!(
                param_value.fast_slice_mut(),
                sink.dense_gradient().fast_slice(),
                squared_gradient.fast_slice_mut()
            ) {
                let gradient = gradient + *value * self.l2;
                *squared_gradient += numerics::pow2(gradient);
                *value -= learning_rate / (self.eps + squared_gradient.sqrt()) * gradient;
            }
        }

        sink.sparse_gradient
            .as_slice()
            .iter()
            .for_each(|&(ref index_vec, ref grad)| {
                for (grad_idx, &param_idx) in index_vec.iter().enumerate() {
                    let grad_row = grad.subview(Axis(0), grad_idx);
                    let mut param_row = param_value.subview_mut(Axis(0), param_idx);
                    let mut squared_row = squared_gradient.subview_mut(Axis(0), param_idx);

                    for (value, &gradient, squared_gradient) in izip!(
                        param_row.fast_slice_mut(),
                        grad_row.into_slice().unwrap(),
                        squared_row.fast_slice_mut()
                    ) {
                        let gradient = gradient + *value * self.l2;
                        *squared_gradient += numerics::pow2(gradient);
                        *value -= learning_rate / (self.eps + squared_gradient.sqrt()) * gradient;
                    }
                }
            });
    }
}

impl Optimizer for Adagrad {
    /// Perform a single SGD step.
    fn step(&self, parameters: &[Variable<ParameterNode>]) {
        for parameter in parameters {
            self.inner_step(&parameter.node.value, parameter.node.gradient.borrow_mut())
        }
    }
}
