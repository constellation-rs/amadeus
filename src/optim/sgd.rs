use super::barrier::{SynchronizationBarrier, SynchronizationBarrierGuard};
use super::Optimizer;
use {numerics, ParameterNode, Variable};

use ndarray::Axis;

/// Standard stochastic gradient descent optimizer with a fixed learning rate.
pub struct SGD {
    learning_rate: f32,
    parameters: Vec<Variable<ParameterNode>>,
    clamp: Option<(f32, f32)>,
    sync_barrier: Option<SynchronizationBarrierGuard>,
}

impl SGD {
    /// Create a new optimizer instance with a given set of parameters.
    pub fn new(parameters: Vec<Variable<ParameterNode>>) -> Self {
        SGD {
            learning_rate: 0.05,
            parameters: parameters,
            clamp: None,
            sync_barrier: None,
        }
    }

    /// Set the learning rate.
    pub fn learning_rate(mut self, learning_rate: f32) -> Self {
        self.learning_rate = learning_rate;
        self
    }

    /// Use the optimizer in synchrnous mode.
    pub fn synchronized(mut self, barrier: &SynchronizationBarrier) -> Self {
        self.sync_barrier = Some(barrier.register_thread());
        self
    }

    /// Set the clamp bounds.
    pub fn clamp(mut self, min: f32, max: f32) -> Self {
        self.clamp = Some((min, max));
        self
    }

    /// Perform a single SGD step.
    fn do_step(&self, parameter: &Variable<ParameterNode>) {
        let learning_rate = self.learning_rate;
        let mut sink = parameter.node.gradient.borrow_mut();
        let param_value = unsafe { parameter.node.value.value_mut() };

        if let Some((min, max)) = self.clamp {
            sink.clamp(min, max);
        }

        if sink.has_dense {
            param_value.scaled_add(-self.learning_rate, sink.dense_gradient());
        }

        for &(ref index_vec, ref grad) in sink.sparse_gradient.as_slice() {
            for (grad_idx, &param_idx) in index_vec.iter().enumerate() {
                let grad_row = grad.subview(Axis(0), grad_idx);
                let mut param_row = param_value.subview_mut(Axis(0), param_idx);

                numerics::map_add_assign_slice(
                    param_row.into_slice().unwrap(),
                    grad_row.into_slice().unwrap(),
                    |x| -learning_rate * x,
                );
            }
        }
    }
}

impl Optimizer for SGD {
    fn step(&self) {
        if let Some(ref barrier) = self.sync_barrier {
            barrier.start_wait();
            {
                let _ = barrier.lock();

                for parameter in &self.parameters {
                    self.do_step(parameter);
                }
            }

            barrier.end_wait();
        } else {
            for parameter in &self.parameters {
                self.do_step(parameter);
            }
        }
    }
}
