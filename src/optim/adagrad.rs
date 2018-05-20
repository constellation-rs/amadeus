use super::barrier::{SynchronizationBarrier, SynchronizationBarrierGuard};
use super::Optimizer;
use numerics::{ArraySlice, ArraySliceMut};
use {numerics, ParameterNode, Variable};

use ndarray::Axis;

/// Adagrad optimizer, scaled the learning rate by the inverse of previously
/// accumulated gradients.
pub struct Adagrad {
    learning_rate: f32,
    l2: f32,
    parameters: Vec<Variable<ParameterNode>>,
    clamp: Option<(f32, f32)>,
    eps: f32,
    sync_barrier: Option<SynchronizationBarrierGuard>,
}

impl Adagrad {
    /// Create a new optimizer instance with a given set of parameters.
    pub fn new(parameters: Vec<Variable<ParameterNode>>) -> Self {
        Adagrad {
            learning_rate: 0.05,
            l2: 0.0,
            parameters: parameters,
            clamp: None,
            eps: 1e-10,
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

    /// Set the L2 penalty.
    pub fn l2_penalty(mut self, l2_penalty: f32) -> Self {
        self.l2 = l2_penalty;
        self
    }

    /// Decay weights.
    pub fn decay_weights(&mut self, penalty: f32) {
        for parameter in &self.parameters {
            let mut param_value = unsafe { parameter.node.value.value_mut() };

            param_value
                .as_slice_mut()
                .unwrap()
                .iter_mut()
                .for_each(|x| *x -= x.signum() * penalty * numerics::pow2(*x));
        }
    }

    fn do_step(&self, parameter: &Variable<ParameterNode>) {
        let learning_rate = self.learning_rate;

        let mut sink = parameter.node.gradient.borrow_mut();

        if let Some((min, max)) = self.clamp {
            sink.clamp(min, max);
        }

        let param_value = unsafe { parameter.node.value.value_mut() };
        let squared_gradient = unsafe { parameter.node.value.squared_gradient_mut() };

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
            .for_each(|(ref index_vec, ref grad)| {
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
