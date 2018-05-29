use super::barrier::SynchronizationBarrier;
use super::{InnerOptimizer, Optimizer, SynchronizedOptimizer};
use std::ops::DerefMut;
use std::sync::Arc;
use {numerics, Arr, HogwildParameter, ParameterNode, Variable};

use ndarray::Axis;

struct AdamParameters<'params> {
    value: &'params mut Arr,
    m: &'params mut Arr,
    v: &'params mut Arr,
    t: &'params mut i32,
}

/// ADAM optimizer.
pub struct Adam {
    learning_rate: f32,
    l2: f32,
    beta_m: f32,
    beta_v: f32,
    eps: f32,
    clamp: Option<(f32, f32)>,
    pub(in optim) sync_barrier: SynchronizationBarrier,
}

impl Adam {
    /// Build new optimizer object.
    pub fn new() -> Self {
        Self {
            learning_rate: 0.05,
            l2: 0.0,
            beta_m: 0.9,
            beta_v: 0.999,
            eps: 1.0e-8,
            clamp: None,
            sync_barrier: SynchronizationBarrier::default(),
        }
    }

    /// Return a synchoronised wrapper for this optimizer.
    pub fn synchronized(&self) -> SynchronizedOptimizer<Self> {
        SynchronizedOptimizer::new(self, self.sync_barrier.register_thread())
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

    fn param_fields<'par>(&self, value: &'par Arc<HogwildParameter>) -> AdamParameters<'par> {
        AdamParameters {
            value: unsafe { value.value_mut() },
            m: unsafe { value.squared_gradient_mut() },
            v: unsafe { value.moments_mut() },
            t: unsafe { value.num_updates_mut() },
        }
    }

    #[inline(always)]
    fn update(&self, value: &mut f32, gradient: f32, m: &mut f32, v: &mut f32, t: &i32) {
        // Apply L2 to gradient.
        let gradient = gradient + *value * self.l2;

        // Update m and v.
        *m = self.beta_m * *m + (1.0 - self.beta_m) * gradient;
        *v = self.beta_v * *v + (1.0 - self.beta_v) * numerics::pow2(gradient);

        let m_hat = *m / (1.0 - self.beta_m.powi(*t));
        let v_hat = *v / (1.0 - self.beta_v.powi(*t));

        *value -= self.learning_rate / (v_hat.sqrt() + self.eps) * m_hat;
    }
}

impl InnerOptimizer for Adam {
    fn inner_step<T: DerefMut<Target = ::nodes::GradientAccumulator>>(
        &self,
        param: &Arc<HogwildParameter>,
        mut sink: T,
    ) {
        if let Some((min, max)) = self.clamp {
            sink.clamp(min, max);
        }

        let param = self.param_fields(param);

        // Increment number of updates
        *param.t = param.t.saturating_add(1);

        if sink.has_dense {
            for (value, &gradient, m, v) in izip!(
                param.value.as_slice_mut().unwrap(),
                sink.dense_gradient().as_slice().unwrap(),
                param.m.as_slice_mut().unwrap(),
                param.v.as_slice_mut().unwrap(),
            ) {
                self.update(value, gradient, m, v, param.t);
            }
        }

        for &(ref index_vec, ref grad) in sink.sparse_gradient.as_slice() {
            for (grad_idx, &param_idx) in index_vec.iter().enumerate() {
                let mut value_row = param.value.subview_mut(Axis(0), param_idx);
                let grad_row = grad.subview(Axis(0), grad_idx);
                let mut m_row = param.m.subview_mut(Axis(0), param_idx);
                let mut v_row = param.v.subview_mut(Axis(0), param_idx);

                for (value, &gradient, m, v) in izip!(
                    value_row.as_slice_mut().unwrap(),
                    grad_row.into_slice().unwrap(),
                    m_row.as_slice_mut().unwrap(),
                    v_row.as_slice_mut().unwrap(),
                ) {
                    self.update(value, gradient, m, v, param.t);
                }
            }
        }
    }
}

impl Optimizer for Adam {
    /// Perform a single SGD step.
    fn step(&self, parameters: &[Variable<ParameterNode>]) {
        for parameter in parameters {
            self.inner_step(&parameter.node.value, parameter.node.gradient.borrow_mut())
        }
    }
}
