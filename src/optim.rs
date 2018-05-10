use super::{numerics, Arr, ParameterNode, SynchronizationBarrier, SynchronizationBarrierGuard,
            Variable};

use ndarray::Axis;

pub trait Optimizer {
    fn step(&self);
}

struct AdamParameters<'params> {
    value: &'params mut Arr,
    m: &'params mut Arr,
    v: &'params mut Arr,
    t: &'params mut Arr,
}

pub struct Adam {
    learning_rate: f32,
    l2: f32,
    beta_m: f32,
    beta_v: f32,
    eps: f32,
    parameters: Vec<Variable<ParameterNode>>,
    clamp: Option<(f32, f32)>,
    sync_barrier: Option<SynchronizationBarrierGuard>,
}

impl Adam {
    pub fn new(parameters: Vec<Variable<ParameterNode>>) -> Self {
        Self {
            learning_rate: 0.05,
            l2: 0.0,
            beta_m: 0.9,
            beta_v: 0.999,
            eps: 1.0e-8,
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

    /// Use synchronous parallel training.
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

    fn param_fields<'par>(&self, parameter: &'par Variable<ParameterNode>) -> AdamParameters<'par> {
        AdamParameters {
            value: unsafe { parameter.node.value.value_mut() },
            m: unsafe { parameter.node.value.squared_gradient_mut() },
            v: unsafe { parameter.node.value.moments_mut() },
            t: unsafe { parameter.node.value.num_updates_mut() },
        }
    }

    #[inline(always)]
    fn update(&self, value: &mut f32, gradient: f32, m: &mut f32, v: &mut f32, t: &mut f32) {
        // Apply L2 to gradient.
        let gradient = gradient + *value * self.l2;

        // Update t. I _think_ this overflows to saturate at std::f32::MAX;
        *t += 1.0;

        // Update m and v.
        *m = self.beta_m * *m + (1.0 - self.beta_m) * gradient;
        *v = self.beta_v * *v + (1.0 - self.beta_v) * numerics::pow2(gradient);

        let m_hat = *m / (1.0 - self.beta_m.powf(*t));
        let v_hat = *v / (1.0 - self.beta_v.powf(*t));

        *value -= self.learning_rate / (v_hat.sqrt() + self.eps) * m_hat;
    }

    fn do_step(&self, parameter: &Variable<ParameterNode>) {
        let mut sink = parameter.node.gradient.borrow_mut();

        if let Some((min, max)) = self.clamp {
            sink.clamp(min, max);
        }

        let param = self.param_fields(parameter);

        if sink.has_dense {
            for (value, &gradient, m, v, t) in izip!(
                param.value.as_slice_mut().unwrap(),
                sink.dense_gradient().as_slice().unwrap(),
                param.m.as_slice_mut().unwrap(),
                param.v.as_slice_mut().unwrap(),
                param.t.as_slice_mut().unwrap(),
            ) {
                self.update(value, gradient, m, v, t);
            }
        }

        if sink.has_sparse {
            for &(ref index_vec, ref grad) in sink.sparse_gradient.iter() {
                for (grad_idx, &param_idx) in index_vec.iter().enumerate() {
                    let mut value_row = param.value.subview_mut(Axis(0), param_idx);
                    let grad_row = grad.subview(Axis(0), grad_idx);
                    let mut m_row = param.m.subview_mut(Axis(0), param_idx);
                    let mut v_row = param.v.subview_mut(Axis(0), param_idx);
                    let mut t_row = param.t.subview_mut(Axis(0), param_idx);

                    for (value, &gradient, m, v, t) in izip!(
                        value_row.as_slice_mut().unwrap(),
                        grad_row.into_slice().unwrap(),
                        m_row.as_slice_mut().unwrap(),
                        v_row.as_slice_mut().unwrap(),
                        t_row.as_slice_mut().unwrap(),
                    ) {
                        self.update(value, gradient, m, v, t);
                    }
                }
            }
        }
    }
}

impl Optimizer for Adam {
    /// Perform a single SGD step.
    fn step(&self) {
        if let Some(ref barrier) = self.sync_barrier {
            {
                let _ = barrier.lock();

                for parameter in &self.parameters {
                    self.do_step(parameter);
                }
            }

            barrier.wait();
        } else {
            for parameter in &self.parameters {
                self.do_step(parameter);
            }
        }
    }
}
