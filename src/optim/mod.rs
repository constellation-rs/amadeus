//! Optimization module.
//!
//! Contains a number of optimizers.
use std::ops::DerefMut;
use std::sync::Arc;
use {HogwildParameter, ParameterNode, Variable};
mod adagrad;
mod adam;
mod barrier;
mod sgd;

pub use self::adagrad::Adagrad;
pub use self::adam::Adam;
pub use self::barrier::SynchronizationBarrier;
pub use self::sgd::SGD;

/// Core trait implemented by all optimizer methods.
pub trait Optimizer {
    /// Perform a single SGD step.
    fn step(&self, parameters: &[Variable<ParameterNode>]);
}

pub trait Synchronizable {
    fn synchronized(&self) -> SynchronizedOptimizer<Self>
    where
        Self: Sized;
}

pub(crate) trait InnerOptimizer {
    fn inner_step<T: DerefMut<Target = ::nodes::GradientAccumulator>>(
        &self,
        parameter: &Arc<HogwildParameter>,
        sink: T,
    );
}

pub struct SynchronizedOptimizer<'a, T: 'a> {
    optimizer: &'a T,
    barrier_guard: barrier::SynchronizationBarrierGuard,
}

impl<'a, T: 'a> SynchronizedOptimizer<'a, T> {
    pub fn new(optimizer: &'a T, barrier_guard: barrier::SynchronizationBarrierGuard) -> Self {
        ::std::thread::sleep_ms(5);
        SynchronizedOptimizer {
            optimizer: optimizer,
            barrier_guard: barrier_guard,
        }
    }
}

macro_rules! impl_sync_optimizer {
    ($type:ty) => {
        impl<'a> Optimizer for SynchronizedOptimizer<'a, $type> {
            fn step(&self, parameters: &[Variable<ParameterNode>]) {
                // Push down this thread's gradients to the shared
                // accumulators.
                for parameter in parameters {
                    parameter.gradient_push_down();
                }

                // Wait until all the other threads have finished push down.
                self.barrier_guard.start_wait();

                // Start.
                for parameter in parameters {
                    // Attempt to get the lock. If unsuccessful, skip this parameter:
                    // another thread is taking care of it already.
                    if let Ok(mut lock) = parameter.node.value.try_gradient_accumulator() {
                        if let Some(ref mut sink) = lock.deref_mut() {
                            self.optimizer.inner_step(&parameter.node.value, sink);
                        } else {
                            panic!("Sink should always be instantiated.")
                        }

                        if let Some(ref mut sink) = lock.deref_mut() {
                            sink.zero_gradient();
                        } else {
                            panic!("Sink should always be instantiated.")
                        }
                    }
                }

                // Wait until all the other threads have finished updating.
                self.barrier_guard.end_wait();
            }
        }

        impl Synchronizable for $type {
            fn synchronized(&self) -> SynchronizedOptimizer<Self> {
                self.synchronized()
            }
        }
    };
}

// TODO: impl using generics, atm it's painful because of private type leakage
impl_sync_optimizer!(SGD);
impl_sync_optimizer!(Adagrad);
impl_sync_optimizer!(Adam);

macro_rules! impl_optimizer_enum {
    ($(($tag:ident, $type:ty)),*) => {
        pub enum Optimizers {
        $(
            $tag($type),
        )*
        }

        impl Optimizer for Optimizers {
            fn step(&self, parameters: &[Variable<ParameterNode>]) {
                match self {
                    $(
                        Optimizers::$tag(val) => val.step(parameters),
                        )*
                }
            }
        }
    }
}

impl_optimizer_enum!((SGD, SGD), (Adagrad, Adagrad), (Adam, Adam));
