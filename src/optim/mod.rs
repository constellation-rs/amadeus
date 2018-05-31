//! Optimization module.
//!
//! Contains a number of optimizers.
use std::cell::Cell;
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
    fn synchronized(&self, num_threads: usize) -> Vec<SynchronizedOptimizer<Self>>
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
    num_updates: Cell<usize>,
    optimizer: &'a T,
    barrier_guard: barrier::SynchronizationBarrierGuard,
}

impl<'a, T: 'a> SynchronizedOptimizer<'a, T> {
    pub fn new(optimizer: &'a T, barrier_guard: barrier::SynchronizationBarrierGuard) -> Self {
        SynchronizedOptimizer {
            num_updates: Cell::new(0),
            optimizer: optimizer,
            barrier_guard: barrier_guard,
        }
    }
}

macro_rules! impl_sync_optimizer {
    ($type:ty) => {
        impl<'a> Optimizer for SynchronizedOptimizer<'a, $type> {
            fn step(&self, parameters: &[Variable<ParameterNode>]) {
                self.num_updates.set(self.num_updates.get() + 1);

                if self.num_updates.get() == 8 {
                    let _barrier = self.barrier_guard.synchronize();
                    self.optimizer.step(parameters);

                    self.num_updates.set(0);
                }
            }
        }
    };
}

// TODO: impl using generics, atm it's painful because of private type leakage
impl_sync_optimizer!(SGD);
impl_sync_optimizer!(Adagrad);
impl_sync_optimizer!(Adam);
impl_sync_optimizer!(Optimizers);

macro_rules! impl_optimizer_enum {
    ($(($tag:ident, $type:ty)),*) => {
        pub enum Optimizers {
        $(
            $tag($type),
        )*
        }

        impl Optimizers {
            fn register_thread(&self) -> barrier::SynchronizationBarrierGuard {
                match self {
                $(
                    Optimizers::$tag(val) => val.sync_barrier.register_thread(),
                )*
                }
            }
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

        impl Synchronizable for Optimizers {
            fn synchronized(&self, num_threads: usize) -> Vec<SynchronizedOptimizer<Self>> {
                (0..num_threads)
                    .map(|_| SynchronizedOptimizer::new(self, self.register_thread()))
                    .collect()
            }
        }
    }
}

impl_optimizer_enum!((SGD, SGD), (Adagrad, Adagrad), (Adam, Adam));
