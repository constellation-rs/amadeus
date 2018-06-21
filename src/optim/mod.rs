//! Optimization module.
//!
//! Contains a number of optimizers.
use std::cell::Cell;
use {ParameterNode, Variable};
mod adagrad;
mod adam;
mod barrier;
mod sgd;

pub use self::adagrad::Adagrad;
pub use self::adam::Adam;
use self::barrier::SynchronizationBarrier;
pub use self::sgd::SGD;

/// Core trait implemented by all optimizer methods.
pub trait Optimizer {
    /// Perform a single SGD step.
    fn step(&self, parameters: &[Variable<ParameterNode>]);
}

/// Trait implemented by synchronizable optimizers.
///
/// Using a set of synchronized optimizers guarantees that parameter
/// updates will always happen in the same order, guaranteeing reproducible
/// results at the price of some performance relative to asynchronous parallel
/// optimization.
pub trait Synchronizable {
    /// Synchronize this optimizer, producing a set of synchronized optimimzers
    /// to be used by individual fitting threads.
    fn synchronized(&self, num_threads: usize) -> Vec<SynchronizedOptimizer<Self>>
    where
        Self: Sized,
    {
        self.synchronized_with_step(num_threads, 8)
    }
    /// Synchronize this optimizer, producing a set of synchronized optimimzers
    /// to be used by individual fitting threads. The threads will synchonize
    /// their updates every `step_size` steps.
    fn synchronized_with_step(
        &self,
        num_threads: usize,
        step_size: usize,
    ) -> Vec<SynchronizedOptimizer<Self>>
    where
        Self: Sized;
}

/// Synchronized optimizer wrapper.
#[derive(Debug)]
pub struct SynchronizedOptimizer<'a, T: 'a> {
    step_size: usize,
    num_updates: Cell<usize>,
    optimizer: &'a T,
    barrier_guard: barrier::SynchronizationBarrierGuard,
}

impl<'a, T: 'a> SynchronizedOptimizer<'a, T> {
    fn new(
        optimizer: &'a T,
        barrier_guard: barrier::SynchronizationBarrierGuard,
        step_size: usize,
    ) -> Self {
        SynchronizedOptimizer {
            step_size: step_size,
            num_updates: Cell::new(0),
            optimizer: optimizer,
            barrier_guard: barrier_guard,
        }
    }
}

impl<'a, T> Optimizer for SynchronizedOptimizer<'a, T>
where
    T: Optimizer,
{
    fn step(&self, parameters: &[Variable<ParameterNode>]) {
        self.num_updates.set(self.num_updates.get() + 1);

        if self.num_updates.get() == self.step_size {
            let _barrier = self.barrier_guard.synchronize();
            self.optimizer.step(parameters);

            self.num_updates.set(0);
        }
    }
}

impl<T> Synchronizable for T
where
    T: Optimizer + Sized,
{
    fn synchronized_with_step(
        &self,
        num_threads: usize,
        step_size: usize,
    ) -> Vec<SynchronizedOptimizer<T>> {
        let barrier = SynchronizationBarrier::default();

        (0..num_threads)
            .map(|_| SynchronizedOptimizer::new(self, barrier.register_thread(), step_size))
            .collect()
    }
}

macro_rules! impl_optimizer_enum {
    ($(($tag:ident, $type:ty)),*) => {
        /// Enum containing all optimizers.
        ///
        /// Makes runtime switching between optimizers slightly more ergonomic.
        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub enum Optimizers {
            $(
                #[allow(missing_docs)]
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
