//! Optimization module.
//!
//! Contains a number of optimizers.
mod adagrad;
mod adam;
mod barrier;
mod sgd;

/// Core trait implemented by all optimizer methods.
pub trait Optimizer {
    /// Perform a single SGD step.
    fn step(&self);
}

pub use self::adagrad::Adagrad;
pub use self::adam::Adam;
pub use self::barrier::SynchronizationBarrier;
pub use self::sgd::SGD;
