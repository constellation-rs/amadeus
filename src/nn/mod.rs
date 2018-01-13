//! Neural network layers.

pub mod recurrent;

use rand;
use rand::distributions::{IndependentSample, Normal};

use Arr;

/// Return a Xavier-normal initialised random array.
pub fn xavier_normal(rows: usize, cols: usize) -> Arr {
    let normal = Normal::new(0.0, 1.0 / (rows as f64).sqrt());
    Arr::zeros((rows, cols)).map(|_| normal.ind_sample(&mut rand::thread_rng()) as f32)
}
