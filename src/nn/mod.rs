//! Neural network components.

pub mod losses;
pub mod lstm;

use rand;
use rand::distributions::{Distribution, Normal, Uniform};

use Arr;

/// Return a Xavier-normal initialised random array.
pub fn xavier_normal(rows: usize, cols: usize) -> Arr {
    let normal = Normal::new(0.0, 1.0 / (rows as f64).sqrt());
    Arr::zeros((rows, cols)).map(|_| normal.sample(&mut rand::thread_rng()) as f32)
}

/// Return a random matrix with values drawn uniformly from `(min, max)`.
pub fn uniform<R: rand::Rng>(rows: usize, cols: usize, min: f32, max: f32, rng: &mut R) -> Arr {
    let dist = Uniform::new(min, max);
    Arr::zeros((rows, cols)).map(|_| dist.sample(rng) as f32)
}
