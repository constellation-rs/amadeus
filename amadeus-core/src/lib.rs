#![doc(html_root_url = "https://docs.rs/amadeus-core/0.1.7")]
#![feature(atomic_min_max)]
#![feature(never_type)]
#![feature(specialization)]
#![feature(read_initializer)]

pub mod dist_iter;
pub mod file;
pub mod into_dist_iter;
pub mod misc_serde;
pub mod pool;
mod source;
pub mod util;

pub use source::*;
