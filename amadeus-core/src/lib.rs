#![doc(html_root_url = "https://docs.rs/amadeus-core/0.1.4")]
#![feature(atomic_min_max)]
#![feature(specialization)]
#![feature(never_type)]
#![feature(core_intrinsics)]
#![feature(read_initializer)]

pub mod dist_iter;
pub mod file;
pub mod into_dist_iter;
pub mod misc_serde;
pub mod pool;
mod source;
pub mod util;

pub use source::*;
