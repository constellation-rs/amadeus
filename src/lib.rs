//! SIMD-accelerated implementations of various [streaming algorithms](https://en.wikipedia.org/wiki/Streaming_algorithm).
//!
//! **[Crates.io](https://crates.io/crates/streaming_algorithms) │ [Repo](https://github.com/alecmocatta/streaming_algorithms)**
//!
//! This library is a work in progress. PRs are very welcome! Currently implemented algorithms include:
//!
//!  * Count–min sketch
//!  * Top k (Count–min sketch plus a doubly linked hashmap to track heavy hitters / top k keys when ordered by aggregated value)
//!  * HyperLogLog
//!  * Reservoir sampling
//!
//! A goal of this library is to enable composition of these algorithms; for example Top k + HyperLogLog to enable an approximate version of something akin to `SELECT key FROM table GROUP BY key ORDER BY COUNT(DISTINCT value) DESC LIMIT k`.
//!
//! Run your application with `RUSTFLAGS="-C target-cpu=native"` to benefit from the SIMD-acceleration like so:
//!
//! ```bash
//! 	RUSTFLAGS="-C target-cpu=native" cargo run --release
//! ```
//!
//! See [this gist](https://gist.github.com/debasishg/8172796) for a good list of further algorithms to be implemented. Other resources are [Probabilistic data structures – Wikipedia](https://en.wikipedia.org/wiki/Category:Probabilistic_data_structures), [DataSketches – A similar Java library originating at Yahoo](https://datasketches.github.io/), and [Algebird  – A similar Java library originating at Twitter](https://github.com/twitter/algebird).
//!
//! As these implementations are often in hot code paths, unsafe is used, albeit only when necessary to a) achieve the asymptotically optimal algorithm or b) mitigate an observed bottleneck.

#![doc(html_root_url = "https://docs.rs/streaming_algorithms/0.1.0")]
#![feature(
	nll,
	tool_lints,
	non_modrs_mods,
	specialization,
	stdsimd,
	mmx_target_feature,
	convert_id
)]
#![warn(
	missing_copy_implementations,
	missing_debug_implementations,
	missing_docs,
	trivial_numeric_casts,
	unused_extern_crates,
	unused_import_braces,
	unused_qualifications,
	unused_results,
)] // from https://github.com/rust-unofficial/patterns/blob/master/anti_patterns/deny-warnings.md
#![allow(dead_code, stable_features)]
#![warn(clippy::pedantic)]
#![allow(
	clippy::doc_markdown,
	clippy::inline_always,
	clippy::stutter,
	clippy::if_not_else,
	clippy::op_ref,
	clippy::needless_pass_by_value,
	clippy::cast_possible_truncation,
	clippy::cast_sign_loss,
	clippy::cast_precision_loss,
	clippy::cast_lossless,
	clippy::float_cmp,
)]

extern crate twox_hash;
#[macro_use]
extern crate serde_derive;
extern crate packed_simd;
extern crate rand;
extern crate serde;

mod count_min;
mod distinct;
mod linked_list;
mod ordered_linked_list;
mod sample;
mod top;
mod traits;

pub use count_min::*;
pub use distinct::*;
pub use sample::*;
pub use top::*;
pub use traits::*;
