//! SIMD-accelerated implementations of various [streaming algorithms](https://en.wikipedia.org/wiki/Streaming_algorithm).
//!
//! <p style="font-family: 'Fira Sans',sans-serif;padding:0.3em 0"><strong>
//! <a href="https://crates.io/crates/streaming_algorithms">📦&nbsp;&nbsp;Crates.io</a>&nbsp;&nbsp;│&nbsp;&nbsp;<a href="https://github.com/alecmocatta/streaming_algorithms">📑&nbsp;&nbsp;GitHub</a>&nbsp;&nbsp;│&nbsp;&nbsp;<a href="https://constellation.zulipchat.com/#narrow/stream/213236-subprojects">💬&nbsp;&nbsp;Chat</a>
//! </strong></p>
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
//! Run your application with `RUSTFLAGS="-C target-cpu=native"` and the `nightly` feature to benefit from the SIMD-acceleration like so:
//!
//! ```bash
//! RUSTFLAGS="-C target-cpu=native" cargo run --features "streaming_algorithms/nightly" --release
//! ```
//!
//! See [this gist](https://gist.github.com/debasishg/8172796) for a good list of further algorithms to be implemented. Other resources are [Probabilistic data structures – Wikipedia](https://en.wikipedia.org/wiki/Category:Probabilistic_data_structures), [DataSketches – A similar Java library originating at Yahoo](https://datasketches.github.io/), and [Algebird  – A similar Java library originating at Twitter](https://github.com/twitter/algebird).
//!
//! As these implementations are often in hot code paths, unsafe is used, albeit only when necessary to a) achieve the asymptotically optimal algorithm or b) mitigate an observed bottleneck.

#![doc(html_root_url = "https://docs.rs/streaming_algorithms/0.3.0")]
#![warn(
	missing_copy_implementations,
	missing_debug_implementations,
	missing_docs,
	trivial_casts,
	trivial_numeric_casts,
	unused_import_braces,
	unused_qualifications,
	unused_results,
	clippy::pedantic
)]
// from https://github.com/rust-unofficial/patterns/blob/master/anti_patterns/deny-warnings.md
#![allow(
	dead_code,
	clippy::doc_markdown,
	clippy::inline_always,
	clippy::module_name_repetitions,
	clippy::if_not_else,
	clippy::needless_pass_by_value,
	clippy::suspicious_op_assign_impl,
	clippy::float_cmp,
	clippy::unsafe_derive_deserialize,
	clippy::must_use_candidate,
	clippy::unused_self
)]

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

// TODO: replace all instances of the following with a.try_into().unwrap() if/when that exists https://github.com/rust-lang/rust/pull/47857
#[allow(
	clippy::cast_possible_truncation,
	clippy::cast_sign_loss,
	clippy::cast_precision_loss,
	clippy::cast_lossless
)]
fn f64_to_usize(a: f64) -> usize {
	assert!(a.is_sign_positive() && a <= usize::max_value() as f64 && a.fract() == 0.0);
	a as usize
}

#[allow(
	clippy::cast_possible_truncation,
	clippy::cast_sign_loss,
	clippy::cast_precision_loss
)]
fn f64_to_u8(a: f64) -> u8 {
	assert!(a.is_sign_positive() && a <= f64::from(u8::max_value()) && a.fract() == 0.0);
	a as u8
}

#[allow(clippy::cast_precision_loss, clippy::cast_lossless)]
fn usize_to_f64(a: usize) -> f64 {
	assert!(a as u64 <= 1_u64 << 53);
	a as f64
}
#[allow(clippy::cast_precision_loss)]
fn u64_to_f64(a: u64) -> f64 {
	assert!(a <= 1_u64 << 53);
	a as f64
}
