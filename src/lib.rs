//! Performant implementations of various [streaming algorithms](https://en.wikipedia.org/wiki/Streaming_algorithm).
//!
//! **[Crates.io](https://crates.io/crates/streaming_algorithms) │ [Repo](https://github.com/alecmocatta/streaming_algorithms)**
//!
//! This library is a work in progress. See the docs for what algorithms are currently implemented.
//!
//! See [here](https://gist.github.com/debasishg/8172796) for a good list of algorithms to be implemented.
//!
//! As these implementations are often in hot code paths, unsafe is used, albeit only when justified.
//!
//! This library leverages the following prioritisation when deciding whether `unsafe` is justified for a particular implementation:
//!  1. Asymptotically optimal algorithm
//!  2. Trivial safety (i.e. no `unsafe` at all or extremely limited `unsafe` trivially contained to one or two lines)
//!  3. Constant-factor optimisations

#![doc(html_root_url = "https://docs.rs/streaming_algorithms/0.1.0")]
#![feature(nll)]
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
#![cfg_attr(feature = "cargo-clippy", warn(clippy_pedantic))]
#![cfg_attr(
	feature = "cargo-clippy",
	allow(
		inline_always,
		stutter,
		if_not_else,
		op_ref,
		needless_pass_by_value
	)
)]

extern crate twox_hash;
#[macro_use]
extern crate serde_derive;
extern crate rand;

mod linked_list;
mod most_frequent;
mod ordered_linked_list;
mod sample;

pub use most_frequent::*;
pub use sample::*;
