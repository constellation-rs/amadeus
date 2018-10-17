//! Harmonious distributed data analysis in Rust
//!
//! **[Crates.io](https://crates.io/crates/amadeus) │ [Repo](https://github.com/alecmocatta/amadeus)**
//!
//! This library is very nascent.

#![doc(html_root_url = "https://docs.rs/amadeus/0.1.1")]
#![feature(
	unboxed_closures,
	fnbox,
	never_type,
	fn_traits,
	specialization,
	core_intrinsics,
	nll
)]
#![warn(
	// missing_copy_implementations,
	// missing_debug_implementations,
	// missing_docs,
	// trivial_numeric_casts,
	// unused_extern_crates,
	// unused_import_braces,
	// unused_qualifications,
	// unused_results
)]
// from https://github.com/rust-unofficial/patterns/blob/master/anti_patterns/deny-warnings.md
// #![allow(dead_code, stable_features)]
#![warn(clippy::pedantic)]
#![allow(
	where_clauses_object_safety,
	clippy::doc_markdown,
	clippy::inline_always,
	clippy::stutter,
	clippy::similar_names,
	clippy::if_not_else,
	clippy::op_ref,
	clippy::needless_pass_by_value,
	clippy::items_after_statements,
	clippy::float_cmp
)]

#[macro_use]
extern crate serde_closure;
extern crate serde;
extern crate serde_traitobject;
#[macro_use]
extern crate serde_derive;
extern crate constellation;
extern crate either;
extern crate rand;
extern crate streaming_algorithms;

pub mod dist_iter;
pub mod into_dist_iter;
pub mod process_pool;
