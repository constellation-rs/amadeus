//! Harmonious distributed data analysis in Rust
//!
//! **[Crates.io](https://crates.io/crates/amadeus) │ [Repo](https://github.com/alecmocatta/amadeus)**
//!
//! This library is very nascent. 3 parts: process pool; sources/sinks (Data/Value); analytics;

#![doc(html_root_url = "https://docs.rs/amadeus/0.1.1")]
#![feature(
	unboxed_closures,
	never_type,
	specialization,
	core_intrinsics,
	step_trait,
	test,
	type_alias_impl_trait,
	bind_by_move_pattern_guards
)]
#![warn(
	// missing_copy_implementations,
	// missing_debug_implementations,
	// missing_docs,
	trivial_numeric_casts,
	unused_extern_crates,
	unused_import_braces,
	unused_qualifications,
	unused_results
)]
// from https://github.com/rust-unofficial/patterns/blob/master/anti_patterns/deny-warnings.md
#![warn(clippy::pedantic)]
#![allow(
	where_clauses_object_safety,
	clippy::inline_always,
	clippy::stutter,
	clippy::similar_names,
	clippy::needless_pass_by_value,
	clippy::trivially_copy_pass_by_ref,
	clippy::unit_arg,
	clippy::if_not_else,
	clippy::type_complexity
)]

#[macro_use]
extern crate serde_closure;
#[macro_use]
extern crate serde_derive;
// extern crate test;

pub mod data;
pub mod dist_iter;
pub mod into_dist_iter;
pub mod no_pool;
pub mod process_pool;
pub mod source;
pub mod thread_pool;

#[doc(inline)]
pub use crate::dist_iter::{DistributedIterator, FromDistributedIterator};
#[doc(inline)]
pub use crate::into_dist_iter::{IntoDistributedIterator, IteratorExt};
#[doc(inline)]
pub use crate::process_pool::ProcessPool;

pub mod prelude {
	#[doc(inline)]
	pub use super::{
		dist_iter::{DistributedIteratorMulti, Identity}, process_pool::JoinHandle, source::*, DistributedIterator, FromDistributedIterator, IntoDistributedIterator, IteratorExt, ProcessPool
	};
}
