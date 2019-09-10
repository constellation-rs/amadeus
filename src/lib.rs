//! Harmonious distributed data analysis in Rust
//!
//! **[Crates.io](https://crates.io/crates/amadeus) │ [Repo](https://github.com/alecmocatta/amadeus)**
//!
//! This library is very nascent. 3 parts: process pool; sources/sinks (Data/Value); analytics;

#![doc(html_root_url = "https://docs.rs/amadeus/0.1.1")]
#![doc(
	html_logo_url = "https://raw.githubusercontent.com/alecmocatta/amadeus/master/logo.svg?sanitize=true"
)]
#![feature(
	unboxed_closures,
	never_type,
	specialization,
	core_intrinsics,
	step_trait,
	test,
	// type_alias_impl_trait,
	bind_by_move_pattern_guards
)]
#![warn(
	// missing_copy_implementations,
	// missing_debug_implementations,
	// missing_docs,
	trivial_numeric_casts,
	unused_import_braces,
	unused_qualifications,
	unused_results,
	// unreachable_pub,
	clippy::pedantic,
)]
#![allow(
	where_clauses_object_safety,
	clippy::inline_always,
	clippy::module_name_repetitions,
	clippy::similar_names,
	clippy::needless_pass_by_value,
	clippy::trivially_copy_pass_by_ref,
	clippy::if_not_else,
	clippy::type_complexity,
	clippy::unseparated_literal_suffix,
	clippy::doc_markdown,
	clippy::use_self,
	clippy::module_inception,
	clippy::unreadable_literal,
	clippy::default_trait_access,
	clippy::match_same_arms
)]

#[macro_use]
extern crate serde_closure;

pub mod data;
pub mod pool;
pub mod source;

pub use amadeus_core::{dist_iter, into_dist_iter};

#[doc(inline)]
pub use crate::{
	data::{Data, Value}, dist_iter::{DistributedIterator, FromDistributedIterator}, into_dist_iter::{IntoDistributedIterator, IteratorExt}, pool::util::FutureExt1
};

pub mod prelude {
	#[cfg(feature = "constellation")]
	#[doc(inline)]
	pub use super::pool::ProcessPool;
	#[cfg(feature = "aws")]
	#[doc(inline)]
	pub use super::source::aws::{AwsError, AwsRegion, CloudfrontRow, S3Directory, S3File};
	#[doc(inline)]
	pub use super::{
		data, data::{Date, Decimal, Downcast, DowncastImpl, Enum, Group, List, Map, Time, Timestamp}, dist_iter::{DistributedIteratorMulti, Identity}, pool::LocalPool, pool::ThreadPool, source::*, Data, DistributedIterator, FromDistributedIterator, FutureExt1, IntoDistributedIterator, IteratorExt, Value
	};
	#[doc(inline)]
	pub use amadeus_core::pool::{LocalPool as _, ProcessPool as _, ThreadPool as _};
}
