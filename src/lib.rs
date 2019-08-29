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
pub mod no_pool;
pub mod process_pool;
pub mod source;
pub mod thread_pool;

mod impls {
	use super::process_pool;
	use serde::{de::DeserializeOwned, Serialize};
	impl amadeus_core::dist_iter::ProcessPool for process_pool::ProcessPool {
		type JoinHandle = process_pool::JoinHandle<Box<dyn serde_traitobject::Any + Send>>;

		fn processes(&self) -> usize {
			process_pool::ProcessPool::processes(self)
		}
		fn spawn<F, T>(&self, work: F) -> Self::JoinHandle
		where
			F: FnOnce() -> T + Serialize + DeserializeOwned + 'static,
			T: Send + Serialize + DeserializeOwned + 'static,
		{
			process_pool::ProcessPool::spawn(
				self,
				FnOnce!([work] move || Box::new(work()) as Box<dyn serde_traitobject::Any + Send>),
			)
		}
	}
	impl amadeus_core::dist_iter::JoinHandle
		for process_pool::JoinHandle<Box<dyn serde_traitobject::Any + Send>>
	{
		fn join<T>(self) -> Result<T, ()>
		where
			T: Send + Serialize + DeserializeOwned + 'static,
		{
			process_pool::JoinHandle::join(self).map(|x| *x.into_any_send().downcast().unwrap())
		}
	}
}
pub use amadeus_core::{dist_iter, into_dist_iter};

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
