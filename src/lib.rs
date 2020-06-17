//! Harmonious distributed data processing & analysis in Rust.
//!
//! <p style="font-family: 'Fira Sans',sans-serif;padding:0.3em 0"><strong>
//! <a href="https://crates.io/crates/amadeus">📦&nbsp;&nbsp;Crates.io</a>&nbsp;&nbsp;│&nbsp;&nbsp;<a href="https://github.com/constellation-rs/amadeus">📑&nbsp;&nbsp;GitHub</a>&nbsp;&nbsp;│&nbsp;&nbsp;<a href="https://constellation.zulipchat.com/#narrow/stream/213231-amadeus">💬&nbsp;&nbsp;Chat</a>
//! </strong></p>

#![doc(html_root_url = "https://docs.rs/amadeus/0.2.0")]
#![doc(
	html_logo_url = "https://raw.githubusercontent.com/alecmocatta/amadeus/master/logo.svg?sanitize=true"
)]
#![feature(specialization)]
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
	clippy::match_same_arms,
	clippy::must_use_candidate,
	clippy::missing_errors_doc
)]
#![deny(unsafe_code)]

pub mod data;
pub mod pool;
pub mod source;

pub use amadeus_core::{dist_pipe, dist_sink, dist_stream, into_dist_stream};

#[doc(inline)]
pub use crate::{
	data::{Data, List, Value}, dist_sink::{FromDistributedStream, FromParallelStream}, dist_stream::{DistributedStream, ParallelStream}, into_dist_stream::{IntoDistributedStream, IntoParallelStream, IteratorExt}, source::Source
};

// pub mod pool {
// 	pub use amadeus_core::pool::

pub mod dist {
	pub mod prelude {
		#[cfg(feature = "constellation")]
		#[doc(no_inline)]
		pub use super::super::pool::ProcessPool;
		#[cfg(feature = "aws")]
		#[doc(no_inline)]
		pub use super::super::source::aws::{
			AwsCredentials, AwsError, AwsRegion, CloudfrontRow, S3Directory, S3File
		};
		#[doc(no_inline)]
		pub use super::super::{
			data, data::{
				Date, DateTime, DateTimeWithoutTimezone, DateWithoutTimezone, Decimal, Downcast, DowncastFrom, Enum, Group, Time, TimeWithoutTimezone, Timezone
			}, dist_pipe::DistributedPipe, dist_stream::Identity, pool::ThreadPool, source::*, Data, DistributedStream, FromDistributedStream, IntoDistributedStream, IteratorExt, List, Value
		};
		#[doc(no_inline)]
		pub use serde_closure::{Fn, FnMut, FnOnce};
	}
}
pub mod prelude {
	#[cfg(feature = "constellation")]
	#[doc(no_inline)]
	pub use super::pool::ProcessPool;
	#[cfg(feature = "aws")]
	#[doc(no_inline)]
	pub use super::source::aws::{
		AwsCredentials, AwsError, AwsRegion, CloudfrontRow, S3Directory, S3File
	};
	#[doc(no_inline)]
	pub use super::{
		data, data::{
			Date, DateTime, DateTimeWithoutTimezone, DateWithoutTimezone, Decimal, Downcast, DowncastFrom, Enum, Group, Time, TimeWithoutTimezone, Timezone
		}, dist_pipe::ParallelPipe, dist_stream::Identity, pool::ThreadPool, source::*, Data, FromParallelStream, IntoParallelStream, IteratorExt, List, ParallelStream, Value
	};
	#[doc(no_inline)]
	pub use serde_closure::{Fn, FnMut, FnOnce};
}
