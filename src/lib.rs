//! Harmonious distributed data processing & analysis in Rust.
//!
//! <p style="font-family: 'Fira Sans',sans-serif;padding:0.3em 0"><strong>
//! <a href="https://crates.io/crates/amadeus">📦&nbsp;&nbsp;Crates.io</a>&nbsp;&nbsp;│&nbsp;&nbsp;<a href="https://github.com/constellation-rs/amadeus">📑&nbsp;&nbsp;GitHub</a>&nbsp;&nbsp;│&nbsp;&nbsp;<a href="https://constellation.zulipchat.com/#narrow/stream/213231-amadeus">💬&nbsp;&nbsp;Chat</a>
//! </strong></p>

#![doc(html_root_url = "https://docs.rs/amadeus/0.2.2")]
#![doc(
	html_logo_url = "https://raw.githubusercontent.com/constellation-rs/amadeus/master/logo.svg?sanitize=true"
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
	unreachable_pub,
	clippy::pedantic,
)]
#![allow(
	incomplete_features,
	clippy::module_name_repetitions,
	clippy::similar_names,
	clippy::if_not_else,
	clippy::must_use_candidate,
	clippy::missing_errors_doc
)]
#![deny(unsafe_code)]

#[cfg(doctest)]
doc_comment::doctest!("../README.md");

pub mod data;
pub mod pool;
pub mod source;

pub use amadeus_core::{into_par_stream, par_pipe, par_sink, par_stream};

#[doc(inline)]
pub use crate::{
	data::{Data, List, Value}, into_par_stream::{IntoDistributedStream, IntoParallelStream, IteratorExt}, par_sink::{FromDistributedStream, FromParallelStream}, par_stream::{DistributedStream, ParallelStream}, source::{Destination, Source}
};

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
			}, par_pipe::DistributedPipe, par_stream::Identity, pool::ThreadPool, source::*, Data, DistributedStream, FromDistributedStream, IntoDistributedStream, IteratorExt, List, Value
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
		}, par_pipe::ParallelPipe, par_stream::Identity, pool::ThreadPool, source::*, Data, FromParallelStream, IntoParallelStream, IteratorExt, List, ParallelStream, Value
	};
}
