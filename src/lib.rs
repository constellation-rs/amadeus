//! Harmonious distributed data processing & analysis in Rust.
//!
//! <p style="font-family: 'Fira Sans',sans-serif;padding:0.3em 0"><strong>
//! <a href="https://crates.io/crates/amadeus">📦&nbsp;&nbsp;Crates.io</a>&nbsp;&nbsp;│&nbsp;&nbsp;<a href="https://github.com/constellation-rs/amadeus">📑&nbsp;&nbsp;GitHub</a>&nbsp;&nbsp;│&nbsp;&nbsp;<a href="https://constellation.zulipchat.com/#narrow/stream/213231-amadeus">💬&nbsp;&nbsp;Chat</a>
//! </strong></p>

#![doc(html_root_url = "https://docs.rs/amadeus/0.4.2")]
#![doc(
	html_logo_url = "https://raw.githubusercontent.com/constellation-rs/amadeus/master/logo.svg?sanitize=true"
)]
#![cfg_attr(nightly, feature(unboxed_closures))]
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
	clippy::if_not_else,
	clippy::inline_always,
	clippy::missing_errors_doc,
	clippy::missing_safety_doc,
	clippy::module_name_repetitions,
	clippy::must_use_candidate,
	clippy::similar_names
)]
#![deny(unsafe_code)]

#[cfg(all(not(nightly), feature = "parquet"))]
compile_error!("The Amadeus Parquet connector currently requires nightly");

#[cfg(all(
	feature = "aws",
	feature = "parquet",
	feature = "constellation",
	doctest
))]
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
		pub use crate::pool::ProcessPool;
		#[cfg(feature = "aws")]
		#[doc(no_inline)]
		pub use crate::source::aws::{
			AwsCredentials, AwsError, AwsRegion, CloudfrontRow, S3Directory, S3File
		};
		#[doc(no_inline)]
		pub use crate::{
			data::{
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
	pub use crate::pool::ProcessPool;
	#[cfg(feature = "aws")]
	#[doc(no_inline)]
	pub use crate::source::aws::{
		AwsCredentials, AwsError, AwsRegion, CloudfrontRow, S3Directory, S3File
	};
	#[doc(no_inline)]
	pub use crate::{
		data::{
			Date, DateTime, DateTimeWithoutTimezone, DateWithoutTimezone, Decimal, Downcast, DowncastFrom, Enum, Group, Time, TimeWithoutTimezone, Timezone
		}, par_pipe::ParallelPipe, par_stream::Identity, pool::ThreadPool, source::*, Data, FromParallelStream, IntoParallelStream, IteratorExt, List, ParallelStream, Value
	};
}

#[cfg(feature = "aws")]
#[doc(hidden)]
pub use amadeus_aws;
#[cfg(feature = "commoncrawl")]
#[doc(hidden)]
pub use amadeus_commoncrawl;
#[doc(hidden)]
pub use amadeus_core;
#[cfg(feature = "parquet")]
#[doc(hidden)]
pub use amadeus_parquet;
#[cfg(feature = "postgres")]
#[doc(hidden)]
pub use amadeus_postgres;
#[cfg(feature = "amadeus-serde")]
#[doc(hidden)]
pub use amadeus_serde;
#[doc(hidden)]
pub use amadeus_types;
