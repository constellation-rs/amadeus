//! Harmonious distributed data processing & analysis in Rust.
//!
//! <p style="font-family: 'Fira Sans',sans-serif;padding:0.3em 0"><strong>
//! <a href="https://crates.io/crates/amadeus">📦&nbsp;&nbsp;Crates.io</a>&nbsp;&nbsp;│&nbsp;&nbsp;<a href="https://github.com/constellation-rs/amadeus">📑&nbsp;&nbsp;GitHub</a>&nbsp;&nbsp;│&nbsp;&nbsp;<a href="https://constellation.zulipchat.com/#narrow/stream/213231-amadeus">💬&nbsp;&nbsp;Chat</a>
//! </strong></p>
//!
//! This is a support crate of [Amadeus](https://github.com/constellation-rs/amadeus) and is not intended to be used directly. All functionality is re-exposed in [`amadeus`](https://docs.rs/amadeus/0.3/amadeus/).

#![doc(html_root_url = "https://docs.rs/amadeus-core/0.3.5")]
#![cfg_attr(nightly, feature(unboxed_closures))]
#![recursion_limit = "25600"]
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
	clippy::module_name_repetitions,
	clippy::if_not_else,
	clippy::similar_names,
	clippy::type_repetition_in_bounds,
	clippy::missing_errors_doc,
	clippy::must_use_candidate,
	clippy::unsafe_derive_deserialize,
	clippy::inline_always,
	clippy::option_option,
	clippy::default_trait_access,
	clippy::filter_map,
	clippy::wildcard_imports,
	clippy::needless_pass_by_value
)]
#![deny(unsafe_code)]

macro_rules! impl_par_dist {
	($($body:tt)*) => {
		$($body)*
		const _: () = {
			use $crate::impl_par_dist::*;
			#[allow(unused_imports)]
			use $crate::impl_par_dist::{combiner_par_sink,folder_par_sink};
			$($body)*
		};
	}
}
mod impl_par_dist {
	#[allow(unused_imports)]
	pub(crate) use crate::{
		combiner_dist_sink as combiner_par_sink, folder_dist_sink as folder_par_sink, par_pipe::DistributedPipe as ParallelPipe, par_sink::{DistributedSink as ParallelSink, FromDistributedStream as FromParallelStream}, par_stream::DistributedStream as ParallelStream, pool::ProcessSend as Send
	};
}

macro_rules! impl_par_dist_rename {
	($($body:tt)*) => {
		$($body)*
		rename! { [
			ParallelStream DistributedStream
			ParallelSink DistributedSink
			ParallelPipe DistributedPipe
			FromParallelStream FromDistributedStream
			IntoParallelStream IntoDistributedStream
			ParStream DistStream
			Send ProcessSend
			IterParStream IterDistStream
			into_par_stream into_dist_stream
			par_stream dist_stream
			assert_parallel_sink assert_distributed_sink
			assert_parallel_pipe assert_distributed_pipe
			assert_parallel_stream assert_distributed_stream
		] $($body)* }
	}
}
macro_rules! rename {
	([$($from:ident $to:ident)*] $($body:tt)*) => (rename!(@inner [$] [$($from $to)*] $($body)*););
	(@inner [$d:tt] [$($from:ident $to:ident)*] $($body:tt)*) => (
		macro_rules! __rename {
			$(
				(@munch [$d ($d done:tt)*] $from $d ($d body:tt)*) => (__rename!{@munch [$d ($d done)* $to] $d ($d body)*});
			)*
			(@munch [$d ($d done:tt)*] { $d ($d head:tt)* } $d ($d body:tt)*) => (__rename!{@munch [$d ($d done)* { __rename!{$d ($d head)*} }] $d ($d body)*});
			(@munch [$d ($d done:tt)*] $d head:tt $d ($d body:tt)*) => (__rename!{@munch [$d ($d done)* $d head] $d ($d body)*});
			(@munch [$d ($d done:tt)*]) => ($d ($d done)*);
			($d ($d body:tt)*) => (__rename!{@munch [] $d ($d body)*});
		}
		__rename!($($body)*);
	);
}

pub mod file;
pub mod into_par_stream;
pub mod misc_serde;
pub mod par_pipe;
pub mod par_sink;
pub mod par_stream;
pub mod pipe;
pub mod pool;
mod source;
pub mod util;

pub use source::*;
