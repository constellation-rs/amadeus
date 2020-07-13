//! Harmonious distributed data processing & analysis in Rust.
//!
//! <p style="font-family: 'Fira Sans',sans-serif;padding:0.3em 0"><strong>
//! <a href="https://crates.io/crates/amadeus">📦&nbsp;&nbsp;Crates.io</a>&nbsp;&nbsp;│&nbsp;&nbsp;<a href="https://github.com/constellation-rs/amadeus">📑&nbsp;&nbsp;GitHub</a>&nbsp;&nbsp;│&nbsp;&nbsp;<a href="https://constellation.zulipchat.com/#narrow/stream/213231-amadeus">💬&nbsp;&nbsp;Chat</a>
//! </strong></p>
//!
//! This is a support crate of [Amadeus](https://github.com/constellation-rs/amadeus) and is not intended to be used directly. These types are re-exposed in [`amadeus::source`](https://docs.rs/amadeus/0.3/amadeus/source/index.html).

#![doc(html_root_url = "https://docs.rs/amadeus-commoncrawl/0.3.1")]
#![cfg_attr(nightly, feature(type_alias_impl_trait))]
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
	clippy::doc_markdown,
	clippy::inline_always,
	clippy::missing_errors_doc
)]
#![deny(unsafe_code)]

mod commoncrawl;
mod parser;

use async_compression::futures::bufread::GzipDecoder; // TODO: use stream or https://github.com/alexcrichton/flate2-rs/pull/214
use futures::{io::BufReader, AsyncBufReadExt, FutureExt, Stream, StreamExt, TryStreamExt};
use reqwest_resume::ClientExt;
use serde_closure::FnMutNamed;
use std::{io, time};

use amadeus_core::{
	into_par_stream::IntoDistributedStream, par_stream::DistributedStream, util::DistParStream, Source
};
use amadeus_types::Webpage;

use commoncrawl::WarcParser;

/// See https://commoncrawl.s3.amazonaws.com/crawl-data/index.html
#[derive(Clone, Debug)]
pub struct CommonCrawl {
	urls: Vec<String>,
}
impl CommonCrawl {
	/// CC-MAIN-2020-24
	pub async fn new(id: &str) -> Result<Self, reqwest::Error> {
		let url = format!(
			"https://commoncrawl.s3.amazonaws.com/crawl-data/{}/warc.paths.gz",
			id
		);
		let body = reqwest::ClientBuilder::new()
			.timeout(time::Duration::new(120, 0))
			.build()
			.unwrap()
			.resumable()
			.get(url.parse().unwrap())
			.send();
		let body = body
			.await?
			.bytes_stream()
			.map_err(|e| io::Error::new(io::ErrorKind::Other, e));
		let body = BufReader::new(body.into_async_read());
		let mut body = GzipDecoder::new(body); // Content-Encoding isn't set, so decode manually
		body.multiple_members(true);

		let urls = BufReader::new(body)
			.lines()
			.map(|url: Result<String, io::Error>| -> String {
				format!("http://commoncrawl.s3.amazonaws.com/{}", url.unwrap())
			})
			.collect()
			.await;
		Ok(Self { urls })
	}
}

#[cfg(not(nightly))]
type Output = std::pin::Pin<Box<dyn Stream<Item = Result<Webpage<'static>, io::Error>> + Send>>;
#[cfg(nightly)]
type Output = impl Stream<Item = Result<Webpage<'static>, io::Error>> + Send;

FnMutNamed! {
	pub type Closure<> = |self|url=> String| -> Output where {
		#[allow(clippy::let_and_return)]
		let ret = async move {
				let body = reqwest_resume::get(url.parse().unwrap()).await.unwrap();
				let body = body
					.bytes_stream()
					.map_err(|e| io::Error::new(io::ErrorKind::Other, e));
				let body = BufReader::new(body.into_async_read());
				let mut body = GzipDecoder::new(body); // Content-Encoding isn't set, so decode manually
				body.multiple_members(true);
				WarcParser::new(body)
			}
			.flatten_stream();
		#[cfg(not(nightly))]
		let ret = ret.boxed();
		ret
	}
}

impl Source for CommonCrawl {
	type Item = Webpage<'static>;
	type Error = io::Error;

	type ParStream = DistParStream<Self::DistStream>;
	#[cfg(not(nightly))]
	#[allow(clippy::type_complexity)]
	type DistStream = amadeus_core::par_stream::FlatMap<
		amadeus_core::into_par_stream::IterDistStream<std::vec::IntoIter<String>>,
		Closure,
	>;
	#[cfg(nightly)]
	type DistStream = impl DistributedStream<Item = Result<Self::Item, Self::Error>>;

	fn par_stream(self) -> Self::ParStream {
		DistParStream::new(self.dist_stream())
	}
	#[allow(clippy::let_and_return)]
	fn dist_stream(self) -> Self::DistStream {
		self.urls.into_dist_stream().flat_map(Closure::new())
	}
}
