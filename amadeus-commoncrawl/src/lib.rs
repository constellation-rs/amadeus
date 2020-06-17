#![doc(html_root_url = "https://docs.rs/amadeus-commoncrawl/0.2.0")]
#![feature(type_alias_impl_trait)]

mod commoncrawl;
mod parser;

use async_compression::futures::bufread::GzipDecoder; // TODO: use stream or https://github.com/alexcrichton/flate2-rs/pull/214
use futures::{io::BufReader, AsyncBufReadExt, FutureExt, StreamExt, TryStreamExt};
use reqwest_resume::ClientExt;
use serde_closure::*;
use std::{io, time};

use amadeus_core::{
	into_par_stream::IntoDistributedStream, par_stream::{DistributedStream, ParallelStream}, util::DistParStream, Source
};
use amadeus_types::Webpage;

use commoncrawl::WarcParser;

/// See https://commoncrawl.s3.amazonaws.com/crawl-data/index.html
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
			.map(FnMut!(|url: Result<String, io::Error>| -> String {
				format!("http://commoncrawl.s3.amazonaws.com/{}", url.unwrap())
			}))
			.collect()
			.await;
		Ok(Self { urls })
	}
}

impl Source for CommonCrawl {
	type Item = Webpage<'static>;
	type Error = io::Error;

	#[cfg(not(feature = "doc"))]
	type ParStream = impl ParallelStream<Item = Result<Self::Item, Self::Error>>;
	#[cfg(feature = "doc")]
	type ParStream = amadeus_core::util::ImplParallelStream<Result<Self::Item, Self::Error>>;
	#[cfg(not(feature = "doc"))]
	type DistStream = impl DistributedStream<Item = Result<Self::Item, Self::Error>>;
	#[cfg(feature = "doc")]
	type DistStream = amadeus_core::util::ImplDistributedStream<Result<Self::Item, Self::Error>>;

	fn par_stream(self) -> Self::ParStream {
		DistParStream::new(self.dist_stream())
	}
	#[allow(clippy::let_and_return)]
	fn dist_stream(self) -> Self::DistStream {
		let ret = self
			.urls
			.into_dist_stream()
			.flat_map(FnMut!(|url: String| async move {
				let body = reqwest_resume::get(url.parse().unwrap()).await.unwrap();
				let body = body
					.bytes_stream()
					.map_err(|e| io::Error::new(io::ErrorKind::Other, e));
				let body = BufReader::new(body.into_async_read());
				let mut body = GzipDecoder::new(body); // Content-Encoding isn't set, so decode manually
				body.multiple_members(true);
				WarcParser::new(body)
			}
			.flatten_stream()));
		#[cfg(feature = "doc")]
		let ret = amadeus_core::util::ImplDistributedStream::new(ret);
		ret
	}
}
