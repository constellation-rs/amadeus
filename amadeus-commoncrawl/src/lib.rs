#![doc(html_root_url = "https://docs.rs/amadeus-commoncrawl/0.1.2")]
#![feature(type_alias_impl_trait)]

mod commoncrawl;
mod parser;

use flate2::read::MultiGzDecoder;
use reqwest_resume::ClientExt;
use serde_closure::*;
use std::{
	io::{self, BufRead, BufReader}, iter, time
};

use amadeus_core::{dist_iter::DistributedIterator, into_dist_iter::IteratorExt, Source};
use amadeus_types::Webpage;

use commoncrawl::WarcParser;

/// See https://commoncrawl.s3.amazonaws.com/crawl-data/index.html
/// CC-MAIN-2018-43
pub struct CommonCrawl {
	body: reqwest_resume::Response,
}
impl CommonCrawl {
	pub fn new(id: &str) -> Result<Self, reqwest::Error> {
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
			.send()?;
		Ok(Self { body })
	}
}

impl Source for CommonCrawl {
	type Item = Webpage<'static>;
	type Error = io::Error;

	type DistIter = impl DistributedIterator<Item = Result<Self::Item, Self::Error>>;
	type Iter = iter::Empty<Result<Self::Item, Self::Error>>;

	fn dist_iter(self) -> Self::DistIter {
		let body = MultiGzDecoder::new(self.body); // Content-Encoding isn't set, so decode manually

		BufReader::new(body)
			.lines()
			.map(FnMut!(|url: Result<String, io::Error>| -> String {
				format!("http://commoncrawl.s3.amazonaws.com/{}", url.unwrap())
			}))
			.dist()
			.flat_map(FnMut!(|url: String| {
				let body = reqwest::ClientBuilder::new()
					.timeout(time::Duration::new(120, 0))
					.build()
					.unwrap()
					.resumable()
					.get(url.parse().unwrap())
					.send()
					.unwrap();
				let body = MultiGzDecoder::new(body);
				WarcParser::new(body)
			}))
	}
	fn iter(self) -> Self::Iter {
		iter::empty()
	}
}
