//! # Distributed parsing and analysis of 3.25 billion webpages
//!
//! This example finds the top 100k most included JavaScript scripts in the
//! 3.25 billion page, 255 TiB Common Crawl dataset.
//!
//! The download, parsing and analysis is farmed out to a process pool
//! leveraging Amadeus, the distributed data analysis library for Rust.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example common_crawl --release -- 16
//! ```
//! where `16` is the number of processes with which to initialize the pool.
//! Defaults to 10 if omitted.
//!
//! It can also be run distributed on a [`constellation`](https://github.com/.../constellation)
//! cluster like so:
//! ```bash
//! cargo deploy 10.0.0.1 --example common_crawl --release -- 1000
//! ```
//! where `10.0.0.1` is the address of the master. See [here](https://github.com/.../constellation)
//! for instructions on setting up the cluster.

#![warn(
	missing_copy_implementations,
	missing_debug_implementations,
	missing_docs,
	trivial_numeric_casts,
	unused_extern_crates,
	unused_import_braces,
	unused_qualifications,
	unused_results
)]
// from https://github.com/rust-unofficial/patterns/blob/master/anti_patterns/deny-warnings.md
// #![warn(clippy::pedantic)]
#![allow(where_clauses_object_safety, clippy::all)]

#[macro_use]
extern crate serde_closure;

use amadeus::prelude::*;
use constellation::{init, Resources};
use reqwest_resume::ClientExt;
use std::{
	env, io::{BufRead, BufReader}, time
};
use warc_parser::WebpageOwned;

fn main() {
	init(Resources::default());

	// Accept the number of processes at the command line, defaulting to 10
	let processes = env::args()
		.nth(1)
		.and_then(|arg| arg.parse::<usize>().ok())
		.unwrap_or(5);

	let pool = ProcessPool::new(processes, Resources::default()).unwrap();
	// let pool = amadeus::no_pool::NoPool;

	let body = reqwest::get(
		"http://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2018-30/warc.paths.gz",
	)
	.unwrap();
	let body = flate2::read::MultiGzDecoder::new(body); // Content-Encoding isn't set, so decode manually

	let top: (
		((
			// Vec<u32>,
			),),
		(
			u32,
			// std::collections::HashSet<u32>,
			streaming_algorithms::Top<u32,usize>,
			streaming_algorithms::Top<usize,streaming_algorithms::HyperLogLogMagnitude<Vec<u8>>>,
			streaming_algorithms::SampleUnstable<u32>,
		),
	) = BufReader::new(body)
		.lines()
		.map(|url| format!("http://commoncrawl.s3.amazonaws.com/{}", url.unwrap()))
		.take(7)
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
			let body = flate2::read::MultiGzDecoder::new(body);
			warc_parser::WarcParser::new(body).take(1000).map(Result::unwrap)
		}))
		.multi(
			&pool,
			((
				// Identity
				// 	.map(FnMut!(|x: WebpageOwned| -> usize { x.contents.len() }))
				// 	.map(FnMut!(|x: usize| -> u32 { x as u32 }))
				// 	.collect(),
				// (),
				// Identity
				// 	.map(FnMut!(|x: WebpageOwned| -> usize {
				// 		x.contents.len()
				// 	}))
				// 	.map(FnMut!(|x: usize| -> u32 { x as u32 }))
				// 	.collect(),
			),),
			(
				Identity
					.map(FnMut!(|x: &WebpageOwned| -> usize { x.contents.len() }))
					.map(FnMut!(|x: usize| -> u32 { x as u32 }))
					.fold(
						FnMut!(|| 0_u32),
						FnMut!(|a: u32, b: either::Either<u32, u32>| a + b.into_inner()),
					),
				// .sum(),
				// Identity
				// 	.map(FnMut!(|x: &WebpageOwned| -> usize { x.contents.len() }))
				// 	.map(FnMut!(|x: usize| -> u32 { x as u32 }))
				// 	.collect(),
				Identity
					.map(FnMut!(|x: &WebpageOwned| -> usize { x.contents.len() }))
					.map(FnMut!(|x: usize| -> u32 { x as u32 }))
					.most_frequent(100, 0.99, 2.0/1000.0),
				Identity
					.map(FnMut!(|x: &WebpageOwned| { (x.contents.len(),x.contents[..5].to_owned()) }))
					.most_distinct(100, 0.99, 2.0/1000.0, 0.0808),
				Identity
					.cloned()
					.map(FnMut!(|x: WebpageOwned| -> usize { x.contents.len() }))
					.map(FnMut!(|x: usize| -> u32 { x as u32 }))
					.sample_unstable(100),
			),
		);
	println!("{:?}", top);
}
