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
// #![allow(dead_code, stable_features)]
#![warn(clippy::pedantic)]
#![allow(
	where_clauses_object_safety,
)]

#[macro_use]
extern crate serde_closure;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate amadeus;
extern crate constellation;
extern crate flate2;
extern crate rand;
extern crate reqwest;
extern crate reqwest_resume;
extern crate select;
extern crate streaming_algorithms;
extern crate warc_parser;

use amadeus::{distributed_iterator, process_pool};
use constellation::{init, Resources};
use distributed_iterator::{DistributedIterator, DistributedIteratorExecute, IteratorExt};
use rand::Rng;
use reqwest_resume::ClientExt;
use std::{
	any, collections::{HashSet, VecDeque}, env, io::{self, BufRead, BufReader, Read}, marker, mem, str, sync::{atomic, Arc, Mutex, RwLock}, thread, time
};
use streaming_algorithms::Top;

fn main() {
	init(Resources::default());

	// Accept the number of processes at the command line, defaulting to 10
	let processes = env::args()
		.nth(1)
		.and_then(|arg| arg.parse::<usize>().ok())
		.unwrap_or(5);

	let pool = process_pool::ProcessPool::new(processes, Resources::default());

	let body = reqwest::get(
		"http://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2018-30/warc.paths.gz",
	)
	.unwrap();
	let body = flate2::read::MultiGzDecoder::new(body); // Content-Encoding isn't set, so decode manually

	let top: (Vec<u32>, Vec<u32>) = BufReader::new(body)
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
			let mut parser = warc_parser::WarcParser::new(body);
			(0..)
				.map(move |_| parser.next().unwrap().map(|x| x.to_owned()))
				.take_while(|x| x.is_some())
				.map(|x| x.unwrap())
		}))
		.execute(
			&pool,
			distributed_iterator::Identity
				.map(FnMut!(|x: warc_parser::WebpageOwned| -> usize {
					x.contents.len()
				}))
				.map(FnMut!(|x: usize| -> u32 { x as u32 }))
				.collect(),
			distributed_iterator::Identity
				.map(FnMut!(|x: &warc_parser::WebpageOwned| -> usize {
					x.contents.len()
				}))
				.map(FnMut!(|x: usize| -> u32 { x as u32 }))
				.collect(),
		);

	println!("{:?}", top);
}
