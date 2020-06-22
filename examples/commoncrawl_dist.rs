//! # Distributed parsing and analysis of 3.25 billion webpages
//!
//! This example finds the most prevalent 100 IP addresses in the 3.25 billion
//! page, 255 TiB Common Crawl dataset.
//!
//! The download, parsing and analysis is farmed out to a process pool
//! leveraging Amadeus, the distributed data processing library for Rust.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example common_crawl --release -- 16
//! ```
//!
//! where `16` is the number of processes with which to initialize the pool.
//! Defaults to the maximum available if omitted.
//!
//! It can also be run distributed on a [`constellation`](https://github.com/constellation-rs/constellation)
//! cluster like so:
//!
//! ```bash
//! cargo deploy 10.0.0.1 --example common_crawl --release -- 1000
//! ```
//!
//! where `10.0.0.1` is the address of the master. See [here](https://github.com/constellation-rs/constellation)
//! for instructions on setting up the cluster.

use amadeus::dist::prelude::*;
use constellation::{init, Resources};
use data::Webpage;
use std::env;

#[allow(unreachable_code)]
fn main() {
	init(Resources::default());

	tokio::runtime::Builder::new()
		.threaded_scheduler()
		.enable_all()
		.build()
		.unwrap()
		.block_on(async {
			return; // TODO: runs for a long time

			// Accept the number of processes at the command line, defaulting to the maximum available
			let processes = env::args().nth(1).and_then(|arg| arg.parse::<usize>().ok());

			let pool = ProcessPool::new(processes, None, Resources::default()).unwrap();

			let webpages = CommonCrawl::new("CC-MAIN-2020-24").await.unwrap();

			let (count, (most_frequent_ips, most_diverse_ips)) = webpages
				.dist_stream()
				.map(FnMut!(|webpage: Result<_, _>| webpage.unwrap()))
				.fork(
					&pool,
					Identity
						.map(FnMut!(|webpage: Webpage<'static>| webpage))
						.count(),
					(
						Identity
							.map(FnMut!(|webpage: &Webpage<'static>| webpage.ip))
							.most_frequent(100, 0.99, 2.0 / 1000.0),
						Identity
							.map(FnMut!(|webpage: &Webpage<'static>| {
								(webpage.ip, webpage.url.host_str().unwrap().to_owned())
							}))
							.most_distinct(100, 0.99, 2.0 / 1000.0, 0.0808),
					),
				)
				.await;

			println!("Of the {} webpages processed, these are the most prevalent host IP addresses: {:?}", count, most_frequent_ips);
			println!("and these are the IP addresses hosting the most distinct domains: {:?}", most_diverse_ips);
		})
}
