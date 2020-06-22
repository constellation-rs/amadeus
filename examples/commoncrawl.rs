//! # Distributed parsing and analysis of 3.25 billion webpages
//!
//! This example finds the most prevalent 100 IP addresses in the 3.25 billion
//! page, 255 TiB Common Crawl dataset.
//!
//! The download, parsing and analysis is farmed out to a thread pool
//! leveraging Amadeus, the distributed data processing library for Rust.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example common_crawl --release
//! ```

use amadeus::dist::prelude::*;
use data::Webpage;

#[allow(unreachable_code)]
#[tokio::main]
async fn main() {
	return; // TODO: runs for a long time

	let pool = ThreadPool::new(None).unwrap();

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

	println!(
		"Of the {} webpages processed, these are the most prevalent host IP addresses: {:?}",
		count, most_frequent_ips
	);
	println!(
		"and these are the IP addresses hosting the most distinct domains: {:?}",
		most_diverse_ips
	);
}
