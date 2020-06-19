//! # Distributed parsing and analysis of 3.25 billion webpages
//!
//! This example finds the most prevalent 100 IP addresses in the 3.25 billion
//! page, 255 TiB Common Crawl dataset.
//!
//! The download, parsing and analysis is farmed out to a process pool
//! leveraging Amadeus, the distributed data analysis library for Rust.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example common_crawl --release
//! ```

use amadeus::prelude::*;
// use data::CloudfrontRow;

#[allow(unreachable_code)]
#[tokio::main]
async fn main() {
	let _pool = &ThreadPool::new(None).unwrap();

	let _rows = Cloudfront::new_with(
		AwsRegion::UsEast1,
		"us-east-1.data-analytics",
		"cflogworkshop/raw/cf-accesslogs/",
		AwsCredentials::Anonymous,
	)
	.await
	.unwrap();

	// let (sample, (most_visited_pages, count2)) = rows
	// 	.par_stream()
	// 	.pipe_fork(
	// 		pool,
	// 		Identity.sample(),
	// 		(
	// 			Identity
	// 				.map(FnMut!(|webpage: &Webpage<'static>| webpage.ip))
	// 				.most_frequent(100, 0.99, 2.0 / 1000.0),
	// 			Identity
	// 				.map(FnMut!(|webpage: &Webpage<'static>| {
	// 					(webpage.ip, webpage.url.host_str().unwrap().to_owned())
	// 				}))
	// 				.most_distinct(100, 0.99, 2.0 / 1000.0, 0.0808),
	// 			Identity.cloned().count()
	// 		),
	// 	)
	// 	.await;
	// assert_eq!(count, count2);
	// assert_eq!(count, 207_928);

	// let (count, (most_frequent_ips, most_diverse_ips)) = webpages
	// 	.dist_stream()
	// 	.map(FnMut!(|webpage: Result<_, _>| webpage.unwrap()))
	// 	.pipe_fork(
	// 		&pool,
	// 		Identity
	// 			.map(FnMut!(|webpage: Webpage<'static>| webpage))
	// 			.count(),
	// 		(
	// 		),
	// 	)
	// 	.await;

	// println!(
	// 	"Of the {} webpages processed, these are the most prevalent host IP addresses: {:?}",
	// 	count, most_frequent_ips
	// );
	// println!(
	// 	"and these are the IP addresses hosting the most distinct domains: {:?}",
	// 	most_diverse_ips
	// );
}
