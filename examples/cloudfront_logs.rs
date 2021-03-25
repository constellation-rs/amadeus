//! # Web log analytics
//!
//! This example
//!
//! The download, parsing and analysis is farmed out to a thread pool
//! leveraging Amadeus, the distributed data processing library for Rust.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example cloudfront_logs --release
//! ```

use amadeus::prelude::*;

#[allow(unreachable_code)]
#[tokio::main]
async fn main() {
	let pool = &ThreadPool::new(None, None).unwrap();

	let rows = Cloudfront::new_with(
		AwsRegion::UsEast1,
		"us-east-1.data-analytics",
		"cflogworkshop/raw/cf-accesslogs/",
		AwsCredentials::Anonymous,
	)
	.await
	.unwrap();

	let (sample, histogram) = rows
		.par_stream()
		.map(Result::unwrap)
		.fork(
			pool,
			Identity.sample_unstable(10),
			Identity
				.map(|row: &CloudfrontRow| (row.time.truncate_minutes(60), ()))
				.group_by(Identity.count()),
		)
		.await;

	let mut histogram = histogram.into_iter().collect::<Vec<_>>();
	histogram.sort();

	assert_eq!(histogram.iter().map(|(_, c)| c).sum::<usize>(), 207_928);

	println!("sample: {:#?}", sample);
	println!(
		"histogram:\n    {}",
		histogram
			.into_iter()
			.map(|(time, count)| format!("{}: {}", time, count))
			.collect::<Vec<_>>()
			.join("\n    ")
	);
}
