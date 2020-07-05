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
	let pool = &ThreadPool::new(None).unwrap();

	let rows = Cloudfront::new_with(
		AwsRegion::UsEast1,
		"us-east-1.data-analytics",
		"cflogworkshop/raw/cf-accesslogs/",
		AwsCredentials::Anonymous,
	)
	.await
	.unwrap();

	let histogram = rows // (sample, (histogram, count)) = rows
		.par_stream()
		.map(Result::unwrap)
		.pipe(
			pool,
			// Identity.sample_unstable(10),
			// (
			Identity
				.map(|row: CloudfrontRow| (row.time.truncate_minutes(60), ()))
				.group_by(
					Identity.count()
					// || 0,
					// |count, fold| count + fold.map_left(|()| 1).into_inner(),
				),
			// Identity.map(|row: CloudfrontRow| (row.time.truncate_minutes(60), ())).group_by(Identity.count()),
			// 	Identity.count(),
			// ),
		)
		.await;
	let mut histogram = histogram.into_iter().collect::<Vec<_>>();
	histogram.sort();

	// println!("{} log lines analysed.", count);
	// println!("sample: {:#?}", sample);
	println!(
		"histogram:\n    {}",
		histogram
			.into_iter()
			.map(|(time, count)| format!("{}: {}", time, count))
			.collect::<Vec<_>>()
			.join("\n    ")
	);
}
