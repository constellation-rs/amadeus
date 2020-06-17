#![type_length_limit = "13470730"]
#![allow(clippy::suspicious_map)]

use amadeus::prelude::*;
use std::time::SystemTime;

#[tokio::test]
async fn cloudfront() {
	let pool = &ThreadPool::new(None);

	let start = SystemTime::now();

	let _ = ParallelPipe::<&Result<CloudfrontRow, AwsError>>::count(Identity);

	let rows = Cloudfront::new_with(
		AwsRegion::UsEast1,
		"us-east-1.data-analytics",
		"cflogworkshop/raw/cf-accesslogs/",
		AwsCredentials::Anonymous,
	)
	.await
	.unwrap();

	let ((), (count, count2)) = rows
		.par_stream()
		.pipe_fork(
			pool,
			Identity.for_each(|x: Result<CloudfrontRow, _>| {
				let _x = x.unwrap();
				// println!("{:?}", x.url);
			}),
			(
				Identity.map(|_: &Result<_, _>| ()).count(),
				Identity.cloned().count(),
				// Identity.for_each(|_: &Result<_, _>| ())
			),
		)
		.await;
	assert_eq!(count, count2);
	assert_eq!(count, 207_928);

	println!("in {:?}", start.elapsed().unwrap());
}
