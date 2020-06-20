#![type_length_limit = "13470730"]
#![allow(clippy::suspicious_map)]

use amadeus::prelude::*;
use std::time::SystemTime;

#[tokio::test]
async fn cloudfront() {
	let pool = &ThreadPool::new(None).unwrap();

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

	let ((), (count, count2, (), list)): ((), (usize, usize, (), List<CloudfrontRow>)) = rows
		.par_stream()
		.pipe_fork(
			pool,
			Identity.for_each(|x: Result<CloudfrontRow, _>| {
				let _x = x.unwrap();
				// println!("{:?}", x.url);
			}),
			(
				Identity.map(|_: &_| ()).count(),
				Identity.count(),
				Identity.for_each(|_: &_| ()),
				Identity.cloned().map(Result::unwrap).collect(),
			),
		)
		.await;
	assert_eq!(count, count2);
	assert_eq!(count, 207_928);

	assert_eq!(list.len(), count);
	for _el in list {}

	println!("in {:?}", start.elapsed().unwrap());
}
