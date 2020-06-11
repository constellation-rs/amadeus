#![type_length_limit = "13470730"]

use amadeus::prelude::*;
#[cfg(feature = "constellation")]
use constellation::*;
use std::time::{Duration, SystemTime};

fn main() {
	#[cfg(feature = "constellation")]
	init(Resources::default());

	tokio::runtime::Builder::new()
		.threaded_scheduler()
		.enable_all()
		.build()
		.unwrap()
		.block_on(async {
			let thread_pool_time = {
				let thread_pool = ThreadPool::new(None);
				run(&thread_pool).await
			};
			#[cfg(feature = "constellation")]
			let process_pool_time = {
				let process_pool = ProcessPool::new(None, None, Resources::default()).unwrap();
				run(&process_pool).await
			};
			#[cfg(not(feature = "constellation"))]
			let process_pool_time = "-";

			println!("in {:?} {:?}", thread_pool_time, process_pool_time);
		})
}

async fn run<P: amadeus_core::pool::ProcessPool>(pool: &P) -> Duration {
	let start = SystemTime::now();

	let _ = DistributedPipe::<&Result<CloudfrontRow, AwsError>>::count(Identity);

	let rows = Cloudfront::new_with(
		AwsRegion::UsEast1,
		"us-east-1.data-analytics",
		"cflogworkshop/raw/cf-accesslogs/",
		AwsCredentials::Anonymous,
	)
	.await
	.unwrap();

	let ((), (count, count2)) = rows
		.dist_stream()
		.fork(
			pool,
			Identity.for_each(FnMut!(|x: Result<CloudfrontRow, _>| {
				let _x = x.unwrap();
				// println!("{:?}", x.url);
			})),
			(
				Identity.map(FnMut!(|_x: &Result<_, _>| {})).count(),
				Identity.cloned().count(),
				// DistributedPipe::<&Result<CloudfrontRow, AwsError>>::count(Identity),
			),
		)
		.await;
	assert_eq!(count, count2);
	assert_eq!(count, 207_928);

	start.elapsed().unwrap()
}
