use amadeus::dist::prelude::*;
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
				let thread_pool = ThreadPool::new(None).unwrap();
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

	let rows = Cloudfront::new_with(
		AwsRegion::UsEast1,
		"us-east-1.data-analytics",
		"cflogworkshop/raw/cf-accesslogs/",
		AwsCredentials::Anonymous,
	)
	.await
	.unwrap();

	let ((), (count, count2, (), list, sorted)): ((), (usize, usize, (), List<CloudfrontRow>, _)) =
		rows.clone()
			.dist_stream()
			.fork(
				pool,
				Identity.for_each(FnMut!(|x: Result<CloudfrontRow, _>| {
					let _x = x.unwrap();
					// println!("{:?}", x.url);
				})),
				(
					Identity.map(FnMut!(|_: &_| ())).count(),
					Identity.count(),
					Identity.for_each(FnMut!(|_: &_| ())),
					Identity
						.cloned()
						.map(FnMut!(|x: Result<_, _>| x.unwrap()))
						.collect(),
					Identity
						.cloned()
						.map(FnMut!(|x: Result<_, _>| x.unwrap()))
						.sort_n_by(
							100,
							Fn!(|a: &CloudfrontRow, b: &CloudfrontRow| a
								.remote_ip
								.cmp(&b.remote_ip)),
						),
				),
			)
			.await;
	assert_eq!(count, count2);
	assert_eq!(count, 207_928);

	assert_eq!(list.len(), count);
	for _el in list {}

	let count3 = rows
		.dist_stream()
		.pipe(pool, Identity.pipe(Identity.count()))
		.await;
	assert_eq!(count3, 207_928);

	println!("{:#?}", sorted);

	start.elapsed().unwrap()
}
