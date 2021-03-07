#[cfg(feature = "constellation")]
use constellation::*;
use futures::FutureExt;
use std::{
	panic, panic::AssertUnwindSafe, time::{Duration, SystemTime}
};

use amadeus::dist::prelude::*;

fn main() {
	if cfg!(miri) {
		return;
	}
	#[cfg(feature = "constellation")]
	init(Resources::default());

	tokio::runtime::Builder::new()
		.threaded_scheduler()
		.enable_all()
		.build()
		.unwrap()
		.block_on(async {
			let thread_pool_time = {
				let thread_pool = ThreadPool::new(None, None).unwrap();
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

async fn run<P: amadeus_core::pool::ProcessPool + std::panic::RefUnwindSafe>(pool: &P) -> Duration {
	let start = SystemTime::now();

	let res = AssertUnwindSafe((0i32..1_000).into_dist_stream().for_each(
		pool,
		FnMut!(|i| if i == 500 {
			panic!("this is intended to panic")
		}),
	))
	.catch_unwind()
	.await;

	assert!(res.is_err());

	println!("done");

	start.elapsed().unwrap()
}
