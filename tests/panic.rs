#[cfg(feature = "constellation")]
use constellation::*;
use futures::FutureExt;
use std::{
	env, panic, panic::AssertUnwindSafe, time::{Duration, SystemTime}
};

use amadeus::prelude::*;

#[tokio::main]
async fn main() {
	#[cfg(feature = "constellation")]
	init(Resources::default());

	// Accept the number of processes at the command line, defaulting to 10
	let processes = env::args()
		.nth(1)
		.and_then(|arg| arg.parse::<usize>().ok())
		.unwrap_or(10);

	let local_pool_time = {
		let local_pool = LocalPool::new();
		run(&local_pool).await
	};
	let thread_pool_time = {
		let thread_pool = ThreadPool::new(processes).unwrap();
		run(&thread_pool).await
	};
	#[cfg(feature = "constellation")]
	let process_pool_time = {
		let process_pool = ProcessPool::new(processes, 1, Resources::default()).unwrap();
		run(&process_pool).await
	};
	#[cfg(not(feature = "constellation"))]
	let process_pool_time = "-";

	println!(
		"in {:?} {:?} {:?}",
		local_pool_time, thread_pool_time, process_pool_time
	);
}

async fn run<P: amadeus_core::pool::ProcessPool + std::panic::RefUnwindSafe>(pool: &P) -> Duration {
	let start = SystemTime::now();

	let res = AssertUnwindSafe((0i32..1_000).into_dist_iter().for_each(
		pool,
		FnMut!(|i| if i == 500 {
			panic!("boom")
		}),
	))
	.catch_unwind()
	.await;

	assert!(res.is_err());

	println!("done");

	start.elapsed().unwrap()
}
