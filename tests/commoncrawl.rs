#[cfg(feature = "constellation")]
use constellation::*;
use std::{
	env, time::{Duration, SystemTime}
};

use amadeus::prelude::*;
use data::Webpage;

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

async fn run<P: amadeus_core::pool::ProcessPool>(pool: &P) -> Duration {
	let start = SystemTime::now();

	CommonCrawl::new("CC-MAIN-2018-43")
		.await
		.unwrap()
		.dist_iter()
		.all(
			pool,
			FnMut!(move |x: Result<Webpage<'static>, _>| -> bool {
				let _x = x.unwrap();
				// println!("{}", x.url);
				start.elapsed().unwrap() < Duration::new(10, 0)
			}),
		)
		.await;

	start.elapsed().unwrap()
}
