#[cfg(feature = "constellation")]
use constellation::*;
use std::time::{Duration, SystemTime};

use amadeus::prelude::*;
use data::Webpage;

#[tokio::main]
async fn main() {
	#[cfg(feature = "constellation")]
	init(Resources::default());

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
}

async fn run<P: amadeus_core::pool::ProcessPool>(pool: &P) -> Duration {
	let start = SystemTime::now();

	let rows = CommonCrawl::new("CC-MAIN-2018-43").await.unwrap();
	rows.dist_stream()
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
