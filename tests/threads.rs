#[cfg(feature = "constellation")]
use constellation::*;
use futures::future::join_all;
use rand::{Rng, SeedableRng};
use std::{
	convert::TryInto, thread, time::{self, Duration, SystemTime}
};

use amadeus::prelude::*;

#[tokio::main]
async fn main() {
	#[cfg(feature = "constellation")]
	init(Resources::default());

	let thread_pool_time = {
		let thread_pool = ThreadPool::new(None);
		run(&thread_pool, 10).await
	};
	#[cfg(feature = "constellation")]
	let process_pool_time = {
		let process_pool = ProcessPool::new(None, None, Resources::default()).unwrap();
		run(&process_pool, processes * 10)
	};
	#[cfg(not(feature = "constellation"))]
	let process_pool_time = "-";

	println!("in {:?} {:?}", thread_pool_time, process_pool_time);
}

async fn run<P: amadeus_core::pool::ProcessPool>(pool: &P, parallel: usize) -> Duration {
	let start = SystemTime::now();

	join_all((0..parallel).map(|i| async move {
		let ret = pool
			.spawn(FnOnce!(move |&_| async move {
				let mut rng = rand::rngs::SmallRng::seed_from_u64(i.try_into().unwrap());
				thread::sleep(rng.gen_range(time::Duration::new(0, 0), time::Duration::new(2, 0)));
				format!("warm greetings from job {}", i)
			}))
			.await;
		println!("{}", ret.unwrap());
	}))
	.await;

	start.elapsed().unwrap()
}
