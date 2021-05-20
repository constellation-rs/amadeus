use futures::future::join_all;
use rand::{rngs::SmallRng, Rng, SeedableRng};
use std::{
	collections::HashSet, convert::TryInto, time::{Duration, SystemTime}
};
use tokio::time::delay_for as sleep;

use amadeus::dist::prelude::*;

#[tokio::test(threaded_scheduler)]
#[cfg_attr(miri, ignore)]
async fn threads() {
	let start = SystemTime::now();

	let pool = &ThreadPool::new(None, None).unwrap();
	let parallel = 1000;

	join_all((0..parallel).map(|i| async move {
		let ret = pool
			.spawn(move || async move {
				let mut rng = SmallRng::seed_from_u64(i.try_into().unwrap());
				sleep(rng.gen_range(Duration::new(0, 0), Duration::new(2, 0))).await;
				format!("warm greetings from job {}", i)
			})
			.await;
		println!("{}", ret.unwrap());
	}))
	.await;

	println!("in {:?}", start.elapsed().unwrap());
}

#[tokio::test(threaded_scheduler)]
#[cfg_attr(miri, ignore)]
async fn user_set_core_count() {
	let num_cores = 4;

	let pool = &ThreadPool::new(Some(num_cores), Some(1)).unwrap();
	let parallel = 1000;

	let ret = join_all((0..parallel).map(|_| async move {
		pool.spawn(move || async {
			std::thread::sleep(std::time::Duration::from_millis(10));
			std::thread::current().id()
		})
		.await
		.unwrap()
	}))
	.await;

	let unique_thread_ids: HashSet<_> = ret.into_iter().collect();

	println!("Number of cores used: {}", unique_thread_ids.len());
	assert_eq!(unique_thread_ids.len(), num_cores);
}
