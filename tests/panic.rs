use futures::FutureExt;
use std::{panic, panic::AssertUnwindSafe, time::SystemTime};

use amadeus::dist::prelude::*;

#[tokio::test]
async fn cloudfront() {
	let start = SystemTime::now();

	let pool = &ThreadPool::new(None);

	let res = AssertUnwindSafe((0i32..1_000).into_dist_stream().for_each(
		pool,
		FnMut!(|i| if i == 500 {
			panic!("boom")
		}),
	))
	.catch_unwind()
	.await;

	assert!(res.is_err());

	println!("in {:?}", start.elapsed().unwrap());
}
