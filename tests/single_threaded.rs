#![allow(clippy::suspicious_map)]

use futures::{future, future::BoxFuture, FutureExt};
use std::{error::Error, future::Future, time::SystemTime};

use amadeus::prelude::*;
use amadeus_core::pool::ThreadPool;

#[test]
#[cfg_attr(miri, ignore)]
fn single_threaded() {
	let start = SystemTime::now();

	let pool = &LocalPool;

	let _ = (0..100)
		.map(|i| format!("string {}", i))
		.par()
		.fork(
			pool,
			Identity.sample_unstable(10),
			(
				Identity
					.map(|row: &String| (row[..8].to_owned(), ()))
					.group_by(Identity.count()),
				Identity.count(),
				Identity.for_each(|_: &_| ()),
				Identity.map(|_: &_| ()).count(),
			),
		)
		.now_or_never()
		.unwrap();

	println!("in {:?}", start.elapsed().unwrap());
}

#[derive(Clone)]
struct LocalPool;

impl ThreadPool for LocalPool {
	fn threads(&self) -> usize {
		1
	}
	fn spawn<F, Fut, T>(&self, work: F) -> BoxFuture<'static, Result<T, Box<dyn Error + Send>>>
	where
		F: FnOnce() -> Fut + Send + 'static,
		Fut: Future<Output = T> + 'static,
		T: Send + 'static,
	{
		Box::pin(future::lazy(|_| work().now_or_never().unwrap()).map(Ok))
	}
	unsafe fn spawn_unchecked<'a, F, Fut, T>(
		&self, work: F,
	) -> BoxFuture<'a, Result<T, Box<dyn Error + Send>>>
	where
		F: FnOnce() -> Fut + Send + 'a,
		Fut: Future<Output = T> + 'a,
		T: Send + 'a,
	{
		Box::pin(future::lazy(|_| work().now_or_never().unwrap()).map(Ok))
	}
}
