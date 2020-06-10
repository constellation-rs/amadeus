#[cfg(feature = "constellation")]
mod process;
mod thread;
pub(crate) mod util;

use futures::future::{BoxFuture, TryFutureExt};
use std::{error::Error, future::Future};

#[cfg(feature = "constellation")]
pub use process::ProcessPool;
pub use thread::ThreadPool;

use amadeus_core::pool::{
	ProcessPool as ProcessPoolTrait, ProcessSend, ThreadPool as ThreadPoolTrait
};

type Result<T> = std::result::Result<T, Box<dyn Error + Send>>;

#[cfg(feature = "constellation")]
impl ProcessPoolTrait for ProcessPool {
	type ThreadPool = ThreadPool;

	fn processes(&self) -> usize {
		ProcessPool::processes(self)
	}
	fn spawn<F, Fut, T>(&self, work: F) -> BoxFuture<'static, Result<T>>
	where
		F: FnOnce(&Self::ThreadPool) -> Fut + ProcessSend,
		Fut: Future<Output = T> + 'static,
		T: ProcessSend,
	{
		Box::pin(ProcessPool::spawn(self, work).map_err(|e| Box::new(e) as _))
	}
}

impl ProcessPoolTrait for ThreadPool {
	type ThreadPool = Self;

	fn processes(&self) -> usize {
		1
	}
	fn spawn<F, Fut, T>(&self, work: F) -> BoxFuture<'static, Result<T>>
	where
		F: FnOnce(&Self::ThreadPool) -> Fut + ProcessSend,
		Fut: Future<Output = T> + 'static,
		T: ProcessSend,
	{
		let self_ = self.clone();
		Box::pin(ThreadPool::spawn(self, move || work(&self_)).map_err(|e| Box::new(e) as _))
	}
}

impl ThreadPoolTrait for ThreadPool {
	fn threads(&self) -> usize {
		ThreadPool::threads(self)
	}
	fn spawn<F, Fut, T>(&self, work: F) -> BoxFuture<'static, Result<T>>
	where
		F: FnOnce() -> Fut + Send + 'static,
		Fut: Future<Output = T> + 'static,
		T: Send + 'static,
	{
		Box::pin(ThreadPool::spawn(self, work).map_err(|e| Box::new(e) as _))
	}
}
