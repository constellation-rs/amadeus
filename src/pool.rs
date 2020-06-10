mod local;
#[cfg(feature = "constellation")]
mod process;
pub(crate) mod util;

pub use local::ThreadPool;
#[cfg(feature = "constellation")]
pub use process::ProcessPool;

mod process_pool_impls {
	use futures::future::{BoxFuture, TryFutureExt};
	use std::{error::Error, future::Future};

	use amadeus_core::pool::{ProcessPool as Pool, ProcessSend};

	#[cfg(feature = "constellation")]
	use super::ProcessPool;
	use super::ThreadPool;

	type Result<T> = std::result::Result<T, Box<dyn Error + Send>>;

	#[cfg(feature = "constellation")]
	impl Pool for ProcessPool {
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

	impl Pool for ThreadPool {
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
}

mod thread_pool_impls {
	use futures::future::{BoxFuture, TryFutureExt};
	use std::{error::Error, future::Future};

	use amadeus_core::pool::ThreadPool as Pool;

	use super::ThreadPool;

	type Result<T> = std::result::Result<T, Box<dyn Error + Send>>;

	impl Pool for ThreadPool {
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
}
