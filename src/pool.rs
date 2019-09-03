mod local;
mod process;
mod thread;
mod util;

pub use local::LocalPool;
pub use process::ProcessPool;
pub use thread::ThreadPool;

mod process_pool_impls {
	use futures::future::TryFutureExt;
	use std::{error::Error, future::Future, pin::Pin};

	use amadeus_core::pool::{ProcessPool as Pool, ProcessSend};

	use super::{LocalPool, ProcessPool, ThreadPool};

	type Result<T> = std::result::Result<T, Box<dyn Error + Send>>;

	impl Pool for ProcessPool {
		fn processes(&self) -> usize {
			ProcessPool::processes(self)
		}
		fn spawn<F, T>(&self, work: F) -> Pin<Box<dyn Future<Output = Result<T>> + Send>>
		where
			F: FnOnce() -> T + ProcessSend,
			T: ProcessSend,
		{
			Pin::from(Box::new(
				ProcessPool::spawn(self, work).map_err(|e| Box::new(e) as _),
			))
		}
	}

	impl Pool for ThreadPool {
		fn processes(&self) -> usize {
			ThreadPool::threads(self)
		}
		fn spawn<F, T>(&self, work: F) -> Pin<Box<dyn Future<Output = Result<T>> + Send>>
		where
			F: FnOnce() -> T + ProcessSend,
			T: ProcessSend,
		{
			Pin::from(Box::new(
				ThreadPool::spawn(self, work).map_err(|e| Box::new(e) as _),
			))
		}
	}

	impl Pool for LocalPool {
		fn processes(&self) -> usize {
			1
		}
		fn spawn<F, T>(&self, work: F) -> Pin<Box<dyn Future<Output = Result<T>> + Send>>
		where
			F: FnOnce() -> T + ProcessSend,
			T: ProcessSend,
		{
			Pin::from(Box::new(
				LocalPool::spawn(self, work).map_err(|e| Box::new(e) as _),
			))
		}
	}
}

mod thread_pool_impls {
	use futures::future::TryFutureExt;
	use std::{error::Error, future::Future, pin::Pin};

	use amadeus_core::pool::ThreadPool as Pool;

	use super::{LocalPool, ThreadPool};

	type Result<T> = std::result::Result<T, Box<dyn Error + Send>>;

	impl Pool for ThreadPool {
		fn threads(&self) -> usize {
			ThreadPool::threads(self)
		}
		fn spawn<F, T>(&self, work: F) -> Pin<Box<dyn Future<Output = Result<T>> + Send>>
		where
			F: FnOnce() -> T + Send + 'static,
			T: Send + 'static,
		{
			Pin::from(Box::new(
				ThreadPool::spawn(self, work).map_err(|e| Box::new(e) as _),
			))
		}
	}

	impl Pool for LocalPool {
		fn threads(&self) -> usize {
			1
		}
		fn spawn<F, T>(&self, work: F) -> Pin<Box<dyn Future<Output = Result<T>> + Send>>
		where
			F: FnOnce() -> T + Send + 'static,
			T: Send + 'static,
		{
			Pin::from(Box::new(
				LocalPool::spawn(self, work).map_err(|e| Box::new(e) as _),
			))
		}
	}
}

mod local_pool_impls {
	use futures::future::TryFutureExt;
	use std::{error::Error, future::Future, pin::Pin};

	use amadeus_core::pool::LocalPool as Pool;

	use super::LocalPool;

	type Result<T> = std::result::Result<T, Box<dyn Error + Send>>;

	impl Pool for LocalPool {
		fn spawn<F, T>(&self, work: F) -> Pin<Box<dyn Future<Output = Result<T>> + Send>>
		where
			F: FnOnce() -> T + 'static,
			T: Send + 'static,
		{
			Pin::from(Box::new(
				LocalPool::spawn(self, work).map_err(|e| Box::new(e) as _),
			))
		}
	}
}
