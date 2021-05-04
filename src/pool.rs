#[cfg(feature = "constellation")]
mod process;
mod thread;
pub(crate) mod util;

use futures::future::TryFutureExt;
use serde_closure::traits;
use std::{any::Any, error::Error, future::Future};

#[cfg(feature = "constellation")]
pub use process::ProcessPool;
pub use thread::ThreadPool;

use amadeus_core::{
	async_drop::BoxFuturePinnedAsyncDrop, pool::{ProcessPool as ProcessPoolTrait, ProcessSend, ThreadPool as ThreadPoolTrait}
};

type ProcessResult<T> = Result<T, Box<dyn Error + Send>>;
type ThreadResult<T> = Result<T, Box<dyn Any + Send>>;

#[cfg(feature = "constellation")]
#[cfg_attr(not(nightly), serde_closure::desugar)]
impl ProcessPoolTrait for ProcessPool {
	type ThreadPool = ThreadPool;

	fn processes(&self) -> usize {
		ProcessPool::processes(self)
	}
	fn spawn<F, Fut, T>(&self, work: F) -> BoxFuturePinnedAsyncDrop<'static, ProcessResult<T>>
	where
		F: traits::FnOnce(&Self::ThreadPool) -> Fut + ProcessSend + 'static,
		Fut: Future<Output = T> + 'static,
		T: ProcessSend + 'static,
	{
		Box::pin(ProcessPool::spawn(self, work).map_err(|e| Box::new(e) as _))
	}
	#[allow(unsafe_code)]
	unsafe fn spawn_unchecked<'a, F, Fut, T>(
		&self, work: F,
	) -> BoxFuturePinnedAsyncDrop<'a, ProcessResult<T>>
	where
		F: traits::FnOnce(&Self::ThreadPool) -> Fut + ProcessSend + 'a,
		Fut: Future<Output = T> + 'a,
		T: ProcessSend + 'a,
	{
		Box::pin(ProcessPool::spawn_unchecked(self, work).map_err(|e| Box::new(e) as _))
	}
}

#[cfg_attr(not(nightly), serde_closure::desugar)]
impl ProcessPoolTrait for ThreadPool {
	type ThreadPool = Self;

	fn processes(&self) -> usize {
		1
	}
	fn spawn<F, Fut, T>(&self, work: F) -> BoxFuturePinnedAsyncDrop<'static, ProcessResult<T>>
	where
		F: traits::FnOnce(&Self::ThreadPool) -> Fut + ProcessSend + 'static,
		Fut: Future<Output = T> + 'static,
		T: ProcessSend + 'static,
	{
		let self_ = self.clone();
		let spawn = move || work.call_once((&self_,));
		Box::pin(ThreadPool::spawn(self, spawn).map_err(|e| Box::new(e) as _))
	}
	#[allow(unsafe_code)]
	unsafe fn spawn_unchecked<'a, F, Fut, T>(
		&self, work: F,
	) -> BoxFuturePinnedAsyncDrop<'a, ProcessResult<T>>
	where
		F: traits::FnOnce(&Self::ThreadPool) -> Fut + ProcessSend + 'a,
		Fut: Future<Output = T> + 'a,
		T: ProcessSend + 'a,
	{
		let self_ = self.clone();
		let spawn = move || work.call_once((&self_,));
		Box::pin(ThreadPool::spawn_unchecked(self, spawn).map_err(|e| Box::new(e) as _))
	}
}

impl ThreadPoolTrait for ThreadPool {
	fn threads(&self) -> usize {
		ThreadPool::threads(self)
	}
	fn spawn<F, Fut, T>(&self, work: F) -> BoxFuturePinnedAsyncDrop<'static, ThreadResult<T>>
	where
		F: FnOnce() -> Fut + Send + 'static,
		Fut: Future<Output = T> + 'static,
		T: Send + 'static,
	{
		Box::pin(ThreadPool::spawn(self, work))
	}
	#[allow(unsafe_code)]
	unsafe fn spawn_unchecked<'a, F, Fut, T>(
		&self, work: F,
	) -> BoxFuturePinnedAsyncDrop<'a, ThreadResult<T>>
	where
		F: FnOnce() -> Fut + Send + 'a,
		Fut: Future<Output = T> + 'a,
		T: Send + 'a,
	{
		Box::pin(ThreadPool::spawn_unchecked(self, work))
	}
}
