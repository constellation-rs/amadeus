use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_closure::traits;
use std::{
	error::Error, future::Future, panic::{RefUnwindSafe, UnwindSafe}
};

pub trait ProcessSend: Send + Serialize + for<'de> Deserialize<'de> {}
impl<T: ?Sized> ProcessSend for T where T: Send + Serialize + for<'de> Deserialize<'de> {}

type Result<T> = std::result::Result<T, Box<dyn Error + Send>>;

#[cfg_attr(not(nightly), serde_closure::desugar)]
pub trait ProcessPool: Clone + Send + Sync + RefUnwindSafe + UnwindSafe + Unpin {
	type ThreadPool: ThreadPool + 'static;

	fn processes(&self) -> usize;

	fn spawn<F, Fut, T>(&self, work: F) -> BoxFuture<'static, Result<T>>
	where
		F: traits::FnOnce(&Self::ThreadPool) -> Fut + ProcessSend + 'static,
		Fut: Future<Output = T> + 'static,
		T: ProcessSend + 'static;

	/// # Safety
	///
	/// Must be polled to completion before dropping. Unsound to forget it without having polled to completion.
	#[allow(unsafe_code)]
	unsafe fn spawn_unchecked<'a, F, Fut, T>(&self, work: F) -> BoxFuture<'a, Result<T>>
	where
		F: traits::FnOnce(&Self::ThreadPool) -> Fut + ProcessSend + 'a,
		Fut: Future<Output = T> + 'a,
		T: ProcessSend + 'a;
}

pub trait ThreadPool: Clone + Send + Sync + RefUnwindSafe + UnwindSafe + Unpin {
	fn threads(&self) -> usize;

	fn spawn<F, Fut, T>(&self, work: F) -> BoxFuture<'static, Result<T>>
	where
		F: FnOnce() -> Fut + Send + 'static,
		Fut: Future<Output = T> + 'static,
		T: Send + 'static;

	/// # Safety
	///
	/// Must be polled to completion before dropping. Unsound to forget it without having polled to completion.
	#[allow(unsafe_code)]
	unsafe fn spawn_unchecked<'a, F, Fut, T>(&self, work: F) -> BoxFuture<'a, Result<T>>
	where
		F: FnOnce() -> Fut + Send + 'a,
		Fut: Future<Output = T> + 'a,
		T: Send + 'a;
}

#[cfg_attr(not(nightly), serde_closure::desugar)]
impl<P: ?Sized> ProcessPool for &P
where
	P: ProcessPool,
{
	type ThreadPool = P::ThreadPool;

	fn processes(&self) -> usize {
		(*self).processes()
	}
	fn spawn<F, Fut, T>(&self, work: F) -> BoxFuture<'static, Result<T>>
	where
		F: traits::FnOnce(&Self::ThreadPool) -> Fut + ProcessSend + 'static,
		Fut: Future<Output = T> + 'static,
		T: ProcessSend + 'static,
	{
		(*self).spawn(work)
	}
	#[allow(unsafe_code)]
	unsafe fn spawn_unchecked<'a, F, Fut, T>(&self, work: F) -> BoxFuture<'a, Result<T>>
	where
		F: traits::FnOnce(&Self::ThreadPool) -> Fut + ProcessSend + 'a,
		Fut: Future<Output = T> + 'a,
		T: ProcessSend + 'a,
	{
		(*self).spawn_unchecked(work)
	}
}

impl<P: ?Sized> ThreadPool for &P
where
	P: ThreadPool,
{
	fn threads(&self) -> usize {
		(*self).threads()
	}
	fn spawn<F, Fut, T>(&self, work: F) -> BoxFuture<'static, Result<T>>
	where
		F: FnOnce() -> Fut + Send + 'static,
		Fut: Future<Output = T> + 'static,
		T: Send + 'static,
	{
		(*self).spawn(work)
	}
	#[allow(unsafe_code)]
	unsafe fn spawn_unchecked<'a, F, Fut, T>(&self, work: F) -> BoxFuture<'a, Result<T>>
	where
		F: FnOnce() -> Fut + Send + 'a,
		Fut: Future<Output = T> + 'a,
		T: Send + 'a,
	{
		(*self).spawn_unchecked(work)
	}
}
