use serde::{Deserialize, Serialize};
use std::{
	error::Error, future::Future, panic::{RefUnwindSafe, UnwindSafe}, pin::Pin
};

pub trait ProcessSend: Send + Serialize + for<'de> Deserialize<'de> + 'static {}
impl<T: ?Sized> ProcessSend for T where T: Send + Serialize + for<'de> Deserialize<'de> + 'static {}

type Result<T> = std::result::Result<T, Box<dyn Error + Send>>;

pub trait ProcessPool: Clone + Send + Sync + RefUnwindSafe + UnwindSafe + Unpin {
	type ThreadPool: ThreadPool + 'static;

	fn processes(&self) -> usize;
	fn spawn<F, Fut, T>(&self, work: F) -> Pin<Box<dyn Future<Output = Result<T>> + Send>>
	where
		F: FnOnce(&Self::ThreadPool) -> Fut + ProcessSend,
		Fut: Future<Output = T> + 'static,
		T: ProcessSend;
}

pub trait ThreadPool: Clone + Send + Sync + RefUnwindSafe + UnwindSafe + Unpin {
	fn threads(&self) -> usize;
	fn spawn<F, Fut, T>(&self, work: F) -> Pin<Box<dyn Future<Output = Result<T>> + Send>>
	where
		F: FnOnce() -> Fut + Send + 'static,
		Fut: Future<Output = T> + 'static,
		T: Send + 'static;
}

impl<P: ?Sized> ProcessPool for &P
where
	P: ProcessPool,
{
	type ThreadPool = P::ThreadPool;

	fn processes(&self) -> usize {
		(*self).processes()
	}
	fn spawn<F, Fut, T>(&self, work: F) -> Pin<Box<dyn Future<Output = Result<T>> + Send>>
	where
		F: FnOnce(&Self::ThreadPool) -> Fut + ProcessSend,
		Fut: Future<Output = T> + 'static,
		T: ProcessSend,
	{
		(*self).spawn(work)
	}
}

impl<P: ?Sized> ThreadPool for &P
where
	P: ThreadPool,
{
	fn threads(&self) -> usize {
		(*self).threads()
	}
	fn spawn<F, Fut, T>(&self, work: F) -> Pin<Box<dyn Future<Output = Result<T>> + Send>>
	where
		F: FnOnce() -> Fut + Send + 'static,
		Fut: Future<Output = T> + 'static,
		T: Send + 'static,
	{
		(*self).spawn(work)
	}
}
