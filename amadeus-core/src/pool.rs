use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::{
	error::Error, future::Future, panic::{RefUnwindSafe, UnwindSafe}
};

pub trait ProcessSend: Send + Serialize + for<'de> Deserialize<'de> {}
impl<T: ?Sized> ProcessSend for T where T: Send + Serialize + for<'de> Deserialize<'de> {}

type Result<T> = std::result::Result<T, Box<dyn Error + Send>>;

#[cfg_attr(not(feature = "doc"), serde_closure::generalize)]
pub trait ProcessPool: Clone + Send + Sync + RefUnwindSafe + UnwindSafe + Unpin {
	type ThreadPool: ThreadPool + 'static;

	fn processes(&self) -> usize;
	fn spawn<F, Fut, T>(&self, work: F) -> BoxFuture<'static, Result<T>>
	where
		F: FnOnce(&Self::ThreadPool) -> Fut + ProcessSend + 'static,
		Fut: Future<Output = T> + 'static,
		T: ProcessSend + 'static;
}

pub trait ThreadPool: Clone + Send + Sync + RefUnwindSafe + UnwindSafe + Unpin {
	fn threads(&self) -> usize;
	fn spawn<F, Fut, T>(&self, work: F) -> BoxFuture<'static, Result<T>>
	where
		F: FnOnce() -> Fut + Send + 'static,
		Fut: Future<Output = T> + 'static,
		T: Send + 'static;
}

#[cfg_attr(not(feature = "doc"), serde_closure::generalize)]
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
		F: FnOnce(&Self::ThreadPool) -> Fut + ProcessSend + 'static,
		Fut: Future<Output = T> + 'static,
		T: ProcessSend + 'static,
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
	fn spawn<F, Fut, T>(&self, work: F) -> BoxFuture<'static, Result<T>>
	where
		F: FnOnce() -> Fut + Send + 'static,
		Fut: Future<Output = T> + 'static,
		T: Send + 'static,
	{
		(*self).spawn(work)
	}
}
