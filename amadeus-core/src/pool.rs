use serde::{Deserialize, Serialize};
use std::{
	error::Error, future::Future, panic::{RefUnwindSafe, UnwindSafe}, pin::Pin
};

pub trait ProcessSend: Send + Serialize + for<'de> Deserialize<'de> + 'static {}
impl<T: ?Sized> ProcessSend for T where T: Send + Serialize + for<'de> Deserialize<'de> + 'static {}

type Result<T> = std::result::Result<T, Box<dyn Error + Send>>;

pub trait ProcessPool: Send + Sync + RefUnwindSafe + UnwindSafe + Unpin {
	fn processes(&self) -> usize;
	fn spawn<F, T>(&self, work: F) -> Pin<Box<dyn Future<Output = Result<T>> + Send>>
	where
		F: FnOnce() -> T + ProcessSend,
		T: ProcessSend;
}

pub trait ThreadPool: Send + Sync + RefUnwindSafe + UnwindSafe + Unpin {
	fn threads(&self) -> usize;
	fn spawn<F, T>(&self, work: F) -> Pin<Box<dyn Future<Output = Result<T>> + Send>>
	where
		F: FnOnce() -> T + Send + 'static,
		T: Send + 'static;
}

pub trait LocalPool: Send + Sync + RefUnwindSafe + UnwindSafe + Unpin {
	fn spawn<F, T>(&self, work: F) -> Pin<Box<dyn Future<Output = Result<T>> + Send>>
	where
		F: FnOnce() -> T + 'static,
		T: Send + 'static;
}

impl<P: ?Sized> ProcessPool for &P
where
	P: ProcessPool,
{
	fn processes(&self) -> usize {
		(*self).processes()
	}
	fn spawn<F, T>(&self, work: F) -> Pin<Box<dyn Future<Output = Result<T>> + Send>>
	where
		F: FnOnce() -> T + ProcessSend,
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
	fn spawn<F, T>(&self, work: F) -> Pin<Box<dyn Future<Output = Result<T>> + Send>>
	where
		F: FnOnce() -> T + Send + 'static,
		T: Send + 'static,
	{
		(*self).spawn(work)
	}
}

impl<P: ?Sized> LocalPool for &P
where
	P: LocalPool,
{
	fn spawn<F, T>(&self, work: F) -> Pin<Box<dyn Future<Output = Result<T>> + Send>>
	where
		F: FnOnce() -> T + 'static,
		T: Send + 'static,
	{
		(*self).spawn(work)
	}
}
