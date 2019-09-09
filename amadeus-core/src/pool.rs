use serde::{Deserialize, Serialize};
use std::{error::Error, future::Future, pin::Pin};

pub trait ProcessSend: Send + Serialize + for<'de> Deserialize<'de> + 'static {}
impl<T: ?Sized> ProcessSend for T where T: Send + Serialize + for<'de> Deserialize<'de> + 'static {}

type Result<T> = std::result::Result<T, Box<dyn Error + Send>>;

pub trait ProcessPool {
	fn processes(&self) -> usize;
	fn spawn<F, T>(&self, work: F) -> Pin<Box<dyn Future<Output = Result<T>> + Send>>
	where
		F: FnOnce() -> T + ProcessSend,
		T: ProcessSend;
}

pub trait ThreadPool {
	fn threads(&self) -> usize;
	fn spawn<F, T>(&self, work: F) -> Pin<Box<dyn Future<Output = Result<T>> + Send>>
	where
		F: FnOnce() -> T + Send + 'static,
		T: Send + 'static;
}

pub trait LocalPool {
	fn spawn<F, T>(&self, work: F) -> Pin<Box<dyn Future<Output = Result<T>> + Send>>
	where
		F: FnOnce() -> T + 'static,
		T: Send + 'static;
}
