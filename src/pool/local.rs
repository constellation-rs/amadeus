use futures::{future, FutureExt, TryFutureExt};
use std::{
	future::Future, panic::{RefUnwindSafe, UnwindSafe}, sync::Arc
};
use tokio::task::spawn;

use super::util::Panicked;

const DEFAULT_TASKS_PER_CORE: usize = 100;

#[derive(Debug)]
struct ThreadPoolInner {
	logical_cores: usize,
	tasks_per_core: usize,
}

// #[derive(Copy, Clone, Default, Debug)]
#[derive(Debug)]
pub struct ThreadPool(Arc<ThreadPoolInner>);
impl ThreadPool {
	pub fn new(tasks_per_core: Option<usize>) -> Self {
		// let runtime = Builder::new()
		// 	.basic_scheduler()
		// 	// .threaded_scheduler().core_threads(1).max_threads(1)
		// 	.enable_all()
		// 	.build()
		// 	.unwrap();
		let logical_cores = num_cpus::get();
		let tasks_per_core = tasks_per_core.unwrap_or(DEFAULT_TASKS_PER_CORE);
		ThreadPool(Arc::new(ThreadPoolInner {
			logical_cores,
			tasks_per_core,
		}))
	}
	pub fn threads(&self) -> usize {
		self.0.logical_cores * self.0.tasks_per_core
	}
	pub fn spawn<F, Fut, T>(&self, work: F) -> impl Future<Output = Result<T, Panicked>> + Send
	where
		F: FnOnce() -> Fut + Send + 'static,
		Fut: Future<Output = T> + 'static,
		T: Send + 'static,
	{
		let _self = self;
		spawn(DuckSend(future::lazy(|_| work()).flatten()))
			.map_err(tokio::task::JoinError::into_panic)
			.map_err(Panicked::from)
	}
}

impl Clone for ThreadPool {
	/// Cloning a pool will create a new handle to the pool.
	/// The behavior is similar to [Arc](https://doc.rust-lang.org/stable/std/sync/struct.Arc.html).
	///
	/// We could for example submit jobs from multiple threads concurrently.
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl UnwindSafe for ThreadPool {}
impl RefUnwindSafe for ThreadPool {}

// TODO remove when spawn_pinned exists https://github.com/tokio-rs/tokio/issues/2545

use pin_project::pin_project;
use std::{
	pin::Pin, task::{Context, Poll}
};
#[pin_project]
struct DuckSend<F>(#[pin] F);
impl<F> Future for DuckSend<F>
where
	F: Future,
{
	type Output = F::Output;

	fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		assert!(<F as IsSend>::is_send(), "{}", std::any::type_name::<F>());
		self.project().0.poll(cx)
	}
}
#[allow(unsafe_code)]
unsafe impl<F> Send for DuckSend<F> {}

trait IsSend {
	fn is_send() -> bool;
}
impl<T: ?Sized> IsSend for T {
	default fn is_send() -> bool {
		false
	}
}
impl<T: ?Sized> IsSend for T
where
	T: Send,
{
	fn is_send() -> bool {
		true
	}
}
