use futures::TryFutureExt;
use std::{
	future::Future, io, panic::{RefUnwindSafe, UnwindSafe}, sync::Arc
};
use tokio::{
	runtime::Handle, task::{JoinError, LocalSet}
};

use super::util::{assert_sync_and_send, Panicked};

const DEFAULT_TASKS_PER_CORE: usize = 100;

#[derive(Debug)]
struct ThreadPoolInner {
	logical_cores: usize,
	tasks_per_core: usize,
}

#[derive(Debug)]
pub struct ThreadPool(Arc<ThreadPoolInner>);
impl ThreadPool {
	pub fn new(tasks_per_core: Option<usize>) -> io::Result<Self> {
		let logical_cores = num_cpus::get();
		let tasks_per_core = tasks_per_core.unwrap_or(DEFAULT_TASKS_PER_CORE);
		Ok(ThreadPool(Arc::new(ThreadPoolInner {
			logical_cores,
			tasks_per_core,
		})))
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
		spawn_pinned(|| work())
			.map_err(JoinError::into_panic)
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

fn _assert() {
	let _ = assert_sync_and_send::<ThreadPool>;
}

fn spawn_pinned<F, Fut, T>(task: F) -> impl Future<Output = Result<T, JoinError>> + Send
where
	F: FnOnce() -> Fut + Send + 'static,
	Fut: Future<Output = T> + 'static,
	T: Send + 'static,
{
	thread_local! {
		static LOCAL: LocalSet = LocalSet::new();
	}
	let handle = Handle::current();
	let handle1 = handle.clone();
	handle.spawn_blocking(move || LOCAL.with(|local| handle1.block_on(local.run_until(task()))))
}

#[cfg(test)]
mod tests {
	use super::*;

	use futures::future::join_all;
	use std::sync::{
		atomic::{AtomicUsize, Ordering}, Arc
	};

	#[tokio::test]
	async fn spawn_pinned_() {
		const TASKS: usize = 1000;
		const ITERS: usize = 1000;
		let count = Arc::new(AtomicUsize::new((1..TASKS).sum()));
		for _ in 0..ITERS {
			join_all((0..TASKS).map(|i| {
				let count = count.clone();
				spawn_pinned(move || async move {
					let _ = count.fetch_sub(i, Ordering::Relaxed);
				})
			}))
			.await
			.into_iter()
			.collect::<Result<(), _>>()
			.unwrap();
			assert_eq!(count.load(Ordering::Relaxed), 0);
			count.store((1..TASKS).sum(), Ordering::Relaxed);
		}
	}
}
