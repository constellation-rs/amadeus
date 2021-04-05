use futures::{ready, TryFutureExt};
use pin_project::{pin_project, pinned_drop};
use std::{
	future::Future, io, panic::{RefUnwindSafe, UnwindSafe}, pin::Pin, sync::Arc, task::{Context, Poll}
};

#[cfg(not(target_arch = "wasm32"))]
use tokio::task::JoinError;
#[cfg(target_arch = "wasm32")]
use {
	futures::{future, FutureExt}, std::panic::AssertUnwindSafe
};

use super::util::{assert_sync_and_send, Panicked};

const DEFAULT_TASKS_PER_CORE: usize = 100;

#[derive(Debug)]
struct ThreadPoolInner {
	threads: usize,
	tasks: usize,
	#[cfg(not(target_arch = "wasm32"))]
	pool: Pool,
}

#[derive(Debug)]
pub struct ThreadPool(Arc<ThreadPoolInner>);
impl ThreadPool {
	pub fn new(threads: Option<usize>, tasks: Option<usize>) -> io::Result<Self> {
		let threads = if let Some(threads) = threads {
			threads
		} else if !cfg!(target_arch = "wasm32") {
			num_cpus::get()
		} else {
			1
		};

		let tasks = tasks.unwrap_or(DEFAULT_TASKS_PER_CORE);
		#[cfg(not(target_arch = "wasm32"))]
		let pool = Pool::new(threads);
		Ok(ThreadPool(Arc::new(ThreadPoolInner {
			threads,
			tasks,
			#[cfg(not(target_arch = "wasm32"))]
			pool,
		})))
	}
	pub fn threads(&self) -> usize {
		self.0.threads * self.0.tasks
	}
	pub fn spawn<F, Fut, T>(&self, task: F) -> impl Future<Output = Result<T, Panicked>> + Send
	where
		F: FnOnce() -> Fut + Send + 'static,
		Fut: Future<Output = T> + 'static,
		T: Send + 'static,
	{
		#[cfg(not(target_arch = "wasm32"))]
		return self
			.0
			.pool
			.spawn_pinned(task)
			.map_err(JoinError::into_panic)
			.map_err(Panicked::from);
		#[cfg(target_arch = "wasm32")]
		{
			let _self = self;
			let (remote, remote_handle) = AssertUnwindSafe(future::lazy(|_| task()).flatten())
				.catch_unwind()
				.map_err(Into::into)
				.remote_handle();
			wasm_bindgen_futures::spawn_local(remote);
			remote_handle
		}
	}
	#[allow(unsafe_code)]
	pub unsafe fn spawn_unchecked<'a, F, Fut, T>(
		&self, task: F,
	) -> impl Future<Output = Result<T, Panicked>> + Send + 'a
	where
		F: FnOnce() -> Fut + Send + 'a,
		Fut: Future<Output = T> + 'a,
		T: Send + 'a,
	{
		#[cfg(not(target_arch = "wasm32"))]
		return Guard::new(
			self.0
				.pool
				.spawn_pinned_unchecked(task)
				.map_err(JoinError::into_panic)
				.map_err(Panicked::from),
		);
		#[cfg(target_arch = "wasm32")]
		{
			let _self = self;
			let task = std::mem::transmute::<
				Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = Box<dyn Send>>>> + Send>,
				Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = Box<dyn Send>>>> + Send>,
			>(Box::new(|| {
				let fut = Box::pin(task().map(|t| Box::new(t) as Box<dyn Send>));
				std::mem::transmute::<
					Pin<Box<dyn Future<Output = Box<dyn Send>>>>,
					Pin<Box<dyn Future<Output = Box<dyn Send>>>>,
				>(fut)
			}));
			let (remote, remote_handle) = AssertUnwindSafe(future::lazy(|_| task()).flatten())
				.catch_unwind()
				.map_err(Into::into)
				.remote_handle();
			wasm_bindgen_futures::spawn_local(remote);
			Guard::new(remote_handle.map_ok(|t| {
				let t: *mut dyn Send = Box::into_raw(t);
				*Box::from_raw(t as *mut T)
			}))
		}
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

#[pin_project(PinnedDrop)]
struct Guard<F>(#[pin] Option<F>);
impl<F> Guard<F> {
	fn new(f: F) -> Self {
		Self(Some(f))
	}
}
impl<F> Future for Guard<F>
where
	F: Future,
{
	type Output = F::Output;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		match self.as_mut().project().0.as_pin_mut() {
			Some(fut) => {
				let output = ready!(fut.poll(cx));
				self.project().0.set(None);
				Poll::Ready(output)
			}
			None => Poll::Pending,
		}
	}
}
#[pinned_drop]
impl<F> PinnedDrop for Guard<F> {
	fn drop(self: Pin<&mut Self>) {
		if self.project().0.is_some() {
			panic!("dropped before finished polling!");
		}
	}
}

fn _assert() {
	let _ = assert_sync_and_send::<ThreadPool>;
}

// TODO: remove once spawn_pinned exists: https://github.com/tokio-rs/tokio/issues/2545
#[cfg(not(target_arch = "wasm32"))]
mod pool {
	use async_channel::{bounded, Sender};
	use futures::{future::RemoteHandle, FutureExt};
	use std::{any::Any, future::Future, mem, panic::AssertUnwindSafe, pin::Pin};
	use tokio::{
		runtime::Handle, task::{JoinError, LocalSet}
	};

	type Request = Box<dyn FnOnce() -> Box<dyn Future<Output = Response>> + Send>;
	type Response = Result<Box<dyn Any + Send>, Box<dyn Any + Send>>;

	#[derive(Debug)]
	pub(super) struct Pool {
		sender: Sender<(Request, Sender<RemoteHandle<Response>>)>,
	}
	impl Pool {
		pub(super) fn new(threads: usize) -> Self {
			let handle = Handle::current();
			let handle1 = handle.clone();
			let (sender, receiver) = bounded::<(Request, Sender<RemoteHandle<Response>>)>(1);
			for _ in 0..threads {
				let receiver = receiver.clone();
				let handle = handle.clone();
				let _ = handle1.spawn_blocking(move || {
					let local = LocalSet::new();
					handle.block_on(local.run_until(async {
						while let Ok((task, sender)) = receiver.recv().await {
							let _ = local.spawn_local(async move {
								let (remote, remote_handle) = Pin::from(task()).remote_handle();
								let _ = sender.send(remote_handle).await;
								remote.await;
							});
						}
					}))
				});
			}
			Self { sender }
		}
		pub(super) fn spawn_pinned<F, Fut, T>(
			&self, task: F,
		) -> impl Future<Output = Result<T, JoinError>> + Send
		where
			F: FnOnce() -> Fut + Send + 'static,
			Fut: Future<Output = T> + 'static,
			T: Send + 'static,
		{
			let sender = self.sender.clone();
			async move {
				let task: Request = Box::new(|| {
					Box::new(
						AssertUnwindSafe(task().map(|t| Box::new(t) as Box<dyn Any + Send>))
							.catch_unwind(),
					)
				});
				let (sender_, receiver) = bounded::<RemoteHandle<Response>>(1);
				sender.send((task, sender_)).await.unwrap();
				let res = receiver.recv().await;
				let res = res.unwrap().await;
				#[allow(deprecated)]
				res.map(|x| *Box::<dyn Any + Send>::downcast(x).unwrap())
					.map_err(JoinError::panic)
			}
		}
		#[allow(unsafe_code)]
		pub(super) unsafe fn spawn_pinned_unchecked<'a, F, Fut, T>(
			&self, task: F,
		) -> impl Future<Output = Result<T, JoinError>> + Send + 'a
		where
			F: FnOnce() -> Fut + Send + 'a,
			Fut: Future<Output = T> + 'a,
			T: Send + 'a,
		{
			let sender = self.sender.clone();
			async move {
				let task: Box<dyn FnOnce() -> Box<dyn Future<Output = Response>> + Send> =
					Box::new(|| {
						Box::new(
							AssertUnwindSafe(task().map(|t| {
								let t: Box<dyn Send> = Box::new(t);
								let t: Box<dyn Any + Send> = mem::transmute(t);
								t
							}))
							.catch_unwind(),
						)
					});
				let task: Box<dyn FnOnce() -> Box<dyn Future<Output = Response>> + Send> =
					mem::transmute(task);
				let (sender_, receiver) = bounded::<RemoteHandle<Response>>(1);
				sender.send((task, sender_)).await.unwrap();
				let res = receiver.recv().await;
				let res = res.unwrap().await;
				#[allow(deprecated)]
				res.map(|t| {
					let t: *mut dyn Any = Box::into_raw(t);
					*Box::from_raw(t.cast())
				})
				.map_err(JoinError::panic)
			}
		}
	}

	#[cfg(test)]
	mod tests {
		use super::*;

		use futures::future::join_all;
		use std::sync::{
			atomic::{AtomicUsize, Ordering}, Arc
		};

		#[tokio::test(threaded_scheduler)]
		#[cfg_attr(miri, ignore)]
		async fn spawn_pinned() {
			const TASKS: usize = 1000;
			const ITERS: usize = 200;
			const THREADS: usize = 4;
			let pool = Pool::new(THREADS);
			let count = Arc::new(AtomicUsize::new((1..TASKS).sum()));
			for _ in 0..ITERS {
				join_all((0..TASKS).map(|i| {
					let count = count.clone();
					pool.spawn_pinned(move || async move {
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
}
#[cfg(not(target_arch = "wasm32"))]
use pool::Pool;
