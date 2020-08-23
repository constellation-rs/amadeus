use derive_new::new;
use futures::TryFutureExt;
use pin_project::pin_project;
use std::{
	any::Any, future::Future, io, panic::{RefUnwindSafe, UnwindSafe}, pin::Pin, sync::Arc, task::{Context, Poll}
};

#[cfg(not(target_arch = "wasm32"))]
use tokio::task::JoinError;
#[cfg(target_arch = "wasm32")]
use {
	futures::{future, FutureExt}, std::panic::AssertUnwindSafe
};

use amadeus_core::async_drop::PinnedAsyncDrop;

use super::util::assert_sync_and_send;

const DEFAULT_TASKS_PER_CORE: usize = 100;

#[derive(Debug)]
struct ThreadPoolInner {
	logical_cores: usize,
	tasks_per_core: usize,
	#[cfg(not(target_arch = "wasm32"))]
	pool: Pool,
}

#[derive(Debug)]
pub struct ThreadPool(Arc<ThreadPoolInner>);
impl ThreadPool {
	pub fn new(tasks_per_core: Option<usize>) -> io::Result<Self> {
		let logical_cores = if !cfg!(target_arch = "wasm32") {
			num_cpus::get()
		} else {
			1
		};
		let tasks_per_core = tasks_per_core.unwrap_or(DEFAULT_TASKS_PER_CORE);
		#[cfg(not(target_arch = "wasm32"))]
		let pool = Pool::new(logical_cores);
		Ok(ThreadPool(Arc::new(ThreadPoolInner {
			logical_cores,
			tasks_per_core,
			#[cfg(not(target_arch = "wasm32"))]
			pool,
		})))
	}
	pub fn threads(&self) -> usize {
		self.0.logical_cores * self.0.tasks_per_core
	}
	pub fn spawn<F, Fut, T>(
		&self, task: F,
	) -> JoinGuard<impl Future<Output = Result<T, Box<dyn Any + Send>>> + PinnedAsyncDrop + Send>
	where
		F: FnOnce() -> Fut + Send + 'static,
		Fut: Future<Output = T> + 'static,
		T: Send + 'static,
	{
		#[cfg(not(target_arch = "wasm32"))]
		return JoinGuard::new(
			self.0
				.pool
				.spawn_pinned(task)
				.map_err(JoinError::into_panic),
		);
		#[cfg(target_arch = "wasm32")]
		{
			let _self = self;
			let (remote, remote_handle) = AssertUnwindSafe(future::lazy(|_| task()).flatten())
				.catch_unwind()
				.map_err(Into::into)
				.remote_handle();
			wasm_bindgen_futures::spawn_local(remote);
			JoinGuard::new(remote_handle)
		}
	}
	#[allow(unsafe_code)]
	pub unsafe fn spawn_unchecked<'a, F, Fut, T>(
		&self, task: F,
	) -> JoinGuard<impl Future<Output = Result<T, Box<dyn Any + Send>>> + PinnedAsyncDrop + Send + 'a>
	where
		F: FnOnce() -> Fut + Send + 'a,
		Fut: Future<Output = T> + 'a,
		T: Send + 'a,
	{
		#[cfg(not(target_arch = "wasm32"))]
		return JoinGuard::new(
			self.0
				.pool
				.spawn_pinned_unchecked(task)
				.map_err(JoinError::into_panic),
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
			JoinGuard::new(remote_handle.map_ok(|t| {
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

#[pin_project]
#[derive(new)]
pub struct JoinGuard<F>(#[pin] F);
impl<F> Future for JoinGuard<F>
where
	F: Future,
{
	type Output = F::Output;

	#[inline(always)]
	fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		self.project().0.poll(cx)
	}
}
impl<F> PinnedAsyncDrop for JoinGuard<F>
where
	F: Future + PinnedAsyncDrop,
{
	fn poll_drop_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
		self.project().0.poll_drop_ready(cx)
	}
}

fn _assert() {
	let _ = assert_sync_and_send::<ThreadPool>;
}

// TODO: remove once spawn_pinned exists: https://github.com/tokio-rs/tokio/issues/2545
#[cfg(not(target_arch = "wasm32"))]
mod pool {
	use async_channel::{bounded, Sender};
	use futures::{
		future::{join_all, AbortHandle, Abortable, Aborted, Fuse, FusedFuture, RemoteHandle}, FutureExt
	};
	use pin_project::{pin_project, pinned_drop};
	use std::{
		any::Any, future::Future, mem, panic::AssertUnwindSafe, pin::Pin, task::{Context, Poll}
	};
	use tokio::{
		runtime::Handle, task, task::{JoinError, JoinHandle, LocalSet}
	};

	use amadeus_core::async_drop::PinnedAsyncDrop;

	type Request = Box<dyn FnOnce() -> Box<dyn Future<Output = Response>> + Send>;
	type Response = Result<Result<Box<dyn Any + Send>, Box<dyn Any + Send>>, Aborted>;

	#[derive(Debug)]
	pub(super) struct Pool {
		sender: Option<Sender<(Request, Sender<RemoteHandle<Response>>)>>,
		threads: Vec<JoinHandle<()>>,
	}
	impl Pool {
		pub(super) fn new(threads: usize) -> Self {
			let handle = Handle::current();
			let handle1 = handle.clone();
			let (sender, receiver) = bounded::<(Request, Sender<RemoteHandle<Response>>)>(1);
			let threads = (0..threads)
				.map(|_| {
					let receiver = receiver.clone();
					let handle = handle.clone();
					handle1.spawn_blocking(move || {
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
					})
				})
				.collect();
			let sender = Some(sender);
			Self { sender, threads }
		}
		pub(super) fn spawn_pinned<F, Fut, T>(
			&self, task: F,
		) -> JoinGuard<impl Future<Output = Result<T, JoinError>> + Send>
		where
			F: FnOnce() -> Fut + Send + 'static,
			Fut: Future<Output = T> + 'static,
			T: Send + 'static,
		{
			let sender = self.sender.as_ref().unwrap().clone();
			let (abort_handle, abort_registration) = AbortHandle::new_pair();
			JoinGuard::new(
				async move {
					let task: Request = Box::new(|| {
						Box::new(Abortable::new(
							AssertUnwindSafe(task().map(|t| Box::new(t) as Box<dyn Any + Send>))
								.catch_unwind(),
							abort_registration,
						))
					});
					let (sender_, receiver) = bounded::<RemoteHandle<Response>>(1);
					sender.send((task, sender_)).await.unwrap();
					drop(sender);
					let res = receiver.recv().await;
					let res = res.unwrap().await;
					#[allow(deprecated)]
					match res {
						Ok(Ok(res)) => Ok(*Box::<dyn Any + Send>::downcast(res).unwrap()),
						Ok(Err(panic)) => Err(JoinError::panic(panic)),
						Err(Aborted) => Err(JoinError::cancelled()),
					}
				},
				abort_handle,
			)
		}
		#[allow(unsafe_code)]
		pub(super) unsafe fn spawn_pinned_unchecked<'a, F, Fut, T>(
			&self, task: F,
		) -> JoinGuard<impl Future<Output = Result<T, JoinError>> + Send + 'a>
		where
			F: FnOnce() -> Fut + Send + 'a,
			Fut: Future<Output = T> + 'a,
			T: Send + 'a,
		{
			let sender = self.sender.as_ref().unwrap().clone();
			let (abort_handle, abort_registration) = AbortHandle::new_pair();
			JoinGuard::new(
				async move {
					let task: Box<dyn FnOnce() -> Box<dyn Future<Output = Response>> + Send> =
						Box::new(|| {
							Box::new(Abortable::new(
								AssertUnwindSafe(task().map(|t| {
									let t: Box<dyn Send> = Box::new(t);
									let t: Box<dyn Any + Send> = mem::transmute(t);
									t
								}))
								.catch_unwind(),
								abort_registration,
							))
						});
					let task: Box<dyn FnOnce() -> Box<dyn Future<Output = Response>> + Send> =
						mem::transmute(task);
					let (sender_, receiver) = bounded::<RemoteHandle<Response>>(1);
					sender.send((task, sender_)).await.unwrap();
					drop(sender);
					let res = receiver.recv().await;
					let res = res.unwrap().await;
					#[allow(deprecated)]
					match res {
						Ok(Ok(res)) => {
							let t: *mut dyn Any = Box::into_raw(res);
							Ok(*Box::from_raw(t as *mut T))
						}
						Ok(Err(panic)) => Err(JoinError::panic(panic)),
						Err(Aborted) => Err(JoinError::cancelled()),
					}
				},
				abort_handle,
			)
		}
	}
	impl Drop for Pool {
		fn drop(&mut self) {
			let _ = self.sender.take().unwrap();
			task::block_in_place(|| {
				Handle::current().block_on(join_all(mem::take(&mut self.threads)))
			})
			.into_iter()
			.collect::<Result<(), _>>()
			.unwrap();
		}
	}

	#[pin_project(PinnedDrop)]
	pub(crate) struct JoinGuard<F>(#[pin] Fuse<F>, AbortHandle)
	where
		F: Future;
	impl<F> JoinGuard<F>
	where
		F: Future,
	{
		#[inline(always)]
		fn new(f: F, abort_handle: AbortHandle) -> Self {
			Self(f.fuse(), abort_handle)
		}
	}
	impl<F> Future for JoinGuard<F>
	where
		F: Future,
	{
		type Output = F::Output;

		#[inline(always)]
		fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
			self.project().0.poll(cx)
		}
	}
	impl<F> PinnedAsyncDrop for JoinGuard<F>
	where
		F: Future,
	{
		fn poll_drop_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
			if !self.0.is_terminated() {
				self.project().0.poll(cx).map(drop)
			} else {
				Poll::Ready(())
			}
		}
	}
	#[pinned_drop]
	impl<F> PinnedDrop for JoinGuard<F>
	where
		F: Future,
	{
		fn drop(self: Pin<&mut Self>) {
			assert!(self.0.is_terminated());
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
