use serde::{Deserialize, Serialize};
use std::{
	any::Any, error::Error, fmt::{self, Debug, Display}, future::Future, mem, pin::Pin, sync::{
		atomic::{AtomicBool, AtomicUsize, Ordering}, Mutex
	}, task::{Context, Poll, Waker}
};

#[cfg(feature = "constellation")]
pub use constellation::FutureExt1;

#[cfg(not(feature = "constellation"))]
mod future_ext {
	use futures::{future::Future, pin_mut};
	use std::{
		sync::Arc, task::{Context, Poll}, thread::{self, Thread}
	};

	/// Extension trait to provide convenient [`block()`](FutureExt1::block) method on futures.
	///
	/// Named `FutureExt1` to avoid clashing with [`futures::future::FutureExt`].
	pub trait FutureExt1: Future {
		/// Convenience method over `futures::executor::block_on(future)`.
		fn block(self) -> Self::Output
		where
			Self: Sized,
		{
			// futures::executor::block_on(self) // Not reentrant for some reason
			struct ThreadNotify {
				thread: Thread,
			}
			impl futures::task::ArcWake for ThreadNotify {
				fn wake_by_ref(arc_self: &Arc<Self>) {
					arc_self.thread.unpark();
				}
			}
			let f = self;
			pin_mut!(f);
			let thread_notify = Arc::new(ThreadNotify {
				thread: thread::current(),
			});
			let waker = futures::task::waker_ref(&thread_notify);
			let mut cx = Context::from_waker(&waker);
			loop {
				if let Poll::Ready(t) = f.as_mut().poll(&mut cx) {
					return t;
				}
				thread::park();
			}
		}
	}
	impl<T: ?Sized> FutureExt1 for T where T: Future {}
}
#[cfg(not(feature = "constellation"))]
pub use future_ext::FutureExt1;

#[cfg(feature = "constellation")]
#[derive(Debug)]
pub struct RoundRobin(AtomicUsize, usize);
#[cfg(feature = "constellation")]
impl RoundRobin {
	pub fn new(start: usize, limit: usize) -> Self {
		Self(AtomicUsize::new(start), limit)
	}
	pub fn get(&self) -> usize {
		let i = self.0.fetch_add(1, Ordering::Relaxed);
		if i >= self.1 && i % self.1 == 0 {
			let _ = self.0.fetch_sub(self.1, Ordering::Relaxed);
		}
		i % self.1
	}
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Panicked(String);
impl From<Box<dyn Any + Send>> for Panicked {
	fn from(e: Box<dyn Any + Send>) -> Self {
		// https://github.com/rust-lang/rust/blob/b43eb4235ac43c822d903ad26ed806f34cc1a14a/src/libstd/panicking.rs#L179-L185
		Self(
			e.downcast::<String>()
				.map(|x| *x)
				.or_else(|e| e.downcast::<&str>().map(|x| (*x).to_owned()))
				.unwrap_or_else(|_| String::from("Box<Any>")),
		)
	}
}
impl Error for Panicked {
	fn description(&self) -> &str {
		&self.0
	}
}
impl Display for Panicked {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}
impl Debug for Panicked {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		Debug::fmt(&self.0, f)
	}
}

pub struct OnDrop<F: FnOnce()>(Option<F>);
impl<F: FnOnce()> OnDrop<F> {
	pub fn new(f: F) -> Self {
		Self(Some(f))
	}
	pub fn cancel(mut self) {
		let _ = self.0.take().unwrap();
		mem::forget(self);
	}
}
impl<F: FnOnce()> Drop for OnDrop<F> {
	fn drop(&mut self) {
		self.0.take().unwrap()();
	}
}

#[derive(Debug)]
pub struct Synchronize {
	nonce: AtomicUsize,
	running: AtomicBool,
	wake: Mutex<Vec<Waker>>,
}
impl Synchronize {
	pub fn new() -> Self {
		Self {
			nonce: AtomicUsize::new(0),
			running: AtomicBool::new(false),
			wake: Mutex::new(Vec::new()),
		}
	}
	pub fn synchronize<'a, F>(&'a self, f: F) -> impl Future<Output = ()> + 'a
	where
		F: Future<Output = ()> + 'a,
		Self: 'a,
	{
		async move {
			let nonce = self.nonce.load(Ordering::SeqCst);
			if !self.running.compare_and_swap(false, true, Ordering::SeqCst) {
				let on_drop = OnDrop::new(|| self.running.store(false, Ordering::SeqCst));
				f.await;
				on_drop.cancel();
				let _ = self.nonce.fetch_add(1, Ordering::SeqCst);
				self.running.store(false, Ordering::SeqCst);
				for waker in mem::replace(&mut *self.wake.lock().unwrap(), Vec::new()) {
					waker.wake();
				}
				return;
			}
			futures::future::poll_fn(|cx| {
				self.wake.lock().unwrap().push(cx.waker().clone());
				if nonce != self.nonce.load(Ordering::SeqCst) {
					Poll::Ready(())
				} else {
					Poll::Pending
				}
			})
			.await
		}
	}
}

pub struct ImplSync<F>(F);
impl<F> ImplSync<F> {
	pub unsafe fn new(f: F) -> Self {
		Self(f)
	}
}
impl<F> Future for ImplSync<F>
where
	F: Future,
{
	type Output = F::Output;
	fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		unsafe { self.map_unchecked_mut(|s| &mut s.0) }.poll(cx)
	}
}
unsafe impl<F> Sync for ImplSync<F> {}

pub fn assert_sync_and_send<T: Send + Sync>(t: T) -> T {
	t
}
