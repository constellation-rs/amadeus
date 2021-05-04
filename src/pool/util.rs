#![allow(dead_code)]

use futures::future;
use serde::{Deserialize, Serialize};
use std::{
	any::Any, error::Error, fmt::{self, Debug, Display}, future::Future, mem, sync::{
		atomic::{AtomicBool, AtomicUsize, Ordering}, Mutex
	}, task::{Poll, Waker}
};

#[cfg(feature = "constellation")]
#[derive(Debug)]
pub(crate) struct RoundRobin(AtomicUsize, usize);
#[cfg(feature = "constellation")]
impl RoundRobin {
	pub(crate) fn new(start: usize, limit: usize) -> Self {
		Self(AtomicUsize::new(start), limit)
	}
	pub(crate) fn get(&self) -> usize {
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

pub(crate) struct OnDrop<F: FnOnce()>(Option<F>);
impl<F: FnOnce()> OnDrop<F> {
	pub(crate) fn new(f: F) -> Self {
		Self(Some(f))
	}
	pub(crate) fn cancel(mut self) {
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
pub(crate) struct Synchronize {
	nonce: AtomicUsize,
	running: AtomicBool,
	wake: Mutex<Vec<Waker>>,
}
impl Synchronize {
	pub(crate) fn new() -> Self {
		Self {
			nonce: AtomicUsize::new(0),
			running: AtomicBool::new(false),
			wake: Mutex::new(Vec::new()),
		}
	}
	pub(crate) async fn synchronize<F>(&self, f: F)
	where
		F: Future<Output = ()>,
	{
		let nonce = self.nonce.load(Ordering::SeqCst);
		if self
			.running
			.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
			.is_ok()
		{
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
		future::poll_fn(|cx| {
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

pub(crate) fn assert_sync_and_send<T: Send + Sync>(t: T) -> T {
	t
}
