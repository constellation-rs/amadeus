// TODO: get rid of 'static and boxing

use constellation::FutureExt1;
use futures::{sink::SinkExt, stream::StreamExt};
use std::{
	any::Any, collections::VecDeque, fmt, future::Future, io, mem, panic, sync::{Arc, Mutex}, thread::{self, JoinHandle}
};

use super::util::{assert_sync_and_send, ImplSync, OnDrop, Panicked, Synchronize};

type Request = Box<dyn FnOnce() -> Response + Send>;
type Response = Box<dyn Any + Send>;

const CHANNEL_SIZE: usize = 100;

struct ThreadPoolQueue {
	queue: VecDeque<Queued<Result<Response, Panicked>>>,
	received: usize,
	tail: usize,
}
impl fmt::Debug for ThreadPoolQueue {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.debug_struct("ThreadPoolQueue")
			.field("queue", &())
			.field("received", &self.received)
			.field("tail", &self.tail)
			.finish()
	}
}

#[derive(Debug)]
enum Queued<T> {
	Awaiting,
	Got(T),
	Taken,
}
impl<T> Queued<T> {
	fn received(&mut self, t: T) {
		if let Self::Awaiting = self {
			*self = Self::Got(t);
		}
	}
	fn take(&mut self) -> T {
		if let Self::Got(t) = mem::replace(self, Self::Taken) {
			t
		} else {
			panic!()
		}
	}
	fn drop_(&mut self) {
		*self = Self::Taken;
	}
}

struct Channels {
	thread_receiver: Mutex<futures::channel::mpsc::Receiver<Option<Request>>>,
	thread_sender: Mutex<futures::channel::mpsc::Sender<Result<Response, Panicked>>>,
}
impl fmt::Debug for Channels {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.debug_struct("Channels").finish()
	}
}

#[derive(Debug)]
struct ThreadPoolInner {
	threads: Vec<JoinHandle<()>>,
	pool_sender: futures::lock::Mutex<futures::channel::mpsc::Sender<Option<Request>>>,
	pool_receiver:
		futures::lock::Mutex<futures::channel::mpsc::Receiver<Result<Response, Panicked>>>,
	channels: Arc<Channels>,
	inner: Mutex<ThreadPoolQueue>,
	synchronize: Synchronize,
}
impl ThreadPoolInner {
	fn new(threads: usize) -> Result<Self, io::Error> {
		let (mut pool_sender, thread_receiver) = futures::channel::mpsc::channel(CHANNEL_SIZE);
		let (thread_sender, pool_receiver) = futures::channel::mpsc::channel(CHANNEL_SIZE);
		let channels = Arc::new(Channels {
			thread_receiver: Mutex::new(thread_receiver),
			thread_sender: Mutex::new(thread_sender),
		});
		let mut threads_vec: Vec<JoinHandle<_>> = Vec::with_capacity(threads);
		for i in 0..threads {
			let thread_channels = channels.clone();
			let child = thread::Builder::new()
				.name(format!("amadeus-{}", i))
				.spawn(move || {
					let channels = thread_channels;
					while let Some(work) = {
						let mut thread_receiver = channels.thread_receiver.lock().unwrap();
						thread_receiver.next().block().unwrap()
					} {
						let ret = panic::catch_unwind(panic::AssertUnwindSafe(work));
						let ret = ret.map_err(Panicked::from);
						let mut thread_sender = channels.thread_sender.lock().unwrap();
						thread_sender.send(ret).block().unwrap();
					}
				});
			if let Err(err) = child {
				for _ in 0..i {
					pool_sender.send(None).block().unwrap();
				}
				for thread in threads_vec.drain(..) {
					thread.join().unwrap();
				}
				return Err(err);
			}
			threads_vec.push(child.unwrap());
		}
		let (queue, received, tail) = (VecDeque::new(), 0, 0);
		Ok(Self {
			threads: threads_vec,
			pool_sender: futures::lock::Mutex::new(pool_sender),
			pool_receiver: futures::lock::Mutex::new(pool_receiver),
			channels,
			inner: Mutex::new(ThreadPoolQueue {
				queue,
				received,
				tail,
			}),
			synchronize: Synchronize::new(),
		})
	}
	fn threads(&self) -> usize {
		self.threads.len()
	}
	async fn spawn<F: FnOnce() -> T + Send + 'static, T: Send + 'static>(
		&self, work: F,
	) -> Result<T, Panicked> {
		let mut pool_sender_lock = self.pool_sender.lock().await;
		pool_sender_lock
			.send(Some(
				Box::new(move || Box::new(work()) as Response) as Request
			))
			.await
			.unwrap();
		drop(pool_sender_lock);
		let index;
		{
			// https://github.com/rust-lang/rust/issues/57478
			let mut inner_lock = self.inner.lock().unwrap();
			inner_lock.queue.push_back(Queued::Awaiting);
			index = inner_lock.tail + inner_lock.queue.len() - 1;
			drop(inner_lock);
		};
		let on_drop = OnDrop::new(|| {
			let mut inner_lock = self.inner.lock().unwrap();
			let offset = index - inner_lock.tail;
			inner_lock.queue[offset].drop_();
			while let Some(Queued::Taken) = inner_lock.queue.front() {
				let _ = inner_lock.queue.pop_front().unwrap();
				inner_lock.tail += 1;
			}
			drop(inner_lock);
		});
		while self.inner.lock().unwrap().received <= index {
			self.synchronize
				.synchronize(async {
					if self.inner.lock().unwrap().received > index {
						return;
					}
					let mut pool_receiver = self.pool_receiver.lock().await;
					let res = pool_receiver.next().await.unwrap();
					drop(pool_receiver);
					let mut inner_lock = self.inner.lock().unwrap();
					let offset = inner_lock.received - inner_lock.tail;
					inner_lock.queue[offset].received(res);
					inner_lock.received += 1;
					drop(inner_lock);
				})
				.await;
		}
		on_drop.cancel();
		let mut inner_lock = self.inner.lock().unwrap();
		let offset = index - inner_lock.tail;
		let boxed = inner_lock.queue[offset].take();
		while let Some(Queued::Taken) = inner_lock.queue.front() {
			let _ = inner_lock.queue.pop_front().unwrap();
			inner_lock.tail += 1;
		}
		drop(inner_lock);
		boxed.map(|boxed| *Box::<dyn Any>::downcast::<T>(boxed).unwrap())
	}
}
impl Drop for ThreadPoolInner {
	fn drop(&mut self) {
		for _ in 0..self.threads.len() {
			let mut pool_sender = self.pool_sender.lock().block();
			pool_sender.send(None).block().unwrap();
		}
		for thread in self.threads.drain(..) {
			thread.join().unwrap();
		}
	}
}

// a Box<dyn st::Any + Send>, which is what auto un-Sync's us, can only be accessed from a single thread.
unsafe impl Sync for ThreadPoolInner {}

#[derive(Debug)]
pub struct ThreadPool(Arc<ThreadPoolInner>);
impl ThreadPool {
	pub fn new(threads: usize) -> Result<Self, io::Error> {
		Ok(Self(Arc::new(ThreadPoolInner::new(threads)?)))
	}
	pub fn threads(&self) -> usize {
		self.0.threads()
	}
	pub fn spawn<F: FnOnce() -> T + Send + 'static, T: Send + 'static>(
		&self, work: F,
	) -> impl Future<Output = Result<T, Panicked>> + 'static {
		let inner = self.0.clone();
		let future = async move { inner.spawn(work).await };
		assert_sync_and_send(unsafe { ImplSync::new(future) })
	}
}

fn _assert() {
	let _ = assert_sync_and_send::<ThreadPool>;
}
