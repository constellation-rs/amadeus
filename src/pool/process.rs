use constellation::{spawn, Receiver, Resources, Sender, SpawnError};
use futures::{future::LocalBoxFuture, FutureExt};
use serde_closure::{traits, FnOnce};
use serde_traitobject as st;
use std::{
	any, collections::VecDeque, fmt, future::Future, mem, panic::{self, RefUnwindSafe, UnwindSafe}, sync::{Arc, Mutex}
};

use amadeus_core::pool::ProcessSend;

use super::{
	util::{assert_sync_and_send, OnDrop, Panicked, RoundRobin, Synchronize}, ThreadPool
};

#[cfg_attr(not(nightly), serde_closure::desugar)]
type Request = st::Box<dyn st::sc::FnOnce(&ThreadPool) -> LocalBoxFuture<'static, Response> + Send>;
type Response = Box<dyn st::Any + Send>;

mod future_ext {
	use futures::{future::Future, pin_mut};
	use std::{
		sync::Arc, task::{Context, Poll}, thread::{self, Thread}
	};

	/// Extension trait to provide convenient [`block()`](FutureExt1::block) method on futures.
	///
	/// Named `FutureExt1` to avoid clashing with [`futures::future::FutureExt`].
	pub(crate) trait FutureExt1: Future {
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
use future_ext::FutureExt1;

#[derive(Debug)]
struct Process {
	sender: Sender<Option<Request>>,
	receiver: Receiver<Result<Response, Panicked>>,
	inner: Mutex<ProcessInner>,
	synchronize: Synchronize,
}
struct ProcessInner {
	queue: VecDeque<Queued<Result<Response, Panicked>>>,
	received: usize,
	tail: usize,
}
impl fmt::Debug for ProcessInner {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.debug_struct("ProcessInner")
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

#[derive(Debug)]
struct ProcessPoolInner {
	processes: Vec<Process>,
	i: RoundRobin,
}
impl ProcessPoolInner {
	#[allow(clippy::double_parens)] // TODO: work out what's triggering this
	fn new(
		processes: Option<usize>, threads: Option<usize>, tasks: Option<usize>,
		resources: Resources,
	) -> Result<Self, SpawnError> {
		let processes = processes.unwrap_or(3); // TODO!
		let mut processes_vec = Vec::with_capacity(processes);
		for _ in 0..processes {
			let child = spawn(
				resources,
				FnOnce!(move |parent| {
					tokio::runtime::Builder::new()
						.threaded_scheduler()
						.enable_all()
						.build()
						.unwrap()
						.block_on(async {
							let receiver = Receiver::<Option<Request>>::new(parent);
							let sender = Sender::<Result<Response, Panicked>>::new(parent);

							let thread_pool = ThreadPool::new(threads, tasks).unwrap();

							while let Some(work) = receiver.recv().await.unwrap() {
								let ret = panic::catch_unwind(panic::AssertUnwindSafe(|| {
									work.into_box().call_once_box((&thread_pool,))
								}));
								let ret = match ret {
									Ok(t) => panic::AssertUnwindSafe(t).catch_unwind().await,
									Err(e) => Err(e),
								}
								.map_err(Panicked::from);
								sender.send(ret).await;
							}
						})
				}),
			)
			.block();
			if let Err(err) = child {
				for Process { sender, .. } in processes_vec {
					// TODO: select
					sender.send(None).block();
				}
				return Err(err);
			}
			let child = child.unwrap();

			let sender = Sender::new(child);
			let receiver = Receiver::new(child);

			let (queue, received, tail) = (VecDeque::new(), 0, 0);

			processes_vec.push(Process {
				sender,
				receiver,
				inner: Mutex::new(ProcessInner {
					queue,
					received,
					tail,
				}),
				synchronize: Synchronize::new(),
			})
		}
		let i = RoundRobin::new(0, processes_vec.len());
		Ok(Self {
			processes: processes_vec,
			i,
		})
	}
	fn processes(&self) -> usize {
		self.processes.len()
	}
	async fn spawn<F, Fut, T>(&self, work: F) -> Result<T, Panicked>
	where
		F: for<'a> traits::FnOnce<(&'a ThreadPool,), Output = Fut> + ProcessSend + 'static,
		Fut: Future<Output = T> + 'static,
		T: ProcessSend + 'static,
	{
		let process_index = self.i.get();
		let process = &self.processes[process_index];
		let x = process
			.sender
			.send(Some(st::Box::new(FnOnce!(move |thread_pool: &_| {
				let work: F = work;
				work.call_once((thread_pool,))
					.map(|res| Box::new(res) as Response)
					.boxed_local()
			})) as Request));
		x.await;
		let index;
		{
			// https://github.com/rust-lang/rust/issues/57478
			let mut process_inner_lock = process.inner.lock().unwrap();
			process_inner_lock.queue.push_back(Queued::Awaiting);
			index = process_inner_lock.tail + process_inner_lock.queue.len() - 1;
			drop(process_inner_lock);
		}
		let on_drop = OnDrop::new(|| {
			let mut process_inner_lock = process.inner.lock().unwrap();
			let offset = index - process_inner_lock.tail;
			process_inner_lock.queue[offset].drop_();
			while let Some(Queued::Taken) = process_inner_lock.queue.front() {
				let _ = process_inner_lock.queue.pop_front().unwrap();
				process_inner_lock.tail += 1;
			}
			drop(process_inner_lock);
		});
		while process.inner.lock().unwrap().received <= index {
			process
				.synchronize
				.synchronize(async {
					if process.inner.lock().unwrap().received > index {
						return;
					}
					let z = process.receiver.recv().await;
					let t = z.unwrap();
					let mut process_inner_lock = process.inner.lock().unwrap();
					let offset = process_inner_lock.received - process_inner_lock.tail;
					process_inner_lock.queue[offset].received(t);
					process_inner_lock.received += 1;
					drop(process_inner_lock);
				})
				.await;
		}
		on_drop.cancel();
		let mut process_inner_lock = process.inner.lock().unwrap();
		let offset = index - process_inner_lock.tail;
		let boxed = process_inner_lock.queue[offset].take();
		while let Some(Queued::Taken) = process_inner_lock.queue.front() {
			let _ = process_inner_lock.queue.pop_front().unwrap();
			process_inner_lock.tail += 1;
		}
		drop(process_inner_lock);
		boxed.map(|boxed| *Box::<dyn any::Any>::downcast::<T>(boxed.into_any_send()).unwrap())
	}
	#[allow(unsafe_code)]
	async unsafe fn spawn_unchecked<'a, F, Fut, T>(&self, work: F) -> Result<T, Panicked>
	where
		F: for<'b> traits::FnOnce<(&'b ThreadPool,), Output = Fut> + ProcessSend + 'a,
		Fut: Future<Output = T> + 'a,
		T: ProcessSend + 'a,
	{
		let process_index = self.i.get();
		let process = &self.processes[process_index];
		let request = st::Box::new(FnOnce!(move |thread_pool: &_| {
			let work: F = work;
			work.call_once((thread_pool,))
				.map(|response| {
					let response = bincode::serialize(&response).unwrap();
					Box::new(response) as Response
				})
				.boxed_local()
		}));
		let request = mem::transmute::<
			st::Box<dyn st::sc::FnOnce(&ThreadPool) -> LocalBoxFuture<'a, Response> + Send>,
			st::Box<dyn st::sc::FnOnce(&ThreadPool) -> LocalBoxFuture<'static, Response> + Send>,
		>(request);
		let x = process.sender.send(Some(request));
		x.await;
		let index;
		{
			// https://github.com/rust-lang/rust/issues/57478
			let mut process_inner_lock = process.inner.lock().unwrap();
			process_inner_lock.queue.push_back(Queued::Awaiting);
			index = process_inner_lock.tail + process_inner_lock.queue.len() - 1;
			drop(process_inner_lock);
		}
		let on_drop = OnDrop::new(|| {
			let mut process_inner_lock = process.inner.lock().unwrap();
			let offset = index - process_inner_lock.tail;
			process_inner_lock.queue[offset].drop_();
			while let Some(Queued::Taken) = process_inner_lock.queue.front() {
				let _ = process_inner_lock.queue.pop_front().unwrap();
				process_inner_lock.tail += 1;
			}
			drop(process_inner_lock);
		});
		while process.inner.lock().unwrap().received <= index {
			process
				.synchronize
				.synchronize(async {
					if process.inner.lock().unwrap().received > index {
						return;
					}
					let z = process.receiver.recv().await;
					let t = z.unwrap();
					let mut process_inner_lock = process.inner.lock().unwrap();
					let offset = process_inner_lock.received - process_inner_lock.tail;
					process_inner_lock.queue[offset].received(t);
					process_inner_lock.received += 1;
					drop(process_inner_lock);
				})
				.await;
		}
		on_drop.cancel();
		let mut process_inner_lock = process.inner.lock().unwrap();
		let offset = index - process_inner_lock.tail;
		let boxed = process_inner_lock.queue[offset].take();
		while let Some(Queued::Taken) = process_inner_lock.queue.front() {
			let _ = process_inner_lock.queue.pop_front().unwrap();
			process_inner_lock.tail += 1;
		}
		drop(process_inner_lock);
		boxed.map(|boxed| {
			bincode::deserialize(
				&Box::<dyn any::Any>::downcast::<Vec<u8>>(boxed.into_any_send()).unwrap(),
			)
			.unwrap()
		})
	}
}
impl Drop for ProcessPoolInner {
	fn drop(&mut self) {
		for Process { sender, .. } in &self.processes {
			// TODO: select, incl recv
			sender.send(None).block();
		}
	}
}

#[derive(Debug)]
pub struct ProcessPool(Arc<ProcessPoolInner>);
#[cfg_attr(not(nightly), serde_closure::desugar)]
impl ProcessPool {
	pub fn new(
		processes: Option<usize>, threads: Option<usize>, tasks: Option<usize>,
		resources: Resources,
	) -> Result<Self, SpawnError> {
		Ok(Self(Arc::new(ProcessPoolInner::new(
			processes, threads, tasks, resources,
		)?)))
	}
	pub fn processes(&self) -> usize {
		self.0.processes()
	}
	pub fn spawn<F, Fut, T>(&self, work: F) -> impl Future<Output = Result<T, Panicked>> + Send
	where
		F: traits::FnOnce(&ThreadPool) -> Fut + ProcessSend + 'static,
		Fut: Future<Output = T> + 'static,
		T: ProcessSend + 'static,
	{
		let inner = self.0.clone();
		async move { inner.spawn(work).await }
	}
	#[allow(unsafe_code)]
	pub unsafe fn spawn_unchecked<'a, F, Fut, T>(
		&self, work: F,
	) -> impl Future<Output = Result<T, Panicked>> + Send + 'a
	where
		F: traits::FnOnce(&ThreadPool) -> Fut + ProcessSend + 'a,
		Fut: Future<Output = T> + 'a,
		T: ProcessSend + 'a,
	{
		let inner = self.0.clone();
		async move { inner.spawn_unchecked(work).await }
	}
}

impl Clone for ProcessPool {
	/// Cloning a pool will create a new handle to the pool.
	/// The behavior is similar to [Arc](https://doc.rust-lang.org/stable/std/sync/struct.Arc.html).
	///
	/// We could for example submit jobs from multiple threads concurrently.
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl UnwindSafe for ProcessPool {}
impl RefUnwindSafe for ProcessPool {}

fn _assert() {
	let _ = assert_sync_and_send::<ProcessPool>;
}
