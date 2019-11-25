use constellation::*;
use serde_closure::FnOnce;
use serde_traitobject as st;
use std::{
	any, collections::VecDeque, fmt, future::Future, mem, panic::{self, RefUnwindSafe, UnwindSafe}, sync::{Arc, Mutex}
};

use amadeus_core::pool::ProcessSend;

use super::util::{assert_sync_and_send, OnDrop, Panicked, RoundRobin, Synchronize};

// type Request = Box<dyn st::FnOnce() -> Response + Send>; // when #![feature(unboxed_closures)] is stable
type Request = Box<dyn st::FnOnce<(), Output = Response> + Send>;
type Response = Box<dyn st::Any + Send>;

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
	threads: usize,
	i: RoundRobin,
}
impl ProcessPoolInner {
	fn new(processes: usize, threads: usize, resources: Resources) -> Result<Self, SpawnError> {
		let mut processes_vec = Vec::with_capacity(processes);
		for _ in 0..processes {
			let child = spawn(
				resources,
				FnOnce!(move |parent| {
					let receiver = Receiver::<Option<Request>>::new(parent);
					let sender = Sender::<Result<Response, Panicked>>::new(parent);

					let _ = threads;

					while let Some(work) = receiver.recv().block().unwrap() {
						let ret = panic::catch_unwind(panic::AssertUnwindSafe(work));
						let ret = ret.map_err(Panicked::from);
						sender.send(ret).block();
					}
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
			threads,
			i,
		})
	}
	fn processes(&self) -> usize {
		self.processes.len()
	}
	fn threads(&self) -> usize {
		self.threads
	}
	async fn spawn<F: FnOnce() -> T + ProcessSend, T: ProcessSend>(
		&self, work: F,
	) -> Result<T, Panicked> {
		let process_index = self.i.get();
		let process = &self.processes[process_index];
		let x = process.sender.send(Some(Box::new(FnOnce!(move || {
			let work: F = work;
			Box::new(work()) as Response
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
impl ProcessPool {
	pub fn new(processes: usize, threads: usize, resources: Resources) -> Result<Self, SpawnError> {
		Ok(Self(Arc::new(ProcessPoolInner::new(
			processes, threads, resources,
		)?)))
	}
	pub fn processes(&self) -> usize {
		self.0.processes()
	}
	pub fn threads(&self) -> usize {
		self.0.threads()
	}
	pub fn spawn<F: FnOnce() -> T + ProcessSend, T: ProcessSend>(
		&self, work: F,
	) -> impl Future<Output = Result<T, Panicked>> + Send + 'static {
		// TODO: + Sync
		let inner = self.0.clone();
		async move { inner.spawn(work).await }
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
