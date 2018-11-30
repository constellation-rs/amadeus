// TODO: Is 'static or Any friendlier in user-facing API?

use constellation::*;
use serde::{de::DeserializeOwned, ser::Serialize};
use serde_traitobject as st;
use std::{
	any, collections::VecDeque, fmt, marker::PhantomData, mem, panic, sync::{atomic, Arc, Mutex, RwLock}
};

#[derive(Debug)]
struct RoundRobin(atomic::AtomicUsize, usize);
impl RoundRobin {
	fn new(start: usize, limit: usize) -> Self {
		RoundRobin(atomic::AtomicUsize::new(start), limit)
	}
	fn get(&self) -> usize {
		let i = self.0.fetch_add(1, atomic::Ordering::Relaxed);
		if i >= self.1 && i % self.1 == 0 {
			let _ = self.0.fetch_sub(self.1, atomic::Ordering::Relaxed);
		}
		i % self.1
	}
}

type Request = dyn st::FnBox() -> st::Box<Response>;
type Response = dyn st::Any + Send;

#[derive(Debug)]
struct Process {
	sender: Sender<Option<st::Box<Request>>>,
	receiver: Receiver<Option<st::Box<Response>>>,
	inner: RwLock<ProcessInner>,
	recv: Mutex<()>,
}
struct ProcessInner {
	queue: VecDeque<Queued<Option<st::Box<Response>>>>,
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
		if let Queued::Awaiting = self {
			*self = Queued::Got(t);
		}
	}
	fn take(&mut self) -> T {
		if let Queued::Got(t) = mem::replace(self, Queued::Taken) {
			t
		} else {
			panic!()
		}
	}
	fn drop_(&mut self) {
		*self = Queued::Taken;
	}
}

#[derive(Debug)]
struct ProcessPoolInner {
	processes: Vec<Process>,
	i: RoundRobin,
}
impl ProcessPoolInner {
	fn new(processes: usize, resources: Resources) -> Result<Self, ()> {
		let mut processes_vec = Vec::with_capacity(processes);
		for _ in 0..processes {
			let child = spawn(
				resources,
				FnOnce!(|parent| {
					let receiver = Receiver::<Option<st::Box<Request>>>::new(parent);
					let sender = Sender::<Option<st::Box<Response>>>::new(parent);

					while let Some(work) = receiver.recv().unwrap() {
						let ret = panic::catch_unwind(panic::AssertUnwindSafe(work));
						sender.send(ret.ok());
					}
				}),
			);
			if let Err(err) = child {
				for Process { sender, .. } in processes_vec {
					// TODO: select
					sender.send(None);
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
				inner: RwLock::new(ProcessInner {
					queue,
					received,
					tail,
				}),
				recv: Mutex::new(()),
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
	fn spawn<
		F: any::Any + FnOnce() -> T + Serialize + DeserializeOwned,
		T: any::Any + Send + Serialize + DeserializeOwned,
	>(
		&self, work: F,
	) -> JoinHandleInner<T> {
		let process_index = self.i.get();
		let process = &self.processes[process_index];
		process
			.sender
			.send(Some(st::Box::new(FnOnce!([work] move || {
				let work: F = work;
				st::Box::new(work()) as st::Box<Response>
			})) as st::Box<Request>));
		let mut process_inner = process.inner.write().unwrap();
		process_inner.queue.push_back(Queued::Awaiting);
		JoinHandleInner(
			process_index,
			process_inner.tail + process_inner.queue.len() - 1,
			PhantomData,
		)
	}
	fn join<T: any::Any>(&self, key: JoinHandleInner<T>) -> Result<T, ()> {
		let process = &self.processes[key.0];
		{
			// println!("{:?} recv lock {}", thread::current().name().unwrap(), key.0);
			let _x = process.recv.lock().unwrap();
			// println!("{:?} process inner read", thread::current().name().unwrap());
			while process.inner.read().unwrap().received <= key.1 {
				// println!("{:?} /process inner read", thread::current().name().unwrap());
				// let mut z = None;
				// // println!("{:?} recv", thread::current().name().unwrap());
				// run(vec![Box::new(process.receiver.selectable_recv(|t| {
				// 	z = Some(t.unwrap());
				// }))]);
				let z = process.receiver.recv();
				// println!("{:?} /recv", thread::current().name().unwrap());
				let t = z.unwrap();
				// let x = Box::<any::Any>::downcast::<String>(unsafe{std::ptr::read(&t)}.into_any_send_sync()).unwrap();
				// println!("{}: got {}", thread::current().name().unwrap(), x);
				let process_inner = &mut *process.inner.write().unwrap();
				process_inner.queue[process_inner.received - process_inner.tail].received(t);
				process_inner.received += 1;
				// println!("{:?} recvd2 {}", thread::current().name().unwrap(), x);
				// mem::forget(x);
			}
			// println!("{:?} /recv lock {}", thread::current().name().unwrap(), key.0);
		}
		// println!("{:?} process lock", thread::current().name().unwrap());
		let process_inner = &mut *process.inner.write().unwrap();
		let boxed: Option<st::Box<_>> = process_inner.queue[key.1 - process_inner.tail].take();
		while let Some(Queued::Taken) = process_inner.queue.front() {
			let _ = process_inner.queue.pop_front().unwrap();
			process_inner.tail += 1;
		}
		// println!("{:?} /process lock", thread::current().name().unwrap());
		boxed
			.map(|boxed| *Box::<any::Any>::downcast::<T>(boxed.into_any_send()).unwrap())
			.ok_or(())
	}
	fn drop_<T: any::Any>(&self, key: JoinHandleInner<T>) {
		let process = &self.processes[key.0];
		// println!("{:?} process lock", thread::current().name().unwrap());
		let process_inner = &mut *process.inner.write().unwrap();
		process_inner.queue[key.1 - process_inner.tail].drop_();
		while let Some(Queued::Taken) = process_inner.queue.front() {
			let _ = process_inner.queue.pop_front().unwrap();
			process_inner.tail += 1;
		}
		// println!("{:?} /process lock", thread::current().name().unwrap());
	}
}
impl Drop for ProcessPoolInner {
	fn drop(&mut self) {
		for Process { sender, .. } in &self.processes {
			// TODO: select, incl recv
			sender.send(None);
		}
	}
}

// an st::Box<Response>, which is what auto un-Sync's us, can only be accessed from a single thread.
unsafe impl Sync for ProcessPoolInner {}

struct JoinHandleInner<T: any::Any>(usize, usize, PhantomData<fn() -> T>);

#[derive(Debug)]
pub struct ProcessPool(Arc<ProcessPoolInner>);
impl ProcessPool {
	pub fn new(processes: usize, resources: Resources) -> Result<Self, ()> {
		Ok(ProcessPool(Arc::new(ProcessPoolInner::new(
			processes, resources,
		)?)))
	}
	pub fn processes(&self) -> usize {
		self.0.processes()
	}
	pub fn spawn<
		F: FnOnce() -> T + Serialize + DeserializeOwned + 'static,
		T: Send + Serialize + DeserializeOwned + 'static,
	>(
		&self, work: F,
	) -> JoinHandle<T> {
		JoinHandle(self.0.clone(), Some(self.0.spawn(work)))
	}
}

pub struct JoinHandle<T: 'static>(Arc<ProcessPoolInner>, Option<JoinHandleInner<T>>);
impl<T: 'static> JoinHandle<T> {
	pub fn join(mut self) -> Result<T, ()> {
		self.0.join(self.1.take().unwrap())
	}
}
impl<T: 'static> Drop for JoinHandle<T> {
	fn drop(&mut self) {
		if let Some(handle) = self.1.take() {
			self.0.drop_(handle)
		}
	}
}
impl<T> fmt::Debug for JoinHandle<T> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.pad("JoinHandle { .. }")
	}
}

fn _assert_sync_and_send() {
	fn _assert_both<T: Send + Sync>() {}
	_assert_both::<JoinHandle<()>>();
	_assert_both::<ProcessPool>();
}
