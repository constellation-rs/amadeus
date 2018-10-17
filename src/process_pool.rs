use constellation::*;
use serde::{de::DeserializeOwned, ser::Serialize};
use serde_traitobject as st;
use std::{
	any, collections::VecDeque, marker, mem, sync::{atomic, Arc, Mutex, RwLock}
};

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

struct Process {
	sender: Sender<Option<st::Box<st::FnBox() -> st::Box<st::Any + Send + Sync>>>>,
	receiver: Receiver<st::Box<st::Any + Send + Sync>>,
	inner: RwLock<ProcessInner>,
	recv: Mutex<()>,
}
struct ProcessInner {
	queue: VecDeque<Queued<st::Box<st::Any + Send + Sync>>>,
	received: usize,
	tail: usize,
}

enum Queued<T> {
	Awaiting,
	Got(T),
	Taken,
}
impl<T> Queued<T> {
	fn received(&mut self, t: T) {
		if let Queued::Awaiting = self {
			mem::replace(self, Queued::Got(t));
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
		mem::replace(self, Queued::Taken);
	}
}

struct ProcessPoolInner {
	processes: Vec<Process>,
	i: RoundRobin,
}
impl ProcessPoolInner {
	fn new(processes: usize, resources: Resources) -> Self {
		let processes = (0..processes)
			.map(|_| {
				let child = spawn(
					resources,
					FnOnce!(|parent| {
						let receiver = Receiver::<
							Option<st::Box<st::FnBox() -> st::Box<st::Any + Send + Sync>>>,
						>::new(parent);
						let sender = Sender::<st::Box<st::Any + Send + Sync>>::new(parent);

						while let Some(work) = receiver.recv().unwrap() {
							let ret = work();
							sender.send(ret);
						}
					}),
				)
				.expect("Unable to allocate process!");

				let sender = Sender::new(child);
				let receiver = Receiver::new(child);

				let (queue, received, tail) = (VecDeque::new(), 0, 0);

				Process {
					sender,
					receiver,
					inner: RwLock::new(ProcessInner {
						queue,
						received,
						tail,
					}),
					recv: Mutex::new(()),
				}
			})
			.collect::<Vec<_>>();
		let i = RoundRobin::new(0, processes.len());
		ProcessPoolInner { processes, i }
	}
	fn processes(&self) -> usize {
		self.processes.len()
	}
	fn spawn<
		F: FnOnce() -> T + Serialize + DeserializeOwned + 'static,
		T: any::Any + Send + Sync + Serialize + DeserializeOwned,
	>(
		&self, work: F,
	) -> JoinHandleInner<T> {
		let process_index = self.i.get();
		let process = &self.processes[process_index];
		process
			.sender
			.send(Some(st::Box::new(FnOnce!([work] move || {
				let work: F = work;
				st::Box::new(work()) as st::Box<st::Any+Send+Sync>
			}))
				as st::Box<st::FnBox() -> st::Box<st::Any + Send + Sync>>));
		let mut process_inner = process.inner.write().unwrap();
		process_inner.queue.push_back(Queued::Awaiting);
		JoinHandleInner(
			process_index,
			process_inner.tail + process_inner.queue.len() - 1,
			marker::PhantomData,
		)
	}
	fn join<T: any::Any>(&self, key: JoinHandleInner<T>) -> T {
		let process = &self.processes[key.0];
		{
			// println!("{:?} recv lock {}", thread::current().name().unwrap(), key.0);
			let _x = process.recv.lock().unwrap();
			// println!("{:?} process inner read", thread::current().name().unwrap());
			while process.inner.read().unwrap().received <= key.1 {
				// println!("{:?} /process inner read", thread::current().name().unwrap());
				let mut z = None;
				// println!("{:?} recv", thread::current().name().unwrap());
				run(vec![Box::new(process.receiver.selectable_recv(|t| {
					z = Some(t.unwrap());
				}))]);
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
		let boxed: st::Box<_> = process_inner.queue[key.1 - process_inner.tail].take();
		while let Some(Queued::Taken) = process_inner.queue.front() {
			let _ = process_inner.queue.pop_front().unwrap();
			process_inner.tail += 1;
		}
		// println!("{:?} /process lock", thread::current().name().unwrap());
		*Box::<any::Any>::downcast::<T>(boxed.into_any_send_sync()).unwrap()
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

struct JoinHandleInner<T: any::Any>(usize, usize, marker::PhantomData<fn() -> T>);

pub struct ProcessPool(Arc<ProcessPoolInner>);
impl ProcessPool {
	pub fn new(processes: usize, resources: Resources) -> Self {
		ProcessPool(Arc::new(ProcessPoolInner::new(processes, resources)))
	}
	pub fn processes(&self) -> usize {
		self.0.processes()
	}
	pub fn spawn<
		F: FnOnce() -> T + Serialize + DeserializeOwned + 'static,
		T: any::Any + Send + Sync + Serialize + DeserializeOwned,
	>(
		&self, work: F,
	) -> JoinHandle<T> {
		JoinHandle(self.0.clone(), Some(self.0.spawn(work)))
	}
}

pub struct JoinHandle<T: any::Any>(Arc<ProcessPoolInner>, Option<JoinHandleInner<T>>);
impl<T: any::Any> JoinHandle<T> {
	pub fn join(mut self) -> T {
		self.0.join(self.1.take().unwrap())
	}
}
impl<T: any::Any> Drop for JoinHandle<T> {
	fn drop(&mut self) {
		if let Some(handle) = self.1.take() {
			self.0.drop_(handle)
		}
	}
}
