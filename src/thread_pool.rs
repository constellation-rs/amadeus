// TODO: Is 'static or Any friendlier in user-facing API?

use std::{
	any::Any, boxed::FnBox, collections::VecDeque, fmt, io, marker::PhantomData, mem, panic, sync::{atomic, Arc, Mutex, RwLock}, thread
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

type Request = dyn FnBox() -> Box<Response> + Send;
type Response = dyn Any + Send;

#[derive(Debug)]
struct Thread {
	sender: crossbeam_channel::Sender<Option<Box<Request>>>,
	receiver: crossbeam_channel::Receiver<Option<Box<Response>>>,
	inner: RwLock<ThreadInner>,
	recv: Mutex<()>,
}
struct ThreadInner {
	queue: VecDeque<Queued<Option<Box<Response>>>>,
	received: usize,
	tail: usize,
}
impl fmt::Debug for ThreadInner {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.debug_struct("ThreadInner")
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
struct ThreadPoolInner {
	processes: Vec<Thread>,
	i: RoundRobin,
}
impl ThreadPoolInner {
	fn new(processes: usize) -> Result<Self, io::Error> {
		let mut processes_vec = Vec::with_capacity(processes);
		for _ in 0..processes {
			let (sender, child_receiver) = crossbeam_channel::bounded(0); // TODO: increase cap
			let (child_sender, receiver) = crossbeam_channel::bounded(0); // TODO: increase cap
			let child = thread::Builder::new().spawn(move || {
				while let Some(work) = child_receiver.recv().unwrap() {
					let ret = panic::catch_unwind(panic::AssertUnwindSafe(work));
					child_sender.send(ret.ok()).unwrap();
				}
			});
			if let Err(err) = child {
				for Thread { sender, .. } in processes_vec {
					// TODO: select
					sender.send(None).unwrap();
				}
				return Err(err);
			}
			let child = child.unwrap();

			let (queue, received, tail) = (VecDeque::new(), 0, 0);

			processes_vec.push(Thread {
				sender,
				receiver,
				inner: RwLock::new(ThreadInner {
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
	fn spawn<F: Any + FnOnce() -> T + Send, T: Any + Send>(&self, work: F) -> JoinHandleInner<T> {
		let process_index = self.i.get();
		let process = &self.processes[process_index];
		process
			.sender
			.send(Some(Box::new(FnOnce!([work] move || {
				let work: F = work;
				Box::new(work()) as Box<Response>
			})) as Box<Request>))
			.unwrap();
		let mut process_inner = process.inner.write().unwrap();
		process_inner.queue.push_back(Queued::Awaiting);
		JoinHandleInner(
			process_index,
			process_inner.tail + process_inner.queue.len() - 1,
			PhantomData,
		)
	}
	fn join<T: Any>(&self, key: JoinHandleInner<T>) -> Result<T, ()> {
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
				// let x = Box::<Any>::downcast::<String>(unsafe{std::ptr::read(&t)}.into_any_send_sync()).unwrap();
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
		let boxed: Option<Box<_>> = process_inner.queue[key.1 - process_inner.tail].take();
		while let Some(Queued::Taken) = process_inner.queue.front() {
			let _ = process_inner.queue.pop_front().unwrap();
			process_inner.tail += 1;
		}
		// println!("{:?} /process lock", thread::current().name().unwrap());
		boxed
			.map(|boxed| *Box::<Any>::downcast::<T>(boxed).unwrap())
			.ok_or(())
	}
	fn drop_<T: Any>(&self, key: JoinHandleInner<T>) {
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
impl Drop for ThreadPoolInner {
	fn drop(&mut self) {
		for Thread { sender, .. } in &self.processes {
			// TODO: select, incl recv
			sender.send(None).unwrap();
		}
	}
}

// an Box<Response>, which is what auto un-Sync's us, can only be accessed from a single thread.
unsafe impl Sync for ThreadPoolInner {}

struct JoinHandleInner<T: Any>(usize, usize, PhantomData<fn() -> T>);

#[derive(Debug)]
pub struct ThreadPool(Arc<ThreadPoolInner>);
impl ThreadPool {
	pub fn new(processes: usize) -> Result<Self, io::Error> {
		Ok(ThreadPool(Arc::new(ThreadPoolInner::new(processes)?)))
	}
	pub fn processes(&self) -> usize {
		self.0.processes()
	}
	pub fn spawn<F: FnOnce() -> T + Send + 'static, T: Send + 'static>(
		&self, work: F,
	) -> JoinHandle<T> {
		JoinHandle(self.0.clone(), Some(self.0.spawn(work)))
	}
}

pub struct JoinHandle<T: 'static>(Arc<ThreadPoolInner>, Option<JoinHandleInner<T>>);
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
	_assert_both::<ThreadPool>();
}
