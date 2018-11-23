#[derive(Copy, Clone, Default, Debug)]
pub struct NoPool;
impl NoPool {
	pub fn new() -> Self {
		NoPool
	}
	pub fn processes(&self) -> usize {
		1
	}
	pub fn spawn<F: FnOnce() -> T, T>(&self, work: F) -> JoinHandle<T> {
		JoinHandle(work())
	}
}

#[derive(Debug)]
pub struct JoinHandle<T>(T);
impl<T> JoinHandle<T> {
	pub fn join(self) -> Result<T, ()> {
		Ok(self.0)
	}
}
