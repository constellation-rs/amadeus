use std::panic;

use super::util::Panicked;

#[derive(Copy, Clone, Default, Debug)]
pub struct LocalPool;
impl LocalPool {
	pub fn new() -> Self {
		LocalPool
	}
	pub fn processes() -> usize {
		1
	}
	pub fn spawn<F: FnOnce() -> T, T>(
		work: F,
	) -> impl std::future::Future<Output = Result<T, Panicked>> {
		let ret = panic::catch_unwind(panic::AssertUnwindSafe(work));
		let ret = ret.map_err(Panicked::from);
		futures::future::ready(ret)
	}
}
