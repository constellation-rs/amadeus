use std::panic;

use super::util::Panicked;

#[derive(Copy, Clone, Default, Debug)]
pub struct LocalPool(());
impl LocalPool {
	pub fn new() -> Self {
		LocalPool(())
	}
	pub fn spawn<F: FnOnce() -> T, T>(
		&self, work: F,
	) -> impl std::future::Future<Output = Result<T, Panicked>> {
		let _self = self;
		let ret = panic::catch_unwind(panic::AssertUnwindSafe(work));
		let ret = ret.map_err(Panicked::from);
		futures::future::ready(ret)
	}
}
