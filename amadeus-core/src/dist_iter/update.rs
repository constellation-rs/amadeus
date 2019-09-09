use serde::{Deserialize, Serialize};

use super::{Consumer, ConsumerMulti, DistributedIterator, DistributedIteratorMulti};
use crate::pool::ProcessSend;

#[must_use]
pub struct Update<I, F> {
	i: I,
	f: F,
}
impl<I, F> Update<I, F> {
	pub(super) fn new(i: I, f: F) -> Self {
		Self { i, f }
	}
}

impl<I: DistributedIterator, F> DistributedIterator for Update<I, F>
where
	F: FnMut(&mut I::Item) + Clone + ProcessSend,
{
	type Item = I::Item;
	type Task = UpdateConsumer<I::Task, F>;

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.i.size_hint()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.i.next_task().map(|task| {
			let f = self.f.clone();
			UpdateConsumer { task, f }
		})
	}
}

impl<I: DistributedIteratorMulti<Source>, F, Source> DistributedIteratorMulti<Source>
	for Update<I, F>
where
	F: FnMut(&mut <I as DistributedIteratorMulti<Source>>::Item) + Clone + ProcessSend,
{
	type Item = I::Item;
	type Task = UpdateConsumer<I::Task, F>;

	fn task(&self) -> Self::Task {
		let task = self.i.task();
		let f = self.f.clone();
		UpdateConsumer { task, f }
	}
}

#[derive(Serialize, Deserialize)]
pub struct UpdateConsumer<T, F> {
	task: T,
	f: F,
}

impl<C: Consumer, F> Consumer for UpdateConsumer<C, F>
where
	F: FnMut(&mut C::Item) + Clone,
{
	type Item = C::Item;

	fn run(self, i: &mut impl FnMut(Self::Item) -> bool) -> bool {
		let (task, mut f) = (self.task, self.f);
		task.run(&mut |mut item| {
			f(&mut item);
			i(item)
		})
	}
}

impl<C: ConsumerMulti<Source>, F, Source> ConsumerMulti<Source> for UpdateConsumer<C, F>
where
	F: FnMut(&mut <C as ConsumerMulti<Source>>::Item) + Clone,
{
	type Item = C::Item;

	fn run(&self, source: Source, i: &mut impl FnMut(Self::Item) -> bool) -> bool {
		let (task, f) = (&self.task, &self.f);
		task.run(source, &mut |mut item| {
			f.clone()(&mut item);
			i(item)
		})
	}
}
