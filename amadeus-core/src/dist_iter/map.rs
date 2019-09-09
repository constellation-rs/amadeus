use serde::{Deserialize, Serialize};

use super::{Consumer, ConsumerMulti, DistributedIterator, DistributedIteratorMulti};
use crate::pool::ProcessSend;

#[must_use]
pub struct Map<I, F> {
	i: I,
	f: F,
}
impl<I, F> Map<I, F> {
	pub(super) fn new(i: I, f: F) -> Self {
		Self { i, f }
	}
}

impl<I: DistributedIterator, F, R> DistributedIterator for Map<I, F>
where
	F: FnMut(I::Item) -> R + Clone + ProcessSend,
{
	type Item = R;
	type Task = MapConsumer<I::Task, F>;

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.i.size_hint()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.i.next_task().map(|task| {
			let f = self.f.clone();
			MapConsumer { task, f }
		})
	}
}

impl<I: DistributedIteratorMulti<Source>, F, R, Source> DistributedIteratorMulti<Source>
	for Map<I, F>
where
	F: FnMut(<I as DistributedIteratorMulti<Source>>::Item) -> R + Clone + ProcessSend,
{
	type Item = R;
	type Task = MapConsumer<I::Task, F>;

	fn task(&self) -> Self::Task {
		let task = self.i.task();
		let f = self.f.clone();
		MapConsumer { task, f }
	}
}

#[derive(Serialize, Deserialize)]
pub struct MapConsumer<T, F> {
	task: T,
	f: F,
}

impl<C: Consumer, F, R> Consumer for MapConsumer<C, F>
where
	F: FnMut(C::Item) -> R + Clone,
{
	type Item = R;

	fn run(self, i: &mut impl FnMut(Self::Item) -> bool) -> bool {
		let (task, mut f) = (self.task, self.f);
		task.run(&mut |item| i(f(item)))
	}
}

impl<C: ConsumerMulti<Source>, F, R, Source> ConsumerMulti<Source> for MapConsumer<C, F>
where
	F: FnMut(<C as ConsumerMulti<Source>>::Item) -> R + Clone,
{
	type Item = R;

	fn run(&self, source: Source, i: &mut impl FnMut(Self::Item) -> bool) -> bool {
		let (task, f) = (&self.task, &self.f);
		task.run(source, &mut |item| i(f.clone()(item)))
	}
}
