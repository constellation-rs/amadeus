use serde::{Deserialize, Serialize};

use super::{Consumer, ConsumerMulti, DistributedIterator, DistributedIteratorMulti};

#[must_use]
pub struct FlatMap<I, F> {
	i: I,
	f: F,
}
impl<I, F> FlatMap<I, F> {
	pub(super) fn new(i: I, f: F) -> Self {
		Self { i, f }
	}
}

impl<I: DistributedIterator, F, R: IntoIterator> DistributedIterator for FlatMap<I, F>
where
	F: FnMut(I::Item) -> R + Clone + Serialize + for<'de> Deserialize<'de> + 'static,
{
	type Item = R::Item;
	type Task = FlatMapConsumer<I::Task, F>;

	fn size_hint(&self) -> (usize, Option<usize>) {
		(0, None)
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.i.next_task().map(|task| {
			let f = self.f.clone();
			FlatMapConsumer { task, f }
		})
	}
}

impl<I: DistributedIteratorMulti<Source>, F, R: IntoIterator, Source>
	DistributedIteratorMulti<Source> for FlatMap<I, F>
where
	F: FnMut(<I as DistributedIteratorMulti<Source>>::Item) -> R
		+ Clone
		+ Serialize
		+ for<'de> Deserialize<'de>
		+ 'static,
{
	type Item = R::Item;
	type Task = FlatMapConsumer<I::Task, F>;

	fn task(&self) -> Self::Task {
		let task = self.i.task();
		let f = self.f.clone();
		FlatMapConsumer { task, f }
	}
}

#[derive(Serialize, Deserialize)]
pub struct FlatMapConsumer<T, F> {
	task: T,
	f: F,
}

impl<C: Consumer, F: FnMut(C::Item) -> R + Clone, R: IntoIterator> Consumer
	for FlatMapConsumer<C, F>
{
	type Item = R::Item;

	fn run(self, i: &mut impl FnMut(Self::Item) -> bool) -> bool {
		let (task, mut f) = (self.task, self.f);
		task.run(&mut |item| {
			for x in f(item) {
				if !i(x) {
					return false;
				}
			}
			true
		})
	}
}

impl<C: ConsumerMulti<Source>, F, R: IntoIterator, Source> ConsumerMulti<Source>
	for FlatMapConsumer<C, F>
where
	F: FnMut(<C as ConsumerMulti<Source>>::Item) -> R + Clone,
{
	type Item = R::Item;

	fn run(&self, source: Source, i: &mut impl FnMut(Self::Item) -> bool) -> bool {
		let (task, f) = (&self.task, &self.f);
		task.run(source, &mut |item| {
			for x in f.clone()(item) {
				if !i(x) {
					return false;
				}
			}
			true
		})
	}
}
