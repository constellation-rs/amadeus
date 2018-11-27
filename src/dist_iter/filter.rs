use super::{Consumer, ConsumerMulti, DistributedIterator, DistributedIteratorMulti};

#[must_use]
pub struct Filter<I, F> {
	i: I,
	f: F,
}
impl<I, F> Filter<I, F> {
	pub(super) fn new(i: I, f: F) -> Self {
		Self { i, f }
	}
}

impl<I: DistributedIterator, F> DistributedIterator for Filter<I, F>
where
	F: FnMut(&I::Item) -> bool + Clone,
{
	type Item = I::Item;
	type Task = FilterConsumer<I::Task, F>;

	fn size_hint(&self) -> (usize, Option<usize>) {
		(0, self.i.size_hint().1)
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.i.next_task().map(|task| {
			let f = self.f.clone();
			FilterConsumer { task, f }
		})
	}
}

impl<I: DistributedIteratorMulti<Source>, F, Source> DistributedIteratorMulti<Source>
	for Filter<I, F>
where
	F: FnMut(&<I as DistributedIteratorMulti<Source>>::Item) -> bool + Clone,
{
	type Item = I::Item;
	type Task = FilterConsumer<I::Task, F>;

	fn task(&self) -> Self::Task {
		let task = self.i.task();
		let f = self.f.clone();
		FilterConsumer { task, f }
	}
}

#[derive(Serialize, Deserialize)]
pub struct FilterConsumer<T, F> {
	task: T,
	f: F,
}

impl<C: Consumer, F> Consumer for FilterConsumer<C, F>
where
	F: FnMut(&C::Item) -> bool + Clone,
{
	type Item = C::Item;

	fn run(self, i: &mut impl FnMut(Self::Item) -> bool) -> bool {
		let (task, mut f) = (self.task, self.f);
		task.run(&mut |item| {
			if f(&item) {
				return i(item);
			}
			true
		})
	}
}

impl<C: ConsumerMulti<Source>, F, Source> ConsumerMulti<Source> for FilterConsumer<C, F>
where
	F: FnMut(&<C as ConsumerMulti<Source>>::Item) -> bool + Clone,
{
	type Item = C::Item;

	fn run(&self, source: Source, i: &mut impl FnMut(Self::Item) -> bool) -> bool {
		let (task, f) = (&self.task, &self.f);
		task.run(source, &mut |item| {
			if f.clone()(&item) {
				return i(item);
			}
			true
		})
	}
}
