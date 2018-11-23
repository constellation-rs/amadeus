use super::{Consumer, ConsumerMulti, DistributedIterator, DistributedIteratorMulti};

#[must_use]
pub struct Inspect<I, F> {
	i: I,
	f: F,
}
impl<I, F> Inspect<I, F> {
	pub(super) fn new(i: I, f: F) -> Self {
		Self { i, f }
	}
}

impl<I: DistributedIterator, F> DistributedIterator for Inspect<I, F>
where
	F: FnMut(&I::Item) + Clone,
{
	type Item = I::Item;
	type Task = InspectConsumer<I::Task, F>;

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.i.size_hint()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.i.next_task().map(|task| {
			let f = self.f.clone();
			InspectConsumer { task, f }
		})
	}
}

impl<I: DistributedIteratorMulti<Source>, F, Source> DistributedIteratorMulti<Source>
	for Inspect<I, F>
where
	F: FnMut(&<I as DistributedIteratorMulti<Source>>::Item) + Clone,
{
	type Item = I::Item;
	type Task = InspectConsumer<I::Task, F>;

	fn task(&self) -> Self::Task {
		let task = self.i.task();
		let f = self.f.clone();
		InspectConsumer { task, f }
	}
}

#[derive(Serialize, Deserialize)]
pub struct InspectConsumer<T, F> {
	task: T,
	f: F,
}

impl<C: Consumer, F> Consumer for InspectConsumer<C, F>
where
	F: FnMut(&C::Item) + Clone,
{
	type Item = C::Item;

	fn run(self, i: &mut impl FnMut(Self::Item)) {
		let (task, mut f) = (self.task, self.f);
		task.run(&mut |item| {
			f(&item);
			i(item)
		})
	}
}

impl<C: ConsumerMulti<Source>, F, Source> ConsumerMulti<Source> for InspectConsumer<C, F>
where
	F: FnMut(&<C as ConsumerMulti<Source>>::Item) + Clone,
{
	type Item = C::Item;

	fn run(&self, source: Source, i: &mut impl FnMut(Self::Item)) {
		let (task, f) = (&self.task, &self.f);
		task.run(source, &mut |item| {
			f.clone()(&item);
			i(item)
		})
	}
}
