use super::{ConsumerMulti, DistributedIteratorMulti};
use std::marker;

pub struct Cloned<I, Source, T> {
	i: I,
	marker: marker::PhantomData<fn(Source, T)>,
}
impl<I, Source, T> Cloned<I, Source, T> {
	pub(super) fn new(i: I) -> Self {
		Self {
			i,
			marker: marker::PhantomData,
		}
	}
}

// impl<'a,I,T:'a> DistributedIterator for Cloned<I>
// where
// 	I: DistributedIterator<Item = &'a T>,
// 	T: Clone,
// {
// 	type Item = T;
// 	type Task = ClonedConsumer<I::Task>;
// 	fn size_hint(&self) -> (usize, Option<usize>) {
// 		self.i.size_hint()
// 	}
// 	fn next_task(&mut self) -> Option<Self::Task> {
// 		self.i.next_task().map(|task| ClonedConsumer { task })
// 	}
// }
// I'm a bit confused as to why this works and it isn't for Cloned<I, &'a Source, T>
// https://github.com/rust-lang/rust/issues/55731
// https://play.rust-lang.org/?version=nightly&mode=debug&edition=2015&gist=238651c4992913bcd62b68b4832fcd9a
// https://play.rust-lang.org/?version=nightly&mode=debug&edition=2015&gist=2f1da304878b050cc313c0279047b0fa
impl<'a, I, Source, T: 'a> DistributedIteratorMulti<&'a Source> for Cloned<I, Source, T>
where
	I: DistributedIteratorMulti<&'a Source, Item = &'a T>,
	T: Clone,
{
	type Item = T;
	type Task = ClonedConsumer<I::Task>;

	fn task(&self) -> Self::Task {
		let task = self.i.task();
		ClonedConsumer { task }
	}
}
#[derive(Serialize, Deserialize)]
pub struct ClonedConsumer<T> {
	task: T,
}
// impl<'a,C,T:'a> Consumer for ClonedConsumer<C>
// where
// 	C: Consumer<Item = &'a T>,
// 	T: Clone,
// {
// 	type Item = T;
// 	fn run(self, i: &mut impl FnMut(Self::Item)) {
// 		self.task.run(&mut |item| i(item.clone()))
// 	}
// }
impl<'a, C, Source, T: 'a> ConsumerMulti<&'a Source> for ClonedConsumer<C>
where
	C: ConsumerMulti<&'a Source, Item = &'a T>,
	T: Clone,
{
	type Item = T;

	fn run(&self, source: &'a Source, i: &mut impl FnMut(Self::Item)) {
		self.task.run(source, &mut |item| i(item.clone()))
	}
}
