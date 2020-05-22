use futures::{pin_mut, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{ConsumerMulti, ConsumerMultiAsync, DistributedIteratorMulti};
use crate::sink::{Sink, SinkMap};

#[must_use]
pub struct Cloned<I, T, Source> {
	i: I,
	marker: PhantomData<fn(Source, T)>,
}
impl<I, T, Source> Cloned<I, T, Source> {
	pub(super) fn new(i: I) -> Self {
		Self {
			i,
			marker: PhantomData,
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
impl<'a, I, Source, T: 'a> DistributedIteratorMulti<&'a Source> for Cloned<I, T, Source>
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
#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct ClonedConsumer<T> {
	#[pin]
	task: T,
}
impl<'a, C, Source, T: 'a> ConsumerMulti<&'a Source> for ClonedConsumer<C>
where
	C: ConsumerMulti<&'a Source, Item = &'a T>,
	T: Clone,
{
	type Item = T;
	type Async = ClonedConsumer<C::Async>;
	fn into_async(self) -> Self::Async {
		ClonedConsumer {
			task: self.task.into_async(),
		}
	}
}
// impl<'a,C,T:'a> Consumer for ClonedConsumer<C>
// where
// 	C: Consumer<Item = &'a T>,
// 	T: Clone,
// {
// 	type Item = T;
// 	fn run(self, i: &mut impl FnMut(Self::Item) -> bool) -> bool {
// 		self.task.run(&mut |item| i(item.clone()))
// 	}
// }
impl<'a, C, Source: 'a, T: 'a> ConsumerMultiAsync<&'a Source> for ClonedConsumer<C>
where
	C: ConsumerMultiAsync<&'a Source, Item = &'a T>,
	T: Clone,
{
	type Item = T;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = &'a Source>>,
		sink: Pin<&mut impl Sink<Self::Item>>,
	) -> Poll<()> {
		let sink = SinkMap::new(sink, |item: &T| item.clone());
		pin_mut!(sink);
		self.project().task.poll_run(cx, stream, sink)
	}
}
