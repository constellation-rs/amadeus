use futures::{pin_mut, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	pin::Pin, task::{Context, Poll}
};

use super::{
	Consumer, ConsumerAsync, ConsumerMulti, ConsumerMultiAsync, DistributedIterator, DistributedIteratorMulti
};
use crate::{
	pool::ProcessSend, sink::{Sink, SinkMap}
};

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
	F: FnMut(&I::Item) + Clone + ProcessSend,
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
	F: FnMut(&<I as DistributedIteratorMulti<Source>>::Item) + Clone + ProcessSend,
{
	type Item = I::Item;
	type Task = InspectConsumer<I::Task, F>;

	fn task(&self) -> Self::Task {
		let task = self.i.task();
		let f = self.f.clone();
		InspectConsumer { task, f }
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct InspectConsumer<T, F> {
	#[pin]
	task: T,
	f: F,
}

impl<C: Consumer, F> Consumer for InspectConsumer<C, F>
where
	F: FnMut(&C::Item) + Clone,
{
	type Item = C::Item;
	type Async = InspectConsumer<C::Async, F>;
	fn into_async(self) -> Self::Async {
		InspectConsumer {
			task: self.task.into_async(),
			f: self.f,
		}
	}
}
impl<C: ConsumerMulti<Source>, F, Source> ConsumerMulti<Source> for InspectConsumer<C, F>
where
	F: FnMut(&C::Item) + Clone,
{
	type Item = C::Item;
	type Async = InspectConsumer<C::Async, F>;
	fn into_async(self) -> Self::Async {
		InspectConsumer {
			task: self.task.into_async(),
			f: self.f,
		}
	}
}

impl<C: ConsumerAsync, F> ConsumerAsync for InspectConsumer<C, F>
where
	F: FnMut(&C::Item) + Clone,
{
	type Item = C::Item;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, sink: Pin<&mut impl Sink<Self::Item>>,
	) -> Poll<()> {
		let mut self_ = self.project();
		let (task, f) = (self_.task, &mut self_.f);
		let sink = SinkMap::new(sink, |item| {
			f(&item);
			item
		});
		pin_mut!(sink);
		task.poll_run(cx, sink)
	}
}

impl<C: ConsumerMultiAsync<Source>, F, Source> ConsumerMultiAsync<Source> for InspectConsumer<C, F>
where
	F: FnMut(&<C as ConsumerMultiAsync<Source>>::Item) + Clone,
{
	type Item = C::Item;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
		sink: Pin<&mut impl Sink<Self::Item>>,
	) -> Poll<()> {
		let mut self_ = self.project();
		let (task, f) = (self_.task, &mut self_.f);
		let sink = SinkMap::new(sink, |item| {
			f(&item);
			item
		});
		pin_mut!(sink);
		task.poll_run(cx, stream, sink)
	}
}
