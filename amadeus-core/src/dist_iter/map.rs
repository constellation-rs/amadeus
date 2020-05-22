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

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct MapConsumer<C, F> {
	#[pin]
	task: C,
	f: F,
}

impl<C: Consumer, F, R> Consumer for MapConsumer<C, F>
where
	F: FnMut(C::Item) -> R + Clone,
{
	type Item = R;
	type Async = MapConsumer<C::Async, F>;
	fn into_async(self) -> Self::Async {
		MapConsumer {
			task: self.task.into_async(),
			f: self.f,
		}
	}
}
impl<C: ConsumerMulti<Source>, F, R, Source> ConsumerMulti<Source> for MapConsumer<C, F>
where
	F: FnMut(C::Item) -> R + Clone,
{
	type Item = R;
	type Async = MapConsumer<C::Async, F>;
	fn into_async(self) -> Self::Async {
		MapConsumer {
			task: self.task.into_async(),
			f: self.f,
		}
	}
}

impl<C: ConsumerAsync, F, R> ConsumerAsync for MapConsumer<C, F>
where
	F: FnMut(C::Item) -> R + Clone,
{
	type Item = R;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, sink: Pin<&mut impl Sink<Self::Item>>,
	) -> Poll<()> {
		let mut self_ = self.project();
		let (task, f) = (self_.task, &mut self_.f);
		let sink = SinkMap::new(sink, |item| f(item));
		pin_mut!(sink);
		task.poll_run(cx, sink)
	}
}

impl<C: ConsumerMultiAsync<Source>, F, R, Source> ConsumerMultiAsync<Source> for MapConsumer<C, F>
where
	F: FnMut(<C as ConsumerMultiAsync<Source>>::Item) -> R + Clone,
{
	type Item = R;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
		sink: Pin<&mut impl Sink<Self::Item>>,
	) -> Poll<()> {
		let mut self_ = self.project();
		let (task, f) = (self_.task, &mut self_.f);
		let sink = SinkMap::new(sink, |item| f(item));
		pin_mut!(sink);
		task.poll_run(cx, stream, sink)
	}
}
